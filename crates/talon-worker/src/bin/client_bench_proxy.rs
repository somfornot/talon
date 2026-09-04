//! Benchmark-only Talon frame proxy with deterministic delay/fault injection.
//!
//! The proxy is a standalone binary on purpose: production worker code has no
//! benchmark branches. Each downstream TCP connection is handled sequentially,
//! matching the Talon client pool's one-in-flight-request-per-connection model.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use serde::Serialize;
use talon_transport::{
    encode_typed_error, read_frame, DataErrorCode, MsgType, ReadFrameError, DEFAULT_READ_TIMEOUT,
};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Parser)]
#[command(name = "talon-client-bench-proxy", about)]
struct Args {
    /// Address exposed to Talon clients.
    #[arg(long, default_value = "127.0.0.1:17901")]
    listen: SocketAddr,
    /// Real worker data-plane address.
    #[arg(long)]
    upstream: SocketAddr,
    /// HTTP address exposing a JSON counter snapshot at /stats.
    #[arg(long, default_value = "127.0.0.1:17902")]
    admin_listen: SocketAddr,
    /// Delay applied immediately before every downstream response.
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    /// Return typed Unavailable for every Nth downstream attempt (0 disables).
    #[arg(long, default_value_t = 0)]
    fail_every: u64,
    /// Per-frame and upstream-connect timeout.
    #[arg(long, default_value_t = DEFAULT_READ_TIMEOUT.as_millis() as u64)]
    timeout_ms: u64,
}

#[derive(Default)]
struct ProxyStats {
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    peak_connections: AtomicU64,
    attempts: AtomicU64,
    active_requests: AtomicU64,
    peak_requests: AtomicU64,
    forwarded: AtomicU64,
    other_frames: AtomicU64,
    injected_unavailable: AtomicU64,
    upstream_errors: AtomicU64,
}

#[derive(Debug, Serialize)]
struct StatsSnapshot {
    accepted_connections: u64,
    active_connections: u64,
    peak_connections: u64,
    attempts: u64,
    active_requests: u64,
    peak_requests: u64,
    forwarded: u64,
    other_frames: u64,
    injected_unavailable: u64,
    upstream_errors: u64,
}

impl ProxyStats {
    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            peak_connections: self.peak_connections.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            active_requests: self.active_requests.load(Ordering::Relaxed),
            peak_requests: self.peak_requests.load(Ordering::Relaxed),
            forwarded: self.forwarded.load(Ordering::Relaxed),
            other_frames: self.other_frames.load(Ordering::Relaxed),
            injected_unavailable: self.injected_unavailable.load(Ordering::Relaxed),
            upstream_errors: self.upstream_errors.load(Ordering::Relaxed),
        }
    }
}

struct AtomicActivity<'a> {
    counter: &'a AtomicU64,
}

impl Drop for AtomicActivity<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn enter<'a>(counter: &'a AtomicU64, peak: &AtomicU64) -> AtomicActivity<'a> {
    let current = counter.fetch_add(1, Ordering::Relaxed) + 1;
    peak.fetch_max(current, Ordering::Relaxed);
    AtomicActivity { counter }
}

async fn stats_handler(State(stats): State<Arc<ProxyStats>>) -> Json<StatsSnapshot> {
    Json(stats.snapshot())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind proxy data listener {}", args.listen))?;
    let admin = TcpListener::bind(args.admin_listen)
        .await
        .with_context(|| format!("bind proxy admin listener {}", args.admin_listen))?;
    let stats = Arc::new(ProxyStats::default());
    let app = Router::new()
        .route("/stats", get(stats_handler))
        .with_state(Arc::clone(&stats));
    tokio::spawn(async move {
        if let Err(error) = axum::serve(admin, app).await {
            eprintln!("proxy admin server stopped: {error}");
        }
    });

    println!(
        "{{\"event\":\"ready\",\"listen\":\"{}\",\"upstream\":\"{}\",\"admin\":\"{}\",\"delay_ms\":{},\"fail_every\":{}}}",
        args.listen, args.upstream, args.admin_listen, args.delay_ms, args.fail_every
    );
    serve_proxy(
        listener,
        args.upstream,
        Duration::from_millis(args.delay_ms),
        args.fail_every,
        Duration::from_millis(args.timeout_ms),
        stats,
    )
    .await
}

async fn serve_proxy(
    listener: TcpListener,
    upstream_addr: SocketAddr,
    response_delay: Duration,
    fail_every: u64,
    timeout: Duration,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    loop {
        let (downstream, _) = listener.accept().await.context("accept downstream")?;
        stats.accepted_connections.fetch_add(1, Ordering::Relaxed);
        let stats = Arc::clone(&stats);
        tokio::spawn(async move {
            let _connection = enter(&stats.active_connections, &stats.peak_connections);
            if let Err(error) = handle_connection(
                downstream,
                upstream_addr,
                response_delay,
                fail_every,
                timeout,
                Arc::clone(&stats),
            )
            .await
            {
                eprintln!("proxy connection stopped: {error:#}");
            }
        });
    }
}

async fn handle_connection(
    mut downstream: TcpStream,
    upstream_addr: SocketAddr,
    response_delay: Duration,
    fail_every: u64,
    timeout: Duration,
    stats: Arc<ProxyStats>,
) -> Result<()> {
    downstream
        .set_nodelay(true)
        .context("set TCP_NODELAY on downstream")?;
    let mut upstream = None;
    loop {
        let (request_header, request_payload) = match read_frame(&mut downstream, timeout).await {
            Ok(frame) => frame,
            Err(ReadFrameError::Eof) => return Ok(()),
            Err(error) => return Err(error).context("read downstream frame"),
        };
        let is_benchmark_read = matches!(
            request_header.msg_type,
            MsgType::GetVersionedRange | MsgType::GetVersionedRangeTenant
        );
        let attempt = if is_benchmark_read {
            stats.attempts.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            stats.other_frames.fetch_add(1, Ordering::Relaxed);
            0
        };
        let _request = enter(&stats.active_requests, &stats.peak_requests);

        if is_benchmark_read && fail_every != 0 && attempt % fail_every == 0 {
            stats.injected_unavailable.fetch_add(1, Ordering::Relaxed);
            delay(response_delay).await;
            write_unavailable(
                &mut downstream,
                request_header.request_id,
                "benchmark-injected unavailable",
            )
            .await?;
            continue;
        }

        if upstream.is_none() {
            upstream = match tokio::time::timeout(timeout, TcpStream::connect(upstream_addr)).await
            {
                Ok(Ok(stream)) => {
                    stream
                        .set_nodelay(true)
                        .context("set TCP_NODELAY on upstream")?;
                    Some(stream)
                }
                Ok(Err(error)) => {
                    stats.upstream_errors.fetch_add(1, Ordering::Relaxed);
                    delay(response_delay).await;
                    write_unavailable(
                        &mut downstream,
                        request_header.request_id,
                        &format!("benchmark proxy upstream connect failed: {error}"),
                    )
                    .await?;
                    continue;
                }
                Err(_) => {
                    stats.upstream_errors.fetch_add(1, Ordering::Relaxed);
                    delay(response_delay).await;
                    write_unavailable(
                        &mut downstream,
                        request_header.request_id,
                        "benchmark proxy upstream connect timed out",
                    )
                    .await?;
                    continue;
                }
            };
        }

        let stream = upstream.as_mut().expect("upstream was connected");
        let exchange = async {
            stream.write_all(&request_header.encode()).await?;
            stream.write_all(&request_payload).await?;
            stream.flush().await?;
            read_frame(stream, timeout)
                .await
                .map_err(std::io::Error::other)
        };
        let response = match tokio::time::timeout(timeout, exchange).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                stats.upstream_errors.fetch_add(1, Ordering::Relaxed);
                upstream = None;
                delay(response_delay).await;
                write_unavailable(
                    &mut downstream,
                    request_header.request_id,
                    &format!("benchmark proxy upstream exchange failed: {error}"),
                )
                .await?;
                continue;
            }
            Err(_) => {
                stats.upstream_errors.fetch_add(1, Ordering::Relaxed);
                upstream = None;
                delay(response_delay).await;
                write_unavailable(
                    &mut downstream,
                    request_header.request_id,
                    "benchmark proxy upstream exchange timed out",
                )
                .await?;
                continue;
            }
        };
        let (response_header, response_payload) = response;
        if response_header.request_id != request_header.request_id {
            stats.upstream_errors.fetch_add(1, Ordering::Relaxed);
            upstream = None;
            delay(response_delay).await;
            write_unavailable(
                &mut downstream,
                request_header.request_id,
                "benchmark proxy upstream returned a mismatched request_id",
            )
            .await?;
            continue;
        }

        if is_benchmark_read {
            stats.forwarded.fetch_add(1, Ordering::Relaxed);
        }
        delay(response_delay).await;
        downstream.write_all(&response_header.encode()).await?;
        downstream.write_all(&response_payload).await?;
        downstream.flush().await?;
    }
}

async fn delay(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
    }
}

async fn write_unavailable(
    stream: &mut TcpStream,
    request_id: u32,
    message: &str,
) -> std::io::Result<()> {
    stream
        .write_all(&encode_typed_error(
            request_id,
            DataErrorCode::Unavailable,
            message,
        ))
        .await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use talon_core::{Backend, ObjectId, Version};
    use talon_transport::{
        decode_error_payload, encode_versioned_request, response_header_ok, ControlMessage, Flags,
        FrameHeader, RangeRequest, VersionedRangeRequest,
    };
    use tokio::sync::oneshot;

    async fn start_proxy(
        upstream_addr: SocketAddr,
        response_delay: Duration,
        fail_every: u64,
    ) -> (SocketAddr, Arc<ProxyStats>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stats = Arc::new(ProxyStats::default());
        let task_stats = Arc::clone(&stats);
        let task = tokio::spawn(async move {
            serve_proxy(
                listener,
                upstream_addr,
                response_delay,
                fail_every,
                Duration::from_secs(1),
                task_stats,
            )
            .await
            .unwrap();
        });
        (address, stats, task)
    }

    fn request(request_id: u32) -> Vec<u8> {
        encode_versioned_request(
            request_id,
            &VersionedRangeRequest {
                request: RangeRequest {
                    object: ObjectId::new(Backend::Azure, "container", "object"),
                    offset: 17,
                    len: 3,
                },
                version: Version::new("exact-version"),
            },
        )
        .unwrap()
    }

    async fn exchange(address: SocketAddr, encoded: &[u8]) -> (FrameHeader, Vec<u8>) {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(encoded).await.unwrap();
        stream.flush().await.unwrap();
        read_frame(&mut stream, Duration::from_secs(1))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn forwards_complete_frames_and_delays_responses() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let (header, payload) = read_frame(&mut stream, Duration::from_secs(1))
                .await
                .unwrap();
            seen_tx.send((header, payload)).unwrap();
            let mut response = response_header_ok(header.request_id, 3).to_vec();
            response.extend_from_slice(&[7, 8, 9]);
            stream.write_all(&response).await.unwrap();
        });
        let (proxy_addr, stats, proxy) =
            start_proxy(upstream_addr, Duration::from_millis(20), 0).await;
        let encoded = request(91);
        let started = Instant::now();
        let (response_header, response_payload) = exchange(proxy_addr, &encoded).await;

        assert!(started.elapsed() >= Duration::from_millis(15));
        assert_eq!(response_header.request_id, 91);
        assert_eq!(response_payload, [7, 8, 9]);
        let (seen_header, seen_payload) = seen_rx.await.unwrap();
        assert_eq!(seen_header.encode(), encoded[..16]);
        assert_eq!(seen_payload, encoded[16..]);
        assert_eq!(stats.forwarded.load(Ordering::Relaxed), 1);
        proxy.abort();
    }

    #[tokio::test]
    async fn injects_typed_unavailable_with_original_request_id() {
        let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = unused.local_addr().unwrap();
        let (proxy_addr, stats, proxy) = start_proxy(upstream_addr, Duration::ZERO, 1).await;
        let (header, payload) = exchange(proxy_addr, &request(1234)).await;

        assert_eq!(header.request_id, 1234);
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(
            decode_error_payload(&payload).code,
            DataErrorCode::Unavailable
        );
        assert_eq!(stats.injected_unavailable.load(Ordering::Relaxed), 1);
        assert_eq!(stats.forwarded.load(Ordering::Relaxed), 0);
        proxy.abort();
    }

    #[tokio::test]
    async fn forwards_control_frames_without_counting_or_injecting_them() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let (header, _) = read_frame(&mut stream, Duration::from_secs(1))
                .await
                .unwrap();
            let response = talon_transport::encode(
                header.request_id,
                &ControlMessage::ObjectStat {
                    size: 64 << 20,
                    version: "exact-version".into(),
                },
            )
            .unwrap();
            stream.write_all(&response).await.unwrap();
        });
        let (proxy_addr, stats, proxy) = start_proxy(upstream_addr, Duration::ZERO, 1).await;
        let encoded = talon_transport::encode(
            55,
            &ControlMessage::StatObject {
                object: ObjectId::new(Backend::Azure, "container", "object"),
            },
        )
        .unwrap();
        let (header, _) = exchange(proxy_addr, &encoded).await;

        assert_eq!(header.request_id, 55);
        assert!(!header.flags.contains(Flags::ERROR));
        assert_eq!(stats.attempts.load(Ordering::Relaxed), 0);
        assert_eq!(stats.other_frames.load(Ordering::Relaxed), 1);
        assert_eq!(stats.injected_unavailable.load(Ordering::Relaxed), 0);
        proxy.abort();
    }

    #[tokio::test]
    async fn converts_upstream_disconnect_to_typed_unavailable() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let _ = read_frame(&mut stream, Duration::from_secs(1)).await;
        });
        let (proxy_addr, stats, proxy) = start_proxy(upstream_addr, Duration::ZERO, 0).await;
        let (header, payload) = exchange(proxy_addr, &request(77)).await;

        assert_eq!(header.request_id, 77);
        assert!(header.flags.contains(Flags::ERROR));
        assert_eq!(
            decode_error_payload(&payload).code,
            DataErrorCode::Unavailable
        );
        assert_eq!(stats.upstream_errors.load(Ordering::Relaxed), 1);
        proxy.abort();
    }
}
