//! Controlled end-to-end load test for the native Rust client read path.
//!
//! Unlike `talon-loadgen`, which connects directly to a production worker, this
//! benchmark drives the real [`talon_rust_client::Client`] through coordinator
//! membership lookup, client-side placement, the connection pool, versioned
//! worker requests, retry, and forced membership refresh. Protocol-compatible
//! loopback peers make service latency and failure timing deterministic.
//!
//! This is a manual capacity tool, not a CI gate. Absolute QPS depends on the
//! host and the synthetic worker copies response bytes through userspace; use
//! the results to compare client settings and failure-path overhead on the same
//! machine, not as a production worker capacity claim.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use talon_core::{Backend, NodeId, NodeInfo, NodeRole};
use talon_rust_client::{Client, ObjectId, ObjectStat, DEFAULT_MAX_IN_FLIGHT_BLOCK_READS};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use talon_transport::{
    decode, decode_versioned_request, encode, encode_typed_error, response_header_ok,
    ControlMessage, DataErrorCode, MsgType, ZonedNodeInfo,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::task::JoinHandle;

const BENCH_VERSION: &str = "client-path-bench-v1";
const RESPONSE_BYTE: u8 = 0xa5;
// A default listener backlog near 128 creates a false one-second SYN-retry
// cliff when a large logical read opens more than 128 worker connections at
// once. Keep the harness above every supported sweep value so it measures the
// Client rather than the synthetic peer's accept queue.
const LISTEN_BACKLOG: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Scenario {
    /// Stable membership and successful worker responses.
    Steady,
    /// A stable worker returns a retryable error every N attempts.
    Failure,
    /// Membership alternates between two workers while reads are active.
    Membership,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::Steady => "steady",
            Self::Failure => "failure",
            Self::Membership => "membership",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "client-path-benchmark",
    about = "Benchmark the Talon Client read path with controlled latency and failures",
    after_help = "Examples:\n  just client-path-bench --seconds 5 --concurrency 1,8,32,64,128,256\n  just client-path-bench --scenario steady --concurrency 1 --blocks-per-read 256 --block-concurrency 8,32,64,128"
)]
struct Args {
    /// Logical reads kept in flight, comma-separated sweep.
    #[arg(long, default_value = "1,8,32,64,128,256", value_delimiter = ',')]
    concurrency: Vec<usize>,

    /// Per-logical-read block windows to sweep.
    #[arg(long, default_value = "64", value_delimiter = ',')]
    block_concurrency: Vec<usize>,

    /// Aggregate worker requests allowed across every Client clone.
    #[arg(long, default_value_t = DEFAULT_MAX_IN_FLIGHT_BLOCK_READS)]
    max_in_flight: usize,

    /// Synthetic service delays per worker attempt, in microseconds.
    #[arg(long, default_value = "1000", value_delimiter = ',')]
    delay_us: Vec<u64>,

    /// Workloads to run, comma-separated.
    #[arg(
        long,
        value_enum,
        default_value = "steady,failure,membership",
        value_delimiter = ','
    )]
    scenario: Vec<Scenario>,

    /// Seconds measured for each matrix cell.
    #[arg(long, default_value_t = 3)]
    seconds: u64,

    /// Warmup seconds excluded from every matrix cell.
    #[arg(long, default_value_t = 1)]
    warmup: u64,

    /// Logical block size used by the Client.
    #[arg(long, default_value_t = 4096)]
    block_bytes: u32,

    /// Contiguous blocks fetched by each logical read.
    #[arg(long, default_value_t = 1)]
    blocks_per_read: usize,

    /// Number of object blocks cycled through by the workload.
    #[arg(long, default_value_t = 4096)]
    working_set_blocks: usize,

    /// In the failure scenario, every Nth worker attempt returns Unavailable.
    #[arg(long, default_value_t = 100)]
    failure_every: u64,

    /// Interval between worker membership changes in milliseconds.
    #[arg(long, default_value_t = 500)]
    membership_switch_ms: u64,

    /// Tokio worker threads. Defaults to the host's available parallelism.
    #[arg(long)]
    runtime_threads: Option<usize>,

    /// Emit one JSON object per matrix cell instead of a table.
    #[arg(long)]
    json: bool,
}

impl Args {
    fn validate(&self) -> Result<()> {
        if self.concurrency.is_empty() || self.concurrency.contains(&0) {
            bail!("--concurrency values must all be non-zero");
        }
        if self.block_concurrency.is_empty() || self.block_concurrency.contains(&0) {
            bail!("--block-concurrency values must all be non-zero");
        }
        if self.max_in_flight == 0 {
            bail!("--max-in-flight must be non-zero");
        }
        if self.delay_us.is_empty() {
            bail!("--delay-us must contain at least one value");
        }
        if self.scenario.is_empty() {
            bail!("--scenario must contain at least one value");
        }
        if self.seconds == 0 {
            bail!("--seconds must be non-zero");
        }
        if self.block_bytes == 0 {
            bail!("--block-bytes must be non-zero");
        }
        if self.blocks_per_read == 0 {
            bail!("--blocks-per-read must be non-zero");
        }
        if self.working_set_blocks < self.blocks_per_read {
            bail!("--working-set-blocks must be at least --blocks-per-read");
        }
        if self.scenario.contains(&Scenario::Failure) && self.failure_every == 0 {
            bail!("--failure-every must be non-zero for the failure scenario");
        }
        if self.scenario.contains(&Scenario::Membership) && self.membership_switch_ms == 0 {
            bail!("--membership-switch-ms must be non-zero for the membership scenario");
        }
        if self.runtime_threads == Some(0) {
            bail!("--runtime-threads must be non-zero");
        }
        let _ = self.read_bytes()?;
        let _ = self.object_bytes()?;
        Ok(())
    }

    fn read_bytes(&self) -> Result<usize> {
        (self.block_bytes as usize)
            .checked_mul(self.blocks_per_read)
            .context("read size overflows usize")
    }

    fn object_bytes(&self) -> Result<u64> {
        u64::from(self.block_bytes)
            .checked_mul(self.working_set_blocks as u64)
            .context("object size overflows u64")
    }
}

#[derive(Default)]
struct ClusterState {
    active_worker: AtomicUsize,
    membership_queries: AtomicU64,
    stat_queries: AtomicU64,
    attempts: AtomicU64,
    successes: AtomicU64,
    injected_failures: AtomicU64,
    stale_failures: AtomicU64,
    served_bytes: AtomicU64,
    accepted_connections: AtomicU64,
    active_requests: AtomicUsize,
    peak_requests: AtomicUsize,
    membership_switches: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct ClusterSnapshot {
    membership_queries: u64,
    stat_queries: u64,
    attempts: u64,
    successes: u64,
    injected_failures: u64,
    stale_failures: u64,
    served_bytes: u64,
    accepted_connections: u64,
    membership_switches: u64,
}

impl ClusterState {
    fn snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            membership_queries: self.membership_queries.load(Ordering::Relaxed),
            stat_queries: self.stat_queries.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            injected_failures: self.injected_failures.load(Ordering::Relaxed),
            stale_failures: self.stale_failures.load(Ordering::Relaxed),
            served_bytes: self.served_bytes.load(Ordering::Relaxed),
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            membership_switches: self.membership_switches.load(Ordering::Relaxed),
        }
    }

    fn reset_peak(&self) {
        self.peak_requests.store(
            self.active_requests.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }
}

impl ClusterSnapshot {
    fn since(self, before: Self) -> Self {
        Self {
            membership_queries: self
                .membership_queries
                .saturating_sub(before.membership_queries),
            stat_queries: self.stat_queries.saturating_sub(before.stat_queries),
            attempts: self.attempts.saturating_sub(before.attempts),
            successes: self.successes.saturating_sub(before.successes),
            injected_failures: self
                .injected_failures
                .saturating_sub(before.injected_failures),
            stale_failures: self.stale_failures.saturating_sub(before.stale_failures),
            served_bytes: self.served_bytes.saturating_sub(before.served_bytes),
            accepted_connections: self
                .accepted_connections
                .saturating_sub(before.accepted_connections),
            membership_switches: self
                .membership_switches
                .saturating_sub(before.membership_switches),
        }
    }
}

struct ActiveRequest(Arc<ClusterState>);

impl ActiveRequest {
    fn enter(state: &Arc<ClusterState>) -> Self {
        let active = state.active_requests.fetch_add(1, Ordering::Relaxed) + 1;
        state.peak_requests.fetch_max(active, Ordering::Relaxed);
        Self(Arc::clone(state))
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

struct SyntheticCluster {
    coordinator_addr: String,
    state: Arc<ClusterState>,
    listener_tasks: Vec<JoinHandle<()>>,
}

impl SyntheticCluster {
    async fn start(
        scenario: Scenario,
        delay: Duration,
        failure_every: u64,
        block_bytes: u32,
        object_bytes: u64,
    ) -> Result<Self> {
        let state = Arc::new(ClusterState::default());
        let payload = Arc::new(vec![RESPONSE_BYTE; block_bytes as usize]);
        let (first_addr, first_task) = spawn_worker(
            0,
            scenario,
            delay,
            failure_every,
            Arc::clone(&payload),
            Arc::clone(&state),
        )
        .await?;
        let (second_addr, second_task) = spawn_worker(
            1,
            scenario,
            delay,
            failure_every,
            payload,
            Arc::clone(&state),
        )
        .await?;
        let (coordinator_addr, coordinator_task) =
            spawn_coordinator([first_addr, second_addr], object_bytes, Arc::clone(&state)).await?;
        Ok(Self {
            coordinator_addr,
            state,
            listener_tasks: vec![first_task, second_task, coordinator_task],
        })
    }

    fn start_membership_churn(&self, interval: Duration, stop: Arc<AtomicBool>) -> JoinHandle<()> {
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            while !stop.load(Ordering::Acquire) {
                tokio::time::sleep(interval).await;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let next = 1 - state.active_worker.load(Ordering::Acquire);
                state.active_worker.store(next, Ordering::Release);
                state.membership_switches.fetch_add(1, Ordering::Relaxed);
            }
        })
    }
}

impl Drop for SyntheticCluster {
    fn drop(&mut self) {
        for task in &self.listener_tasks {
            task.abort();
        }
    }
}

async fn spawn_worker(
    worker_index: usize,
    scenario: Scenario,
    delay: Duration,
    failure_every: u64,
    payload: Arc<Vec<u8>>,
    state: Arc<ClusterState>,
) -> Result<(String, JoinHandle<()>)> {
    let listener = bind_loopback()?;
    let addr = listener.local_addr()?.to_string();
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => return,
            };
            state.accepted_connections.fetch_add(1, Ordering::Relaxed);
            let state = Arc::clone(&state);
            let payload = Arc::clone(&payload);
            tokio::spawn(async move {
                if let Err(error) = serve_worker_connection(
                    socket,
                    worker_index,
                    scenario,
                    delay,
                    failure_every,
                    payload,
                    state,
                )
                .await
                {
                    if error.kind() != std::io::ErrorKind::UnexpectedEof
                        && error.kind() != std::io::ErrorKind::ConnectionReset
                    {
                        eprintln!("synthetic worker connection failed: {error}");
                    }
                }
            });
        }
    });
    Ok((addr, task))
}

async fn serve_worker_connection(
    mut socket: TcpStream,
    worker_index: usize,
    scenario: Scenario,
    delay: Duration,
    failure_every: u64,
    payload: Arc<Vec<u8>>,
    state: Arc<ClusterState>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    loop {
        let mut header_bytes = [0_u8; HEADER_LEN];
        socket.read_exact(&mut header_bytes).await?;
        let header = match FrameHeader::decode(&header_bytes) {
            Ok(header) => header,
            Err(error) => {
                socket
                    .write_all(&encode_typed_error(
                        0,
                        DataErrorCode::InvalidRequest,
                        format!("invalid frame: {error}"),
                    ))
                    .await?;
                continue;
            }
        };
        let mut body = vec![0_u8; header.length as usize];
        socket.read_exact(&mut body).await?;
        let mut frame = header_bytes.to_vec();
        frame.extend_from_slice(&body);

        let attempt = state.attempts.fetch_add(1, Ordering::Relaxed) + 1;
        let _active = ActiveRequest::enter(&state);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }

        let request = match decode_versioned_request(&frame) {
            Ok((_, request)) => request,
            Err(error) => {
                socket
                    .write_all(&encode_typed_error(
                        header.request_id,
                        DataErrorCode::InvalidRequest,
                        format!("expected versioned range request: {error}"),
                    ))
                    .await?;
                continue;
            }
        };
        if request.version.as_str() != BENCH_VERSION {
            socket
                .write_all(&encode_typed_error(
                    header.request_id,
                    DataErrorCode::VersionMismatch,
                    "benchmark version mismatch",
                ))
                .await?;
            continue;
        }

        if scenario == Scenario::Membership
            && state.active_worker.load(Ordering::Acquire) != worker_index
        {
            state.stale_failures.fetch_add(1, Ordering::Relaxed);
            socket
                .write_all(&encode_typed_error(
                    header.request_id,
                    DataErrorCode::Unavailable,
                    "worker removed from synthetic membership",
                ))
                .await?;
            continue;
        }
        if scenario == Scenario::Failure && attempt % failure_every == 0 {
            state.injected_failures.fetch_add(1, Ordering::Relaxed);
            socket
                .write_all(&encode_typed_error(
                    header.request_id,
                    DataErrorCode::Unavailable,
                    "injected retryable worker failure",
                ))
                .await?;
            continue;
        }

        let len = match usize::try_from(request.request.len) {
            Ok(len) if len <= payload.len() => len,
            _ => {
                socket
                    .write_all(&encode_typed_error(
                        header.request_id,
                        DataErrorCode::InvalidRequest,
                        "range exceeds synthetic block payload",
                    ))
                    .await?;
                continue;
            }
        };
        socket
            .write_all(&response_header_ok(header.request_id, len as u32))
            .await?;
        socket.write_all(&payload[..len]).await?;
        state.successes.fetch_add(1, Ordering::Relaxed);
        state.served_bytes.fetch_add(len as u64, Ordering::Relaxed);
    }
}

async fn spawn_coordinator(
    workers: [String; 2],
    object_bytes: u64,
    state: Arc<ClusterState>,
) -> Result<(String, JoinHandle<()>)> {
    let listener = bind_loopback()?;
    let addr = listener.local_addr()?.to_string();
    let workers = Arc::new(workers);
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(_) => return,
            };
            let workers = Arc::clone(&workers);
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(error) =
                    serve_coordinator_connection(socket, workers, object_bytes, state).await
                {
                    if error.kind() != std::io::ErrorKind::UnexpectedEof
                        && error.kind() != std::io::ErrorKind::ConnectionReset
                    {
                        eprintln!("synthetic coordinator connection failed: {error}");
                    }
                }
            });
        }
    });
    Ok((addr, task))
}

fn bind_loopback() -> std::io::Result<TcpListener> {
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind("127.0.0.1:0".parse().expect("valid loopback address"))?;
    socket.listen(LISTEN_BACKLOG)
}

async fn serve_coordinator_connection(
    mut socket: TcpStream,
    workers: Arc<[String; 2]>,
    object_bytes: u64,
    state: Arc<ClusterState>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    loop {
        let mut header_bytes = [0_u8; HEADER_LEN];
        socket.read_exact(&mut header_bytes).await?;
        let header = FrameHeader::decode(&header_bytes).map_err(std::io::Error::other)?;
        if header.msg_type != MsgType::Control {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected control frame",
            ));
        }
        let mut body = vec![0_u8; header.length as usize];
        socket.read_exact(&mut body).await?;
        let mut frame = header_bytes.to_vec();
        frame.extend_from_slice(&body);
        let (_, request) = decode(&frame).map_err(std::io::Error::other)?;
        let worker_index = state.active_worker.load(Ordering::Acquire);
        let node = NodeInfo {
            id: NodeId::new(format!("synthetic-worker-{worker_index}")),
            address: workers[worker_index].clone(),
            role: NodeRole::Worker,
        };
        let response = match request {
            ControlMessage::MembershipQueryV2 {} => {
                state.membership_queries.fetch_add(1, Ordering::Relaxed);
                ControlMessage::MembershipListV2 {
                    nodes: vec![ZonedNodeInfo {
                        info: node,
                        zone: Some("bench-zone".into()),
                    }],
                }
            }
            ControlMessage::MembershipQuery {} => {
                state.membership_queries.fetch_add(1, Ordering::Relaxed);
                ControlMessage::MembershipList { nodes: vec![node] }
            }
            ControlMessage::StatObject { .. } => {
                state.stat_queries.fetch_add(1, Ordering::Relaxed);
                ControlMessage::ObjectStat {
                    size: object_bytes,
                    version: BENCH_VERSION.into(),
                }
            }
            other => ControlMessage::Ack {
                ok: false,
                detail: Some(format!("unsupported benchmark request: {other:?}")),
            },
        };
        let response = encode(header.request_id, &response).map_err(std::io::Error::other)?;
        socket.write_all(&response).await?;
    }
}

#[derive(Default)]
struct TaskSamples {
    successful_reads: u64,
    failed_reads: u64,
    successful_bytes: u64,
    latencies_ns: Vec<u64>,
    first_error: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn drive_client(
    client: Client,
    object: ObjectId,
    stat: ObjectStat,
    read_bytes: usize,
    block_bytes: u32,
    blocks_per_read: usize,
    working_set_blocks: usize,
    seed: usize,
    recording: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> TaskSamples {
    let mut samples = TaskSamples {
        latencies_ns: Vec::with_capacity(1 << 14),
        ..TaskSamples::default()
    };
    let mut dst = vec![0_u8; read_bytes];
    let start_positions = working_set_blocks - blocks_per_read + 1;
    let mut sequence = seed;

    while !stop.load(Ordering::Acquire) {
        let measure_this = recording.load(Ordering::Acquire);
        let start_block = sequence % start_positions;
        let offset = (start_block as u64) * u64::from(block_bytes);
        let started = Instant::now();
        let result = client
            .read_into(&object, offset, &mut dst, Some(&stat))
            .await;
        if measure_this {
            match result {
                Ok(written)
                    if written == read_bytes
                        && dst.first() == Some(&RESPONSE_BYTE)
                        && dst.last() == Some(&RESPONSE_BYTE) =>
                {
                    samples.successful_reads += 1;
                    samples.successful_bytes += written as u64;
                    samples
                        .latencies_ns
                        .push(started.elapsed().as_nanos() as u64);
                }
                Ok(written) => {
                    samples.failed_reads += 1;
                    samples.first_error.get_or_insert_with(|| {
                        format!("invalid response: wrote {written}/{read_bytes} bytes")
                    });
                }
                Err(error) => {
                    samples.failed_reads += 1;
                    samples.first_error.get_or_insert_with(|| error.to_string());
                }
            }
        }
        sequence = sequence.wrapping_add(17);
    }
    samples
}

#[derive(Debug, Clone, Serialize)]
struct Run {
    scenario: String,
    delay_us: u64,
    concurrency: usize,
    block_concurrency: usize,
    max_in_flight: usize,
    block_bytes: u32,
    blocks_per_read: usize,
    working_set_blocks: usize,
    failure_every: u64,
    membership_switch_ms: u64,
    elapsed_seconds: f64,
    successful_reads: u64,
    failed_reads: u64,
    logical_qps: f64,
    worker_attempt_qps: f64,
    goodput_mib_s: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
    worker_attempts: u64,
    worker_successes: u64,
    injected_failures: u64,
    stale_failures: u64,
    membership_queries: u64,
    membership_switches: u64,
    stat_queries: u64,
    accepted_connections: u64,
    peak_worker_requests: usize,
    first_error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct RunConfig {
    scenario: Scenario,
    delay_us: u64,
    concurrency: usize,
    block_concurrency: usize,
}

async fn run_one(args: &Args, config: RunConfig) -> Result<Run> {
    let read_bytes = args.read_bytes()?;
    let object_bytes = args.object_bytes()?;
    let delay = Duration::from_micros(config.delay_us);
    let cluster = SyntheticCluster::start(
        config.scenario,
        delay,
        args.failure_every,
        args.block_bytes,
        object_bytes,
    )
    .await?;
    let client = Client::new(cluster.coordinator_addr.clone(), args.block_bytes)?
        .with_max_concurrent_block_reads(config.block_concurrency)?
        .with_max_in_flight_block_reads(args.max_in_flight)?;
    let object = ObjectId::new(Backend::S3, "benchmark", "client-path.bin");
    let stat = ObjectStat {
        size: object_bytes,
        version: BENCH_VERSION.into(),
    };
    let recording = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::with_capacity(config.concurrency);
    for seed in 0..config.concurrency {
        tasks.push(tokio::spawn(drive_client(
            client.clone(),
            object.clone(),
            stat.clone(),
            read_bytes,
            args.block_bytes,
            args.blocks_per_read,
            args.working_set_blocks,
            seed,
            Arc::clone(&recording),
            Arc::clone(&stop),
        )));
    }

    tokio::time::sleep(Duration::from_secs(args.warmup)).await;
    cluster.state.reset_peak();
    let before = cluster.state.snapshot();
    let churn = (config.scenario == Scenario::Membership).then(|| {
        cluster.start_membership_churn(
            Duration::from_millis(args.membership_switch_ms),
            Arc::clone(&stop),
        )
    });
    recording.store(true, Ordering::Release);
    let measure_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    stop.store(true, Ordering::Release);

    let mut successful_reads = 0_u64;
    let mut failed_reads = 0_u64;
    let mut successful_bytes = 0_u64;
    let mut latencies = Vec::new();
    let mut first_error = None;
    for task in tasks {
        let task_samples = task.await.context("client load task panicked")?;
        successful_reads += task_samples.successful_reads;
        failed_reads += task_samples.failed_reads;
        successful_bytes += task_samples.successful_bytes;
        latencies.extend(task_samples.latencies_ns);
        if first_error.is_none() {
            first_error = task_samples.first_error;
        }
    }
    let elapsed = measure_start.elapsed();
    if let Some(churn) = churn {
        churn.abort();
    }
    let cluster_delta = cluster.state.snapshot().since(before);
    latencies.sort_unstable();
    let seconds = elapsed.as_secs_f64();
    let percentile = |quantile: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let index = (((latencies.len() - 1) as f64) * quantile) as usize;
        latencies[index] as f64 / 1_000.0
    };

    Ok(Run {
        scenario: config.scenario.as_str().into(),
        delay_us: config.delay_us,
        concurrency: config.concurrency,
        block_concurrency: config.block_concurrency,
        max_in_flight: args.max_in_flight,
        block_bytes: args.block_bytes,
        blocks_per_read: args.blocks_per_read,
        working_set_blocks: args.working_set_blocks,
        failure_every: args.failure_every,
        membership_switch_ms: args.membership_switch_ms,
        elapsed_seconds: seconds,
        successful_reads,
        failed_reads,
        logical_qps: successful_reads as f64 / seconds,
        worker_attempt_qps: cluster_delta.attempts as f64 / seconds,
        goodput_mib_s: successful_bytes as f64 / (1024.0 * 1024.0) / seconds,
        p50_us: percentile(0.50),
        p95_us: percentile(0.95),
        p99_us: percentile(0.99),
        max_us: latencies.last().copied().unwrap_or(0) as f64 / 1_000.0,
        worker_attempts: cluster_delta.attempts,
        worker_successes: cluster_delta.successes,
        injected_failures: cluster_delta.injected_failures,
        stale_failures: cluster_delta.stale_failures,
        membership_queries: cluster_delta.membership_queries,
        membership_switches: cluster_delta.membership_switches,
        stat_queries: cluster_delta.stat_queries,
        accepted_connections: cluster_delta.accepted_connections,
        peak_worker_requests: cluster.state.peak_requests.load(Ordering::Relaxed),
        first_error,
    })
}

fn print_table_header(args: &Args) {
    println!(
        "controlled loopback | {} B blocks | {} blocks/read | {} block working set | aggregate cap {} | {}s measured after {}s warmup",
        args.block_bytes,
        args.blocks_per_read,
        args.working_set_blocks,
        args.max_in_flight,
        args.seconds,
        args.warmup
    );
    println!(
        "{:>10} {:>8} {:>6} {:>5} {:>11} {:>11} {:>10} {:>9} {:>9} {:>7} {:>6} {:>6} {:>6} {:>6}",
        "scenario",
        "delay",
        "reads",
        "win",
        "logical/s",
        "worker/s",
        "MiB/s",
        "p50 us",
        "p99 us",
        "errors",
        "memb",
        "stale",
        "inject",
        "peak"
    );
    println!("{}", "-".repeat(130));
}

fn print_run(run: &Run) {
    println!(
        "{:>10} {:>7}us {:>6} {:>5} {:>11.0} {:>11.0} {:>10.1} {:>9.1} {:>9.1} {:>7} {:>6} {:>6} {:>6} {:>6}",
        run.scenario,
        run.delay_us,
        run.concurrency,
        run.block_concurrency,
        run.logical_qps,
        run.worker_attempt_qps,
        run.goodput_mib_s,
        run.p50_us,
        run.p99_us,
        run.failed_reads,
        run.membership_queries,
        run.stale_failures,
        run.injected_failures,
        run.peak_worker_requests,
    );
    if let Some(error) = &run.first_error {
        eprintln!(
            "first logical read error ({} / {}us / concurrency {} / window {}): {error}",
            run.scenario, run.delay_us, run.concurrency, run.block_concurrency
        );
    }
}

async fn async_main(args: Args) -> Result<()> {
    if !args.json {
        print_table_header(&args);
    }
    let mut runs = Vec::new();
    for &scenario in &args.scenario {
        for &delay_us in &args.delay_us {
            for &block_concurrency in &args.block_concurrency {
                for &concurrency in &args.concurrency {
                    let run = run_one(
                        &args,
                        RunConfig {
                            scenario,
                            delay_us,
                            concurrency,
                            block_concurrency,
                        },
                    )
                    .await?;
                    if args.json {
                        println!("{}", serde_json::to_string(&run)?);
                    } else {
                        print_run(&run);
                    }
                    runs.push(run);
                }
            }
        }
    }

    if !args.json {
        let mut peaks: BTreeMap<(String, u64, usize), &Run> = BTreeMap::new();
        for run in &runs {
            let key = (run.scenario.clone(), run.delay_us, run.block_concurrency);
            let replace = peaks
                .get(&key)
                .map(|current| run.logical_qps > current.logical_qps)
                .unwrap_or(true);
            if replace {
                peaks.insert(key, run);
            }
        }
        println!("\npeak successful logical QPS in this sweep:");
        for run in peaks.values() {
            println!(
                "  {} @ {}us, window {}: {:.0} reads/s ({:.0} worker attempts/s) at concurrency {}",
                run.scenario,
                run.delay_us,
                run.block_concurrency,
                run.logical_qps,
                run.worker_attempt_qps,
                run.concurrency
            );
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    let threads = args
        .runtime_threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(async_main(args))
}
