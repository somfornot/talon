//! Local latency probe for independent paged cache-miss runs.
//!
//! It warms the middle page of a three-page read, leaving two one-page misses
//! separated by the resident page. The `serial` samples use one shared backend
//! permit to model the former sequential scheduling; `parallel` samples expose
//! the current concurrent leader-run scheduling. Setup and warming are outside
//! the recorded interval.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use talon_core::{Backend, BackendStore, ObjectId, ObjectStat, Result, Version};
use talon_transport::data::RangeRequest;
use talon_worker::{
    BlockIndex, InFlightLoads, PagedBlockStore, WholeBlockStore, WorkerMetrics, WorkerRuntime,
};
use tokio::sync::Semaphore;

const PAGE_SIZE: u32 = 64 << 10;
const OBJECT_LEN: u64 = 3 * PAGE_SIZE as u64;
const BACKEND_DELAY: Duration = Duration::from_millis(20);
const SAMPLES: usize = 21;
static ROOT_SEQ: AtomicUsize = AtomicUsize::new(0);

struct LatencyBackend {
    serial_gate: Option<Arc<Semaphore>>,
}

#[async_trait]
impl BackendStore for LatencyBackend {
    async fn fetch_range(&self, _object: &ObjectId, offset: u64, len: u64) -> Result<Bytes> {
        let permit = match &self.serial_gate {
            Some(gate) => Some(
                gate.acquire()
                    .await
                    .expect("benchmark semaphore is never closed"),
            ),
            None => None,
        };
        tokio::time::sleep(BACKEND_DELAY).await;
        drop(permit);
        Ok(Bytes::from(
            (0..len)
                .map(|i| ((offset + i) % 251) as u8)
                .collect::<Vec<_>>(),
        ))
    }

    async fn head(&self, _object: &ObjectId) -> Result<ObjectStat> {
        Ok(ObjectStat {
            len: OBJECT_LEN,
            version: Version::new("v1"),
        })
    }
}

fn object() -> ObjectId {
    ObjectId::new(Backend::Azure, "bench", "paged-miss-runs")
}

fn tmp_root(mode: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after Unix epoch")
        .as_nanos();
    let seq = ROOT_SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "talon-paged-miss-bench-{mode}-{}-{now}-{seq}",
        std::process::id()
    ))
}

async fn sample(serial: bool) -> Duration {
    let root = tmp_root(if serial { "serial" } else { "parallel" });
    let backend = Arc::new(LatencyBackend {
        serial_gate: serial.then(|| Arc::new(Semaphore::new(1))),
    });
    let runtime = WorkerRuntime::new(
        WholeBlockStore::open(root.join("whole")).expect("open whole store"),
        Arc::new(BlockIndex::new()),
        Arc::new(InFlightLoads::new()),
        backend,
        4 * PAGE_SIZE,
        0,
        WorkerMetrics::new(1 << 30),
    )
    .with_paged_store(
        PagedBlockStore::open(root.join("paged"), PAGE_SIZE).expect("open paged store"),
    );

    // Keep page 1 resident: page 0 and page 2 then form two leader runs for
    // the measured three-page request.
    runtime
        .serve_range(&RangeRequest {
            object: object(),
            offset: u64::from(PAGE_SIZE),
            len: 1,
        })
        .await
        .expect("warm middle page");

    let started = Instant::now();
    let bytes = runtime
        .serve_range(&RangeRequest {
            object: object(),
            offset: 0,
            len: OBJECT_LEN,
        })
        .await
        .expect("read split miss runs");
    let elapsed = started.elapsed();
    assert_eq!(bytes.len(), OBJECT_LEN as usize);
    drop(runtime);
    std::fs::remove_dir_all(root).ok();
    elapsed
}

fn percentile(samples: &mut [Duration], numerator: usize, denominator: usize) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() - 1) * numerator / denominator]
}

fn describe(label: &str, samples: &[Duration]) -> (Duration, Duration) {
    let mut median = samples.to_vec();
    let mut p95 = samples.to_vec();
    let median = percentile(&mut median, 50, 100);
    let p95 = percentile(&mut p95, 95, 100);
    println!("{label:8} p50={median:?} p95={p95:?}");
    (median, p95)
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build Tokio runtime");
    let serial: Vec<_> = (0..SAMPLES)
        .map(|_| runtime.block_on(sample(true)))
        .collect();
    let parallel: Vec<_> = (0..SAMPLES)
        .map(|_| runtime.block_on(sample(false)))
        .collect();
    let (serial_p50, _) = describe("serial", &serial);
    let (parallel_p50, _) = describe("parallel", &parallel);
    let improvement = (1.0 - parallel_p50.as_secs_f64() / serial_p50.as_secs_f64()) * 100.0;
    println!(
        "parallel p50 latency reduction: {improvement:.1}% (backend delay {:?}, {SAMPLES} samples)",
        BACKEND_DELAY
    );
}
