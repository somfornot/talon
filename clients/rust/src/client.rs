use std::sync::Arc;

use crate::{Error, ObjectEntry, ObjectId, ObjectStat, UriError, Version};
use futures::stream::{FuturesUnordered, StreamExt};
use talon_cache_client::{iter_read, BlockReader, BlockSegment, CoordinatorClient, PlacementCache};
use tokio::sync::Semaphore;

const PLACEMENT_TTL_MS: u64 = 30_000;
const REPLICAS_K: u8 = 1;
// Production reads normally touch one 256 MiB block. This private window only
// prevents an unusually large cross-block read from monopolizing the client.
const MAX_CONCURRENT_BLOCK_READS_PER_READ: usize = 8;
/// Default maximum number of worker block requests active across Client clones.
pub const DEFAULT_MAX_IN_FLIGHT_BLOCK_READS: usize = 1024;

/// A reusable, runtime-owning-neutral Talon client.
#[derive(Clone)]
pub struct Client {
    coordinator: CoordinatorClient,
    reader: BlockReader,
    block_size: u32,
    max_in_flight_block_reads: usize,
    block_read_permits: Arc<Semaphore>,
}

impl Client {
    /// Construct a client without creating or selecting a Tokio runtime.
    pub fn new(coordinator: impl Into<String>, block_size: u32) -> Result<Self, Error> {
        if block_size == 0 {
            return Err(Error::InvalidArgument("block_size must be non-zero".into()));
        }
        let coordinator = CoordinatorClient::new(coordinator);
        let cache = Arc::new(PlacementCache::new(PLACEMENT_TTL_MS));
        let reader = BlockReader::new(coordinator.clone(), cache, REPLICAS_K);
        Ok(Self {
            coordinator,
            reader,
            block_size,
            max_in_flight_block_reads: DEFAULT_MAX_IN_FLIGHT_BLOCK_READS,
            block_read_permits: Arc::new(Semaphore::new(DEFAULT_MAX_IN_FLIGHT_BLOCK_READS)),
        })
    }

    /// Set the aggregate worker-request limit shared by this Client's clones.
    ///
    /// Configure this before cloning the Client. The per-read window prevents
    /// one large logical read from monopolizing the aggregate budget; this
    /// limit bounds sockets and worker requests across many simultaneous reads.
    pub fn with_max_in_flight_block_reads(mut self, max: usize) -> Result<Self, Error> {
        if max == 0 {
            return Err(Error::InvalidArgument(
                "max_in_flight_block_reads must be non-zero".into(),
            ));
        }
        self.max_in_flight_block_reads = max;
        self.block_read_permits = Arc::new(Semaphore::new(max));
        Ok(self)
    }

    /// Address of the coordinator used by this client.
    pub fn coordinator_addr(&self) -> &str {
        self.coordinator.addr()
    }

    /// Logical block size used for range planning.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Maximum active worker block requests shared across Client clones.
    pub fn max_in_flight_block_reads(&self) -> usize {
        self.max_in_flight_block_reads
    }

    /// Return an object's current size and source version.
    pub async fn stat(&self, object: &ObjectId) -> Result<ObjectStat, Error> {
        Ok(self.coordinator.stat_object(object).await?)
    }

    /// List objects below a mount-relative prefix.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectEntry>, Error> {
        Ok(self.coordinator.list_objects(prefix).await?)
    }

    /// Read an object range into a newly allocated buffer.
    ///
    /// When `known_stat` is supplied, its version is an exact source identity:
    /// workers may serve cached bytes for that generation or conditionally fill
    /// them from the backend, but never substitute a newer generation. A stale
    /// version therefore returns a version-mismatch error. Unusually large
    /// cross-block reads use an internal fairness window. Across simultaneous
    /// reads and Client clones, at most
    /// [`max_in_flight_block_reads`](Self::max_in_flight_block_reads) worker
    /// requests are active.
    pub async fn read(
        &self,
        object: &ObjectId,
        offset: u64,
        length: Option<u64>,
        known_stat: Option<&ObjectStat>,
    ) -> Result<Vec<u8>, Error> {
        if length == Some(0) {
            return Ok(Vec::new());
        }
        let stat = match known_stat {
            Some(stat) => stat.clone(),
            None => self.stat(object).await?,
        };
        let requested = length.unwrap_or_else(|| stat.size.saturating_sub(offset));
        let planned = requested.min(stat.size.saturating_sub(offset));
        let planned = usize::try_from(planned).map_err(|_| {
            Error::InvalidArgument("planned read length does not fit in usize".into())
        })?;
        // Byte-vector allocations cannot exceed isize::MAX even when usize is wider.
        if planned > isize::MAX as usize {
            return Err(Error::InvalidArgument(
                "planned read length exceeds Vec capacity".into(),
            ));
        }
        if planned == 0 {
            return Ok(Vec::new());
        }

        let mut bytes = vec![0_u8; planned];
        let written = self
            .read_into_resolved(object, offset, &mut bytes, &stat)
            .await?;
        bytes.truncate(written);
        Ok(bytes)
    }

    /// Read an object range into a caller-owned buffer.
    ///
    /// `known_stat` has the same exact-version and bounded-concurrency semantics
    /// as [`read`](Self::read).
    pub async fn read_into(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        known_stat: Option<&ObjectStat>,
    ) -> Result<usize, Error> {
        if dst.is_empty() {
            return Ok(0);
        }
        let stat = match known_stat {
            Some(stat) => stat.clone(),
            None => self.stat(object).await?,
        };
        self.read_into_resolved(object, offset, dst, &stat).await
    }

    async fn read_into_resolved(
        &self,
        object: &ObjectId,
        offset: u64,
        dst: &mut [u8],
        stat: &ObjectStat,
    ) -> Result<usize, Error> {
        let requested = u64::try_from(dst.len())
            .map_err(|_| Error::InvalidArgument("destination length does not fit in u64".into()))?;
        if stat.version.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "object version must be non-empty".into(),
            ));
        }
        let version = Version::new(stat.version.as_str());
        let planned_len = requested.min(stat.size.saturating_sub(offset));
        let planned_len = usize::try_from(planned_len).map_err(|_| {
            Error::InvalidArgument("planned read length does not fit in usize".into())
        })?;
        if planned_len == 0 {
            return Ok(0);
        }
        let mut plan = iter_read(
            object,
            offset,
            requested,
            self.block_size,
            &version,
            stat.size,
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut pending = FuturesUnordered::new();
        let mut rest = &mut dst[..planned_len];
        for _ in 0..MAX_CONCURRENT_BLOCK_READS_PER_READ {
            let Some(segment) = plan.next() else {
                break;
            };
            let (chunk, tail) = rest.split_at_mut(segment.len as usize);
            rest = tail;
            pending.push(read_segment_into(
                &self.reader,
                &self.block_read_permits,
                segment,
                chunk,
                now_ms,
            ));
        }

        let mut written = 0;
        while let Some(result) = pending.next().await {
            written += result?;
            if let Some(segment) = plan.next() {
                let (chunk, tail) = rest.split_at_mut(segment.len as usize);
                rest = tail;
                pending.push(read_segment_into(
                    &self.reader,
                    &self.block_read_permits,
                    segment,
                    chunk,
                    now_ms,
                ));
            }
        }
        Ok(written)
    }
}

async fn read_segment_into(
    reader: &BlockReader,
    block_read_permits: &Semaphore,
    segment: BlockSegment,
    dst: &mut [u8],
    now_ms: u64,
) -> Result<usize, Error> {
    let _permit = block_read_permits
        .acquire()
        .await
        .expect("Client never closes its block-read semaphore");
    reader
        .read_block_into_typed(&segment.block, segment.offset_in_block, dst, now_ms)
        .await
        .map_err(Error::from)
}

/// Parse a `scheme://bucket/key` URI into an object id.
pub fn parse_uri(uri: &str) -> Result<ObjectId, Error> {
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| UriError::MissingScheme {
            uri: uri.to_string(),
        })?;
    let backend = scheme.parse().map_err(|_| UriError::UnknownScheme {
        scheme: scheme.to_string(),
    })?;
    let (bucket, key) = rest.split_once('/').ok_or_else(|| UriError::MissingKey {
        uri: uri.to_string(),
        scheme: scheme.to_string(),
    })?;
    if bucket.is_empty() {
        return Err(UriError::EmptyBucket {
            uri: uri.to_string(),
        }
        .into());
    }
    if key.is_empty() {
        return Err(UriError::EmptyKey {
            uri: uri.to_string(),
        }
        .into());
    }
    Ok(ObjectId::new(backend, bucket, key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use talon_cache_client::CacheReadError;
    use talon_core::{Backend, NodeId, NodeInfo, NodeRole};
    use talon_transport::frame::{FrameHeader, HEADER_LEN};
    use talon_transport::{
        decode_versioned_request, encode_typed_error, response_header_ok, ControlMessage,
        DataErrorCode, RangeRequest,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, Semaphore};

    fn decode_read_request(frame: &[u8]) -> RangeRequest {
        let (_, request) = decode_versioned_request(frame).unwrap();
        assert_eq!(request.version, Version::new("test-version"));
        request.request
    }

    async fn mock_coordinator() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    socket.read_exact(&mut header_bytes).await.unwrap();
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, request) = talon_transport::decode(&frame).unwrap();
                    let response = match request {
                        ControlMessage::StatObject { .. } => ControlMessage::ObjectStat {
                            size: 8192,
                            version: "test-version".into(),
                        },
                        ControlMessage::ListObjects { prefix } => {
                            assert_eq!(prefix, "s3/bucket/data");
                            ControlMessage::ObjectList {
                                entries: vec![ObjectEntry {
                                    path: "s3/bucket/data/a.parquet".into(),
                                    size: 17,
                                }],
                            }
                        }
                        other => panic!("unexpected request: {other:?}"),
                    };
                    socket
                        .write_all(&talon_transport::encode(0, &response).unwrap())
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn mock_worker() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let request = decode_read_request(&frame);
                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_short_worker() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let request = decode_read_request(&frame);
                    let short_len = request.len.saturating_sub(1) as usize;
                    let mut response = response_header_ok(0, short_len as u32).to_vec();
                    response.extend_from_slice(&vec![0_u8; short_len]);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_delayed_worker(
        second_block_started: Arc<Notify>,
        release_first_block: Arc<Notify>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let second_block_started = Arc::clone(&second_block_started);
                let release_first_block = Arc::clone(&release_first_block);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let request = decode_read_request(&frame);
                    if request.offset < 8 {
                        release_first_block.notified().await;
                    } else {
                        second_block_started.notify_one();
                    }
                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    socket.write_all(&response).await.unwrap();
                });
            }
        });
        addr
    }

    async fn mock_failure_worker(
        stalled_block_started: Arc<Notify>,
        stalled_block_disconnected: Arc<Notify>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let stalled_block_started = Arc::clone(&stalled_block_started);
                let stalled_block_disconnected = Arc::clone(&stalled_block_disconnected);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let request = decode_read_request(&frame);
                    if request.offset < 8 {
                        stalled_block_started.notify_one();
                        let mut byte = [0_u8; 1];
                        if socket.read(&mut byte).await.unwrap() == 0 {
                            stalled_block_disconnected.notify_one();
                        }
                    } else {
                        stalled_block_started.notified().await;
                        socket
                            .write_all(&encode_typed_error(
                                0,
                                DataErrorCode::InvalidRequest,
                                "injected block failure",
                            ))
                            .await
                            .unwrap();
                    }
                });
            }
        });
        addr
    }

    async fn mock_read_coordinator(
        worker_addr: String,
        size: u64,
        stat_calls: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let worker_addr = worker_addr.clone();
                let stat_calls = Arc::clone(&stat_calls);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let (_, request) = talon_transport::decode(&frame).unwrap();
                    let response = match request {
                        ControlMessage::StatObject { .. } => {
                            stat_calls.fetch_add(1, Ordering::SeqCst);
                            ControlMessage::ObjectStat {
                                size,
                                version: "test-version".into(),
                            }
                        }
                        ControlMessage::MembershipQuery {} => ControlMessage::MembershipList {
                            nodes: vec![NodeInfo {
                                id: NodeId::new("worker-a"),
                                address: worker_addr,
                                role: NodeRole::Worker,
                            }],
                        },
                        ControlMessage::MembershipQueryV2 {} => ControlMessage::MembershipListV2 {
                            nodes: vec![talon_transport::ZonedNodeInfo {
                                info: NodeInfo {
                                    id: NodeId::new("worker-a"),
                                    address: worker_addr,
                                    role: NodeRole::Worker,
                                },
                                zone: None,
                            }],
                        },
                        other => panic!("unexpected request: {other:?}"),
                    };
                    socket
                        .write_all(&talon_transport::encode(0, &response).unwrap())
                        .await
                        .unwrap();
                });
            }
        });
        addr
    }

    async fn mock_bounded_worker(
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        started: Arc<AtomicUsize>,
        started_notify: Arc<Notify>,
        release: Arc<Semaphore>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let started = Arc::clone(&started);
                let started_notify = Arc::clone(&started_notify);
                let release = Arc::clone(&release);
                tokio::spawn(async move {
                    let mut header_bytes = [0_u8; HEADER_LEN];
                    if socket.read_exact(&mut header_bytes).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&header_bytes).unwrap();
                    let mut payload = vec![0_u8; header.length as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    let mut frame = header_bytes.to_vec();
                    frame.extend_from_slice(&payload);
                    let request = decode_read_request(&frame);

                    let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(active_now, Ordering::SeqCst);
                    started.fetch_add(1, Ordering::SeqCst);
                    started_notify.notify_waiters();
                    release.acquire().await.unwrap().forget();

                    let bytes: Vec<u8> = (0..request.len)
                        .map(|index| ((request.offset + index) % 251) as u8)
                        .collect();
                    let mut response = response_header_ok(0, bytes.len() as u32).to_vec();
                    response.extend_from_slice(&bytes);
                    socket.write_all(&response).await.unwrap();
                    active.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });
        addr
    }

    async fn wait_for_started(started: &AtomicUsize, notify: &Notify, target: usize) {
        loop {
            let notified = notify.notified();
            if started.load(Ordering::SeqCst) >= target {
                return;
            }
            tokio::time::timeout(Duration::from_secs(2), notified)
                .await
                .expect("worker requests did not start");
        }
    }

    async fn read_client(size: u64) -> (Client, Arc<AtomicUsize>) {
        let worker = mock_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, size, Arc::clone(&stat_calls)).await;
        (Client::new(coordinator, 8).unwrap(), stat_calls)
    }

    #[test]
    fn parses_supported_object_uris() {
        for (uri, backend) in [
            ("s3://bucket/key", Backend::S3),
            ("gcs://bucket/key", Backend::Gcs),
            ("az://container/a/b.parquet", Backend::Azure),
        ] {
            let object = parse_uri(uri).unwrap();
            assert_eq!(object.backend, backend);
        }
    }

    #[test]
    fn parse_uri_keeps_nested_keys_intact() {
        let object = parse_uri("az://container/a/b/c.parquet").unwrap();

        assert_eq!(object.bucket, "container");
        assert_eq!(object.object_path, "a/b/c.parquet");
    }

    #[test]
    fn parse_uri_rejects_malformed_input_with_a_useful_message() {
        for (uri, expected) in [
            ("bucket/key", "scheme://bucket/key"),
            ("ftp://bucket/key", "unknown backend scheme"),
            ("az://bucket", "missing an object key"),
            ("az:///key", "empty bucket"),
            ("az://bucket/", "empty object key"),
        ] {
            let message = parse_uri(uri).unwrap_err().to_string();
            assert!(
                message.contains(expected),
                "error for {uri:?} should mention {expected:?}, got {message:?}"
            );
        }
    }

    #[test]
    fn rejects_zero_block_size() {
        let error = Client::new("127.0.0.1:7000", 0)
            .err()
            .expect("zero block size must fail");
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn aggregate_block_read_limit_has_nonzero_default_and_rejects_zero() {
        let client = Client::new("127.0.0.1:7000", 8).unwrap();
        assert_eq!(
            client.max_in_flight_block_reads(),
            DEFAULT_MAX_IN_FLIGHT_BLOCK_READS
        );
        let error = Client::new("127.0.0.1:7000", 8)
            .unwrap()
            .with_max_in_flight_block_reads(0)
            .err()
            .expect("zero aggregate concurrency must be rejected");
        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn default_aggregate_budget_is_exactly_1024_and_shared_by_clones() {
        let client = Client::new("127.0.0.1:7000", 8).unwrap();
        let clone = client.clone();
        assert!(Arc::ptr_eq(
            &client.block_read_permits,
            &clone.block_read_permits
        ));

        let all = Arc::clone(&client.block_read_permits)
            .try_acquire_many_owned(DEFAULT_MAX_IN_FLIGHT_BLOCK_READS as u32)
            .expect("the default budget must contain exactly 1024 permits");
        assert!(clone.block_read_permits.try_acquire().is_err());
        drop(all);
        assert!(clone.block_read_permits.try_acquire().is_ok());
    }

    #[tokio::test]
    async fn stat_returns_coordinator_metadata() {
        let client = Client::new(mock_coordinator().await, 1024).unwrap();

        let stat = client
            .stat(&parse_uri("s3://bucket/key").unwrap())
            .await
            .unwrap();

        assert_eq!(stat.size, 8192);
        assert_eq!(stat.version, "test-version");
    }

    #[tokio::test]
    async fn list_returns_existing_object_entries() {
        let client = Client::new(mock_coordinator().await, 1024).unwrap();

        let entries = client.list("s3/bucket/data").await.unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "s3/bucket/data/a.parquet");
        assert_eq!(entries[0].size, 17);
    }

    #[tokio::test]
    async fn known_stat_skips_coordinator_stat() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };

        let bytes = client.read(&object, 2, Some(6), Some(&stat)).await.unwrap();

        assert_eq!(bytes, vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_stat_is_resolved_once() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();

        let bytes = client.read(&object, 0, Some(4), None).await.unwrap();

        assert_eq!(bytes, vec![0, 1, 2, 3]);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn read_to_end_uses_resolved_size() {
        let (client, _) = read_client(10).await;
        let object = parse_uri("s3://bucket/key").unwrap();

        let bytes = client.read(&object, 4, None, None).await.unwrap();

        assert_eq!(bytes, vec![4, 5, 6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn allocating_read_rejects_length_above_vec_capacity() {
        let client = Client::new("127.0.0.1:1", 8).unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let too_large = isize::MAX as u64 + 1;
        let stat = ObjectStat {
            size: too_large,
            version: "test-version".into(),
        };

        let error = client
            .read(&object, 0, Some(too_large), Some(&stat))
            .await
            .expect_err("lengths above Vec capacity must be rejected");

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn read_into_crossing_eof_returns_short_count() {
        let (client, _) = read_client(10).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 10,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 8];

        let written = client
            .read_into(&object, 6, &mut dst, Some(&stat))
            .await
            .unwrap();

        assert_eq!(written, 4);
        assert_eq!(&dst[..written], &[6, 7, 8, 9]);
    }

    #[tokio::test]
    async fn empty_reads_do_not_stat_or_fetch() {
        let (client, stat_calls) = read_client(16).await;
        let object = parse_uri("s3://bucket/key").unwrap();
        let mut dst = [];

        assert!(client
            .read(&object, 0, Some(0), None)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            client.read_into(&object, 0, &mut dst, None).await.unwrap(),
            0
        );
        assert_eq!(stat_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_block_read_is_concurrent_and_keeps_byte_order() {
        let second_block_started = Arc::new(Notify::new());
        let release_first_block = Arc::new(Notify::new());
        let worker = mock_delayed_worker(
            Arc::clone(&second_block_started),
            Arc::clone(&release_first_block),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = Client::new(coordinator, 8).unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };
        let read = tokio::spawn(async move { client.read(&object, 6, Some(8), Some(&stat)).await });

        let overlapped =
            tokio::time::timeout(Duration::from_millis(250), second_block_started.notified())
                .await
                .is_ok();
        release_first_block.notify_one();
        let bytes = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("read did not finish")
            .unwrap()
            .unwrap();

        assert!(
            overlapped,
            "second block did not start while first was pending"
        );
        assert_eq!(bytes, vec![6, 7, 8, 9, 10, 11, 12, 13]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_block_read_refills_the_internal_eight_request_window() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            Arc::clone(&active),
            Arc::clone(&peak),
            Arc::clone(&started),
            Arc::clone(&started_notify),
            Arc::clone(&release),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 10, stat_calls).await;
        let client = Client::new(coordinator, 1).unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 10,
            version: "test-version".into(),
        };
        let read =
            tokio::spawn(async move { client.read(&object, 0, Some(10), Some(&stat)).await });

        wait_for_started(
            &started,
            &started_notify,
            MAX_CONCURRENT_BLOCK_READS_PER_READ,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            MAX_CONCURRENT_BLOCK_READS_PER_READ,
            "a ninth block started before one of the first eight completed"
        );

        release.add_permits(1);
        wait_for_started(&started, &started_notify, 9).await;
        release.add_permits(9);
        let bytes = tokio::time::timeout(Duration::from_secs(2), read)
            .await
            .expect("bounded read did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(bytes, (0_u8..10).collect::<Vec<_>>());
        assert_eq!(started.load(Ordering::SeqCst), 10);
        assert_eq!(peak.load(Ordering::SeqCst), 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn aggregate_block_limit_is_shared_by_concurrent_reads() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = mock_bounded_worker(
            Arc::clone(&active),
            Arc::clone(&peak),
            Arc::clone(&started),
            Arc::clone(&started_notify),
            Arc::clone(&release),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = Client::new(coordinator, 8)
            .unwrap()
            .with_max_in_flight_block_reads(1)
            .unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };

        let first_client = client.clone();
        let first_object = object.clone();
        let first_stat = stat.clone();
        let first = tokio::spawn(async move {
            first_client
                .read(&first_object, 0, Some(8), Some(&first_stat))
                .await
        });
        let second =
            tokio::spawn(async move { client.read(&object, 8, Some(8), Some(&stat)).await });

        wait_for_started(&started, &started_notify, 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            1,
            "a second logical read bypassed the shared aggregate limit"
        );

        release.add_permits(1);
        wait_for_started(&started, &started_notify, 2).await;
        release.add_permits(1);
        let first_bytes = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first read did not finish")
            .unwrap()
            .unwrap();
        let second_bytes = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second read did not finish")
            .unwrap()
            .unwrap();

        assert_eq!(first_bytes, (0_u8..8).collect::<Vec<_>>());
        assert_eq!(second_bytes, (8_u8..16).collect::<Vec<_>>());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn block_failure_drops_the_other_unfinished_read() {
        let stalled_block_started = Arc::new(Notify::new());
        let stalled_block_disconnected = Arc::new(Notify::new());
        let worker = mock_failure_worker(
            Arc::clone(&stalled_block_started),
            Arc::clone(&stalled_block_disconnected),
        )
        .await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 16, stat_calls).await;
        let client = Client::new(coordinator, 8).unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 16,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 16];

        let error = client
            .read_into(&object, 0, &mut dst, Some(&stat))
            .await
            .expect_err("one failed block must fail the whole read");

        assert!(matches!(
            error,
            Error::Read(CacheReadError::InvalidRequest(message))
                if message == "injected block failure"
        ));
        tokio::time::timeout(
            Duration::from_secs(2),
            stalled_block_disconnected.notified(),
        )
        .await
        .expect("unfinished block connection was not cancelled");
    }

    #[tokio::test]
    async fn short_worker_reply_fails_the_whole_read() {
        let worker = mock_short_worker().await;
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = mock_read_coordinator(worker, 8, stat_calls).await;
        let client = Client::new(coordinator, 8).unwrap();
        let object = parse_uri("s3://bucket/key").unwrap();
        let stat = ObjectStat {
            size: 8,
            version: "test-version".into(),
        };
        let mut dst = [0_u8; 8];

        let error = client
            .read_into(&object, 0, &mut dst, Some(&stat))
            .await
            .expect_err("short worker reply must fail the whole read");

        assert!(matches!(error, Error::Read(CacheReadError::Protocol(_))));
    }
}
