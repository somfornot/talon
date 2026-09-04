//! Python bindings for the Talon client (#312).
//!
//! Adapts the native Rust client rather than reimplementing URI parsing, stat
//! fallback, range planning, placement, or block reads in the binding.
//!
//! # Threading
//!
//! Every blocking call releases the GIL for its duration, so a threaded data
//! loader is limited by the network rather than serialised on the interpreter.
//! The runtime is a multi-threaded Tokio runtime owned by the client, shared by
//! all calls on it.

// pyo3 0.22's #[pymethods] expansion converts every returned error through
// Into<PyErr>, which is a no-op when the error already is one. clippy flags the
// generated code; there is no source-level change that avoids it, and the lint
// is not about anything under our control.
#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use talon_rust_client::{
    parse_uri, CacheReadError, Client as RustClient, Error as RustError,
    ObjectStat as RustObjectStat,
};

/// Runtime construction failures are infrastructure errors.
fn io_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyIOError::new_err(e.to_string())
}

/// Preserve the SDK's input-versus-I/O error distinction at the Python boundary.
fn client_err(error: RustError) -> PyErr {
    let message = error.to_string();
    match error {
        RustError::InvalidUri(_)
        | RustError::InvalidArgument(_)
        | RustError::Read(CacheReadError::InvalidRequest(_)) => PyValueError::new_err(message),
        RustError::Coordinator(_) | RustError::Read(_) => PyIOError::new_err(message),
    }
}

fn complete_known_stat(
    known_version: Option<String>,
    known_size: Option<u64>,
    resolved: RustObjectStat,
) -> RustObjectStat {
    RustObjectStat {
        size: known_size.unwrap_or(resolved.size),
        version: known_version.unwrap_or(resolved.version),
    }
}

/// An object's size and source version.
#[pyclass(module = "talon", frozen)]
#[derive(Clone)]
pub struct ObjectStat {
    /// Total object length in bytes.
    #[pyo3(get)]
    pub size: u64,
    /// Source version (ETag) the object is currently at.
    #[pyo3(get)]
    pub version: String,
}

#[pymethods]
impl ObjectStat {
    fn __repr__(&self) -> String {
        format!("ObjectStat(size={}, version={:?})", self.size, self.version)
    }
}

/// One entry from a listing: a mount-relative path and its size.
#[pyclass(module = "talon", frozen)]
#[derive(Clone)]
pub struct ObjectEntry {
    /// Mount-relative object path.
    #[pyo3(get)]
    pub path: String,
    /// Object size in bytes.
    #[pyo3(get)]
    pub size: u64,
}

#[pymethods]
impl ObjectEntry {
    fn __repr__(&self) -> String {
        format!("ObjectEntry(path={:?}, size={})", self.path, self.size)
    }
}

/// A client for reading objects through a Talon cache cluster.
#[pyclass(module = "talon")]
pub struct Client {
    runtime: Arc<tokio::runtime::Runtime>,
    client: Arc<RustClient>,
}

#[pymethods]
impl Client {
    /// Connect to a coordinator.
    ///
    /// `block_size` must match the workers' configured block size; placement is
    /// computed per block, so a mismatch addresses the wrong blocks. It
    /// defaults to the worker default of 256 MiB.
    #[new]
    #[pyo3(signature = (coordinator, *, block_size = 256 << 20))]
    fn new(coordinator: &str, block_size: u32) -> PyResult<Self> {
        if block_size == 0 {
            return Err(PyValueError::new_err("block_size must be non-zero"));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(io_err)?;
        let client = RustClient::new(coordinator, block_size)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
            client: Arc::new(client),
        })
    }

    /// Read `length` bytes from `uri` starting at `offset`.
    ///
    /// Returns `bytes`. A read at or past end-of-file returns an empty buffer,
    /// and a read overlapping the end is truncated to what exists — POSIX short
    /// read semantics, so callers must check the returned length rather than
    /// assuming they got what they asked for.
    ///
    /// Ranges spanning block boundaries are split and fetched per block, each
    /// benefiting independently from the placement cache.
    ///
    /// `version` and `size` are resolved with a `stat` when omitted. Pass them
    /// to skip that round trip when they are already known — for example when
    /// reading many ranges of the same object. The supplied version is exact:
    /// a worker never substitutes bytes from a newer generation. Ordinary
    /// KiB/MiB reads fit in one production-sized block; independent reads on
    /// this Client share an aggregate budget of 1024 active worker requests.
    /// Rare cross-block reads use an internal fairness window.
    #[pyo3(signature = (uri, *, offset = 0, length = None, version = None, size = None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        uri: &str,
        offset: u64,
        length: Option<u64>,
        version: Option<&str>,
        size: Option<u64>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let object = parse_uri(uri).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let known_version = version.map(str::to_owned);
        let runtime = Arc::clone(&self.runtime);
        let client = Arc::clone(&self.client);

        // Release the GIL: this is network I/O, and holding it would serialise
        // every reader thread in the process on one request.
        let bytes = py.allow_threads(move || {
            runtime.block_on(async move {
                let known_stat = match (known_version, size) {
                    (Some(version), Some(size)) => Some(RustObjectStat { size, version }),
                    (None, None) => None,
                    (known_version, known_size) => {
                        let stat = client.stat(&object).await?;
                        Some(complete_known_stat(known_version, known_size, stat))
                    }
                };
                client
                    .read(&object, offset, length, known_stat.as_ref())
                    .await
            })
        });
        let bytes = bytes.map_err(client_err)?;
        Ok(PyBytes::new_bound(py, &bytes))
    }

    /// Return an object's size and version.
    fn stat(&self, py: Python<'_>, uri: &str) -> PyResult<ObjectStat> {
        let object = parse_uri(uri).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let runtime = Arc::clone(&self.runtime);
        let client = Arc::clone(&self.client);
        let stat =
            py.allow_threads(move || runtime.block_on(async move { client.stat(&object).await }));
        let stat = stat.map_err(client_err)?;
        Ok(ObjectStat {
            size: stat.size,
            version: stat.version,
        })
    }

    /// List objects under a mount-relative prefix, e.g. `az/container/dir`.
    ///
    /// The prefix names a backend and bucket (`az/container`), optionally
    /// followed by a key prefix. Returned paths are in the same namespace, so
    /// they can be passed straight to [`read`](Self::read) after converting to
    /// a URI.
    ///
    /// The control protocol carries one bounded response. If a prefix exceeds
    /// the server's object, page, or payload limit, the call fails explicitly
    /// instead of returning an incomplete list; use a narrower prefix.
    fn list(&self, py: Python<'_>, prefix: &str) -> PyResult<Vec<ObjectEntry>> {
        let runtime = Arc::clone(&self.runtime);
        let client = Arc::clone(&self.client);
        let prefix = prefix.to_string();
        let entries =
            py.allow_threads(move || runtime.block_on(async move { client.list(&prefix).await }));
        let entries = entries.map_err(client_err)?;
        Ok(entries
            .into_iter()
            .map(|e| ObjectEntry {
                path: e.path,
                size: e.size,
            })
            .collect())
    }

    /// The coordinator address this client is connected to.
    #[getter]
    fn coordinator(&self) -> &str {
        self.client.coordinator_addr()
    }

    fn __repr__(&self) -> String {
        format!(
            "Client(coordinator={:?}, block_size={})",
            self.client.coordinator_addr(),
            self.client.block_size()
        )
    }

    /// Support `with talon.Client(...) as client:`.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type = None, _exc_value = None, _traceback = None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        // Connections are pooled and closed when the client drops; nothing to
        // do here, but the context-manager protocol is what Python users expect.
        false
    }
}

#[pymodule]
fn talon(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    m.add_class::<ObjectStat>()?;
    m.add_class::<ObjectEntry>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completing_partial_stat_preserves_caller_version() {
        let completed = complete_known_stat(
            Some("caller-version".into()),
            None,
            RustObjectStat {
                size: 4096,
                version: "coordinator-version".into(),
            },
        );

        assert_eq!(completed.size, 4096);
        assert_eq!(completed.version, "caller-version");
    }

    // `extension-module` intentionally omits libpython linkage on Unix. The
    // Python-client CI job runs these embedding tests without that feature;
    // the all-features workspace job still compiles the extension itself.
    #[cfg(not(feature = "extension-module"))]
    #[test]
    fn invalid_read_argument_raises_value_error() {
        pyo3::prepare_freethreaded_python();
        let client = Client::new("unused", 1).unwrap();
        let oversized = (isize::MAX as u64).saturating_add(1);

        Python::with_gil(|py| {
            let error = match client.read(
                py,
                "s3://bucket/key",
                0,
                Some(oversized),
                Some("version"),
                Some(oversized),
            ) {
                Ok(_) => panic!("oversized read must fail"),
                Err(error) => error,
            };

            assert!(error.is_instance_of::<PyValueError>(py));

            let worker_error = client_err(RustError::Read(CacheReadError::InvalidRequest(
                "worker rejected the range".into(),
            )));
            assert!(worker_error.is_instance_of::<PyValueError>(py));
        });
    }

    #[cfg(not(feature = "extension-module"))]
    #[test]
    fn coordinator_failure_raises_io_error() {
        pyo3::prepare_freethreaded_python();
        let client = Client::new("127.0.0.1:0", 1).unwrap();

        Python::with_gil(|py| {
            let error = match client.stat(py, "s3://bucket/key") {
                Ok(_) => panic!("stat without a coordinator must fail"),
                Err(error) => error,
            };

            assert!(error.is_instance_of::<PyIOError>(py));
        });
    }
}
