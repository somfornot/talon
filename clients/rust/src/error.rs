use talon_cache_client::{CacheReadError, CoordinatorError};

/// Structured failures from parsing an object URI.
#[derive(Debug, thiserror::Error)]
pub enum UriError {
    #[error("expected a scheme://bucket/key URI, got {uri:?} (schemes: s3, gcs, az)")]
    MissingScheme { uri: String },
    #[error("unknown backend scheme {scheme:?}; expected s3, gcs, or az")]
    UnknownScheme { scheme: String },
    #[error("URI is missing an object key: {uri:?} (expected {scheme}://bucket/key)")]
    MissingKey { uri: String, scheme: String },
    #[error("URI has an empty bucket: {uri:?}")]
    EmptyBucket { uri: String },
    #[error("URI has an empty object key: {uri:?}")]
    EmptyKey { uri: String },
}

/// Errors returned by the native Rust client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    InvalidUri(#[from] UriError),
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error(transparent)]
    Read(#[from] CacheReadError),
}
