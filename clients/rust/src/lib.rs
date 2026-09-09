//! Native async client for reading objects through a Talon cache cluster.

mod client;
mod error;

pub use client::{parse_uri, Client};
pub use error::{Error, UriError};
pub use talon_cache_client::ObjectStat;
pub use talon_core::{ObjectId, Version};
pub use talon_transport::ObjectEntry;

pub use talon_telemetry::{RequestOptions, TraceContext, TraceParent};
