//! Native async client for reading objects through a Talon cache cluster.

mod client;
mod error;

pub use client::{parse_uri, Client, DEFAULT_MAX_IN_FLIGHT_BLOCK_READS};
pub use error::{Error, UriError};
pub use talon_cache_client::{CacheReadError, ObjectStat};
pub use talon_core::{ObjectId, Version};
pub use talon_transport::ObjectEntry;
