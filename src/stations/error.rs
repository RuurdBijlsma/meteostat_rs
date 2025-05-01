use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocateStationError {
    #[error("Failed to determine cache directory")]
    CacheDirResolution(#[source] std::io::Error),

    #[error("Failed to create cache directory '{0}'")]
    CacheDirCreation(PathBuf, #[source] std::io::Error),

    #[error("Failed to read cache file '{0}'")]
    CacheRead(PathBuf, #[source] std::io::Error),

    #[error("Failed to write cache file '{0}'")]
    CacheWrite(PathBuf, #[source] std::io::Error),

    #[error("Failed to decode cache data from '{0}'")]
    CacheDecode(PathBuf, #[source] Box<bincode::error::DecodeError>),

    #[error("Failed to encode cache data")]
    CacheEncode(#[source] Box<bincode::error::EncodeError>),

    #[error("Network request failed for {0}")]
    NetworkRequest(String, #[source] reqwest::Error),

    #[error("HTTP request failed for {url} with status {status}")]
    HttpStatus {
        url: String,
        status: reqwest::StatusCode,
        #[source]
        source: reqwest::Error,
    },

    // Covers errors during download stream processing and decompression
    #[error("Data download or decompression failed")]
    DownloadIo(#[from] std::io::Error),

    #[error("Failed to parse JSON data")]
    JsonParse(#[from] serde_json::Error),

    // Covers errors joining tokio blocking tasks
    #[error("Background task failed to complete")]
    TaskJoin(#[from] tokio::task::JoinError),
}
