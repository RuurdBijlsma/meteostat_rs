// src/weather_data/errors.rs (or similar path)
use std::path::PathBuf;
use thiserror::Error;
use chrono::NaiveDate; // Import NaiveDate

#[derive(Debug, Error)]
pub enum WeatherDataError {
    #[error("Failed to resolve cache directory")]
    CacheDirResolution(#[source] std::io::Error),

    #[error("Failed to create cache directory '{0}'")]
    CacheDirCreation(PathBuf, #[source] std::io::Error),

    #[error("Failed to read metadata for cache file '{0}'")]
    CacheMetadataRead(PathBuf, #[source] std::io::Error),

    // Errors during parquet writing (inside blocking task)
    #[error("I/O error writing parquet cache file '{0}'")]
    ParquetWriteIo(PathBuf, #[source] std::io::Error),
    #[error("Encoding error writing parquet cache file '{0}'")]
    ParquetWritePolars(PathBuf, #[source] polars::error::PolarsError),

    #[error("Failed to scan parquet cache file '{0}'")]
    ParquetScan(PathBuf, #[source] polars::error::PolarsError),

    #[error("Network request failed for {0}")]
    NetworkRequest(String, #[source] reqwest::Error),

    #[error("HTTP request failed for {url} with status {status}")]
    HttpStatus {
        url: String,
        status: reqwest::StatusCode,
        #[source]
        source: reqwest::Error,
    },

    #[error("Data download or decompression failed")]
    DownloadIo(#[from] std::io::Error), // Handles stream errors, read_to_end

    // Errors during CSV reading (inside blocking task)
    #[error("I/O error processing CSV data for station '{station}'")]
    CsvReadIo { station: String, #[source] source: std::io::Error },
    #[error("Parsing error processing CSV data for station '{station}'")]
    CsvReadPolars { station: String, #[source] source: polars::error::PolarsError },

    #[error("Background task failed to complete")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Failed processing DataFrame: {0}")] // General Polars errors during DF ops
    DataFrameProcessing(#[from] polars::error::PolarsError),

    // Specific error for when the requested data row isn't found
    #[error("No weather data found for station '{station}' at {date} {hour:02}:00")]
    DataNotFound {
        station: String,
        date: NaiveDate,
        hour: u32,
    },

    #[error("Required column '{0}' not found in DataFrame")]
    ColumnNotFound(String),
}

// Define a specific Result type alias for convenience within this module
pub type Result<T, E = WeatherDataError> = std::result::Result<T, E>;