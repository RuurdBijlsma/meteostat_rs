use crate::types::frequency::Frequency;
use chrono::NaiveDate;
use polars::error::PolarsError;
use std::path::PathBuf;
use std::time::SystemTimeError;
use thiserror::Error;

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
    ParquetWritePolars(PathBuf, #[source] PolarsError),

    #[error("Failed to scan parquet cache file '{0}'")]
    ParquetScan(PathBuf, #[source] PolarsError),

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
    CsvReadIo {
        station: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Parsing error processing CSV data for station '{station}'")]
    CsvReadPolars {
        station: String,
        #[source]
        source: PolarsError,
    },

    #[error("Background task failed to complete")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Failed processing DataFrame: {0}")]
    DataFrameProcessing(#[from] PolarsError),

    // Specific error for when the requested data row isn't found
    #[error("No weather data found for station '{station}' at {date} {hour:02}:00")]
    DataNotFound {
        station: String,
        date: NaiveDate,
        hour: u32,
    },

    #[error("Required column '{0}' not found in DataFrame")]
    ColumnNotFound(String, #[source] PolarsError),

    #[error("CSV column count ({found}) does not match schema length ({expected}) for {data_type} data for station {station}")]
    SchemaMismatch {
        station: String,
        data_type: Frequency,
        expected: usize,
        found: usize,
    },

    #[error("Failed to rename columns for station {station}: {source}")]
    ColumnRenameError {
        station: String,
        source: PolarsError,
    },

    #[error("Polars operation failed for station {station}: {source}")]
    PolarsError {
        station: String,
        #[source]
        source: PolarsError,
    },

    #[error("Unexpected data state, station {station}: {message}")]
    UnexpectedData { message: String, station: String },

    #[error("Failed Polars column operation for station {station}: {source}")]
    ColumnOperationError {
        station: String,
        source: PolarsError,
    },

    #[error("Missing required column '{column}' for station {station}")]
    MissingColumnError { station: String, column: String },

    #[error("Failed to calculate system time difference for {0:?}")]
    SystemTimeCalculation(PathBuf, #[source] SystemTimeError),

    #[error("Failed to delete cache '{0}'")]
    CacheDeletionError(PathBuf, #[source] std::io::Error),
}
