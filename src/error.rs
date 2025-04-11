// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MeteostatError {
    #[error("Network request failed: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Failed to decompress data: {0}")]
    Decompression(#[from] std::io::Error),

    #[error("Failed to parse JSON: {0}")]
    JsonParsing(#[from] serde_json::Error),

    #[error("Failed to parse CSV/Data: {0}")]
    DataParsing(#[from] polars::prelude::PolarsError),

    #[error("No weather stations found in the provided list")]
    NoStationsFound,

    #[error("Could not find a suitable station near the specified location")]
    NoSuitableStation,

    #[error("Station '{0}' not found or has no hourly data (HTTP 404)")]
    StationNotFound(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Timestamp conversion error: {0}")]
    TimestampConversion(String),
}

pub type Result<T> = std::result::Result<T, MeteostatError>;