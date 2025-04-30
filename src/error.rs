use crate::stations::error::LocateStationError;
use crate::types::frequency::Frequency;
use crate::weather_data::error::WeatherDataError;
use polars::frame::DataFrame;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteostatError {
    #[error(transparent)]
    WeatherData(#[from] WeatherDataError),

    #[error(transparent)]
    LocateStation(#[from] LocateStationError),

    #[error("Polars error occurred while filtering data {0}.")]
    PolarsError(#[from] polars::error::PolarsError),

    #[error("Failed to create cache directory '{0}'")]
    CacheDirCreation(PathBuf, #[source] std::io::Error),

    #[error("Failed to determine cache directory")]
    CacheDirResolution(#[from] std::io::Error),

    #[error("Failed to delete cache '{0}'")]
    CacheDeletionError(PathBuf, #[source] std::io::Error),

    #[error("No {granularity} data found for datetime: {datetime} and position: {latitude}, {longitude}.")]
    NoDataFound {
        datetime: String,
        latitude: f64,
        longitude: f64,
        granularity: Frequency,
    },

    #[error("No station within radius: {radius} km, at position {lat}, {lon}")]
    NoStationWithinRadius { radius: f64, lat: f64, lon: f64 },

    #[error("Tried {stations_tried} stations near ({lat}, {lon}) within {radius} km, but failed to fetch data. Last error: {last_error:?}")]
    NoDataFoundForNearbyStations {
        radius: f64,
        lat: f64,
        lon: f64,
        stations_tried: usize,
        last_error: Option<Box<MeteostatError>>,
    },

    #[error("Could not detect frequency variant from dataframe.\n{0}")]
    FrequencyDetectionError(DataFrame),

    #[error("Could not interpret parameter as date(time).")]
    DateParsingError,

    #[error("Cannot get single climate row from just one date.")]
    ClimateSingleDateError,
}
