use std::path::PathBuf;
use crate::stations::error::LocateStationError;
use crate::weather_data::error::WeatherDataError;
use thiserror::Error;
use crate::types::data_source::DataSource;

#[derive(Debug, Error)]
pub enum MeteostatError {
    #[error(transparent)]
    WeatherData(#[from] WeatherDataError),

    #[error(transparent)]
    LocateStation(#[from] LocateStationError),

    #[error("Failed to create cache directory '{0}'")]
    CacheDirCreation(PathBuf, #[source] std::io::Error),

    #[error("Failed to determine cache directory")]
    CacheDirResolution(#[source] std::io::Error),

    #[error("No {granularity} data found for datetime: {datetime} and position: {latitude}, {longitude}.")]
    NoDataFound{datetime: String, latitude: f64, longitude: f64, granularity: DataSource},
}
