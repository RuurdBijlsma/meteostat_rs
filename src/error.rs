use std::path::PathBuf;
use crate::stations::station_error::LocateStationError;
use crate::weather_data::weather_data_error::WeatherDataError;
use thiserror::Error;

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
}
