use crate::error::MeteostatError;
use crate::stations::error::LocateStationError;
use crate::stations::locate_station::StationLocator;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use chrono::Duration;
use std::path::PathBuf;

pub struct Meteostat {
    station_locator: StationLocator,
}

pub struct MeteostatBuilder {
    max_distance_km: f64,
    max_time_diff_hours: i64,
    cache_folder: Option<PathBuf>,
    refresh_cache_after: Duration,
    combine_sources: bool,
}

impl Default for MeteostatBuilder {
    fn default() -> Self {
        Self {
            max_distance_km: 30.0,
            max_time_diff_hours: 0,
            cache_folder: None,
            refresh_cache_after: Duration::days(30),
            combine_sources: true,
        }
    }
}

impl MeteostatBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_distance_km(mut self, km: f64) -> Self {
        self.max_distance_km = km;
        self
    }

    pub fn with_max_time_diff_hours(mut self, hours: i64) -> Self {
        self.max_time_diff_hours = hours;
        self
    }

    pub fn with_cache_folder<P: Into<PathBuf>>(mut self, folder: P) -> Self {
        self.cache_folder = Some(folder.into());
        self
    }

    pub fn with_refresh_cache_after(mut self, duration: Duration) -> Self {
        self.refresh_cache_after = duration;
        self
    }

    pub fn with_combine_sources(mut self, combine: bool) -> Self {
        self.combine_sources = combine;
        self
    }

    pub async fn build(self) -> Result<Meteostat, MeteostatError> {
        let cache_folder = match self.cache_folder {
            Some(p) => p,
            None => get_cache_dir().map_err(MeteostatError::CacheDirResolution)?,
        };
        ensure_cache_dir_exists(&cache_folder)
            .await
            .map_err(|e| LocateStationError::CacheDirCreation(cache_folder.clone(), e))?;

        // let station_locator = StationLocator::init_with_options(
        //     self.max_distance_km,
        //     self.max_time_diff_hours,
        //     cache_folder,
        //     self.refresh_cache_after,
        //     self.combine_sources,
        // ).await?;
        let station_locator = StationLocator::new(&cache_folder).await?;

        Ok(Meteostat { station_locator })
    }
}
