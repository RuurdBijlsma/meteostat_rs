use crate::types::data_source::DataSource;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::weather_data::data_loader::WeatherDataLoader;
use crate::weather_data::error::WeatherDataError;
use crate::weather_data::extractor::DataFrameExtractor;
use chrono::{DateTime, NaiveDate, Utc};
use std::collections::{hash_map::Entry, HashMap};
use std::path::Path;
use tokio::sync::Mutex;

pub struct WeatherFetcher {
    loader: WeatherDataLoader,
    extractor_cache: Mutex<HashMap<(String, DataSource), DataFrameExtractor>>,
}

impl WeatherFetcher {
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            loader: WeatherDataLoader::new(cache_dir),
            extractor_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Gets a DataFrameExtractor for a given station and data source, using the cache if possible.
    async fn get_cached_extractor(
        &self,
        station: &str,
        data_source: DataSource,
    ) -> Result<DataFrameExtractor, WeatherDataError> {
        let key = (station.to_string(), data_source);

        // --- Fast path: Check if already in cache (read lock) ---
        {
            let cache = self.extractor_cache.lock().await;
            if let Some(cached_extractor) = cache.get(&key) {
                // Found in cache, return a clone. Cloning DataFrameExtractor should be cheap
                // as LazyFrame clones are cheap.
                return Ok(cached_extractor.clone());
            }
            // Extractor not in cache, release the lock before loading
        } // Lock guard is dropped here

        // --- Slow path: Load the frame and create the extractor ---
        // This potentially long-running operation happens outside the lock
        let loaded_frame = self.loader.get_frame(data_source, station).await?;
        // Create the extractor instance *once*
        let extractor = DataFrameExtractor::new(loaded_frame, station);

        // --- Insert into cache (write lock) ---
        // Lock again to insert the created extractor
        let mut cache = self.extractor_cache.lock().await;

        // Use Entry API to handle potential race condition:
        match cache.entry(key) {
            Entry::Occupied(entry) => {
                // Someone else created and inserted it while we were loading/creating.
                // Use their version (clone it). Discard the one we just created.
                Ok(entry.get().clone())
            }
            Entry::Vacant(entry) => {
                // We are the first to insert it. Store our created extractor.
                // Clone the extractor: one for the cache, one to return.
                entry.insert(extractor.clone());
                Ok(extractor) // Return the original instance we created
            }
        }
    }


    pub async fn hourly(
        &self,
        station: &str,
        datetime: DateTime<Utc>,
    ) -> Result<HourlyWeatherInfo, WeatherDataError> {
        // Get the cached or newly created extractor
        let extractor = self
            .get_cached_extractor(station, DataSource::Hourly)
            .await?;
        // Use the extractor directly
        extractor.extract_hourly(datetime)
    }

    pub async fn daily(
        &self,
        station: &str,
        date: NaiveDate,
    ) -> Result<DailyWeatherInfo, WeatherDataError> {
        // Get the cached or newly created extractor
        let extractor = self
            .get_cached_extractor(station, DataSource::Daily)
            .await?;
        // Use the extractor directly
        extractor.extract_daily(date)
    }

    pub async fn monthly(
        &self,
        station: &str,
        year: i32,
        month: u32,
    ) -> Result<MonthlyWeatherInfo, WeatherDataError> {
        // Get the cached or newly created extractor
        let extractor = self
            .get_cached_extractor(station, DataSource::Monthly)
            .await?;
        // Use the extractor directly
        extractor.extract_monthly(year, month)
    }

    pub async fn climate_normals(
        &self,
        station: &str,
        start_year: i32,
        end_year: i32,
        month: u32,
    ) -> Result<ClimateNormalInfo, WeatherDataError> {
        // Get the cached or newly created extractor
        let extractor = self
            .get_cached_extractor(station, DataSource::Normals)
            .await?;
        // Use the extractor directly
        extractor.extract_climate_normal(start_year, end_year, month)
    }
}
