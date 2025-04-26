use crate::types::data_source::Frequency;
use crate::weather_data::data_loader::WeatherDataLoader;
use crate::weather_data::error::WeatherDataError;
use polars::prelude::LazyFrame;
use std::collections::{hash_map::Entry, HashMap};
use std::path::Path;
use tokio::sync::Mutex;

pub struct FrameFetcher {
    loader: WeatherDataLoader,
    lazyframe_cache: Mutex<HashMap<(String, Frequency), LazyFrame>>,
}

impl FrameFetcher {
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            loader: WeatherDataLoader::new(cache_dir),
            lazyframe_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Gets a DataFrameExtractor for a given station and data source, using the cache if possible.
    pub async fn get_cache_lazyframe(
        &self,
        station: &str,
        data_source: Frequency,
    ) -> Result<LazyFrame, WeatherDataError> {
        let key = (station.to_string(), data_source);

        // --- Fast path: Check if already in cache (read lock) ---
        {
            let cache = self.lazyframe_cache.lock().await;
            if let Some(cached_extractor) = cache.get(&key) {
                // Found in cache, return a clone.
                return Ok(cached_extractor.clone());
            }
            // Not in cache, release the lock before loading
        } // Lock guard is dropped here

        // --- Slow path: Load the frame and create the extractor ---
        // This potentially long-running operation happens outside the lock
        let loaded_frame = self.loader.get_frame(data_source, station).await?;

        // --- Insert into cache (write lock) ---
        // Lock again to insert the created extractor
        let mut cache = self.lazyframe_cache.lock().await;

        // Use Entry API to handle potential race condition:
        match cache.entry(key) {
            Entry::Occupied(entry) => {
                // Someone else created and inserted it while we were loading/creating.
                // Use their version (clone it). Discard the one we just created.
                Ok(entry.get().clone())
            }
            Entry::Vacant(entry) => {
                // We are the first to insert it. Store our created frame.
                // Clone the frame: one for the cache, one to return.
                entry.insert(loaded_frame.clone());
                Ok(loaded_frame) // Return the original instance we created
            }
        }
    }
}
