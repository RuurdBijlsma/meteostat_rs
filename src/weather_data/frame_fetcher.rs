use crate::types::frequency::Frequency;
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

    pub async fn clear_cache_all(&self) -> Result<(), WeatherDataError> {
        let mut cache = self.lazyframe_cache.lock().await;
        cache.clear();
        Ok(())
    }

    pub async fn clear_cache(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<(), WeatherDataError> {
        let mut cache = self.lazyframe_cache.lock().await;
        cache.remove(&(station.to_string(), frequency));
        Ok(())
    }

    /// Gets a LazyFrame for a given station and frequency, using the cache if possible.
    pub async fn get_cache_lazyframe(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<LazyFrame, WeatherDataError> {
        let key = (station.to_string(), frequency);

        // --- Fast path: Check if already in cache (read lock) ---
        {
            // Lock asynchronously
            let cache = self.lazyframe_cache.lock().await;
            if let Some(cached_frame) = cache.get(&key) {
                // Found in cache, return a clone. LazyFrame clones are cheap.
                return Ok(cached_frame.clone());
            }
            // Not in cache, release the lock before loading
        } // Lock guard is dropped here

        // --- Slow path: Load the frame ---
        // This potentially long-running operation happens outside the lock
        let loaded_frame = self.loader.get_frame(frequency, station).await?;

        // --- Insert into cache (write lock) ---
        // Lock again to insert the created frame
        // Lock asynchronously
        let mut cache = self.lazyframe_cache.lock().await;

        // Use Entry API to handle potential race condition:
        // If another task loaded and inserted the same frame while we were loading,
        // we avoid overwriting it and just return the existing one.
        match cache.entry(key) {
            Entry::Occupied(entry) => {
                // Someone else created and inserted it while we were loading.
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
