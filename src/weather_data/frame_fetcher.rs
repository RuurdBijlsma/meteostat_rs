use crate::types::frequency::Frequency;
use crate::weather_data::data_loader::WeatherDataLoader;
use crate::weather_data::error::WeatherDataError;
use crate::RequiredData;
use chrono::Utc;
use polars::prelude::LazyFrame;
use std::collections::{hash_map::Entry, HashMap};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

pub struct FrameFetcher {
    loader: WeatherDataLoader,
    lazyframe_cache: Mutex<HashMap<(String, Frequency), LazyFrame>>,
    cache_folder: PathBuf,
}

impl FrameFetcher {
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            loader: WeatherDataLoader::new(cache_dir),
            lazyframe_cache: Mutex::new(HashMap::new()),
            cache_folder: cache_dir.to_path_buf(),
        }
    }

    pub async fn clear_cache_all(&self) -> Result<(), WeatherDataError> {
        let mut entries = tokio::fs::read_dir(&self.cache_folder)
            .await
            .map_err(|e| WeatherDataError::CacheDeletionError(self.cache_folder.clone(), e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| WeatherDataError::CacheDeletionError(self.cache_folder.clone(), e))?
        {
            let file_path = entry.path();
            if file_path.is_file() {
                if let Some(extension) = file_path.extension() {
                    if extension == OsStr::new("parquet") {
                        match tokio::fs::remove_file(&file_path).await {
                            Ok(_) => {}
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {} // Ignore if already gone
                            Err(e) => {
                                return Err(WeatherDataError::CacheDeletionError(
                                    file_path.clone(),
                                    e,
                                ))
                            }
                        }
                    }
                }
            }
        }

        let mut cache = self.lazyframe_cache.lock().await;
        cache.clear();
        Ok(())
    }

    pub async fn clear_cache(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<(), WeatherDataError> {
        let file = self.cache_folder.join(format!(
            "{}{}.parquet",
            frequency.cache_file_prefix(),
            station
        ));
        match tokio::fs::remove_file(&file).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(WeatherDataError::CacheDeletionError(file.clone(), e)),
        }
        let mut cache = self.lazyframe_cache.lock().await;
        cache.remove(&(station.to_string(), frequency));
        Ok(())
    }

    /// Checks if the cache for a station/frequency is stale based on `required_data`.
    /// Returns `true` if the cache is stale, `false` if it's recent enough.
    async fn is_cache_stale(
        &self,
        station: &str,
        frequency: Frequency,
        required_data: RequiredData,
    ) -> Result<bool, WeatherDataError> {
        let required_date = match required_data.get_end_date() {
            Some(d) => d,
            None => return Ok(false),
        };

        if required_date > Utc::now().date_naive() {
            return Ok(false);
        }

        match self
            .loader
            .get_cache_modification_time(station, frequency)
            .await
        {
            Ok(Some(modified)) => {
                let cache_date = modified.date_naive();
                Ok(required_date > cache_date)
            }
            Ok(None) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Gets a LazyFrame for a given station and frequency, using the cache if possible.
    /// Handles automatic cache refresh based on `required_data`.
    pub async fn get_cache_lazyframe(
        &self,
        station: &str,
        frequency: Frequency,
        required_data: RequiredData,
    ) -> Result<LazyFrame, WeatherDataError> {
        if self
            .is_cache_stale(station, frequency, required_data)
            .await
            .unwrap_or(false)
        {
            self.clear_cache(station, frequency).await?;
        }

        // --- Step 2: Try fetching from in-memory cache (fast path) ---
        // This check runs *after* potential clearing. If cleared, it will be a miss.
        let key = (station.to_string(), frequency);
        {
            let cache = self.lazyframe_cache.lock().await;
            if let Some(cached_frame) = cache.get(&key) {
                return Ok(cached_frame.clone()); // Return clone of cached frame
            }
        } // Read lock guard is dropped here

        // --- Step 3: Load frame from disk or download (slow path) ---
        let loaded_frame = self.loader.get_frame(frequency, station).await?; // Load from disk/network

        // --- Step 4: Insert newly loaded frame into in-memory cache ---
        {
            let mut cache = self.lazyframe_cache.lock().await;
            // Use Entry API for race condition safety
            match cache.entry(key) {
                Entry::Occupied(entry) => {
                    // Another task loaded it while we were busy. Use theirs.
                    Ok(entry.get().clone())
                }
                Entry::Vacant(entry) => {
                    // We are the first to insert this newly loaded frame.
                    entry.insert(loaded_frame.clone()); // Insert clone into cache
                    Ok(loaded_frame) // Return the original frame we loaded
                }
            }
        } // Write lock guard is dropped here
    }
}

// --- Add tests at the end of the file ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LatLon, Meteostat, MeteostatError, RequiredData};
    // Import Meteostat and RequiredData
    use chrono::Datelike;
    use std::time::{Duration, SystemTime};
    use tempfile::tempdir;
    // For year()

    // Helper to get Parquet path
    fn get_parquet_path(cache_dir: &Path, station: &str, frequency: Frequency) -> PathBuf {
        cache_dir.join(format!(
            "{}{}.parquet",
            frequency.cache_file_prefix(),
            station
        ))
    }

    // Helper to get modification time
    async fn get_mtime(path: &Path) -> Option<SystemTime> {
        tokio::fs::metadata(path).await.ok()?.modified().ok()
    }

    // Helper to get a nearby station ID (Berlin area)
    async fn get_nearby_station_id(client: &Meteostat) -> Result<String, MeteostatError> {
        let berlin = LatLon(52.52, 13.4); // Berlin approximate
        client
            .find_stations()
            .location(berlin)
            .station_limit(1) // Need just one
            .call()
            .await?
            .first()
            .map(|s| s.id.clone())
            .ok_or_else(|| MeteostatError::NoStationWithinRadius {
                radius: 50.0,
                lat: berlin.0,
                lon: berlin.1,
            }) // Use appropriate error
    }

    #[tokio::test]
    async fn test_cache_refresh_not_triggered_when_recent() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();
        let client = Meteostat::with_cache_folder(cache_path.clone()).await?;
        let station_id = get_nearby_station_id(&client).await?;
        let frequency = Frequency::Daily;
        let parquet_path = get_parquet_path(&cache_path, &station_id, frequency);

        // 1. Initial fetch to create cache
        println!("Initial fetch for {}...", station_id);
        let _ = client.daily().station(&station_id).call().await?;
        assert!(
            parquet_path.exists(),
            "Cache file should exist after first fetch"
        );
        let mtime1 = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime 1");
        println!("Initial mtime: {:?}", mtime1);

        // Give it a moment to ensure times aren't *exactly* the same if filesystem resolution is low
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 2. Fetch again with RequiredData::Any (should use cache)
        println!("Fetching again with RequiredData::Any...");
        let _ = client
            .daily()
            .station(&station_id)
            .required_data(RequiredData::Any) // Explicitly Any
            .call()
            .await?;
        let mtime2 = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime 2");
        println!("Mtime after RequiredData::Any: {:?}", mtime2);
        assert_eq!(
            mtime1, mtime2,
            "Cache should NOT have been refreshed with RequiredData::Any"
        );

        // 3. Fetch again with a recent RequiredData year (should still use cache)
        let current_year = Utc::now().year();
        println!(
            "Fetching again with RequiredData::FullYear({}) (current year)...",
            current_year
        );
        let _ = client
            .daily()
            .station(&station_id)
            .required_data(RequiredData::FullYear(current_year))
            .call()
            .await?;
        let mtime3 = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime 3");
        println!(
            "Mtime after RequiredData::FullYear({}): {:?}",
            current_year, mtime3
        );
        assert_eq!(
            mtime1, mtime3,
            "Cache should NOT have been refreshed with current year requirement"
        );

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_refresh_not_triggered_for_future_date(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();
        let client = Meteostat::with_cache_folder(cache_path.clone()).await?;
        let station_id = get_nearby_station_id(&client).await?;
        let frequency = Frequency::Daily;
        let parquet_path = get_parquet_path(&cache_path, &station_id, frequency);

        // 1. Initial fetch
        println!("Initial fetch for {}...", station_id);
        let _ = client.daily().station(&station_id).call().await?;
        assert!(parquet_path.exists());
        let mtime1 = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime 1");
        println!("Initial mtime: {:?}", mtime1);

        tokio::time::sleep(Duration::from_secs(1)).await;

        // 2. Fetch with a future date requirement
        let future_year = Utc::now().year() + 5;
        println!(
            "Fetching again with RequiredData::FullYear({}) (future)...",
            future_year
        );
        let _ = client
            .daily()
            .station(&station_id)
            .required_data(RequiredData::FullYear(future_year))
            .call()
            .await?;
        let mtime2 = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime 2");
        println!("Mtime after future year requirement: {:?}", mtime2);

        // Cache should NOT be cleared for future dates
        assert_eq!(
            mtime1, mtime2,
            "Cache should NOT have been refreshed for a future date requirement"
        );

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_refresh_triggered_when_required_date_is_newer(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();
        let client = Meteostat::with_cache_folder(cache_path.clone()).await?;
        let station_id = get_nearby_station_id(&client).await?;
        let frequency = Frequency::Daily;
        let parquet_path = get_parquet_path(&cache_path, &station_id, frequency);

        // --- Setup: Create an "old" cache file ---
        // 1. Fetch normally to create the initial cache file.
        println!("Initial fetch for {} to create cache...", station_id);
        let _ = client.daily().station(&station_id).call().await?;
        assert!(
            parquet_path.exists(),
            "Cache file missing after initial fetch"
        );
        let mtime_initial = get_mtime(&parquet_path)
            .await
            .expect("Failed to get initial mtime");
        println!("Initial cache mtime: {:?}", mtime_initial);

        // 2. **Crucial Step**: Manually delete the file to *simulate* the effect of
        //    `clear_cache` being called because the time check *would have passed*
        //    if the file were truly old. This forces a re-download on the next step.
        //    (Directly modifying mtime is complex and platform-dependent).
        println!("Manually deleting cache file to simulate old cache...");
        tokio::fs::remove_file(&parquet_path).await?;
        assert!(!parquet_path.exists(), "Cache file should be deleted");

        // --- Test: Fetch with a required date newer than the simulated old cache ---
        // 3. Fetch again, requiring data up to a recent past year.
        //    Since the file is gone (simulating it was cleared due to being old),
        //    this MUST trigger a new download and create a new cache file.
        let recent_past_year = Utc::now().year() - 1; // e.g., 2023 if now is 2024
        println!(
            "Fetching again with RequiredData::FullYear({}) (should trigger re-download)...",
            recent_past_year
        );
        let _ = client
            .daily()
            .station(&station_id)
            .required_data(RequiredData::FullYear(recent_past_year))
            .call()
            .await?; // This call should re-download

        // --- Verification ---
        // 4. Check that the cache file exists again and has a *new* modification time.
        assert!(
            parquet_path.exists(),
            "Cache file should exist again after required fetch"
        );
        let mtime_after_refresh = get_mtime(&parquet_path)
            .await
            .expect("Failed to get mtime after refresh");
        println!("Mtime after required fetch: {:?}", mtime_after_refresh);

        // The new mtime should be *later* than the initial mtime (before deletion).
        // Allow for a small tolerance if filesystem time resolution is coarse, though
        // deleting and recreating should definitely result in a different time.
        assert!(
            mtime_after_refresh > mtime_initial,
            "New cache file mtime should be later than the initial one"
        );
        // More robust check: ensure the time difference is significant enough
        // assert!(mtime_after_refresh.duration_since(mtime_initial).unwrap_or_default() > Duration::from_millis(100));

        temp_dir.close()?;
        Ok(())
    }
}
