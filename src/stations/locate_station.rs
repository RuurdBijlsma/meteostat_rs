use crate::stations::error::LocateStationError;
use crate::types::data_source::{Frequency, RequiredDate};
use crate::types::station::YearRange;
use crate::types::station::{DateRange, Station};
use async_compression::tokio::bufread::GzipDecoder;
use bincode;
use bincode::config::{Configuration, Fixint, LittleEndian};
use chrono::Datelike;
use futures_util::TryStreamExt;
use haversine::{distance, Location as HaversineLocation, Units};
use reqwest::Client;
use rstar::RTree;
use std::io::{self};
use std::path::Path;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

const DATA_URL: &str = "https://bulk.meteostat.net/v2/stations/lite.json.gz";
const BINCODE_CACHE_FILE_NAME: &str = "stations_lite.bin";
const BINCODE_CONFIG: Configuration<LittleEndian, Fixint> =
    bincode::config::standard().with_fixed_int_encoding();

#[derive(Debug, Clone)]
pub struct StationLocator {
    rtree: RTree<Station>,
}

impl StationLocator {
    pub async fn new(cache_dir: &Path) -> Result<Self, LocateStationError> {
        let cache_file = cache_dir.join(BINCODE_CACHE_FILE_NAME);

        let stations: Vec<Station>;

        if cache_file.exists()  {
            // Read file contents in a blocking task
            // Clone cache_path before moving it into the closure
            let path_clone = cache_file.clone();
            stations = tokio::task::spawn_blocking(move || Self::get_cached_stations(&path_clone))
                .await??; // First ? handles JoinError, second handles StationCacheError
        } else {
            println!("Cache file not found. Fetching from URL: {}", DATA_URL);
            stations = Self::fetch_stations().await?;
            // Clone stations before moving into the closure if needed later
            Self::cache_stations(stations.clone(), &cache_file).await?;
        }

        let rtree = RTree::bulk_load(stations);
        Ok(StationLocator { rtree })
    }

    // Update the return type
    fn get_cached_stations(cache_path: &Path) -> Result<Vec<Station>, LocateStationError> {
        let bytes = std::fs::read(cache_path).map_err(|e| {
            LocateStationError::CacheRead(cache_path.to_path_buf(), e) // Add context
        })?;

        // Use map_err to wrap the bincode error
        let (decoded_stations, _) =
            bincode::serde::decode_from_slice::<Vec<Station>, _>(&bytes, BINCODE_CONFIG).map_err(
                |e| LocateStationError::CacheDecode(cache_path.to_path_buf(), Box::from(e)),
            )?;

        Ok(decoded_stations)
    }

    // Update the return type
    async fn fetch_stations() -> Result<Vec<Station>, LocateStationError> {
        let client = Client::new();
        let response = client
            .get(DATA_URL)
            .send()
            .await
            .map_err(|e| LocateStationError::NetworkRequest(DATA_URL.to_string(), e))?; // Wrap reqwest error

        // Check for HTTP status errors explicitly
        let response = match response.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
                // If it's a status error, use the specific variant
                if let Some(status) = e.status() {
                    return Err(LocateStationError::HttpStatus {
                        url: DATA_URL.to_string(),
                        status,
                        source: e,
                    });
                } else {
                    // Otherwise, treat as a general network error for this URL
                    return Err(LocateStationError::NetworkRequest(DATA_URL.to_string(), e));
                }
            }
        };

        // Map the stream error to std::io::Error as required by StreamReader
        let stream = response
            .bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)); // Keep this mapping
        let stream_reader = StreamReader::new(stream);
        let gzip_decoder = GzipDecoder::new(BufReader::new(stream_reader));
        let mut decoder_reader = BufReader::new(gzip_decoder);
        let mut decompressed_json = Vec::with_capacity(20_000_000);

        // read_to_end can return io::Error, which is handled by DownloadIo via #[from]
        decoder_reader.read_to_end(&mut decompressed_json).await?;
        println!(
            "Downloaded and decompressed data ({} bytes)",
            decompressed_json.len(),
        );

        // --- Parsing ---
        let parse_start = std::time::Instant::now();
        // The closure now needs to return Result<Vec<Station>, StationCacheError>
        let stations = tokio::task::spawn_blocking(move || {
            serde_json::from_slice::<Vec<Station>>(&decompressed_json)
                // Error automatically converted via #[from] JsonParse
                .map_err(LocateStationError::from)
        })
            .await??; // First ? for JoinError, second for JsonParse
        println!(
            "Parsed {} stations from JSON in {:?}",
            stations.len(),
            parse_start.elapsed()
        );
        Ok(stations)
    }

    // Update the return type
    async fn cache_stations(
        stations: Vec<Station>,
        cache_path: &Path,
    ) -> Result<(), LocateStationError> {
        let cache_start = std::time::Instant::now();

        // The closure now needs to return Result<Vec<u8>, StationCacheError>
        let bincode_data = tokio::task::spawn_blocking({
            move || {
                bincode::serde::encode_to_vec(stations, BINCODE_CONFIG)
                    // Map the bincode error explicitly
                    .map_err(|e| LocateStationError::CacheEncode(Box::new(e)))
            }
        })
            .await??; // First ? for JoinError, second for CacheEncode

        // Map potential io::Error from write
        tokio::fs::write(&cache_path, &bincode_data)
            .await
            .map_err(|e| LocateStationError::CacheWrite(cache_path.to_path_buf(), e))?;

        println!(
            "Serialized and wrote cache ({} bytes) to {} in {:?}",
            bincode_data.len(),
            cache_path.display(),
            cache_start.elapsed()
        );
        Ok(())
    }

    /// Finds up to N nearest stations matching the criteria.
    ///
    /// Stations are first filtered by maximum distance, then optionally by data
    /// availability (frequency and date/time requirements). Finally, the results
    /// are sorted by actual Haversine distance.
    ///
    /// # Arguments
    /// * `latitude` - Latitude of the center point.
    /// * `longitude` - Longitude of the center point.
    /// * `n_results` - Maximum number of stations to return.
    /// * `max_distance_km` - Maximum distance (radius) in kilometers. Stations
    ///                       further than this are excluded.
    /// * `frequency` - Optional: Filter stations that have data for this specific frequency.
    /// * `required_date` - Optional: If `frequency` is set, further filter by
    ///                      date/time availability. Requires `frequency` to be `Some`.
    ///
    /// # Returns
    /// A vector of tuples `(&Station, f64)`, sorted by distance (f64, in km),
    /// containing at most `n_results` stations meeting all criteria.
    pub fn query(
        &self,
        latitude: f64,
        longitude: f64,
        n_results: usize,
        max_distance_km: f64,
        frequency: Option<Frequency>,     // New parameter
        required_date: Option<RequiredDate>, // New parameter
    ) -> Vec<(&Station, f64)> {
        if n_results == 0 {
            return vec![];
        }
        let query_point = [latitude, longitude];

        // Iterate through nearest neighbors, calculate distance, filter, collect, sort, take.
        let mut candidates_with_dist: Vec<(&Station, f64)> = self
            .rtree
            .nearest_neighbor_iter(&query_point)
            .filter_map(|station| { // Use filter_map to combine steps efficiently
                // --- 1. Calculate Haversine distance ---
                let station_loc = HaversineLocation {
                    latitude: station.location.latitude,
                    longitude: station.location.longitude,
                };
                let dist_km = distance(HaversineLocation { latitude, longitude }, station_loc, Units::Kilometers);

                // --- 2. Filter by max distance ---
                if dist_km > max_distance_km {
                    return None; // Discard if too far
                }

                // --- 3. Filter by inventory criteria (if provided) ---
                if !Self::station_meets_criteria(station, frequency, required_date.as_ref()) {
                    return None; // Discard if inventory doesn't match
                }

                // --- If all filters pass, keep the station and its distance ---
                Some((station, dist_km))
            })
            .collect(); // Collect all stations passing filters

        // --- 4. Sort the filtered results by distance ---
        candidates_with_dist
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // --- 5. Take the top n_results ---
        candidates_with_dist.truncate(n_results);

        candidates_with_dist
    }

    // Helper function to check inventory criteria (add this inside impl StationLocator)
    fn station_meets_criteria(
        station: &Station,
        frequency: Option<Frequency>,
        required_date: Option<&RequiredDate>, // Pass as reference to avoid clone
    ) -> bool {
        // If no frequency requirement is given, the station always passes this filter.
        let freq = match frequency {
            Some(f) => f,
            None => return true,
        };

        // If frequency is specified, but no date requirement, default to RequiredDate::Any
        let req_date = required_date.unwrap_or(&RequiredDate::Any);

        // Dispatch to the appropriate checker based on frequency
        match freq {
            Frequency::Daily => Self::check_date_range_inventory(
                &station.inventory.daily,
                req_date,
            ),
            Frequency::Hourly => Self::check_date_range_inventory(
                &station.inventory.hourly,
                req_date,
            ),
            Frequency::Monthly => Self::check_year_range_inventory(
                &station.inventory.monthly,
                req_date,
            ),
            Frequency::Climate => Self::check_year_range_inventory(
                &station.inventory.normals,
                req_date,
            ),
        }
    }

    // Helper for checking inventories with NaiveDate ranges (Daily, Hourly, Model)
    fn check_date_range_inventory(
        inventory_range: &DateRange,
        required_date: &RequiredDate,
    ) -> bool {
        let Some(inv_start) = inventory_range.start else { return false };
        let Some(inv_end) = inventory_range.end else { return false };

        match required_date {
            RequiredDate::Any => true,
            RequiredDate::SpecificDate(req_date) => {
                inv_start <= *req_date && *req_date <= inv_end
            }
            RequiredDate::DateRange { start: req_start, end: req_end } => {
                // Check if the inventory range COMPLETELY CONTAINS the required range.
                inv_start <= *req_start && inv_end >= *req_end
            }
        }
    }

    // Helper for checking inventories with Year ranges (Monthly, Climate/Normals)
    fn check_year_range_inventory(
        inventory_range: &YearRange,
        required_date: &RequiredDate,
    ) -> bool {
        let Some(inv_start_year) = inventory_range.start else { return false };
        let Some(inv_end_year) = inventory_range.end else { return false };

        match required_date {
            RequiredDate::Any => true,
            RequiredDate::SpecificDate(req_date) => {
                let req_year = req_date.year();
                inv_start_year <= req_year && req_year <= inv_end_year
            }
            RequiredDate::DateRange { start: req_start, end: req_end } => {
                let req_start_year = req_start.year();
                let req_end_year = req_end.year();
                // Check if the inventory year range COMPLETELY CONTAINS the required year range.
                inv_start_year <= req_start_year && inv_end_year >= req_end_year
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::data_source::{Frequency, RequiredDate};
    use crate::types::station::Station;
    use crate::utils::get_cache_dir;
    use chrono::NaiveDate;

    // Helper to get a StationLocator instance, handles caching
    async fn get_locator() -> Result<StationLocator, LocateStationError> {
        let cache_path = get_cache_dir()?;
        Ok(StationLocator::new(&cache_path)
            .await
            .expect("Failed to initialize StationLocator"))
    }

    // Helper to validate basic query results
    fn validate_results(
        results: &Vec<(&Station, f64)>,
        expected_max_len: usize,
        max_distance_km: f64,
    ) {
        assert!(
            results.len() <= expected_max_len,
            "Expected max {} results, but got {}",
            expected_max_len,
            results.len()
        );

        // Check distances are within bounds and sorted
        let mut last_dist = 0.0;
        for (i, (station, dist)) in results.iter().enumerate() {
            assert!(
                *dist <= max_distance_km,
                "Result {} ({}) is farther ({:.2} km) than max_distance_km ({:.2} km)",
                i, station.id, dist, max_distance_km
            );
            assert!(
                *dist >= last_dist,
                "Results are not sorted by distance: result {} ({:.2} km) < previous ({:.2} km)",
                i, dist, last_dist
            );
            last_dist = *dist;
            // println!("  -> Station {}: {:.2} km", station.id, dist); // Optional: print results
        }
    }

    #[tokio::test]
    async fn test_basic_query_no_filters() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 40.7128; // New York City approx.
        let lon = -74.0060;
        let n_results = 5;
        let max_distance_km = 100.0;

        let results = locator.query(lat, lon, n_results, max_distance_km, None, None);

        println!(
            "Basic Query (NYC): Found {} results (max {}) within {} km",
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        assert!(!results.is_empty(), "Expected some results near NYC"); // Should find stations near NYC

        Ok(())
    }

    #[tokio::test]
    async fn test_query_with_frequency_any_date() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 52.5200; // Berlin approx.
        let lon = 13.4050;
        let n_results = 3;
        let max_distance_km = 150.0;
        let frequency = Some(Frequency::Daily);
        let required_date = Some(RequiredDate::Any); // Explicitly Any

        let results = locator.query(
            lat,
            lon,
            n_results,
            max_distance_km,
            frequency,
            required_date, // Can also pass None here, should default to Any check
        );

        println!(
            "Frequency Query (Berlin, Daily, Any): Found {} results (max {}) within {} km",
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        assert!(!results.is_empty(), "Expected some results near Berlin with Daily data");

        // Verify that the results actually have daily data available
        for (station, _) in &results {
            assert!(
                station.inventory.daily.start.is_some() && station.inventory.daily.end.is_some(),
                "Station {} was returned but doesn't have daily start/end dates in inventory",
                station.id
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_query_with_frequency_specific_date() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 34.0522; // Los Angeles approx.
        let lon = -118.2437;
        let n_results = 4;
        let max_distance_km = 200.0;
        let frequency = Some(Frequency::Hourly);
        // Choose a date likely covered by many stations
        let specific_date = NaiveDate::from_ymd_opt(2022, 1, 15).unwrap();
        let required_date = Some(RequiredDate::SpecificDate(specific_date));

        let results = locator.query(
            lat,
            lon,
            n_results,
            max_distance_km,
            frequency,
            required_date,
        );

        println!(
            "Frequency+Date Query (LA, Hourly, {}): Found {} results (max {}) within {} km",
            specific_date,
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        // May or may not find results depending on actual data coverage
        // assert!(!results.is_empty(), "Expected some results near LA with Hourly data for {}", specific_date);

        // Verify results meet the criteria
        for (station, _) in &results {
            let inv = &station.inventory.hourly;
            assert!(
                inv.start.is_some() && inv.end.is_some(),
                "Station {} returned but has no hourly range", station.id
            );
            assert!(
                inv.start.unwrap() <= specific_date && specific_date <= inv.end.unwrap(),
                "Station {} returned but hourly range {:?} to {:?} does not include {}",
                station.id, inv.start, inv.end, specific_date
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_query_with_frequency_date_range_overlap() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 48.8566; // Paris approx.
        let lon = 2.3522;
        let n_results = 2;
        let max_distance_km = 100.0;
        let frequency = Some(Frequency::Monthly);
        // Look for stations with monthly data in the 2010s
        let start_date = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2019, 12, 31).unwrap();
        let required_date = Some(RequiredDate::DateRange {
            start: start_date,
            end: end_date,
        });

        let results = locator.query(
            lat,
            lon,
            n_results,
            max_distance_km,
            frequency,
            required_date,
        );

        println!(
            "Frequency+Range Query (Paris, Monthly, {}-{}): Found {} results (max {}) within {} km",
            start_date.year(), end_date.year(),
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        dbg!(&results);
        // assert!(!results.is_empty(), "Expected some results near Paris with monthly data overlapping 2010-2019");

        // Verify results meet the criteria (overlap check is done in the query function)
        for (station, _) in &results {
            let inv = &station.inventory.monthly;
            assert!(
                inv.start.is_some() && inv.end.is_some(),
                "Station {} returned but has no monthly range", station.id
            );
            let inv_start_year = inv.start.unwrap();
            let inv_end_year = inv.end.unwrap();
            let req_start_year = start_date.year();
            let req_end_year = end_date.year();
            assert!(
                inv_start_year <= req_end_year && inv_end_year >= req_start_year,
                "Station {} returned but monthly year range {}-{} does not overlap with required {}-{}",
                station.id, inv_start_year, inv_end_year, req_start_year, req_end_year
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_query_climate_data() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = -33.8688; // Sydney approx.
        let lon = 151.2093;
        let n_results = 5;
        let max_distance_km = 300.0;
        let frequency = Some(Frequency::Climate);
        // We don't have a specific date requirement for climate normals usually
        let required_date = Some(RequiredDate::Any);

        let results = locator.query(
            lat,
            lon,
            n_results,
            max_distance_km,
            frequency,
            required_date,
        );

        println!(
            "Climate Query (Sydney, Any): Found {} results (max {}) within {} km",
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        // assert!(!results.is_empty(), "Expected some results near Sydney with climate normals data");

        // Verify results have climate data
        for (station, _) in &results {
            assert!(
                station.inventory.normals.start.is_some() && station.inventory.normals.end.is_some(),
                "Station {} was returned but doesn't have normals start/end years in inventory",
                station.id
            );
        }

        Ok(())
    }


    #[tokio::test]
    async fn test_query_no_results_tight_radius() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 0.0; // Middle of the ocean
        let lon = 0.0;
        let n_results = 5;
        let max_distance_km = 1.0; // Very small radius

        let results = locator.query(lat, lon, n_results, max_distance_km, None, None);

        println!(
            "No Results Query (0,0): Found {} results (max {}) within {} km",
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        assert!(
            results.is_empty(),
            "Expected no results for a very small radius at (0,0)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_query_n_results_zero() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 40.7128; // NYC
        let lon = -74.0060;
        let n_results = 0; // Request zero results
        let max_distance_km = 500.0;

        let results = locator.query(lat, lon, n_results, max_distance_km, None, None);

        println!(
            "Zero Results Query (NYC): Found {} results (max {}) within {} km",
            results.len(),
            n_results,
            max_distance_km
        );
        assert!(
            results.is_empty(),
            "Expected no results when n_results is 0"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_query_specific_date_outside_range() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 51.5074; // London
        let lon = 0.1278;
        let n_results = 5;
        let max_distance_km = 100.0;
        let frequency = Some(Frequency::Daily);
        // A date far in the future, unlikely to be in inventory yet
        let specific_date = NaiveDate::from_ymd_opt(2099, 12, 31).unwrap();
        let required_date = Some(RequiredDate::SpecificDate(specific_date));

        let results = locator.query(
            lat,
            lon,
            n_results,
            max_distance_km,
            frequency,
            required_date,
        );

        println!(
            "Date Outside Range Query (London, Daily, {}): Found {} results (max {}) within {} km",
            specific_date,
            results.len(),
            n_results,
            max_distance_km
        );
        validate_results(&results, n_results, max_distance_km);
        // It's highly probable this will return empty, but we check the condition anyway
        for (station, _) in &results {
            let inv = &station.inventory.daily;
            assert!(
                inv.start.is_some() && inv.end.is_some(),
                "Station {} returned but has no daily range", station.id
            );
            assert!(
                inv.start.unwrap() <= specific_date && specific_date <= inv.end.unwrap(),
                "Station {} returned but daily range {:?} to {:?} does not include {}",
                station.id, inv.start, inv.end, specific_date
            );
        }
        // We can't assert!(results.is_empty()) definitively without knowing the exact data,
        // but the validation loop above will fail if an incorrect station is returned.

        Ok(())
    }
}
