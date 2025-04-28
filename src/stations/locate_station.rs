use crate::stations::error::LocateStationError;
use crate::types::frequency::{Frequency, RequiredData};
use crate::types::station::YearRange;
use crate::types::station::{DateRange, Station};
use async_compression::tokio::bufread::GzipDecoder;
use bincode;
use bincode::config::{Configuration, Fixint, LittleEndian};
use chrono::{Datelike, NaiveDate};
use futures_util::TryStreamExt;
use haversine::{distance, Location as HaversineLocation, Units};
use ordered_float::OrderedFloat;
use reqwest::Client;
use rstar::RTree;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
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

// Helper struct for BinaryHeap ordering
struct StationCandidate<'a> {
    distance_km: OrderedFloat<f64>,
    station: &'a Station,
}
// Manual implementations (only compare distance)
impl PartialEq for StationCandidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.distance_km == other.distance_km
    }
}
impl Eq for StationCandidate<'_> {}
impl PartialOrd for StationCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StationCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance_km.cmp(&other.distance_km)
    }
}

impl StationLocator {
    pub async fn new(cache_dir: &Path) -> Result<Self, LocateStationError> {
        let cache_file = cache_dir.join(BINCODE_CACHE_FILE_NAME);

        let stations: Vec<Station>;

        if cache_file.exists() {
            let path_clone = cache_file.clone();
            stations = tokio::task::spawn_blocking(move || Self::get_cached_stations(&path_clone))
                .await??;
        } else {
            println!("Cache file not found. Fetching from URL: {}", DATA_URL);
            stations = Self::fetch_stations().await?;
            Self::cache_stations(stations.clone(), &cache_file).await?;
        }

        let rtree = RTree::bulk_load(stations);
        Ok(StationLocator { rtree })
    }

    // --- Caching and Fetching methods remain the same ---
    fn get_cached_stations(cache_path: &Path) -> Result<Vec<Station>, LocateStationError> {
        let bytes = std::fs::read(cache_path)
            .map_err(|e| LocateStationError::CacheRead(cache_path.to_path_buf(), e))?;
        let (decoded_stations, _) =
            bincode::serde::decode_from_slice::<Vec<Station>, _>(&bytes, BINCODE_CONFIG).map_err(
                |e| LocateStationError::CacheDecode(cache_path.to_path_buf(), Box::from(e)),
            )?;
        Ok(decoded_stations)
    }
    async fn fetch_stations() -> Result<Vec<Station>, LocateStationError> {
        // ... implementation unchanged ...
        let client = Client::new();
        let response = client
            .get(DATA_URL)
            .send()
            .await
            .map_err(|e| LocateStationError::NetworkRequest(DATA_URL.to_string(), e))?;
        let response = match response.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
                if let Some(status) = e.status() {
                    return Err(LocateStationError::HttpStatus {
                        url: DATA_URL.to_string(),
                        status,
                        source: e,
                    });
                } else {
                    return Err(LocateStationError::NetworkRequest(DATA_URL.to_string(), e));
                }
            }
        };
        let stream = response
            .bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));
        let stream_reader = StreamReader::new(stream);
        let gzip_decoder = GzipDecoder::new(BufReader::new(stream_reader));
        let mut decoder_reader = BufReader::new(gzip_decoder);
        let mut decompressed_json = Vec::with_capacity(20_000_000);
        decoder_reader.read_to_end(&mut decompressed_json).await?;
        // println!("Downloaded and decompressed data ({} bytes)", decompressed_json.len()); // Reduce noise
        let parse_start = std::time::Instant::now();
        let stations = tokio::task::spawn_blocking(move || {
            serde_json::from_slice::<Vec<Station>>(&decompressed_json)
                .map_err(LocateStationError::from)
        })
        .await??;
        println!(
            "Parsed {} stations from JSON in {:?}",
            stations.len(),
            parse_start.elapsed()
        );
        Ok(stations)
    }
    async fn cache_stations(
        stations: Vec<Station>,
        cache_path: &Path,
    ) -> Result<(), LocateStationError> {
        // ... implementation unchanged ...
        let cache_start = std::time::Instant::now();
        let bincode_data = tokio::task::spawn_blocking({
            move || {
                bincode::serde::encode_to_vec(stations, BINCODE_CONFIG)
                    .map_err(|e| LocateStationError::CacheEncode(Box::new(e)))
            }
        })
        .await??;
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
    // --- End Caching/Fetching ---

    /// Finds up to N nearest stations matching the criteria. Uses a fast path for simple
    /// proximity queries and a heap-based approach with heuristic limits for filtered queries.
    pub fn query(
        &self,
        latitude: f64,
        longitude: f64,
        n_results: usize,
        max_distance_km: f64,
        frequency: Option<Frequency>,
        required_data: Option<RequiredData>,
    ) -> Vec<(Station, f64)> {
        if n_results == 0 {
            return vec![];
        }

        // --- Fast path: If no inventory filters are applied, use a simpler, faster method ---
        if frequency.is_none() {
            // required_date is ignored if frequency is None by station_meets_criteria,
            // so we can reliably use the fast path here.
            return self.fast_proximity_query(latitude, longitude, n_results, max_distance_km);
        }

        // --- Filtered path: Use heap + heuristic limit ---
        self.filtered_heap_query(
            latitude,
            longitude,
            n_results,
            max_distance_km,
            frequency.unwrap(),
            required_data,
        )
    }

    /// Optimized query for finding nearest stations without inventory filters.
    /// Limits R-tree iteration and performs fewer Haversine calculations.
    fn fast_proximity_query(
        &self,
        latitude: f64,
        longitude: f64,
        n_results: usize,
        max_distance_km: f64,
    ) -> Vec<(Station, f64)> {
        let query_point_rtree = [latitude, longitude];

        // Heuristic limit: Take slightly more than needed to account for distance filtering
        // and Haversine vs R-tree distance differences.
        let candidate_limit = (n_results * 2).max(20); // Check at least 20 or 2x n_results

        let initial_candidates: Vec<_> = self
            .rtree
            .nearest_neighbor_iter(&query_point_rtree)
            .take(candidate_limit)
            .collect();

        let mut stations_with_dist: Vec<(Station, f64)> = initial_candidates
            .into_iter()
            .filter_map(|station| {
                // Use filter_map for combined Haversine calc + distance filter
                let station_loc = HaversineLocation {
                    latitude: station.location.latitude,
                    longitude: station.location.longitude,
                };
                let dist_km = distance(
                    HaversineLocation {
                        latitude,
                        longitude,
                    },
                    station_loc,
                    Units::Kilometers,
                );

                if dist_km <= max_distance_km {
                    Some((station.to_owned(), dist_km))
                } else {
                    None
                }
            })
            .collect();

        // Sort only the candidates that passed the distance filter
        stations_with_dist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        // Return the top N results
        stations_with_dist.truncate(n_results);
        stations_with_dist
    }

    /// Query using BinaryHeap for filtering, with a heuristic limit on R-Tree iteration.
    fn filtered_heap_query(
        &self,
        latitude: f64,
        longitude: f64,
        n_results: usize,
        max_distance_km: f64,
        frequency: Frequency,
        required_date: Option<RequiredData>,
    ) -> Vec<(Station, f64)> {
        let query_point_rtree = [latitude, longitude];
        let mut heap: BinaryHeap<StationCandidate<'_>> = BinaryHeap::with_capacity(n_results);

        // Heuristic limit for filtered queries. Might need tuning.
        // A larger iteration_limit increases chance of correctness but potentially slows down.
        let iteration_limit = n_results + 1;
        let mut items_checked = 0;

        for station in self.rtree.nearest_neighbor_iter(&query_point_rtree) {
            items_checked += 1;

            // --- 1. Check inventory criteria (relatively cheap) ---
            // Pass frequency by value (it's Copy), required_date by ref.
            if !Self::station_meets_criteria(station, Some(frequency), required_date.as_ref()) {
                continue;
            }

            // --- 2. Calculate Haversine distance (more expensive) ---
            let station_loc = HaversineLocation {
                latitude: station.location.latitude,
                longitude: station.location.longitude,
            };
            let dist_km = distance(
                HaversineLocation {
                    latitude,
                    longitude,
                },
                station_loc,
                Units::Kilometers,
            );

            // --- 3. Check max distance ---
            if dist_km * 2.0 > max_distance_km {
                // It's Joever.
                break;
            }
            if dist_km > max_distance_km {
                // Slight chance one of the next stations will be within range.
                continue;
            }

            // --- 4. Manage the heap ---
            let current_candidate = StationCandidate {
                distance_km: OrderedFloat(dist_km),
                station,
            };

            if heap.len() < n_results {
                heap.push(current_candidate);
            } else {
                // unwrap safe: heap is full (len >= n_results >= 1)
                let worst_candidate_distance = heap.peek().unwrap().distance_km;
                if current_candidate.distance_km < worst_candidate_distance {
                    heap.pop(); // Remove element with largest distance
                    heap.push(current_candidate); // Add the better one
                }
            }

            // --- 5. Heuristic Early Exit Check ---
            // If we have checked enough items and the heap is full,
            // assume we are unlikely to find a better candidate later.
            // This is the key performance optimization for filtered queries.
            if items_checked >= iteration_limit && heap.len() == n_results {
                // println!("DEBUG: Filtered query early exit after {} items checked (limit {})", items_checked, iteration_limit); // Optional debug noise
                break;
            }
        } // End R-tree iteration

        // --- 6. Extract results from the heap ---
        let results: Vec<(Station, f64)> = heap
            .into_sorted_vec() // Sorts ascending by distance (based on Ord impl)
            .into_iter()
            .map(|c| (c.station.to_owned(), c.distance_km.into_inner()))
            .collect();

        results
    }

    // --- Inventory check helpers remain the same ---
    fn station_meets_criteria(
        station: &Station,
        frequency: Option<Frequency>,
        required_date: Option<&RequiredData>,
    ) -> bool {
        let freq = match frequency {
            Some(f) => f,
            None => return true, // No filter applied
        };
        let req_date = required_date.unwrap_or(&RequiredData::Any);
        match freq {
            Frequency::Daily => {
                Self::check_date_range_inventory(&station.inventory.daily, req_date)
            }
            Frequency::Hourly => {
                Self::check_date_range_inventory(&station.inventory.hourly, req_date)
            }
            Frequency::Monthly => {
                Self::check_year_range_inventory(&station.inventory.monthly, req_date)
            }
            Frequency::Climate => {
                Self::check_year_range_inventory(&station.inventory.normals, req_date)
            }
        }
    }
    fn check_date_range_inventory(
        inventory_range: &DateRange,
        required_date: &RequiredData,
    ) -> bool {
        let (Some(inv_start), Some(inv_end)) = (inventory_range.start, inventory_range.end) else {
            return false;
        };
        match required_date {
            RequiredData::Any => true,
            RequiredData::SpecificDate(req) => inv_start <= *req && *req <= inv_end,
            RequiredData::DateRange {
                start: req_s,
                end: req_e,
            } => inv_start <= *req_s && inv_end >= *req_e,
            RequiredData::Year(year) => {
                let Some(req_start) = NaiveDate::from_ymd_opt(*year, 1, 1) else {
                    return false;
                };
                let Some(req_end) = NaiveDate::from_ymd_opt(*year, 12, 31) else {
                    return false;
                };
                inv_start <= req_start && inv_end >= req_end
            }
        }
    }
    fn check_year_range_inventory(
        inventory_range: &YearRange,
        required_date: &RequiredData,
    ) -> bool {
        let (Some(inv_start_y), Some(inv_end_y)) = (inventory_range.start, inventory_range.end)
        else {
            return false;
        };
        match required_date {
            RequiredData::Any => true,
            RequiredData::SpecificDate(req) => {
                let req_y = req.year();
                inv_start_y <= req_y && req_y <= inv_end_y
            }
            RequiredData::DateRange {
                start: req_s,
                end: req_e,
            } => {
                let req_s_y = req_s.year();
                let req_e_y = req_e.year();
                inv_start_y <= req_s_y && inv_end_y >= req_e_y
            }
            RequiredData::Year(year) => {
                let req_y = *year;
                inv_start_y <= req_y && req_y <= inv_end_y
            }
        }
    }
}

// --- Tests Module (should still pass, calling the main `query` function) ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::frequency::{Frequency, RequiredData};
    use crate::types::station::Station;
    // Make sure get_cache_dir is available or replace with hardcoded path for tests
    use crate::utils::get_cache_dir;
    use chrono::{Datelike, NaiveDate};

    // Helper to get a StationLocator instance, handles caching
    async fn get_locator() -> Result<StationLocator, LocateStationError> {
        let cache_path = get_cache_dir().expect("Failed to get cache dir for tests");
        tokio::fs::create_dir_all(&cache_path)
            .await
            .expect("Failed to create cache dir");
        Ok(StationLocator::new(&cache_path)
            .await
            .expect("Failed to initialize StationLocator"))
    }

    // Helper to validate basic query results (consider adding back criteria check)
    fn validate_results(
        results: &[(Station, f64)],
        expected_max_len: usize,
        max_distance_km: f64,
        // You might want to pass frequency/required_date back in for deeper validation
        // frequency: Option<Frequency>,
        // required_date: Option<RequiredDate>,
    ) {
        assert!(
            results.len() <= expected_max_len,
            "Expected max {} results, got {}",
            expected_max_len,
            results.len()
        );
        let mut last_dist = -1.0;
        for (i, (station, dist)) in results.iter().enumerate() {
            assert!(
                *dist <= max_distance_km + 1e-9,
                "Result {} ({}) distance {} > max {}",
                i,
                station.id,
                dist,
                max_distance_km
            );
            assert!(
                *dist >= last_dist - 1e-9,
                "Result {} ({}) distance {} < previous {}",
                i,
                station.id,
                dist,
                last_dist
            );
            last_dist = *dist;
            // Add criteria check back if needed:
            // assert!(StationLocator::station_meets_criteria(station, frequency, required_date.as_ref()), "Station {} failed criteria", station.id);
        }
    }

    // --- Individual test cases remain largely the same, calling locator.query(...) ---
    #[tokio::test]
    async fn test_basic_query_no_filters() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 40.7128;
        let lon = -74.0060;
        let n = 5;
        let max_d = 100.0;
        let results = locator.query(lat, lon, n, max_d, None, None);
        println!(
            "Basic Query (NYC): Found {} results (max {}) within {} km",
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        Ok(())
    }
    #[tokio::test]
    async fn test_query_with_frequency_any_date() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 52.5200;
        let lon = 13.4050;
        let n = 3;
        let max_d = 150.0;
        let freq = Some(Frequency::Daily);
        let req_date = Some(RequiredData::Any);
        let results = locator.query(lat, lon, n, max_d, freq, req_date);
        println!(
            "Frequency Query (Berlin, Daily, Any): Found {} results (max {}) within {} km",
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        // Add specific check for Daily data if desired
        for (s, _) in &results {
            assert!(s.inventory.daily.start.is_some());
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_query_with_frequency_specific_date() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 34.0522;
        let lon = -118.2437;
        let n = 4;
        let max_d = 200.0;
        let freq = Some(Frequency::Hourly);
        let specific_date = NaiveDate::from_ymd_opt(2022, 1, 15).unwrap();
        let req_date = Some(RequiredData::SpecificDate(specific_date));
        let results = locator.query(lat, lon, n, max_d, freq, req_date);
        println!(
            "Frequency+Date Query (LA, Hourly, {}): Found {} results (max {}) within {} km",
            specific_date,
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        // Add specific check for date inclusion if desired
        for (s, _) in &results {
            let inv = &s.inventory.hourly;
            assert!(
                inv.start.is_some_and(|st| st <= specific_date)
                    && inv.end.is_some_and(|en| en >= specific_date)
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_query_with_frequency_date_range_complete_containment(
    ) -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 48.8566;
        let lon = 2.3522;
        let n = 2;
        let max_d = 100.0;
        let freq = Some(Frequency::Monthly);
        let start_date = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2019, 12, 31).unwrap();
        let req_date = Some(RequiredData::DateRange {
            start: start_date,
            end: end_date,
        });
        let results = locator.query(lat, lon, n, max_d, freq, req_date);
        println!(
            "Frequency+Range Query (Paris, Monthly, {}-{}): Found {} results (max {}) within {} km",
            start_date.year(),
            end_date.year(),
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        // Add specific check for year range containment if desired
        for (s, _) in &results {
            let inv = &s.inventory.monthly;
            assert!(
                inv.start.is_some_and(|sy| sy <= start_date.year())
                    && inv.end.is_some_and(|ey| ey >= end_date.year())
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_query_climate_data() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = -33.8688;
        let lon = 151.2093;
        let n = 5;
        let max_d = 300.0;
        let freq = Some(Frequency::Climate);
        let req_date = Some(RequiredData::Any);
        let results = locator.query(lat, lon, n, max_d, freq, req_date);
        println!(
            "Climate Query (Sydney, Any): Found {} results (max {}) within {} km",
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        Ok(())
    }
    #[tokio::test]
    async fn test_query_no_results_tight_radius() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 0.0;
        let lon = 0.0;
        let n = 5;
        let max_d = 1.0;
        let results = locator.query(lat, lon, n, max_d, None, None);
        println!(
            "No Results Query (0,0): Found {} results (max {}) within {} km",
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        assert!(results.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn test_query_n_results_zero() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 40.7128;
        let lon = -74.0060;
        let n = 0;
        let max_d = 500.0;
        let results = locator.query(lat, lon, n, max_d, None, None);
        println!(
            "Zero Results Query (NYC): Found {} results (max {}) within {} km",
            results.len(),
            n,
            max_d
        );
        assert!(results.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn test_query_specific_date_outside_range() -> Result<(), LocateStationError> {
        let locator = get_locator().await?;
        let lat = 51.5074;
        let lon = 0.1278;
        let n = 5;
        let max_d = 100.0;
        let freq = Some(Frequency::Daily);
        let specific_date = NaiveDate::from_ymd_opt(2099, 12, 31).unwrap();
        let req_date = Some(RequiredData::SpecificDate(specific_date));
        let results = locator.query(lat, lon, n, max_d, freq, req_date);
        println!(
            "Date Outside Range Query (London, Daily, {}): Found {} results (max {}) within {} km",
            specific_date,
            results.len(),
            n,
            max_d
        );
        validate_results(&results, n, max_d);
        // Most likely empty, validation inside validate_results covers correctness if not empty
        Ok(())
    }
}
