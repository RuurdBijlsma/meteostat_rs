use crate::stations::error::LocateStationError;
use crate::types::station::Station;
use async_compression::tokio::bufread::GzipDecoder;
use bincode;
use bincode::config::{Configuration, Fixint, LittleEndian};
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

pub struct StationLocator {
    rtree: RTree<Station>,
}

impl StationLocator {
    pub async fn new(cache_dir: &Path) -> Result<Self, LocateStationError> {
        let cache_file = cache_dir.join(BINCODE_CACHE_FILE_NAME);

        let stations: Vec<Station>;

        if cache_file.exists() {
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

    /// Finds up to N nearest stations to the given latitude and longitude,
    /// ensuring they are within a specified maximum distance (radius).
    /// Results are sorted by actual Haversine distance.
    ///
    /// # Arguments
    /// * `latitude` - The latitude of the center point.
    /// * `longitude` - The longitude of the center point.
    /// * `n_results` - The maximum number of stations to return.
    /// * `max_distance_km` - The maximum distance (radius) in kilometers from the center point.
    ///                       Stations further than this will be excluded.
    ///
    /// # Returns
    /// A vector of tuples, where each tuple contains a reference to a `Station`
    /// and its calculated distance in kilometers. The vector is sorted by distance
    /// and contains at most `n_results` elements, all within `max_distance_km`.
    pub fn query(
        &self,
        latitude: f64,
        longitude: f64,
        n_results: usize,
        max_distance_km: f64, // New parameter
    ) -> Vec<(&Station, f64)> {
        if n_results == 0 {
            return vec![];
        }
        let query_point = [latitude, longitude];

        // 1. Perform nearest neighbor search using R-Tree.
        // We still use `take(n_results)` as an initial heuristic filter based on the
        // R-tree's distance metric. This avoids calculating Haversine for potentially
        // thousands of points if the tree is large and max_distance_km is generous.
        // If n_results is large and max_distance_km small, this might fetch
        // candidates that are later filtered out.
        let candidates: Vec<&Station> = self
            .rtree
            .nearest_neighbor_iter(&query_point)
            .take(n_results) // Get up to n candidates initially
            .collect();

        if candidates.is_empty() {
            return vec![];
        }

        // 2. Calculate precise Haversine distance for the candidates and filter by max_distance_km.
        let mut stations_with_dist: Vec<(&Station, f64)> = candidates
            .into_iter()
            .map(|station| {
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
                (station, dist_km)
            })
            // Filter out stations beyond the maximum allowed distance
            .filter(|(_, dist_km)| *dist_km <= max_distance_km)
            .collect();

        // 3. Sort the remaining (filtered) list by the calculated Haversine distance.
        stations_with_dist
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // 4. The list is already implicitly capped at n_results (due to the initial .take())
        //    and filtered by distance. Return the result.
        stations_with_dist
    }
}
