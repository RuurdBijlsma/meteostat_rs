use crate::stations::station_error::{LocateStationError, Result};
use crate::types::station::Station;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use async_compression::tokio::bufread::GzipDecoder;
use bincode;
use bincode::config::{Configuration, Fixint, LittleEndian};
use futures_util::TryStreamExt;
use haversine::{distance, Location as HaversineLocation, Units};
use reqwest::Client;
use rstar::RTree;
use std::io::{self}; // Keep this for mapping stream errors
use std::path::{Path, PathBuf};
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
    // Update the return type to use the custom Result
    pub async fn init() -> Result<Self> {
        let cache_path = Self::get_cache_path()?;

        let stations: Vec<Station>;

        if cache_path.exists() {
            // Read file contents in a blocking task
            // Clone cache_path before moving it into the closure
            let path_clone = cache_path.clone();
            stations = tokio::task::spawn_blocking(move || Self::get_cached_stations(&path_clone))
                .await??; // First ? handles JoinError, second handles StationCacheError
        } else {
            println!("Cache file not found. Fetching from URL: {}", DATA_URL);
            stations = Self::fetch_stations().await?;
            // Clone stations before moving into the closure if needed later
            Self::cache_stations(stations.clone(), &cache_path).await?;
        }

        let rtree = RTree::bulk_load(stations);
        Ok(StationLocator { rtree })
    }

    // Update the return type
    fn get_cached_stations(cache_path: &Path) -> Result<Vec<Station>> {
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
    async fn fetch_stations() -> Result<Vec<Station>> {
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

        // Ensure parent dir exists, map potential io::Error
        if let Some(parent) = cache_path.parent() {
            // Assuming ensure_cache_dir_exists returns Result<(), std::io::Error>
            ensure_cache_dir_exists(parent)
                .await
                .map_err(|e| LocateStationError::CacheDirCreation(parent.to_path_buf(), e))?;
        }
        // No else needed, if there's no parent, writing to root might fail later anyway

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

    // Update the return type - assuming get_cache_dir returns Result<PathBuf, std::io::Error>
    fn get_cache_path() -> Result<PathBuf, LocateStationError> {
        get_cache_dir()
            .map(|dir| dir.join(BINCODE_CACHE_FILE_NAME))
            // Map the error from get_cache_dir
            .map_err(LocateStationError::CacheDirResolution)
        // If get_cache_dir returns a custom error handled by CacheUtilError:
        // .map_err(StationCacheError::from)
    }

    /// Finds the N nearest stations to the given latitude and longitude.
    /// Results are sorted by actual Haversine distance.
    /// This function doesn't involve fallible operations needing the new error type.
    pub fn query(&self, latitude: f64, longitude: f64, n_results: usize) -> Vec<(&Station, f64)> {
        if n_results == 0 {
            return vec![];
        }
        let query_point = [latitude, longitude];

        // 1. Perform nearest neighbor search using R-Tree (uses squared Euclidean).
        // R-Tree nearest_neighbor_iter doesn't typically return errors in this usage.
        let candidates: Vec<&Station> = self
            .rtree
            .nearest_neighbor_iter(&query_point)
            .take(n_results)
            .collect();

        if candidates.is_empty() {
            return vec![];
        }

        // 2. Calculate precise Haversine distance for the candidates.
        // Haversine distance calculation doesn't return errors.
        let mut stations_with_dist: Vec<(&Station, f64)> = candidates
            .into_iter()
            .map(|station| {
                let station_loc = HaversineLocation {
                    latitude: station.location.latitude,
                    longitude: station.location.longitude,
                };
                let query_loc = HaversineLocation {
                    latitude,
                    longitude,
                };
                let dist_km = distance(query_loc, station_loc, Units::Kilometers);
                (station, dist_km)
            })
            .collect();

        // 3. Sort the final small list by the calculated Haversine distance.
        // Sorting doesn't return errors, unwrap_or handles potential NaN comparison edge cases.
        stations_with_dist
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        stations_with_dist
    }
}
