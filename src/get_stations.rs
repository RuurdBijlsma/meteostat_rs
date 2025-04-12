use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use anyhow::{Context, Result};
use async_compression::tokio::bufread::GzipDecoder;
use bincode;
use chrono::NaiveDate;
use futures_util::TryStreamExt;
use haversine::{distance, Location as HaversineLocation, Units};
use reqwest::Client;
use rstar::{PointDistance, RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self};
use std::path::{Path, PathBuf};
use bincode::config::{Configuration, Fixint, LittleEndian};
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

// --- Constants ---
const DATA_URL: &str = "https://bulk.meteostat.net/v2/stations/lite.json.gz";
const BINCODE_CACHE_FILE_NAME: &str = "stations_lite.bin";
const BINCODE_CONFIG: Configuration<LittleEndian, Fixint> = bincode::config::standard()
    .with_fixed_int_encoding();

// --- Data Structures ---
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Station {
    pub id: String,
    pub country: String,
    pub region: Option<String>,
    pub timezone: Option<String>,
    pub name: HashMap<String, String>,
    pub identifiers: Identifiers,
    pub location: Location,
    pub inventory: Inventory,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Inventory {
    pub daily: DateRange,
    pub hourly: DateRange,
    pub model: DateRange,
    pub monthly: YearRange,
    pub normals: YearRange,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateRange {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YearRange {
    pub start: Option<i32>,
    pub end: Option<i32>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Identifiers {
    pub national: Option<String>,
    pub wmo: Option<String>,
    pub icao: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Location {
    pub latitude: f64,
    pub longitude: f64,
    pub elevation: Option<i32>,
}

// --- R-Tree Implementations ---
impl RTreeObject for Station {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.location.latitude, self.location.longitude])
    }
}

impl PointDistance for Station {
    // R*Tree uses squared Euclidean distance for performance in its algorithms.
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        // point[0] = query latitude, point[1] = query longitude
        let dx = self.location.latitude - point[0];
        let dy = self.location.longitude - point[1];
        dx * dx + dy * dy
    }
}

// --- StationCache Implementation ---
pub struct StationCache {
    rtree: RTree<Station>,
}

impl StationCache {
    pub async fn init() -> Result<Self> {
        let cache_path = Self::get_cache_path()?;

        let stations: Vec<Station>;

        if cache_path.exists() {
            // Read file contents in a blocking task
            stations = tokio::task::spawn_blocking(move || {
                let bytes = std::fs::read(&cache_path)
                    .with_context(|| format!("Failed to read cache file: {}", cache_path.display()))?;

                let (decoded_stations, _) = bincode::serde::decode_from_slice::<Vec<Station>, _>(&bytes, BINCODE_CONFIG)
                    .map_err(anyhow::Error::from)?;

                Ok::<Vec<Station>, anyhow::Error>(decoded_stations)
            })
                .await??;
        } else {
            println!("Cache file not found. Fetching from URL: {}", DATA_URL);
            stations = Self::fetch_parse_and_cache_stations(&cache_path, BINCODE_CONFIG).await?;
        }

        let rtree = RTree::bulk_load(stations);
        Ok(StationCache { rtree })
    }

    async fn fetch_parse_and_cache_stations(
        cache_path: &Path,
        bincode_config: Configuration<LittleEndian, Fixint>,
    ) -> Result<Vec<Station>> {
        // --- Fetching ---
        let client = Client::new();
        let response = client.get(DATA_URL).send().await?.error_for_status()?;
        let stream = response
            .bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));
        let stream_reader = StreamReader::new(stream);
        let gzip_decoder = GzipDecoder::new(BufReader::new(stream_reader));
        let mut decoder_reader = BufReader::new(gzip_decoder);
        let mut decompressed_json = Vec::with_capacity(20_000_000);
        decoder_reader.read_to_end(&mut decompressed_json).await?;
        println!(
            "Downloaded and decompressed data ({} bytes)",
            decompressed_json.len(),
        );

        // --- Parsing ---
        let parse_start = std::time::Instant::now();
        let stations = tokio::task::spawn_blocking(move || {
            serde_json::from_slice::<Vec<Station>>(&decompressed_json).map_err(anyhow::Error::from)
        })
            .await??;
        println!(
            "Parsed {} stations from JSON in {:?}",
            stations.len(),
            parse_start.elapsed()
        );

        // --- Caching ---
        let cache_start = std::time::Instant::now();
        ensure_cache_dir_exists(cache_path.parent().unwrap()).await?;

        let bincode_data = tokio::task::spawn_blocking({
            let stations_clone = stations.clone();
            let config = bincode_config;
            move || {
                bincode::serde::encode_to_vec(&stations_clone, config).map_err(anyhow::Error::from)
            }
        })
            .await??;

        tokio::fs::write(&cache_path, &bincode_data).await?;
        println!(
            "Serialized and wrote cache ({} bytes) to {} in {:?}",
            bincode_data.len(),
            cache_path.display(),
            cache_start.elapsed()
        );

        Ok(stations)
    }

    // --- Helper functions for Cache Path (from get_stations_gm.rs) ---
    fn get_cache_path() -> Result<PathBuf> {
        get_cache_dir().map(|dir| dir.join(BINCODE_CACHE_FILE_NAME))
    }

    /// Finds the N nearest stations to the given latitude and longitude.
    /// Results are sorted by actual Haversine distance.
    pub fn query(&self, latitude: f64, longitude: f64, n_results: usize) -> Vec<&Station> {
        if n_results == 0 {
            return vec![];
        }
        let query_point = [latitude, longitude];

        // 1. Perform nearest neighbor search using R-Tree (fast, uses squared Euclidean).
        let candidates: Vec<&Station> = self
            .rtree
            .nearest_neighbor_iter(&query_point)
            .take(n_results)
            .collect();

        if candidates.is_empty() {
            return vec![];
        }

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
            .collect();

        // 3. Sort the final small list by the calculated Haversine distance.
        stations_with_dist
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // 4. Extract just the station references, now correctly sorted by Haversine distance.
        stations_with_dist
            .into_iter()
            .map(|(station, _dist)| station)
            .collect()
    }
}
