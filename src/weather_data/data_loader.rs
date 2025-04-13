use crate::utils::get_cache_dir;
use crate::weather_data::weather_data_error::{WeatherDataError};
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::TryStreamExt;
use log::warn;
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client; // Consider making client reusable
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::{fs, task};
use tokio_util::io::StreamReader;
use crate::types::data_source::DataSourceType;

/// Generic function to load a DataFrame for a given station and data type.
/// Handles caching and downloading. Returns a LazyFrame with generic column names.
pub async fn load_dataframe_for_station(
    data_type: DataSourceType,
    station: &str,
) -> Result<LazyFrame, WeatherDataError> {
    let cache_dir = get_cache_dir().map_err(WeatherDataError::CacheDirResolution)?;
    let cache_filename = format!("{}{}.parquet", data_type.cache_file_prefix(), station);
    let parquet_path = cache_dir.join(cache_filename);
    let parquet_path_clone = parquet_path.clone();

    // Check cache
    if fs::metadata(&parquet_path)
        .await
        .map_err(|e| WeatherDataError::CacheMetadataRead(parquet_path_clone, e))
        .is_err()
    {
        warn!(
            "Cache miss for {} data type for station {}. Downloading.",
            data_type, station
        );
        let station_id = station.to_string(); // Clone for download function

        // Download, parse, and write to cache
        // Consider creating a single shared reqwest::Client instance for efficiency
        let client = Client::new();
        let raw_bytes = download_gzipped_csv(&client, data_type, &station_id).await?;
        let df = parse_csv_bytes(raw_bytes, &station_id).await?;

        fs::create_dir_all(&cache_dir)
            .await
            .map_err(|e| WeatherDataError::CacheDirCreation(cache_dir.clone(), e))?;

        write_dataframe_to_parquet(df, &parquet_path).await?;
    }

    // Scan the parquet file
    LazyFrame::scan_parquet(&parquet_path, Default::default())
        .map_err(|e| WeatherDataError::ParquetScan(parquet_path, e))
}

/// Downloads and decompresses data for a specific type and station.
async fn download_gzipped_csv(
    client: &Client,
    data_type: DataSourceType,
    station: &str,
) -> Result<Vec<u8>, WeatherDataError> {
    let url = format!(
        "https://bulk.meteostat.net/v2/{}/{}.csv.gz",
        data_type.path_segment(),
        station
    );

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| WeatherDataError::NetworkRequest(url.clone(), e))?;

    let response = match response.error_for_status() {
        Ok(resp) => resp,
        Err(e) => {
            return Err(if let Some(status) = e.status() {
                WeatherDataError::HttpStatus { url, status, source: e }
            } else {
                WeatherDataError::NetworkRequest(url, e)
            });
        }
    };

    let stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(stream);
    let mut decoder = GzipDecoder::new(stream_reader);
    let mut decompressed = Vec::new();
    // Map IO error during decompression
    decoder
        .read_to_end(&mut decompressed)
        .await
        .map_err(|e| WeatherDataError::DownloadIo(e))?;

    Ok(decompressed)
}

/// Parses raw CSV bytes (without header) into a DataFrame using a blocking task.
async fn parse_csv_bytes(bytes: Vec<u8>, station: &str) -> Result<DataFrame, WeatherDataError> {
    let station_owned = station.to_string();
    task::spawn_blocking(move || {
        let mut temp_file = NamedTempFile::new().map_err(|e| WeatherDataError::CsvReadIo {
            station: station_owned.clone(),
            source: e,
        })?;
        temp_file.write_all(&bytes).map_err(|e| WeatherDataError::CsvReadIo {
            station: station_owned.clone(),
            source: e,
        })?;

        let df = CsvReadOptions::default()
            .with_has_header(false)
            .try_into_reader_with_file_path(Some(temp_file.path().to_path_buf()))
            .map_err(|e| WeatherDataError::CsvReadPolars {
                station: station_owned.clone(),
                source: e,
            })?
            .finish()
            .map_err(|e| WeatherDataError::CsvReadPolars {
                station: station_owned,
                source: e,
            })?;
        Ok(df)
    })
        .await?
}

/// Writes a DataFrame to a Parquet file asynchronously using spawn_blocking.
async fn write_dataframe_to_parquet(mut df: DataFrame, path: &Path) -> Result<(), WeatherDataError> {
    let path_buf = path.to_path_buf();
    task::spawn_blocking(move || {
        let file = std::fs::File::create(&path_buf)
            .map_err(|e| WeatherDataError::ParquetWriteIo(path_buf.clone(), e))?;
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df)
            .map_err(|e| WeatherDataError::ParquetWritePolars(path_buf, e))?;
        Ok::<(), WeatherDataError>(())
    })
        .await??;
    Ok(())
}