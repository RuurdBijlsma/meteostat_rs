use crate::types::data_source::DataSourceType; // Make sure this is imported
use crate::weather_data::error::WeatherDataError;
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::TryStreamExt;
use log::{info, warn}; // Added info log
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::{fs, task};
use tokio_util::io::StreamReader;

pub struct WeatherDataLoader {
    cache_dir: PathBuf,
    download_client: Client,
}

impl WeatherDataLoader {
    pub fn new(cache_dir: &Path) -> WeatherDataLoader {
        let download_client = Client::new();
        WeatherDataLoader {
            cache_dir: cache_dir.to_path_buf(),
            download_client,
        }
    }

    /// Generic function to load a DataFrame for a given station and data type.
    /// Handles caching and downloading. Returns a LazyFrame with schema-specific column names.

    pub async fn get_frame(
        &self,
        data_type: DataSourceType,
        station: &str,
    ) -> Result<LazyFrame, WeatherDataError> {
        let cache_filename = format!("{}{}.parquet", data_type.cache_file_prefix(), station);
        let parquet_path = self.cache_dir.join(&cache_filename);

        if fs::metadata(&parquet_path).await.is_ok() {
            info!(
                "Cache hit for {} data type for station {} at {:?}",
                data_type, station, parquet_path
            );
        } else {
            warn!(
                "Cache miss for {} data type for station {}. Downloading and processing.",
                data_type, station
            );
            let station_id = station.to_string();

            let raw_bytes = self.download(data_type, &station_id).await?;
            // Make df mutable here
            let mut df = Self::csv_to_dataframe(raw_bytes, &station_id, data_type).await?;

            fs::create_dir_all(&self.cache_dir)
                .await
                .map_err(|e| WeatherDataError::CacheDirCreation(self.cache_dir.clone(), e))?;

            // Pass df by value (ownership moves to cache_dataframe)
            Self::cache_dataframe(df, &parquet_path).await?;
            info!(
                "Cached {} data for station {} to {:?}",
                data_type, station, parquet_path
            );
        }

        LazyFrame::scan_parquet(&parquet_path, Default::default())
            .map_err(|e| WeatherDataError::ParquetScan(parquet_path.clone(), e))
    }

    /// Downloads and decompresses data for a specific type and station.
    async fn download(
        &self,
        data_type: DataSourceType,
        station: &str,
    ) -> Result<Vec<u8>, WeatherDataError> {
        let url = format!(
            "https://bulk.meteostat.net/v2/{}/{}.csv.gz",
            data_type.path_segment(),
            station
        );
        info!("Downloading data from {}", url); // Added log

        let response = self
            .download_client
            .get(&url)
            .send()
            .await
            .map_err(|e| WeatherDataError::NetworkRequest(url.clone(), e))?;

        let response = match response.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
                warn!("HTTP error for {}: {:?}", url, e); // Log error details
                return Err(if let Some(status) = e.status() {
                    WeatherDataError::HttpStatus {
                        url,
                        status,
                        source: e,
                    }
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
        info!(
            "Successfully downloaded and decompressed {} bytes for station {}",
            decompressed.len(),
            station
        );
        Ok(decompressed)
    }

    /// Parses raw CSV bytes (without header) into a DataFrame using a blocking task.
    /// Assigns correct column names based on DataSourceType.
    async fn csv_to_dataframe(
        bytes: Vec<u8>,
        station: &str,
        data_type: DataSourceType,
    ) -> Result<DataFrame, WeatherDataError> {
        let station_owned = station.to_string();
        let schema_names = data_type.get_schema_column_names();

        task::spawn_blocking(move || {
            let mut temp_file = NamedTempFile::new().map_err(|e| WeatherDataError::CsvReadIo {
                station: station_owned.clone(),
                source: e,
            })?;
            temp_file
                .write_all(&bytes)
                .map_err(|e| WeatherDataError::CsvReadIo {
                    station: station_owned.clone(),
                    source: e,
                })?;
            temp_file.flush().map_err(|e| WeatherDataError::CsvReadIo {
                station: station_owned.clone(),
                source: e,
            })?;


            let mut df = CsvReadOptions::default()
                .with_has_header(false)
                .try_into_reader_with_file_path(Some(temp_file.path().to_path_buf()))
                .map_err(|e| WeatherDataError::CsvReadPolars {
                    station: station_owned.clone(),
                    source: e,
                })?
                .finish()
                .map_err(|e| WeatherDataError::CsvReadPolars {
                    station: station_owned.clone(),
                    source: e,
                })?;

            if df.width() != schema_names.len() {
                warn!("CSV column count ({}) does not match schema length ({}) for station {} and type {}", df.width(), schema_names.len(), station_owned, data_type);
                return Err(WeatherDataError::SchemaMismatch {
                    station: station_owned,
                    data_type,
                    expected: schema_names.len(),
                    found: df.width(),
                });
            }

            df.set_column_names(schema_names.iter().copied()) // Corrected this line
                .map_err(|e| WeatherDataError::ColumnRenameError {
                    station: station_owned,
                    source: e,
                })?;

            Ok(df)
        })
            .await?
    }

    /// Writes a DataFrame to a Parquet file asynchronously using spawn_blocking.
    /// Takes a mutable reference if modification might happen (like schema changes),
    /// otherwise an immutable ref is fine. ParquetWriter needs `&mut df`.
    async fn cache_dataframe(mut df: DataFrame, path: &Path) -> Result<(), WeatherDataError> {
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
}
