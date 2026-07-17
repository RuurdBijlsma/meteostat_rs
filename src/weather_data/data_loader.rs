use crate::types::frequency::Frequency;
use crate::weather_data::error::WeatherDataError;
use async_compression::tokio::bufread::GzipDecoder;
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::{fs, task};
use tokio_util::io::StreamReader;

#[derive(Debug, Clone)]
pub struct WeatherDataLoader {
    cache_dir: PathBuf,
    download_client: Client,
}

impl WeatherDataLoader {
    pub fn new(cache_dir: &Path) -> Self {
        let download_client = Client::new();
        Self {
            cache_dir: cache_dir.to_path_buf(),
            download_client,
        }
    }

    /// Gets the last modification time of the cached Parquet file for a given
    /// station and frequency.
    ///
    /// This function is cross-platform.
    ///
    /// # Arguments
    ///
    /// * `station` - The ID of the station.
    /// * `frequency` - The data frequency.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(NaiveDateTime))` - If the cache file exists and its modification
    ///   time could be determined (returned as UTC).
    /// * `Ok(None)` - If the cache file does not exist.
    /// * `Err(WeatherDataError)` - For I/O errors reading metadata.
    pub async fn get_cache_modification_time(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<Option<DateTime<Utc>>, WeatherDataError> {
        let cache_filename = format!("{}{}.parquet", frequency.cache_file_prefix(), station);
        let parquet_path = self.cache_dir.join(&cache_filename);

        match fs::metadata(&parquet_path).await {
            Ok(metadata) => {
                // File exists, try to get modification time
                let modified_system_time = metadata
                    .modified()
                    .map_err(|e| WeatherDataError::CacheMetadataRead(parquet_path.clone(), e))?;

                // Convert SystemTime to chrono::DateTime<Utc>
                let modified_datetime_utc: DateTime<Utc> = DateTime::from(modified_system_time);

                Ok(Some(modified_datetime_utc))
            }
            Err(io_err) => {
                if io_err.kind() == std::io::ErrorKind::NotFound {
                    // File doesn't exist, this is a normal cache miss scenario
                    Ok(None)
                } else {
                    // Other error accessing metadata (permissions, etc.)
                    Err(WeatherDataError::CacheMetadataRead(parquet_path, io_err))
                }
            }
        }
    }

    /// Generic function to load a `DataFrame` for a given station and data type.
    /// Handles caching and downloading. Returns a `LazyFrame` with schema-specific column names and types.
    pub async fn get_frame(
        &self,
        data_type: Frequency,
        station: &str,
    ) -> Result<LazyFrame, WeatherDataError> {
        let cache_filename = format!("{}{}.parquet", data_type.cache_file_prefix(), station);
        let parquet_path = self.cache_dir.join(&cache_filename);

        if fs::metadata(&parquet_path).await.is_err() {
            let station_id = station.to_string();

            let raw_bytes = self.download(data_type, &station_id).await?;
            let df = Self::csv_to_dataframe(raw_bytes, &station_id, data_type).await?;

            fs::create_dir_all(&self.cache_dir)
                .await
                .map_err(|e| WeatherDataError::CacheDirCreation(self.cache_dir.clone(), e))?;

            // Pass df by value (ownership moves to cache_dataframe)
            Self::cache_dataframe(df, &parquet_path).await?;
        }

        let pl_path = PlRefPath::try_from_path(&parquet_path)
            .map_err(|e| WeatherDataError::ParquetScan(parquet_path.clone(), e))?;

        LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
            .map_err(|e| WeatherDataError::ParquetScan(parquet_path, e))
    }

    /// Downloads and decompresses data for a specific type and station.
    async fn download(
        &self,
        data_type: Frequency,
        station: &str,
    ) -> Result<Vec<u8>, WeatherDataError> {
        let url = format!(
            "https://bulk.meteostat.net/v2/{}/{}.csv.gz",
            data_type.path_segment(),
            station
        );

        let response = self
            .download_client
            .get(&url)
            .send()
            .await
            .map_err(|e| WeatherDataError::NetworkRequest(url.clone(), e))?;

        let response = match response.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
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

        let stream = response.bytes_stream().map_err(std::io::Error::other);
        let stream_reader = StreamReader::new(stream);
        let mut decoder = GzipDecoder::new(stream_reader);
        let mut decompressed = Vec::new();
        // Map IO error during decompression
        decoder
            .read_to_end(&mut decompressed)
            .await
            .map_err(WeatherDataError::DownloadIo)?;
        Ok(decompressed)
    }

    /// Parses raw CSV bytes (without header) into a `DataFrame` using a blocking task.
    /// Assigns correct column names and casts columns to appropriate data types based on Frequency.
    async fn csv_to_dataframe(
        bytes: Vec<u8>,
        station: &str,
        data_type: Frequency,
    ) -> Result<DataFrame, WeatherDataError> {
        let station_owned = station.to_string();

        task::spawn_blocking(move || {
            // Build the static schema for parsing the CSV columns directly to native types
            let schema = match data_type {
                Frequency::Hourly => Schema::from_iter(vec![
                    Field::new("date".into(), DataType::String),
                    Field::new("hour".into(), DataType::Int64),
                    Field::new("temp".into(), DataType::Float64),
                    Field::new("dwpt".into(), DataType::Float64),
                    Field::new("rhum".into(), DataType::Int64),
                    Field::new("prcp".into(), DataType::Float64),
                    Field::new("snow".into(), DataType::Int64),
                    Field::new("wdir".into(), DataType::Int64),
                    Field::new("wspd".into(), DataType::Float64),
                    Field::new("wpgt".into(), DataType::Float64),
                    Field::new("pres".into(), DataType::Float64),
                    Field::new("tsun".into(), DataType::Int64),
                    Field::new("coco".into(), DataType::Int64),
                ]),
                Frequency::Daily => Schema::from_iter(vec![
                    Field::new("date".into(), DataType::String),
                    Field::new("tavg".into(), DataType::Float64),
                    Field::new("tmin".into(), DataType::Float64),
                    Field::new("tmax".into(), DataType::Float64),
                    Field::new("prcp".into(), DataType::Float64),
                    Field::new("snow".into(), DataType::Int64),
                    Field::new("wdir".into(), DataType::Int64),
                    Field::new("wspd".into(), DataType::Float64),
                    Field::new("wpgt".into(), DataType::Float64),
                    Field::new("pres".into(), DataType::Float64),
                    Field::new("tsun".into(), DataType::Int64),
                ]),
                Frequency::Monthly => Schema::from_iter(vec![
                    Field::new("year".into(), DataType::Int64),
                    Field::new("month".into(), DataType::Int64),
                    Field::new("tavg".into(), DataType::Float64),
                    Field::new("tmin".into(), DataType::Float64),
                    Field::new("tmax".into(), DataType::Float64),
                    Field::new("prcp".into(), DataType::Float64),
                    Field::new("wspd".into(), DataType::Float64),
                    Field::new("pres".into(), DataType::Float64),
                    Field::new("tsun".into(), DataType::Int64),
                ]),
                Frequency::Climate => Schema::from_iter(vec![
                    Field::new("start_year".into(), DataType::Int64),
                    Field::new("end_year".into(), DataType::Int64),
                    Field::new("month".into(), DataType::Int64),
                    Field::new("tmin".into(), DataType::Float64),
                    Field::new("tmax".into(), DataType::Float64),
                    Field::new("prcp".into(), DataType::Float64),
                    Field::new("wspd".into(), DataType::Float64),
                    Field::new("pres".into(), DataType::Float64),
                    Field::new("tsun".into(), DataType::Int64),
                ]),
            };
            let schema_len = schema.len();
            let schema_ref: SchemaRef = Arc::new(schema);

            // Read the initial DataFrame directly from memory with schema
            let df = CsvReadOptions::default()
                .with_has_header(false)
                .with_schema(Some(schema_ref))
                .into_reader_with_file_handle(Cursor::new(bytes))
                .finish()
                .map_err(|e| WeatherDataError::CsvReadPolars {
                    station: station_owned.clone(),
                    source: e,
                })?;

            if df.width() != schema_len {
                return Err(WeatherDataError::SchemaMismatch {
                    station: station_owned,
                    data_type,
                    expected: schema_len,
                    found: df.width(),
                });
            }

            // --- Type Casting and Pre-computation ---
            let mut lazy_df = df.lazy();

            // Common strptime options
            let date_options = StrptimeOptions {
                format: Some("%Y-%m-%d".into()),
                strict: false,
                exact: true,
                cache: true,
            };

            // Apply type parsing logic for date/datetime columns
            lazy_df = match data_type {
                Frequency::Hourly => {
                    lazy_df.with_columns([
                        // Create datetime first from string date and i64 hour
                        (col("date")
                            .str()
                            .strptime(DataType::Date, date_options.clone(), lit("raise"))
                            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                            + duration(DurationArgs::new().with_hours(col("hour"))))
                        .alias("datetime"),
                    ])
                }
                Frequency::Daily => {
                    lazy_df.with_columns([
                        // Parse date string to Date type
                        col("date")
                            .str()
                            .strptime(DataType::Date, date_options, lit("raise"))
                            .alias("date"),
                    ])
                }
                Frequency::Monthly | Frequency::Climate => {
                    // Already parsed natively in CsvReadOptions schema configuration
                    lazy_df
                }
            };

            // Collect the lazy frame to apply transformations and handle potential errors
            let typed_df =
                lazy_df
                    .collect()
                    .map_err(|e| WeatherDataError::ColumnOperationError {
                        station: station_owned.clone(),
                        source: e,
                    })?;

            Ok(typed_df)
        })
        .await?
    }

    /// Writes a `DataFrame` to a Parquet file atomically using a temporary file.
    async fn cache_dataframe(mut df: DataFrame, path: &Path) -> Result<(), WeatherDataError> {
        let path_buf = path.to_path_buf();
        task::spawn_blocking(move || {
            let parent = path_buf.parent().ok_or_else(|| {
                WeatherDataError::ParquetWriteIo(
                    path_buf.clone(),
                    std::io::Error::new(std::io::ErrorKind::NotFound, "No parent directory"),
                )
            })?;
            if path_buf.exists() {
                return Ok::<(), WeatherDataError>(());
            }
            let mut temp_file = NamedTempFile::new_in(parent)
                .map_err(|e| WeatherDataError::ParquetWriteIo(path_buf.clone(), e))?;
            ParquetWriter::new(&mut temp_file)
                .with_compression(ParquetCompression::Snappy)
                .finish(&mut df)
                .map_err(|e| WeatherDataError::ParquetWritePolars(path_buf.clone(), e))?;
            if path_buf.exists() {
                return Ok::<(), WeatherDataError>(());
            }
            if let Err(err) = temp_file.persist(&path_buf) {
                if path_buf.exists() {
                    return Ok::<(), WeatherDataError>(());
                }
                return Err(WeatherDataError::ParquetWriteIo(
                    path_buf.clone(),
                    err.error,
                ));
            }
            Ok::<(), WeatherDataError>(())
        })
        .await??;
        Ok(())
    }
}
