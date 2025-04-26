use crate::types::data_source::Frequency;
use crate::weather_data::error::WeatherDataError;
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::TryStreamExt;
use log::{debug, info, warn};
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use std::io::Write;
use std::path::{Path, PathBuf};
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
    pub fn new(cache_dir: &Path) -> WeatherDataLoader {
        let download_client = Client::new();
        WeatherDataLoader {
            cache_dir: cache_dir.to_path_buf(),
            download_client,
        }
    }

    /// Generic function to load a DataFrame for a given station and data type.
    /// Handles caching and downloading. Returns a LazyFrame with schema-specific column names and types.
    pub async fn get_frame(
        &self,
        data_type: Frequency,
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
            let df = Self::csv_to_dataframe(raw_bytes, &station_id, data_type).await?;

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
        data_type: Frequency,
        station: &str,
    ) -> Result<Vec<u8>, WeatherDataError> {
        let url = format!(
            "https://bulk.meteostat.net/v2/{}/{}.csv.gz",
            data_type.path_segment(),
            station
        );
        info!("Downloading data from {}", url);

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
            .map_err(WeatherDataError::DownloadIo)?;
        info!(
            "Successfully downloaded and decompressed {} bytes for station {}",
            decompressed.len(),
            station
        );
        Ok(decompressed)
    }

    /// Parses raw CSV bytes (without header) into a DataFrame using a blocking task.
    /// Assigns correct column names and casts columns to appropriate data types based on Frequency.
    async fn csv_to_dataframe(
        bytes: Vec<u8>,
        station: &str,
        data_type: Frequency,
    ) -> Result<DataFrame, WeatherDataError> {
        let station_owned = station.to_string();
        let schema_names = data_type.get_schema_column_names(); // Original CSV schema

        task::spawn_blocking(move || {
            let mut temp_file = NamedTempFile::new().map_err(|e| WeatherDataError::CsvReadIo {
                station: station_owned.clone(),
                source: e,
            })?;
            temp_file.write_all(&bytes).map_err(|e| WeatherDataError::CsvReadIo {
                station: station_owned.clone(),
                source: e,
            })?;
            temp_file.flush().map_err(|e| WeatherDataError::CsvReadIo {
                station: station_owned.clone(),
                source: e,
            })?;

            // Read the initial DataFrame - use infer_schema_length(0) to read all as Utf8 first
            let mut df = CsvReadOptions::default()
                .with_has_header(false)
                .with_infer_schema_length(Some(0)) // Read all as Utf8 initially for robust parsing/casting
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

            df.set_column_names(schema_names.iter().copied())
                .map_err(|e| WeatherDataError::ColumnRenameError {
                    station: station_owned.clone(),
                    source: e,
                })?;

            debug!("DataFrame columns renamed for station {}: {:?}", station_owned, df.get_column_names());


            // --- START Type Casting and Pre-computation ---
            let mut lazy_df = df.lazy();

            // Common strptime options
            let date_options = StrptimeOptions {
                format: Some("%Y-%m-%d".into()),
                strict: false, // Be slightly lenient with parsing if needed
                exact: true,
                cache: true,
            };

            // Apply type casting based on frequency using with_columns for efficiency
            lazy_df = match data_type {
                Frequency::Hourly => {
                    // Hourly logic remains the same...
                    lazy_df.with_columns([
                        // Create datetime first from string date and i64 hour
                        (
                            col("date").str().strptime(DataType::Date, date_options.clone(), lit("raise"))
                                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                                + duration(DurationArgs::new().with_hours(col("hour").cast(DataType::Int64)))
                        ).alias("datetime"),
                        // Cast numerical columns
                        col("date").cast(DataType::String),
                        col("hour").cast(DataType::Int64),
                        col("temp").cast(DataType::Float64),
                        col("dwpt").cast(DataType::Float64),
                        col("rhum").cast(DataType::Int64), // integer percentage
                        col("prcp").cast(DataType::Float64),
                        col("snow").cast(DataType::Int64), 
                        col("wdir").cast(DataType::Int64), // Degrees
                        col("wspd").cast(DataType::Float64),
                        col("wpgt").cast(DataType::Float64),
                        col("pres").cast(DataType::Float64),
                        col("tsun").cast(DataType::Int64), // minutes
                        col("coco").cast(DataType::Int64), // Weather condition code
                    ])
                },
                Frequency::Daily => {
                    // Daily logic remains the same...
                    lazy_df.with_columns([
                        // Parse date string to Date type
                        col("date").str().strptime(DataType::Date, date_options.clone(), lit("raise"))
                            .alias("date"), // Overwrite original string date column
                        // Cast numerical columns
                        col("tavg").cast(DataType::Float64),
                        col("tmin").cast(DataType::Float64),
                        col("tmax").cast(DataType::Float64),
                        col("prcp").cast(DataType::Float64),
                        col("snow").cast(DataType::Int64),
                        col("wdir").cast(DataType::Int64),
                        col("wspd").cast(DataType::Float64),
                        col("wpgt").cast(DataType::Float64),
                        col("pres").cast(DataType::Float64),
                        col("tsun").cast(DataType::Int64),
                    ])
                },
                Frequency::Monthly => {
                    lazy_df.with_columns([
                        // Cast year and month first
                        col("year").cast(DataType::Int64),
                        col("month").cast(DataType::Int64),
                        // Cast numerical columns
                        col("tavg").cast(DataType::Float64),
                        col("tmin").cast(DataType::Float64),
                        col("tmax").cast(DataType::Float64),
                        col("prcp").cast(DataType::Float64),
                        col("wspd").cast(DataType::Float64),
                        col("pres").cast(DataType::Float64),
                        col("tsun").cast(DataType::Int64),
                    ])
                },
                Frequency::Climate => {
                    // Climate logic remains the same...
                    lazy_df.with_columns([
                        // Cast year and month
                        col("start_year").cast(DataType::Int64),
                        col("end_year").cast(DataType::Int64),
                        col("month").cast(DataType::Int64),
                        // Cast numerical columns
                        col("tmin").cast(DataType::Float64),
                        col("tmax").cast(DataType::Float64),
                        col("prcp").cast(DataType::Float64),
                        col("wspd").cast(DataType::Float64),
                        col("pres").cast(DataType::Float64),
                        col("tsun").cast(DataType::Int64),
                    ])
                },
            };

            // Collect the lazy frame to apply transformations and handle potential errors
            let typed_df = lazy_df.collect().map_err(|e| WeatherDataError::ColumnOperationError {
                station: station_owned.clone(),
                source: e,
            })?;

            info!("Successfully applied data types for {} data for station {}", data_type, station_owned);
            debug!("Final schema for station {}: {:?}", station_owned, typed_df.schema());

            Ok(typed_df) // Return the transformed DataFrame
        })
            .await? // Unwrap the JoinError
                    // Propagate the inner Result<DataFrame, WeatherDataError>
    }

    /// Writes a DataFrame to a Parquet file asynchronously using spawn_blocking.
    async fn cache_dataframe(mut df: DataFrame, path: &Path) -> Result<(), WeatherDataError> {
        let path_buf = path.to_path_buf();
        task::spawn_blocking(move || {
            let file = std::fs::File::create(&path_buf)
                .map_err(|e| WeatherDataError::ParquetWriteIo(path_buf.clone(), e))?;
            ParquetWriter::new(file)
                .with_compression(ParquetCompression::Snappy) // Snappy is generally a good balance
                .finish(&mut df) // finish consumes df if mutable, or takes &mut df
                .map_err(|e| WeatherDataError::ParquetWritePolars(path_buf, e))?;
            Ok::<(), WeatherDataError>(())
        })
        .await??; // Unwrap JoinError, then unwrap the inner Result
        Ok(())
    }
}
