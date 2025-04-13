use crate::utils::get_cache_dir;
use anyhow::Context;
use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use futures_util::TryStreamExt;
use polars::frame::DataFrame;
use polars::prelude::*;
use reqwest::Client;
use std::convert::TryInto;
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio::io::{self, AsyncReadExt};
use tokio::{fs, task};
use tokio_util::io::StreamReader;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum WeatherCondition {
    Clear = 1,
    Fair = 2,
    Cloudy = 3,
    Overcast = 4,
    Fog = 5,
    FreezingFog = 6,
    LightRain = 7,
    Rain = 8,
    HeavyRain = 9,
    FreezingRain = 10,
    HeavyFreezingRain = 11,
    Sleet = 12,
    HeavySleet = 13,
    LightSnowfall = 14,
    Snowfall = 15,
    HeavySnowfall = 16,
    RainShower = 17,
    HeavyRainShower = 18,
    SleetShower = 19,
    HeavySleetShower = 20,
    SnowShower = 21,
    HeavySnowShower = 22,
    Lightning = 23,
    Hail = 24,
    Thunderstorm = 25,
    HeavyThunderstorm = 26,
    Storm = 27,
}

impl WeatherCondition {
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(WeatherCondition::Clear),
            2 => Some(WeatherCondition::Fair),
            3 => Some(WeatherCondition::Cloudy),
            4 => Some(WeatherCondition::Overcast),
            5 => Some(WeatherCondition::Fog),
            6 => Some(WeatherCondition::FreezingFog),
            7 => Some(WeatherCondition::LightRain),
            8 => Some(WeatherCondition::Rain),
            9 => Some(WeatherCondition::HeavyRain),
            10 => Some(WeatherCondition::FreezingRain),
            11 => Some(WeatherCondition::HeavyFreezingRain),
            12 => Some(WeatherCondition::Sleet),
            13 => Some(WeatherCondition::HeavySleet),
            14 => Some(WeatherCondition::LightSnowfall),
            15 => Some(WeatherCondition::Snowfall),
            16 => Some(WeatherCondition::HeavySnowfall),
            17 => Some(WeatherCondition::RainShower),
            18 => Some(WeatherCondition::HeavyRainShower),
            19 => Some(WeatherCondition::SleetShower),
            20 => Some(WeatherCondition::HeavySleetShower),
            21 => Some(WeatherCondition::SnowShower),
            22 => Some(WeatherCondition::HeavySnowShower),
            23 => Some(WeatherCondition::Lightning),
            24 => Some(WeatherCondition::Hail),
            25 => Some(WeatherCondition::Thunderstorm),
            26 => Some(WeatherCondition::HeavyThunderstorm),
            27 => Some(WeatherCondition::Storm),
            _ => None, // Return None for invalid values
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct WeatherInfo {
    pub date: NaiveDate,
    pub hour: u32,
    pub temperature: Option<f64>,
    pub dew_point: Option<f64>,
    pub relative_humidity: Option<i32>,
    pub precipitation: Option<f64>,
    pub snow: Option<i32>,
    pub wind_direction: Option<i32>,
    pub wind_speed: Option<f64>,
    pub peak_wind_gust: Option<f64>,
    pub pressure: Option<f64>,
    pub sunshine: Option<u32>,
    pub condition: Option<WeatherCondition>,
}

fn get_opt_int<T>(series: &Column, idx: usize) -> Option<T>
where
    T: TryFrom<i64>,
{
    series
        .i64()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|val| val.try_into().ok())
}

fn get_opt_float(series: &Column, idx: usize) -> Option<f64> {
    series.f64().ok().and_then(|ca| ca.get(idx))
}

fn get_opt_condition(series: &Column, idx: usize) -> Option<WeatherCondition> {
    let from_str = series
        .str()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(WeatherCondition::from_i64);

    if from_str.is_some() {
        return from_str;
    }

    get_opt_int::<i64>(series, idx).and_then(WeatherCondition::from_i64)
}

pub async fn get_hourly_from_station(
    station: &str,
    datetime: DateTime<Utc>,
) -> Option<WeatherInfo> {
    let df = get_hourly_lazy(station).await.ok();
    if df.is_none() {
        return None;
    }
    get_hourly_from_df(df.unwrap(), datetime)
}

pub fn get_hourly_from_df(df: LazyFrame, datetime: DateTime<Utc>) -> Option<WeatherInfo> {
    let date_naive = datetime.date_naive();
    let date_string = date_naive.format("%Y-%m-%d").to_string();
    let hour_u32 = datetime.hour();
    let hour_i64 = hour_u32 as i64;

    let filtered_lazy = df
        .filter(col("column_1").eq(lit(date_string))) // Filter on Date type
        .filter(col("column_2").eq(lit(hour_i64))); // Filter on Int64 type

    // --- Add Explain ---
    // match filtered_lazy.explain(true) { // Get optimized plan
    //     Ok(plan_str) => eprintln!("Optimized Plan:\n{}", plan_str),
    //     Err(e) => eprintln!("Failed to get plan: {}", e),
    // }
    // --- End Explain ---

    let filtered = filtered_lazy.collect(); // Now collect
                                            // dbg!(&filtered);
    if filtered.is_err() {
        eprintln!("FILTERED ERR");
        dbg!(&filtered);
        return None;
    }
    let filtered = filtered.unwrap();

    if filtered.height() != 1 {
        return None;
    }

    let temperature = get_opt_float(filtered.column("column_3").ok()?, 0);
    let dew_point = get_opt_float(filtered.column("column_4").ok()?, 0);

    let relative_humidity = get_opt_int::<i32>(filtered.column("column_5").ok()?, 0);

    let precipitation = get_opt_float(filtered.column("column_6").ok()?, 0);

    let snow = get_opt_int::<i32>(filtered.column("column_7").ok()?, 0);

    let wind_direction = get_opt_int::<i32>(filtered.column("column_8").ok()?, 0);

    let wind_speed = get_opt_float(filtered.column("column_9").ok()?, 0);

    let peak_wind_gust = get_opt_float(filtered.column("column_10").ok()?, 0);

    let pressure = get_opt_float(filtered.column("column_11").ok()?, 0);

    let sunshine = get_opt_int::<u32>(filtered.column("column_12").ok()?, 0);

    let condition = get_opt_condition(filtered.column("column_13").ok()?, 0);

    Some(WeatherInfo {
        date: date_naive,
        hour: hour_u32,
        temperature,
        dew_point,
        relative_humidity,
        precipitation,
        snow,
        wind_direction,
        wind_speed,
        peak_wind_gust,
        pressure,
        sunshine,
        condition,
    })
}

pub async fn get_hourly_lazy(station: &str) -> Result<LazyFrame> {
    let cache_dir = get_cache_dir()?;
    let parquet_path = cache_dir.join(format!("hourly-{}.parquet", station));

    // Check if cached file exists
    if fs::metadata(&parquet_path).await.is_err() {
        // If not, download and cache
        let df = download_hourly(station).await?;

        // Ensure cache directory exists
        fs::create_dir_all(&cache_dir).await?;

        // Write to cache
        write_parquet(df, &parquet_path).await?;
    }

    // Return LazyFrame directly from Parquet
    let path_str = parquet_path.to_str().unwrap();
    let lf = LazyFrame::scan_parquet(path_str, Default::default())
        .with_context(|| format!("Failed to scan parquet file at {:?}", parquet_path))?;

    Ok(lf)
}

async fn write_parquet(df: DataFrame, path: &Path) -> Result<()> {
    println!("Writing to: {}", path.display());
    let path = path.to_path_buf();
    task::spawn_blocking(move || {
        let file = std::fs::File::create(&path)?;
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Snappy)
            .finish(&mut df.clone())?;
        Ok(())
    })
    .await?
}

async fn download_hourly(station: &str) -> Result<DataFrame> {
    println!("Downloading hourly: {}", station);
    let url = format!("https://bulk.meteostat.net/v2/hourly/{}.csv.gz", station);
    let response = Client::new().get(&url).send().await?;

    let stream = response
        .bytes_stream()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e));
    let decoder = GzipDecoder::new(StreamReader::new(stream));

    let mut decompressed = Vec::new();
    decoder
        .take(1024 * 1024 * 1024) // 1GB safety limit
        .read_to_end(&mut decompressed)
        .await?;

    task::spawn_blocking(move || {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(&decompressed)?;
        let start = Instant::now();

        let res = CsvReadOptions::default()
            .with_has_header(false)
            .try_into_reader_with_file_path(Some(temp_file.path().into()))?
            .finish()?;

        let duration = start.elapsed();
        println!("Time elapsed: {:?}", duration);

        Ok(res)
    })
    .await?
}
