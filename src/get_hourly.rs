use anyhow::Context;
use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use dirs::home_dir;
use futures_util::TryStreamExt;
use polars::prelude::*;
use reqwest::Client;
use std::path::Path;
use tokio::io::{self, AsyncReadExt};
use tokio::{fs, task};
use tokio_util::io::StreamReader;


pub async fn get_hourly(station: &str) -> Result<DataFrame> {
    println!("[get_hourly] Station: {}", station);
    let cache_dir = home_dir().expect("Couldn't get home dir").join(".cache");
    let parquet_path = cache_dir.join(format!("hourly-{}.parquet", station));

    // Check if cached file exists
    if fs::metadata(&parquet_path).await.is_ok() {
        return get_hourly_from_cache(station, &cache_dir).await;
    }

    // If not, download and cache
    let df = download_hourly(station).await?;

    // Ensure cache directory exists
    fs::create_dir_all(&cache_dir).await?;

    // Write to cache
    write_parquet(df.clone(), &parquet_path).await?;

    Ok(df)
}

async fn write_parquet(df: DataFrame, path: &Path) -> Result<()> {
    println!("Writing to: {}", path.display());
    let path = path.to_path_buf();
    task::spawn_blocking(move || {
        let file = std::fs::File::create(&path)?;
        ParquetWriter::new(file).finish(&mut df.clone())?;
        Ok(())
    }).await?
}

pub async fn get_hourly_from_cache(station: &str, cache_dir: &Path) -> Result<DataFrame> {
    println!("Getting hourly from cache: {}", station);
    let parquet_path = cache_dir.join(format!("hourly-{}.parquet", station));

    task::spawn_blocking(move || {
        let file = std::fs::File::open(&parquet_path)
            .with_context(|| format!("Failed to open parquet file at {:?}", parquet_path))?;

        ParquetReader::new(file)
            .finish()
            .with_context(|| format!("Failed to read parquet file at {:?}", parquet_path))
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
        CsvReader::new(std::io::Cursor::new(decompressed))
            .finish()
            .map_err(Into::into)
    }).await?
}
