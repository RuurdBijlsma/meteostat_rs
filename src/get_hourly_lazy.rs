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

/// Returns a LazyFrame for the requested station.
pub async fn get_hourly_lazy(station: &str) -> Result<LazyFrame> {
    println!("[get_hourly_lazy] Station: {}", station);
    let cache_dir = home_dir().expect("Couldn't get home dir").join(".cache");
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
        ParquetWriter::new(file).finish(&mut df.clone())?;
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
        CsvReader::new(std::io::Cursor::new(decompressed))
            .finish()
            .map_err(Into::into)
    })
        .await?
}
