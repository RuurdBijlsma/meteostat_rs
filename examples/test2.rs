use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::TryStreamExt;
use polars::prelude::*;
use reqwest::Client;
use std::path::PathBuf;
use tempfile::NamedTempFile;
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio_util::io::StreamReader;

#[tokio::main]
async fn main() -> Result<()> {
    let url = "https://bulk.meteostat.net/v2/hourly/10637.csv.gz";
    let client = Client::new();
    let response = client.get(url).send().await?;
    let stream = response.bytes_stream();
    let stream_reader =
        StreamReader::new(stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let mut decoder = GzipDecoder::new(BufReader::new(stream_reader));
    let tmpfile = NamedTempFile::new()?;
    let tmp_path: PathBuf = tmpfile.path().to_path_buf();
    let mut async_file = tokio::fs::File::from_std(tmpfile.reopen()?);
    io::copy(&mut decoder, &mut async_file).await?;
    async_file.flush().await?;
    let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
        let file = std::fs::File::open(&tmp_path)?;
        let df = CsvReader::new(file).finish()?;
        Ok(df)
    })
    .await??;
    println!("DataFrame shape: {:?}", df.shape());
    Ok(())
}
