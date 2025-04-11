use anyhow::Result;
use async_compression::tokio::bufread::GzipDecoder;
use futures_util::StreamExt;
use reqwest::Client;
use tokio::io::{self, AsyncReadExt};
use tokio_util::io::StreamReader;

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Station {
    id: String,
    country: Option<String>,
    region: Option<String>,
    timezone: Option<String>,
    name: Option<HashMap<String, String>>,
    identifiers: Option<Identifiers>,
    location: Option<Location>,
    #[serde(default)]
    inventory: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct Identifiers {
    national: Option<String>,
    wmo: Option<String>,
    icao: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Location {
    latitude: Option<f64>,
    longitude: Option<f64>,
    elevation: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = "https://bulk.meteostat.net/v2/stations/lite.json.gz";
    println!("Fetching data from: {}", url);

    let client = Client::new();
    let response = client.get(url).send().await?.error_for_status()?;
    let stream = response.bytes_stream()
        .map(|result| result.map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
    let stream_reader = StreamReader::new(stream);
    let gzip_decoder = GzipDecoder::new(stream_reader);
    let mut decompressed = Vec::new();
    let mut decoder_reader = io::BufReader::new(gzip_decoder);
    decoder_reader.read_to_end(&mut decompressed).await?;
    let stations: Vec<Station> = serde_json::from_slice(&decompressed)?;
    println!("Successfully loaded {} stations", stations.len());

    Ok(())
}