use chrono::{DateTime, Utc};
use meteostat::{Frequency, LatLon, Meteostat, MeteostatError, MeteostatFrameFilterExt};
use polars::prelude::*;
use std::env;
use std::str::FromStr;
// For parsing DateTime<Utc>

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    configure_polars_display();
    // Initialize the client (uses default cache directory)
    let client = Meteostat::new().await?;

    // --- Example: Get data for a location ---
    let berlin_center = LatLon(52.52, 13.40);
    let hourly_lazy = client
        .from_location()
        .location(berlin_center)
        .frequency(Frequency::Hourly)
        .call()
        .await?;

    // Filter for a specific date
    let start_datetime = DateTime::<Utc>::from_str("2022-01-10T00:00:00Z").unwrap(); // Jan 10 2022 00:00:00 UTC
    let end_datetime = DateTime::<Utc>::from_str("2022-01-10T23:59:59Z").unwrap(); // Jan 10 2022 23:59:59 UTC
    let specific_day_hourly = hourly_lazy
        .filter_hourly(start_datetime, end_datetime)
        .collect()?;

    println!(
        "Hourly data near Berlin for 2022-01-10:\n{}",
        specific_day_hourly.head(Some(5))
    );
    Ok(())
}

fn configure_polars_display() {
    // show every column
    env::set_var("POLARS_FMT_MAX_COLS", "-1");
    // show 20 rows
    env::set_var("POLARS_FMT_MAX_ROWS", "20");
}
