use chrono::{TimeZone, Utc};
use meteostat::{
    Frequency, LatLon, Meteostat, MeteostatError, MeteostatFrameFilterExt, RequiredData,
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    configure_polars_display();

    let meteostat = Meteostat::new().await?;
    let start_utc = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end_utc = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

    let weather_data = meteostat
        .from_location()
        .location(LatLon(30.092355, -97.829935))
        .frequency(Frequency::Hourly)
        .required_data(RequiredData::DateRange {
            start: start_utc.date_naive(),
            end: end_utc.date_naive(),
        }) // Optional: ensures only stations with data for the given date range are used.
        .call()
        .await?
        .filter_hourly(start_utc, end_utc)
        .collect()?;

    println!("{}", weather_data);

    Ok(())
}

fn configure_polars_display() {
    // show every column
    env::set_var("POLARS_FMT_MAX_COLS", "-1");
    // show 20 rows
    env::set_var("POLARS_FMT_MAX_ROWS", "20");
}
