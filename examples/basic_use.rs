use chrono::{TimeZone, Utc};
use meteostat::{LatLon, Meteostat, MeteostatError, RequiredData};
use std::env;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    configure_polars_display();
    let client = Meteostat::new().await?;
    let datetime = Utc.with_ymd_and_hms(2025, 1, 10, 14, 15, 0).unwrap();

    let data = client
        .hourly()
        .location(LatLon(38.0290805555556, 14.0400277777778))
        .required_data(RequiredData::SpecificDate(datetime.date_naive()))
        .max_distance_km(100.0)
        .call()
        .await?
        .frame
        .collect()?;

    println!("{:#?}", data);

    Ok(())
}

fn configure_polars_display() {
    // show every column
    env::set_var("POLARS_FMT_MAX_COLS", "-1");
    // show 20 rows
    env::set_var("POLARS_FMT_MAX_ROWS", "20");
}
