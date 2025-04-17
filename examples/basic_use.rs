use chrono::{DateTime, NaiveDate, Utc};
use meteostat::error::MeteostatError;
use meteostat::meteostat::MeteostatBuilder;
use meteostat::types::bitflags::hourly::RequiredHourlyField;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let lat = 50.0;
    let lon = 5.0;
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    let meteostat = MeteostatBuilder::new()
        .with_station_check_limit(30)
        .with_max_distance_km(100.0)
        .build()
        .await?;

    let basic_data = meteostat.get_hourly(lat, lon, utc).await?;
    dbg!(basic_data);

    let required_fields = RequiredHourlyField::TEMPERATURE | RequiredHourlyField::PRECIPITATION;
    let combined_data = meteostat
        .get_hourly_combined(lat, lon, utc, required_fields)
        .await?;
    dbg!(combined_data);
    Ok(())
}
