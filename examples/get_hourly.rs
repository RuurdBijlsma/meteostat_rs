use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use meteostat::get_hourly::get_hourly_from_station;

#[tokio::main]
async fn main() -> Result<()> {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    let info = get_hourly_from_station("10637", utc).await;
    dbg!(info);
    Ok(())
}
