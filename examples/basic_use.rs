use chrono::{DateTime, NaiveDate, Utc};
use meteostat::error::MeteostatError;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let lat = 50.0;
    let lon = 5.0;
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    
    Ok(())
}
