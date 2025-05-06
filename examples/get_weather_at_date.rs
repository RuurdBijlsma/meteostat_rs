use chrono::NaiveDate;
use meteostat::{LatLon, Meteostat, MeteostatError};
use serde_json::to_string_pretty;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let client = Meteostat::new().await?;
    let specific_date = NaiveDate::from_ymd_opt(2023, 10, 26).unwrap();

    let daily_data = client
        .daily()
        .location(LatLon(52.520008, 13.404954))
        .call()
        .await? // DailyLazyFrame
        .get_at(specific_date)? // Filter for the specific date
        .collect_single_daily()?; // Attempt to collect a single Daily struct

    let json = to_string_pretty(&daily_data).unwrap(); // Convert the struct to JSON for pretty printing
    println!("{}", json);
    Ok(())
}
