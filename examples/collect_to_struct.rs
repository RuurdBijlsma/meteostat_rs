//! Demonstrates collecting Meteostat data directly into Rust structs

use chrono::NaiveDate;
use meteostat::{LatLon, Meteostat, MeteostatError};

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    // --- Setup ---
    let client = Meteostat::new().await?;
    // Period for which we want hourly data:
    let period = NaiveDate::from_ymd_opt(2023, 9, 1).unwrap();

    // --- Hourly Data: Collect a Range into Vec<Hourly> ---
    println!("\n--- Hourly Data (Range to Vec<Hourly>) ---");
    let hourly_vec = client
        .hourly()
        .location(LatLon(52.0836403, 5.1257283))
        .call()
        .await?
        .get_for_period(period)?
        .collect_hourly()?;

    println!(
        "Collected {} hourly records for {}:",
        hourly_vec.len(),
        period
    );
    // Print the first record if available
    if let Some(first_rec) = hourly_vec.first() {
        println!("First entry:\n{:?}", first_rec);
    }
    Ok(())
}
