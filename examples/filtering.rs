use chrono::{TimeZone, Utc};
use meteostat::error::MeteostatError;
use meteostat::filtering::MeteostatFrameFilterExt;
use meteostat::meteostat::{LatLon, Meteostat};
use meteostat::types::data_source::Frequency;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {

    let meteostat = Meteostat::new().await?;
    let lazy_frame = meteostat
        .from_location()
        .location(LatLon {
            lat: 52.520008,
            lon: 13.404954,
        })
        .frequency(Frequency::Hourly)
        .call()
        .await?;

    let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
    let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap();

    // Apply filter
    let filtered_lazy_frame = lazy_frame.filter_hourly(start_utc, end_utc);

    // --- Add Explain ---
    println!(
        "\n--- Optimized Plan for test_get_hourly_location_filtered ---\n{}\n--------------------------------------------------",
        filtered_lazy_frame.clone().explain(true).map_err(MeteostatError::from)?
    );
    // --- End Explain ---

    // Collect
    let frame = filtered_lazy_frame.collect()?;
    dbg!(&frame);

    Ok(())
}
