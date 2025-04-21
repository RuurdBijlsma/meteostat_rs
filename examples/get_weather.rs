use chrono::{DateTime, NaiveDate, Utc};
use meteostat::utils::get_cache_dir;
use meteostat::weather_data::frame_fetcher::FrameFetcher;
use meteostat::weather_data::error::WeatherDataError;

#[tokio::main]
async fn main() -> Result<(), WeatherDataError> {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    let fetcher = FrameFetcher::new(&get_cache_dir()?);
    Ok(())
}
