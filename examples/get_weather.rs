use chrono::{DateTime, NaiveDate, Utc};
use meteostat::weather_data::fetch::{
    fetch_climate_normal, fetch_daily_weather, fetch_hourly_weather, fetch_monthly_weather,
};
use meteostat::weather_data::weather_data_error::WeatherDataError;

#[tokio::main]
async fn main() -> Result<(), WeatherDataError> {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);

    let hourly = fetch_hourly_weather("10637", utc).await?;
    dbg!(hourly);

    let daily = fetch_daily_weather("10637", naive.date()).await?;
    dbg!(daily);

    let monthly = fetch_monthly_weather("10637", 2022, 11).await?;
    dbg!(monthly);

    let climate = fetch_climate_normal("10637", 1991, 2020, 7).await?;
    dbg!(climate);
    Ok(())
}
