use crate::types::data_source::DataSourceType;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::weather_data::data_extractor::{
    extract_climate_normal_from_dataframe, extract_daily_weather_from_dataframe,
    extract_hourly_weather_from_dataframe, extract_monthly_weather_from_dataframe,
};
use crate::weather_data::data_loader::load_dataframe_for_station;
use crate::weather_data::weather_data_error::Result;
use chrono::{DateTime, NaiveDate, Utc};

pub async fn fetch_hourly_weather(
    station: &str,
    datetime: DateTime<Utc>,
) -> Result<HourlyWeatherInfo> {
    let lazy_df = load_dataframe_for_station(DataSourceType::Hourly, station).await?;
    extract_hourly_weather_from_dataframe(lazy_df, station, datetime)
}

pub async fn fetch_daily_weather(station: &str, date: NaiveDate) -> Result<DailyWeatherInfo> {
    let lazy_df = load_dataframe_for_station(DataSourceType::Daily, station).await?;
    extract_daily_weather_from_dataframe(lazy_df, station, date)
}

pub async fn fetch_monthly_weather(
    station: &str,
    year: i32,
    month: u32,
) -> Result<MonthlyWeatherInfo> {
    let lazy_df = load_dataframe_for_station(DataSourceType::Monthly, station).await?;
    extract_monthly_weather_from_dataframe(lazy_df, station, year, month)
}

pub async fn fetch_climate_normal(
    station: &str,
    start_year: i32,
    end_year: i32,
    month: u32,
) -> Result<ClimateNormalInfo> {
    let lazy_df = load_dataframe_for_station(DataSourceType::Normals, station).await?;
    extract_climate_normal_from_dataframe(lazy_df, station, start_year, end_year, month)
}
