use crate::types::data_source::DataSourceType;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::weather_data::data_extractor::{
    extract_climate_normal_from_dataframe, extract_daily_weather_from_dataframe,
    extract_hourly_weather_from_dataframe, extract_monthly_weather_from_dataframe,
};
use crate::weather_data::data_loader::WeatherDataLoader;
use crate::weather_data::error::WeatherDataError;
use chrono::{DateTime, NaiveDate, Utc};
use std::path::Path;

pub struct WeatherFetcher {
    loader: WeatherDataLoader,
}
impl WeatherFetcher {
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            loader: WeatherDataLoader::new(cache_dir),
        }
    }

    pub async fn hourly(
        &self,
        station: &str,
        datetime: DateTime<Utc>,
    ) -> Result<HourlyWeatherInfo, WeatherDataError> {
        let lazy_df = self
            .loader
            .get_frame(DataSourceType::Hourly, station)
            .await?;
        extract_hourly_weather_from_dataframe(lazy_df, station, datetime)
    }

    pub async fn daily(
        &self,
        station: &str,
        date: NaiveDate,
    ) -> Result<DailyWeatherInfo, WeatherDataError> {
        let lazy_df = self
            .loader
            .get_frame(DataSourceType::Daily, station)
            .await?;
        extract_daily_weather_from_dataframe(lazy_df, station, date)
    }

    pub async fn monthly(
        &self,
        station: &str,
        year: i32,
        month: u32,
    ) -> Result<MonthlyWeatherInfo, WeatherDataError> {
        let lazy_df = self
            .loader
            .get_frame(DataSourceType::Monthly, station)
            .await?;
        extract_monthly_weather_from_dataframe(lazy_df, station, year, month)
    }

    pub async fn climate_normals(
        &self,
        station: &str,
        start_year: i32,
        end_year: i32,
        month: u32,
    ) -> Result<ClimateNormalInfo, WeatherDataError> {
        let lazy_df = self
            .loader
            .get_frame(DataSourceType::Normals, station)
            .await?;
        extract_climate_normal_from_dataframe(lazy_df, station, start_year, end_year, month)
    }
}
