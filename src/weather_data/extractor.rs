use crate::types::weather_condition::WeatherCondition;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::weather_data::error::WeatherDataError;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use polars::prelude::*;
use std::convert::TryFrom;
use std::fmt::Debug;

// Shared
const COL_DATE: &str = "date";
const COL_PRCP: &str = "prcp"; // Precipitation
const COL_SNOW: &str = "snow"; // Snow depth / snowfall amount
const COL_WDIR: &str = "wdir"; // Wind direction
const COL_WSPD: &str = "wspd"; // Wind speed
const COL_WPGT: &str = "wpgt"; // Peak wind gust
const COL_PRES: &str = "pres"; // Pressure
const COL_TSUN: &str = "tsun"; // Sunshine duration

// Hourly Specific
const COL_HOUR: &str = "hour";
const COL_TEMP: &str = "temp"; // Temperature (hourly)
const COL_DWPT: &str = "dwpt"; // Dew point
const COL_RHUM: &str = "rhum"; // Relative humidity
const COL_COCO: &str = "coco"; // Weather condition code

// Daily/Monthly/Normals Specific
const COL_TAVG: &str = "tavg"; // Average temperature
const COL_TMIN: &str = "tmin"; // Minimum temperature
const COL_TMAX: &str = "tmax"; // Maximum temperature

// Monthly/Normals Specific
const COL_YEAR: &str = "year";
const COL_MONTH: &str = "month";

// Normals Specific
const COL_START_YEAR: &str = "start_year";
const COL_END_YEAR: &str = "end_year";

/// Retrieves a column by name from a DataFrame.
fn get_column<'a>(df: &'a DataFrame, col: &str) -> Result<&'a Column, WeatherDataError> {
    df.column(col)
        .map_err(|e| WeatherDataError::ColumnNotFound(col.to_string(), e)) // Simplified error mapping
}

/// Extracts an optional integer value from a specific row (index 0) of a Column.
fn get_opt_int<T>(series: &Column, idx: usize) -> Option<T>
where
    T: TryFrom<i64>,
    <T as TryFrom<i64>>::Error: Debug, // Added constraint for better error potential if needed
{
    series
        .i64()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|val| val.try_into().ok())
}

/// Extracts an optional float value from a specific row (index 0) of a Column.
fn get_opt_float(series: &Column, idx: usize) -> Option<f64> {
    series.f64().ok().and_then(|ca| ca.get(idx))
}

/// Extracts an optional WeatherCondition from a specific row (index 0) of a Column.
/// Handles both string representations of numbers and actual numbers.
fn get_opt_condition(series: &Column, idx: usize) -> Option<WeatherCondition> {
    series
        .str()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|s| s.parse::<i64>().ok()) // Handle potential string codes first
        .and_then(WeatherCondition::from_i64)
        .or_else(|| get_opt_int::<i64>(series, idx).and_then(WeatherCondition::from_i64))
}

// --- Helpers to get optional values directly from DataFrame row 0 ---

fn get_df_opt_float(df: &DataFrame, col: &str) -> Result<Option<f64>, WeatherDataError> {
    Ok(get_opt_float(get_column(df, col)?, 0))
}

fn get_df_opt_int<T>(df: &DataFrame, col: &str) -> Result<Option<T>, WeatherDataError>
where
    T: TryFrom<i64>,
    <T as TryFrom<i64>>::Error: Debug,
{
    Ok(get_opt_int::<T>(get_column(df, col)?, 0))
}

fn get_df_opt_condition(
    df: &DataFrame,
    col: &str,
) -> Result<Option<WeatherCondition>, WeatherDataError> {
    Ok(get_opt_condition(get_column(df, col)?, 0))
}

// --- DataFrame Extractor Struct ---

/// Extracts specific weather data records from a Meteostat LazyFrame.
/// Assumes the LazyFrame contains data for a single station.
#[derive(Clone)]
pub struct DataFrameExtractor {
    lazy_df: LazyFrame,
    station_id: String,
}

impl DataFrameExtractor {
    pub fn new(lazy_df: LazyFrame, station_id: &str) -> Self {
        Self {
            lazy_df,
            station_id: station_id.to_string(),
        }
    }

    /// Filters the internal LazyFrame, collects it, and ensures exactly one row is present.
    /// Returns the resulting single-row DataFrame or a DataNotFound error.
    fn filter_and_collect_single(&self, filter_expr: Expr) -> Result<DataFrame, WeatherDataError> {
        let filtered_lazy = self.lazy_df.clone().filter(filter_expr); // Clone LazyFrame for filtering
        let filtered_df = filtered_lazy
            .collect()
            .map_err(|e| WeatherDataError::PolarsError {
                station: self.station_id.clone(),
                source: e,
            })?;

        if filtered_df.height() == 0 {
            Err(WeatherDataError::DataNotFound {
                station: self.station_id.clone(),
                date: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                hour: 0,
            })
        } else if filtered_df.height() > 1 {
            Err(WeatherDataError::UnexpectedData {
                message: format!(
                    "Found multiple rows ({}) matching filter",
                    filtered_df.height()
                ),
                station: self.station_id.clone(),
            })
        } else {
            Ok(filtered_df)
        }
    }

    // --- Extraction Methods ---

    /// Extracts hourly weather data for a specific date and time.
    pub fn extract_hourly(
        &self,
        datetime: DateTime<Utc>,
    ) -> Result<HourlyWeatherInfo, WeatherDataError> {
        let date_naive = datetime.date_naive();
        let date_string = date_naive.format("%Y-%m-%d").to_string();
        let hour_u32 = datetime.hour();
        let hour_i64 = hour_u32 as i64; // match polars datatype

        let filter_expr = col(COL_DATE)
            .eq(lit(date_string))
            .and(col(COL_HOUR).eq(lit(hour_i64)));

        let df = self
            .filter_and_collect_single(filter_expr)
            .map_err(|e| match e {
                // Provide better context for DataNotFound
                WeatherDataError::DataNotFound { station, .. } => WeatherDataError::DataNotFound {
                    station,
                    date: date_naive,
                    hour: hour_u32,
                },
                other => other,
            })?;

        Ok(HourlyWeatherInfo {
            date: date_naive,
            hour: hour_u32,
            temperature: get_df_opt_float(&df, COL_TEMP)?,
            dew_point: get_df_opt_float(&df, COL_DWPT)?,
            relative_humidity: get_df_opt_int::<i32>(&df, COL_RHUM)?,
            precipitation: get_df_opt_float(&df, COL_PRCP)?,
            snow: get_df_opt_int::<i32>(&df, COL_SNOW)?, // Note: Hourly snow is usually amount, not depth
            wind_direction: get_df_opt_int::<i32>(&df, COL_WDIR)?,
            wind_speed: get_df_opt_float(&df, COL_WSPD)?,
            peak_wind_gust: get_df_opt_float(&df, COL_WPGT)?,
            pressure: get_df_opt_float(&df, COL_PRES)?,
            sunshine: get_df_opt_int::<u32>(&df, COL_TSUN)?, // Check u32 vs i32 based on source
            condition: get_df_opt_condition(&df, COL_COCO)?,
        })
    }

    /// Extracts daily weather data for a specific date.
    pub fn extract_daily(&self, date: NaiveDate) -> Result<DailyWeatherInfo, WeatherDataError> {
        let date_string = date.format("%Y-%m-%d").to_string();
        let filter_expr = col(COL_DATE).eq(lit(date_string));

        let df = self
            .filter_and_collect_single(filter_expr)
            .map_err(|e| match e {
                // Provide better context for DataNotFound
                WeatherDataError::DataNotFound { station, .. } => WeatherDataError::DataNotFound {
                    station,
                    date,
                    hour: 0,
                },
                other => other,
            })?;

        Ok(DailyWeatherInfo {
            date,
            temp_avg: get_df_opt_float(&df, COL_TAVG)?,
            temp_min: get_df_opt_float(&df, COL_TMIN)?,
            temp_max: get_df_opt_float(&df, COL_TMAX)?,
            precipitation: get_df_opt_float(&df, COL_PRCP)?,
            snow_depth: get_df_opt_int::<i32>(&df, COL_SNOW)?,
            wind_direction_avg: get_df_opt_int::<i32>(&df, COL_WDIR)?,
            wind_speed_avg: get_df_opt_float(&df, COL_WSPD)?,
            peak_wind_gust: get_df_opt_float(&df, COL_WPGT)?,
            pressure_avg: get_df_opt_float(&df, COL_PRES)?,
            sunshine_total: get_df_opt_int::<i32>(&df, COL_TSUN)?,
        })
    }

    /// Extracts monthly weather summary for a specific year and month.
    pub fn extract_monthly(
        &self,
        year: i32,
        month: u32,
    ) -> Result<MonthlyWeatherInfo, WeatherDataError> {
        let year_i64 = year as i64;
        let month_i64 = month as i64;

        let filter_expr = col(COL_YEAR)
            .eq(lit(year_i64))
            .and(col(COL_MONTH).eq(lit(month_i64)));

        let representative_date = NaiveDate::from_ymd_opt(year, month, 1)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()); // Fallback

        let df = self
            .filter_and_collect_single(filter_expr)
            .map_err(|e| match e {
                WeatherDataError::DataNotFound { station, .. } => WeatherDataError::DataNotFound {
                    station,
                    date: representative_date,
                    hour: 0,
                },
                other => other,
            })?;

        Ok(MonthlyWeatherInfo {
            year,
            month,
            temp_avg: get_df_opt_float(&df, COL_TAVG)?,
            temp_min_avg: get_df_opt_float(&df, COL_TMIN)?,
            temp_max_avg: get_df_opt_float(&df, COL_TMAX)?,
            precipitation_total: get_df_opt_float(&df, COL_PRCP)?,
            wind_speed_avg: get_df_opt_float(&df, COL_WSPD)?,
            pressure_avg: get_df_opt_float(&df, COL_PRES)?,
            sunshine_total: get_df_opt_int::<i32>(&df, COL_TSUN)?, // Check i32 vs u32
        })
    }

    /// Extracts climate normal data for a specific period and month.
    pub fn extract_climate_normal(
        &self,
        start_year: i32,
        end_year: i32,
        month: u32,
    ) -> Result<ClimateNormalInfo, WeatherDataError> {
        let start_year_i64 = start_year as i64;
        let end_year_i64 = end_year as i64;
        let month_i64 = month as i64;

        let filter_expr = col(COL_START_YEAR)
            .eq(lit(start_year_i64))
            .and(col(COL_END_YEAR).eq(lit(end_year_i64)))
            .and(col(COL_MONTH).eq(lit(month_i64)));

        let representative_date = NaiveDate::from_ymd_opt(end_year, month, 1)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()); // Fallback

        let df = self
            .filter_and_collect_single(filter_expr)
            .map_err(|e| match e {
                WeatherDataError::DataNotFound { station, .. } => WeatherDataError::DataNotFound {
                    station,
                    date: representative_date,
                    hour: 0,
                },
                other => other,
            })?;

        Ok(ClimateNormalInfo {
            start_year,
            end_year,
            month,
            temp_min_avg: get_df_opt_float(&df, COL_TMIN)?,
            temp_max_avg: get_df_opt_float(&df, COL_TMAX)?,
            precipitation_avg: get_df_opt_float(&df, COL_PRCP)?,
            wind_speed_avg: get_df_opt_float(&df, COL_WSPD)?,
            pressure_avg: get_df_opt_float(&df, COL_PRES)?,
            sunshine_avg: get_df_opt_int::<i32>(&df, COL_TSUN)?,
        })
    }
}
