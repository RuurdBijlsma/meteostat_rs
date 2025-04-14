use crate::types::weather_condition::WeatherCondition;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::weather_data::error::WeatherDataError;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use polars::prelude::*;
use std::convert::TryFrom;

// Helper functions remain the same
fn get_opt_int<T>(series: &Column, idx: usize) -> Option<T>
where
    T: TryFrom<i64>,
{
    series
        .i64()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|val| val.try_into().ok())
}
fn get_opt_float(series: &Column, idx: usize) -> Option<f64> {
    series.f64().ok().and_then(|ca| ca.get(idx))
}
fn get_opt_condition(series: &Column, idx: usize) -> Option<WeatherCondition> {
    series
        .str()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|s| s.parse::<i64>().ok()) // Handle potential string codes
        .and_then(WeatherCondition::from_i64)
        .or_else(|| get_opt_int::<i64>(series, idx).and_then(WeatherCondition::from_i64))
}

// --- REMOVED rename_columns function ---

// --- Extraction function for HOURLY data ---
pub fn extract_hourly_weather_from_dataframe(
    df: LazyFrame, // No longer needs renaming
    station: &str,
    datetime: DateTime<Utc>,
) -> Result<HourlyWeatherInfo, WeatherDataError> {
    let date_naive = datetime.date_naive();
    let date_string = date_naive.format("%Y-%m-%d").to_string();
    let hour_u32 = datetime.hour();
    let hour_i64 = hour_u32 as i64; // Use i64 for Polars filtering

    // Columns are already named correctly, filter directly
    let filtered_lazy = df // Use df directly
        .filter(col("date").eq(lit(date_string)))
        .filter(col("hour").eq(lit(hour_i64)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound {
            station: station.to_string(),
            date: date_naive,
            hour: hour_u32,
        });
    }

    // Helper macro to get Column or return ColumnNotFound error
    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|e| {
                     // Add context to the error
                    log::error!("Column '{}' not found in DataFrame. Error: {}", $name, e);
                    WeatherDataError::ColumnNotFound($name.to_string())
                })?
        };
    }

    // Extract data using the expected column names
    let temp_series = get_series!(filtered, "temp");
    let dew_series = get_series!(filtered, "dwpt");
    let rh_series = get_series!(filtered, "rhum");
    let precip_series = get_series!(filtered, "prcp");
    let snow_series = get_series!(filtered, "snow");
    let wdir_series = get_series!(filtered, "wdir");
    let wspd_series = get_series!(filtered, "wspd");
    let gust_series = get_series!(filtered, "wpgt");
    let pres_series = get_series!(filtered, "pres");
    let sun_series = get_series!(filtered, "tsun");
    let cond_series = get_series!(filtered, "coco");

    Ok(HourlyWeatherInfo {
        date: date_naive,
        hour: hour_u32,
        temperature: get_opt_float(temp_series, 0),
        dew_point: get_opt_float(dew_series, 0),
        relative_humidity: get_opt_int::<i32>(rh_series, 0),
        precipitation: get_opt_float(precip_series, 0),
        snow: get_opt_int::<i32>(snow_series, 0), // Assuming depth is i32, adjust if needed
        wind_direction: get_opt_int::<i32>(wdir_series, 0),
        wind_speed: get_opt_float(wspd_series, 0),
        peak_wind_gust: get_opt_float(gust_series, 0),
        pressure: get_opt_float(pres_series, 0),
        sunshine: get_opt_int::<u32>(sun_series, 0), // Check type u32 vs i32
        condition: get_opt_condition(cond_series, 0),
    })
}

// --- Extraction function for DAILY data ---
pub fn extract_daily_weather_from_dataframe(
    df: LazyFrame, // No longer needs renaming
    station: &str,
    date: NaiveDate,
) -> Result<DailyWeatherInfo, WeatherDataError> {
    let date_string = date.format("%Y-%m-%d").to_string();

    // Filter directly
    let filtered_lazy = df.filter(col("date").eq(lit(date_string)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        // Consider a more specific error? Or is DataNotFound okay?
        // Maybe add a field to DataNotFound indicating the level (hourly/daily etc.)
        return Err(WeatherDataError::DataNotFound {
            station: station.to_string(),
            date,
            hour: 0, // Using 0 as a placeholder for 'not applicable' or daily level
        });
    }

    macro_rules! get_series {
         ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|e| {
                    log::error!("Column '{}' not found in DataFrame. Error: {}", $name, e);
                    WeatherDataError::ColumnNotFound($name.to_string())
                })?
        };
    }

    // Extract data using expected column names
    Ok(DailyWeatherInfo {
        date,
        temp_avg: get_opt_float(get_series!(filtered, "tavg"), 0),
        temp_min: get_opt_float(get_series!(filtered, "tmin"), 0),
        temp_max: get_opt_float(get_series!(filtered, "tmax"), 0),
        precipitation: get_opt_float(get_series!(filtered, "prcp"), 0),
        snow_depth: get_opt_int::<i32>(get_series!(filtered, "snow"), 0),
        wind_direction_avg: get_opt_int::<i32>(get_series!(filtered, "wdir"), 0),
        wind_speed_avg: get_opt_float(get_series!(filtered, "wspd"), 0),
        peak_wind_gust: get_opt_float(get_series!(filtered, "wpgt"), 0),
        pressure_avg: get_opt_float(get_series!(filtered, "pres"), 0),
        sunshine_total: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0), // Check type i32 vs u32
    })
}

// --- Extraction function for MONTHLY data ---
pub fn extract_monthly_weather_from_dataframe(
    df: LazyFrame, // No longer needs renaming
    station: &str,
    year: i32,
    month: u32,
) -> Result<MonthlyWeatherInfo, WeatherDataError> {
    let year_i64 = year as i64;
    let month_i64 = month as i64;

    // Filter directly
    let filtered_lazy = df
        .filter(col("year").eq(lit(year_i64)))
        .filter(col("month").eq(lit(month_i64)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound {
            station: station.to_string(),
            // Use a representative date for the error context
            date: NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()), // Default fallback
            hour: 0, // Indicates monthly level
        });
    }

    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                 .map_err(|e| {
                    log::error!("Column '{}' not found in DataFrame. Error: {}", $name, e);
                    WeatherDataError::ColumnNotFound($name.to_string())
                })?
        };
    }

    // Extract data using expected column names
    Ok(MonthlyWeatherInfo {
        year,
        month,
        temp_avg: get_opt_float(get_series!(filtered, "tavg"), 0),
        temp_min_avg: get_opt_float(get_series!(filtered, "tmin"), 0),
        temp_max_avg: get_opt_float(get_series!(filtered, "tmax"), 0),
        precipitation_total: get_opt_float(get_series!(filtered, "prcp"), 0),
        wind_speed_avg: get_opt_float(get_series!(filtered, "wspd"), 0),
        pressure_avg: get_opt_float(get_series!(filtered, "pres"), 0),
        sunshine_total: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0), // Check type i32 vs u32
    })
}

// --- Extraction function for NORMALS data ---
pub fn extract_climate_normal_from_dataframe(
    df: LazyFrame, // No longer needs renaming
    station: &str,
    start_year: i32,
    end_year: i32,
    month: u32,
) -> Result<ClimateNormalInfo, WeatherDataError> {
    let start_year_i64 = start_year as i64;
    let end_year_i64 = end_year as i64;
    let month_i64 = month as i64;

    // Filter directly
    let filtered_lazy = df
        .filter(col("start_year").eq(lit(start_year_i64)))
        .filter(col("end_year").eq(lit(end_year_i64)))
        .filter(col("month").eq(lit(month_i64)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound {
            station: station.to_string(),
            // Use a representative date for the error context
            date: NaiveDate::from_ymd_opt(end_year, month, 1).unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()), // Default fallback
            hour: 0, // Indicates normals level
        });
    }

    macro_rules! get_series {
         ($df:expr, $name:expr) => {
            $df.column($name)
                 .map_err(|e| {
                    log::error!("Column '{}' not found in DataFrame. Error: {}", $name, e);
                    WeatherDataError::ColumnNotFound($name.to_string())
                })?
        };
    }

    // Extract data using expected column names
    Ok(ClimateNormalInfo {
        start_year,
        end_year,
        month,
        temp_min_avg: get_opt_float(get_series!(filtered, "tmin"), 0),
        temp_max_avg: get_opt_float(get_series!(filtered, "tmax"), 0),
        precipitation_avg: get_opt_float(get_series!(filtered, "prcp"), 0),
        wind_speed_avg: get_opt_float(get_series!(filtered, "wspd"), 0),
        pressure_avg: get_opt_float(get_series!(filtered, "pres"), 0),
        sunshine_avg: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0), // Check type i32 vs u32
    })
}