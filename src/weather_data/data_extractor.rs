use crate::weather_data::weather_data_error::{WeatherDataError};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use polars::prelude::*;
use std::convert::TryFrom;
use crate::types::data_source::DataSourceType;
use crate::types::weather_condition::WeatherCondition;
use crate::types::weather_data::climate::ClimateNormalInfo;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;

fn get_opt_int<T>(series: &Column, idx: usize) -> Option<T>
where
    T: TryFrom<i64>, // Changed from Column to Series
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
    // Handle potential string representations first if needed (though unlikely now)
    series
        .str()
        .ok()
        .and_then(|ca| ca.get(idx))
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(WeatherCondition::from_i64)
        .or_else(|| get_opt_int::<i64>(series, idx).and_then(WeatherCondition::from_i64))
}


/// Renames columns of a LazyFrame from generic column_N to specific names.
fn rename_columns(ldf: LazyFrame, data_type: DataSourceType) -> LazyFrame {
    let schema_names = data_type.get_schema_column_names();
    let current_names: Vec<String> = (1..=schema_names.len())
        .map(|i| format!("column_{}", i))
        .collect();
    ldf.rename(current_names, &schema_names, false)
}

// --- Extraction function for HOURLY data ---
pub fn extract_hourly_weather_from_dataframe(
    df: LazyFrame,
    station: &str,
    datetime: DateTime<Utc>,
) -> Result<HourlyWeatherInfo, WeatherDataError> {
    let date_naive = datetime.date_naive();
    let date_string = date_naive.format("%Y-%m-%d").to_string();
    let hour_u32 = datetime.hour();
    let hour_i64 = hour_u32 as i64; // Use i64 for Polars filtering

    // Rename columns FIRST for clarity in filtering/selection
    let renamed_df = rename_columns(df, DataSourceType::Hourly);

    let filtered_lazy = renamed_df
        .filter(col("date").eq(lit(date_string))) // Filter by renamed column
        .filter(col("hour").eq(lit(hour_i64)));    // Filter by renamed column

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound {
            station: station.to_string(),
            date: date_naive,
            hour: hour_u32, // Keep hour for specific error
        });
    }

    // Helper macro to get Series or return ColumnNotFound error
    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|_| WeatherDataError::ColumnNotFound($name.to_string()))?
        };
    }

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
        snow: get_opt_int::<i32>(snow_series, 0),
        wind_direction: get_opt_int::<i32>(wdir_series, 0),
        wind_speed: get_opt_float(wspd_series, 0),
        peak_wind_gust: get_opt_float(gust_series, 0),
        pressure: get_opt_float(pres_series, 0),
        sunshine: get_opt_int::<u32>(sun_series, 0), // Check if u32 is correct type
        condition: get_opt_condition(cond_series, 0),
    })
}

// --- Extraction function for DAILY data ---
pub fn extract_daily_weather_from_dataframe(
    df: LazyFrame,
    station: &str,
    date: NaiveDate,
) -> Result<DailyWeatherInfo, WeatherDataError> {
    let date_string = date.format("%Y-%m-%d").to_string();

    let renamed_df = rename_columns(df, DataSourceType::Daily);

    let filtered_lazy = renamed_df.filter(col("date").eq(lit(date_string)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound { // Use the same error for now
            station: station.to_string(),
            date,
            hour: 0, // Indicate daily by setting hour to 0 or similar? Or add specific Daily error?
        });
    }

    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|_| WeatherDataError::ColumnNotFound($name.to_string()))?
        };
    }

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
        sunshine_total: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0),
    })
}

// --- Extraction function for MONTHLY data ---
pub fn extract_monthly_weather_from_dataframe(
    df: LazyFrame,
    station: &str,
    year: i32,
    month: u32,
) -> Result<MonthlyWeatherInfo, WeatherDataError> {
    let year_i64 = year as i64; // Polars needs i64 for literal typically
    let month_i64 = month as i64;

    let renamed_df = rename_columns(df, DataSourceType::Monthly);

    let filtered_lazy = renamed_df
        .filter(col("year").eq(lit(year_i64)))
        .filter(col("month").eq(lit(month_i64)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound { // Adapt error or create new one
            station: station.to_string(),
            // How to best represent year/month in error? Maybe just date?
            date: NaiveDate::from_ymd_opt(year, month, 1).unwrap_or_default(), // Placeholder
            hour: 0, // Indicate monthly
        });
    }

    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|_| WeatherDataError::ColumnNotFound($name.to_string()))?
        };
    }

    Ok(MonthlyWeatherInfo {
        year,
        month,
        temp_avg: get_opt_float(get_series!(filtered, "tavg"), 0),
        temp_min_avg: get_opt_float(get_series!(filtered, "tmin"), 0),
        temp_max_avg: get_opt_float(get_series!(filtered, "tmax"), 0),
        precipitation_total: get_opt_float(get_series!(filtered, "prcp"), 0),
        wind_speed_avg: get_opt_float(get_series!(filtered, "wspd"), 0),
        pressure_avg: get_opt_float(get_series!(filtered, "pres"), 0),
        sunshine_total: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0),
    })
}


// --- Extraction function for NORMALS data ---
pub fn extract_climate_normal_from_dataframe(
    df: LazyFrame,
    station: &str,
    start_year: i32,
    end_year: i32,
    month: u32,
) -> Result<ClimateNormalInfo, WeatherDataError> {
    let start_year_i64 = start_year as i64;
    let end_year_i64 = end_year as i64;
    let month_i64 = month as i64;

    let renamed_df = rename_columns(df, DataSourceType::Normals);

    let filtered_lazy = renamed_df
        .filter(col("start_year").eq(lit(start_year_i64)))
        .filter(col("end_year").eq(lit(end_year_i64)))
        .filter(col("month").eq(lit(month_i64)));

    let filtered = filtered_lazy.collect()?;

    if filtered.height() != 1 {
        return Err(WeatherDataError::DataNotFound { // Adapt error or create new one
            station: station.to_string(),
            // Representing normals period in error? Maybe just date?
            date: NaiveDate::from_ymd_opt(end_year, month, 1).unwrap_or_default(), // Placeholder
            hour: 0, // Indicate normals
        });
    }

    macro_rules! get_series {
        ($df:expr, $name:expr) => {
            $df.column($name)
                .map_err(|_| WeatherDataError::ColumnNotFound($name.to_string()))?
        };
    }

    Ok(ClimateNormalInfo {
        start_year,
        end_year,
        month,
        temp_min_avg: get_opt_float(get_series!(filtered, "tmin"), 0),
        temp_max_avg: get_opt_float(get_series!(filtered, "tmax"), 0),
        precipitation_avg: get_opt_float(get_series!(filtered, "prcp"), 0),
        wind_speed_avg: get_opt_float(get_series!(filtered, "wspd"), 0),
        pressure_avg: get_opt_float(get_series!(filtered, "pres"), 0),
        sunshine_avg: get_opt_int::<i32>(get_series!(filtered, "tsun"), 0),
    })
}