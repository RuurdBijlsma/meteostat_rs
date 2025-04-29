// hourly_frame.rs

//! Contains the `HourlyLazyFrame` structure for handling lazy operations on Meteostat hourly weather data.

use crate::types::traits::any::any_datetime::AnyDateTime;
use crate::types::traits::period::datetime_period::DateTimePeriod;
use crate::{MeteostatError, WeatherCondition};
use chrono::{DateTime, Duration, Timelike, Utc};
use polars::prelude::{col, lit, Expr, LazyFrame};

/// Represents a row of hourly weather data.
/// (Note: This struct is currently not directly used in the lazy frame processing pipeline,
/// but represents the expected structure of a collected row).
#[allow(dead_code)]
struct Hourly {
    datetime: DateTime<Utc>,  // Stored as NaiveDateTime in Polars frame typically
    temperature: Option<f64>, // Use Option for missing values
    dew_point: Option<f64>,
    relative_humidity: Option<i32>,
    precipitation: Option<f64>,
    snow: Option<i32>,
    wind_direction: Option<i32>,
    wind_speed: Option<f64>,
    peak_wind_gust: Option<f64>,
    pressure: Option<f64>,
    sunshine_minutes: Option<i32>, // Note: Sunshine is often poorly populated in hourly data
    condition: Option<WeatherCondition>, // Condition code might be missing
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat hourly weather data.
///
/// This struct provides methods tailored for common operations on hourly datasets,
/// such as filtering by datetime ranges, while retaining the benefits of lazy evaluation.
///
/// Instances are typically obtained via [`crate::Meteostat::hourly`].
///
/// # Note on Datetimes
///
/// Internally, Polars often handles datetime columns as timezone-naive (`NaiveDateTime`)
/// even if the source implies a timezone like UTC. The methods in this struct
/// (`get_range`, `get_at`) handle the conversion to/from `DateTime<Utc>` and use
/// timezone-naive representations (`naive_utc()`) for filtering the underlying frame,
/// assuming the frame's "datetime" column is timezone-naive UTC.
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`)
/// can potentially return a [`polars::prelude::PolarsError`].
///
/// Methods involving datetime parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input datetimes cannot be resolved.
///
/// The initial creation via [`crate::Meteostat::hourly`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)] // Added Clone for convenience
pub struct HourlyLazyFrame {
    /// The underlying Polars LazyFrame containing the hourly data.
    pub frame: LazyFrame,
}

impl HourlyLazyFrame {
    /// Creates a new `HourlyLazyFrame` wrapping the given Polars `LazyFrame`.
    ///
    /// This is typically called internally by the [`crate::Meteostat`] client methods.
    ///
    /// # Arguments
    ///
    /// * `frame` - A `LazyFrame` assumed to contain hourly weather data with the expected schema,
    ///   including a "datetime" column interpretable as timezone-naive UTC.
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    /// Filters the hourly data based on a Polars predicate expression.
    ///
    /// This method allows applying arbitrary filtering logic supported by Polars.
    /// It returns a *new* `HourlyLazyFrame` with the filter applied lazily.
    /// The original `HourlyLazyFrame` remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A Polars [`Expr`] defining the filtering condition.
    ///
    /// # Returns
    ///
    /// A new `HourlyLazyFrame` representing the filtered data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// use polars::prelude::{col, lit, PolarsError};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let berlin = LatLon(52.52, 13.40);
    ///
    /// let hourly_lazy = client.hourly().location(berlin).call().await?;
    ///
    /// // Filter for hours where temperature was below freezing (0 degrees Celsius)
    /// let freezing_hours = hourly_lazy.filter(col("temp").lt(lit(0.0f64)));
    ///
    /// // Collect the results
    /// let df = freezing_hours.frame.collect()?;
    /// println!("Freezing hours found:\n{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// While this method itself doesn't typically error, subsequent operations like `.collect()`
    /// might return a [`polars::prelude::PolarsError`].
    pub fn filter(&self, predicate: Expr) -> HourlyLazyFrame {
        HourlyLazyFrame::new(self.frame.clone().filter(predicate))
    }

    /// Filters the hourly data to include only records within the specified datetime range (inclusive).
    ///
    /// The `start` and `end` arguments can be any type that implements [`AnyDateTime`],
    /// such as `DateTime<Utc>`, `NaiveDate`, [`crate::Month`], or [`crate::Year`]. The trait
    /// resolves these into specific start and end `DateTime<Utc>` values. These are then
    /// converted to `NaiveDateTime` (representing UTC) for filtering the Polars frame.
    ///
    /// # Arguments
    ///
    /// * `start` - The starting datetime boundary (inclusive), implementing [`AnyDateTime`].
    /// * `end` - The ending datetime boundary (inclusive), implementing [`AnyDateTime`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `HourlyLazyFrame` filtered by the datetime range,
    /// or a [`MeteostatError::DateParsingError`] if the datetime conversion fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// use chrono::{DateTime, Utc, TimeZone};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let london = LatLon(51.50, -0.12); // London location
    ///
    /// let hourly_lazy = client.hourly().location(london).call().await?;
    ///
    /// // Get data for a specific 6-hour period on a certain day
    /// let start_dt = Utc.with_ymd_and_hms(2023, 10, 26, 12, 0, 0).unwrap();
    /// let end_dt = Utc.with_ymd_and_hms(2023, 10, 26, 17, 59, 59).unwrap(); // Inclusive end hour
    ///
    /// let period_data_lazy = hourly_lazy.get_range(start_dt, end_dt)?;
    ///
    /// // Collect the result (expecting up to 6 rows)
    /// let df = period_data_lazy.frame.collect()?;
    /// println!("Data for 2023-10-26 12:00 to 17:59:\n{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `start` or `end` cannot be resolved to a `DateTime<Utc>`.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
    pub fn get_range(
        &self,
        start: impl AnyDateTime,
        end: impl AnyDateTime,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        // Resolve inputs to UTC DateTimes
        let start_utc = start
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        let end_utc = end
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .end;

        // Convert to NaiveDateTime for filtering the Polars frame
        let start_naive = start_utc.naive_utc();
        let end_naive = end_utc.naive_utc();

        dbg!(&start_naive);
        dbg!(&end_naive);

        Ok(self.filter(
            col("datetime")
                .gt_eq(lit(start_naive))
                .and(col("datetime").lt_eq(lit(end_naive))),
        ))
    }

    /// Filters the hourly data to get the record closest to a specific datetime, rounded to the nearest hour.
    ///
    /// The `datetime` argument can be any type that implements [`AnyDateTime`]. It is resolved
    /// to a `DateTime<Utc>`, then rounded to the nearest hour (>= 30 minutes rounds up, < 30 rounds down).
    /// The filter then looks for an exact match for the *start* of that rounded hour in the frame's
    /// "datetime" column (as `NaiveDateTime`).
    ///
    /// # Arguments
    ///
    /// * `datetime` - The target datetime, implementing [`AnyDateTime`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `HourlyLazyFrame` filtered to the specific rounded hour,
    /// or a [`MeteostatError::DateParsingError`] if the datetime conversion fails. Collecting
    /// the frame should yield zero or one row.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// use chrono::{DateTime, Utc, TimeZone};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let hourly_lazy = client.hourly().location(paris).call().await?;
    ///
    /// // Get data for the hour closest to 2022-01-01 15:45:00 UTC
    /// // This should round up to 16:00:00 UTC
    /// let target_dt = Utc.with_ymd_and_hms(2022, 1, 1, 15, 45, 0).unwrap();
    ///
    /// let hour_data_lazy = hourly_lazy.get_at(target_dt)?;
    ///
    /// // Collect the result (should be one row if data exists for 16:00)
    /// let df = hour_data_lazy.frame.collect()?;
    /// if df.height() == 1 {
    ///     println!("Data for hour nearest {}:\n{}", target_dt, df);
    ///     // Verify the datetime column shows 16:00:00
    ///     let result_dt_str = df.column("datetime")?.datetime()?.get(0).unwrap().to_string();
    ///     assert!(result_dt_str.contains("16:00:00"));
    /// } else {
    ///     println!("No data found for the hour nearest {} at the nearest station.", target_dt);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `datetime` cannot be resolved.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
    pub fn get_at(&self, datetime: impl AnyDateTime) -> Result<HourlyLazyFrame, MeteostatError> {
        let date_utc = datetime
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start; // Use the start of the range from AnyDateTime

        // Round to the nearest hour
        let rounded_hour_start_utc = if date_utc.minute() >= 30 {
            // Round up: Add an hour, then truncate minutes/seconds/nanos
            (date_utc + Duration::hours(1))
                .with_minute(0)
                .and_then(|dt| dt.with_second(0))
                .and_then(|dt| dt.with_nanosecond(0))
                .expect("Truncating to start of hour after adding hour failed unexpectedly")
        } else {
            // Round down: Truncate minutes/seconds/nanos
            date_utc
                .with_minute(0)
                .and_then(|dt| dt.with_second(0))
                .and_then(|dt| dt.with_nanosecond(0))
                .expect("Truncating to start of hour failed unexpectedly")
        };

        // Filter the LazyFrame for the exact start of the rounded hour (using NaiveDateTime)
        Ok(self.filter(col("datetime").eq(lit(rounded_hour_start_utc.naive_utc()))))
    }

    /// Filters the hourly data to include only records within a specified datetime period.
    ///
    /// This is a convenience method that accepts types implementing [`DateTimePeriod`],
    /// such as [`crate::Year`], [`crate::Month`], or `NaiveDate`. It resolves the period
    /// into a start and end `DateTime<Utc>` and then calls `get_range`.
    ///
    /// # Arguments
    ///
    /// * `period` - The time period (e.g., a specific day or year), implementing [`DateTimePeriod`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `HourlyLazyFrame` filtered by the period's datetime range,
    /// or a [`MeteostatError::DateParsingError`] if the period cannot be resolved.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year};
    /// use chrono::NaiveDate;
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let rome = LatLon(41.90, 12.50);
    ///
    /// let hourly_lazy = client.hourly().location(rome).call().await?;
    ///
    /// // Get all available hourly data for January 1st, 2023
    /// let target_day = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    /// let day_data_lazy = hourly_lazy.get_for_period(target_day)?;
    ///
    /// // Collect the result (expecting up to 24 rows)
    /// let df = day_data_lazy.frame.collect()?;
    /// println!("Data for {} ({} hours found):\n{}", target_day, df.height(), df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `period` cannot be resolved to a datetime range.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
    pub fn get_for_period(
        &self,
        period: impl DateTimePeriod,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        let datetime_period = period
            .get_datetime_period()
            .ok_or(MeteostatError::DateParsingError)?;
        // Delegate to get_range using the resolved start and end datetimes
        self.get_range(datetime_period.start, datetime_period.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError};
    // Import needed types
    use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    use polars::prelude::*;

    // Helper to fetch hourly data for tests - uses Paris Orly ("07147")
    // This station often has good hourly coverage. Berlin Tempelhof ("10384") also works.
    async fn get_test_hourly_frame() -> Result<HourlyLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.hourly().station("07147").await // Paris Orly Airport
    }

    fn ms_to_datetime(ms: i64) -> NaiveDateTime {
        DateTime::from_timestamp_millis(ms).unwrap().naive_utc()
    }

    #[tokio::test]
    async fn test_hourly_frame_new_schema() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;

        // Expected columns for hourly data (subset for testing)
        let expected_cols = ["datetime", "temp", "dwpt", "rhum", "prcp", "wspd", "pres"];

        let df = hourly_lazy.frame.collect()?; // Collect to check schema
        let actual_cols = df.get_column_names();

        for col_name in expected_cols {
            assert!(
                actual_cols.contains(&&PlSmallStr::from_str(col_name)),
                "Expected column '{}' not found in hourly data",
                col_name
            );
        }
        // Check datetime column type
        let dt_col = df.column("datetime")?;
        assert!(matches!(
            dt_col.dtype(),
            DataType::Datetime(TimeUnit::Milliseconds, None)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_frame_filter_temp() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;

        // Filter for hours with temp < 0.0
        let filtered_lazy = hourly_lazy.filter(col("temp").lt(lit(0.0f64)));
        let df = filtered_lazy.frame.collect()?;

        if df.height() > 0 {
            println!("Found {} hours with temp < 0.0", df.height());
            let temp_series = df.column("temp")?.f64()?;
            assert!(temp_series.into_iter().all(|opt_temp| match opt_temp {
                Some(t) => t < 0.0,
                None => true, // Allow nulls
            }));
        } else {
            println!("No hours found with temp < 0.0 in the test data subset.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_frame_get_at_specific_datetime() -> Result<(), Box<dyn std::error::Error>>
    {
        let hourly_lazy = get_test_hourly_frame().await?;
        // Choose a datetime likely to exist
        let target_dt_precise = Utc.with_ymd_and_hms(2021, 5, 20, 14, 25, 0).unwrap();
        // This should round down to 14:00:00
        let expected_hour_start_naive = NaiveDate::from_ymd_opt(2021, 5, 20)
            .unwrap()
            .and_hms_opt(14, 0, 0)
            .unwrap();

        let result_lazy = hourly_lazy.get_at(target_dt_precise)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(
            df.height(),
            1,
            "Expected exactly one row for hour nearest {}",
            target_dt_precise
        );

        // Verify the datetime in that row matches the expected rounded hour start
        let dt_ms = df.column("datetime")?.datetime()?.get(0).unwrap();
        let retrieved_naive_dt = ms_to_datetime(dt_ms);
        assert_eq!(retrieved_naive_dt, expected_hour_start_naive);

        // Test rounding up
        let target_dt_round_up = Utc.with_ymd_and_hms(2021, 5, 20, 14, 35, 0).unwrap();
        // Should round up to 15:00:00
        let expected_hour_start_round_up_naive = NaiveDate::from_ymd_opt(2021, 5, 20)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap();

        let result_round_up_lazy = hourly_lazy.get_at(target_dt_round_up)?;
        let df_round_up = result_round_up_lazy.frame.collect()?;

        assert_eq!(
            df_round_up.height(),
            1,
            "Expected exactly one row for hour nearest {} (rounds up)",
            target_dt_round_up
        );
        let dt_ms = df_round_up.column("datetime")?.datetime()?.get(0).unwrap();
        let retrieved_round_up_naive_dt = ms_to_datetime(dt_ms);
        assert_eq!(
            retrieved_round_up_naive_dt,
            expected_hour_start_round_up_naive
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_frame_get_range_datetime() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        let start_dt = Utc.with_ymd_and_hms(2022, 8, 10, 6, 0, 0).unwrap();
        let end_dt = Utc.with_ymd_and_hms(2022, 8, 10, 11, 59, 59).unwrap(); // Up to (but not including) 12:00
        let expected_rows = 6; // 06:00, 07:00, 08:00, 09:00, 10:00, 11:00

        let result_lazy = hourly_lazy.get_range(start_dt, end_dt)?;
        let df = result_lazy.frame.collect()?;

        // Allow for potentially missing hours
        assert!(
            df.height() <= expected_rows,
            "Should have at most {} rows for the 6-hour period",
            expected_rows
        );
        assert!(
            df.height() > 0,
            "Should find some data for the period {}-{}",
            start_dt,
            end_dt
        );

        // Verify datetimes are within the range (using NaiveDateTime for comparison)
        let start_naive = start_dt.naive_utc();
        let end_naive = end_dt.naive_utc();
        let dt_series = df.column("datetime")?.datetime()?; // ChronoNaiveDateTime series
        assert!(dt_series.into_iter().all(|opt_ndt| {
            match opt_ndt {
                Some(ndt) => ms_to_datetime(ndt) >= start_naive && ms_to_datetime(ndt) <= end_naive,
                None => false, // Datetime should not be null
            }
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_frame_get_for_period_day() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        let target_day = NaiveDate::from_ymd_opt(2020, 11, 5).unwrap();

        let result_lazy = hourly_lazy.get_for_period(target_day)?;
        let df = result_lazy.frame.collect()?;

        // Expect up to 24 rows for a full day
        assert!(
            df.height() <= 24,
            "Should have at most 24 rows for day {}",
            target_day
        );
        assert!(
            df.height() > 12, // Expect decent coverage for a day
            "Should have found a reasonable number of hours for day {}",
            target_day
        );

        // Verify all datetimes fall on the target day
        let dt_series = df.column("datetime")?.datetime()?;
        assert!(dt_series.into_iter().all(|opt_ndt| {
            match opt_ndt {
                Some(ndt) => ms_to_datetime(ndt).date() == target_day,
                None => false,
            }
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_frame_chaining_period_and_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let hourly_lazy = get_test_hourly_frame().await?;
        let target_day = NaiveDate::from_ymd_opt(2022, 1, 15).unwrap();

        // Get data for the target day, then filter for hours with wind speed > 15 km/h (approx 4.17 m/s)
        // Meteostat wind speed is in km/h, so filter directly on that.
        let windy_hours_lazy = hourly_lazy
            .get_for_period(target_day)?
            .filter(col("wspd").gt(lit(15.0f64)));

        let df = windy_hours_lazy.frame.collect()?;

        println!(
            "Found {} hours on {} with wspd > 15.0 km/h",
            df.height(),
            target_day
        );
        assert!(df.height() < 24); // Should be fewer than all hours

        if df.height() > 0 {
            // Verify date and wind speed condition
            let dt_series = df.column("datetime")?.datetime()?;
            let wspd_series = df.column("wspd")?.f64()?;

            for i in 0..df.height() {
                let ndt = dt_series.get(i).unwrap();
                let wspd_val = wspd_series.get(i).unwrap_or(0.0); // Default to 0 if null

                assert_eq!(ms_to_datetime(ndt).date(), target_day);
                assert!(wspd_val > 15.0);
            }
        }

        Ok(())
    }
}
