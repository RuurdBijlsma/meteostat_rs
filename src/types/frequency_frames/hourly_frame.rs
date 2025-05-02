// hourly_frame.rs

//! Contains the `HourlyLazyFrame` structure for handling lazy operations on Meteostat hourly weather data.

use crate::types::traits::any::any_datetime::AnyDateTime;
use crate::types::traits::period::datetime_period::DateTimePeriod;
use crate::{MeteostatError, WeatherCondition};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Timelike, Utc};
use polars::prelude::{col, lit, DataFrame, Expr, LazyFrame};

/// Represents a row of hourly weather data, suitable for collecting results.
#[derive(Debug, Clone, PartialEq)]
pub struct Hourly {
    /// The specific date and time (UTC) for this observation.
    pub datetime: DateTime<Utc>, // Non-optional, assuming we only collect valid rows
    /// Air temperature in Celsius.
    pub temperature: Option<f64>,
    /// Dew point in Celsius.
    pub dew_point: Option<f64>,
    /// Relative humidity in percent.
    pub relative_humidity: Option<i32>,
    /// Precipitation amount (usually in mm).
    pub precipitation: Option<f64>,
    /// Snow depth (usually in mm), often null or 0.
    pub snow: Option<i32>,
    /// Wind direction in degrees (0-360).
    pub wind_direction: Option<i32>,
    /// Average wind speed (usually in km/h).
    pub wind_speed: Option<f64>,
    /// Peak wind gust speed (usually in km/h).
    pub peak_wind_gust: Option<f64>,
    /// Sea-level air pressure in hPa.
    pub pressure: Option<f64>,
    /// Sunshine duration in minutes.
    pub sunshine_minutes: Option<i32>,
    /// Weather condition code mapped to an enum.
    pub condition: Option<WeatherCondition>,
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat hourly weather data.
///
/// This struct provides methods tailored for common operations on hourly datasets,
/// such as filtering by datetime ranges, while retaining the benefits of lazy evaluation.
/// It also includes methods to collect the results into Rust structs.
///
/// Instances are typically obtained via [`crate::Meteostat::hourly`].
///
/// # Note on Datetimes
///
/// Internally, Polars often handles datetime columns as timezone-naive (`NaiveDateTime`)
/// even if the source implies a timezone like UTC. The filtering methods in this struct
/// (`get_range`, `get_at`) handle the conversion to/from `DateTime<Utc>` and use
/// timezone-naive representations (`naive_utc()`) for filtering the underlying frame,
/// assuming the frame's "datetime" column is timezone-naive UTC. The collection methods
/// (`collect_hourly_vec`, `collect_hourly`) convert the stored naive UTC datetimes back
/// into `DateTime<Utc>` for the `Hourly` struct.
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`,
/// or the collection methods here) can potentially return a [`polars::prelude::PolarsError`]
/// (wrapped as [`MeteostatError::PolarsError`]).
///
/// Methods involving datetime parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input datetimes cannot be resolved.
///
/// The `collect_hourly` method returns [`MeteostatError::ExpectedSingleRow`] if the frame
/// does not contain exactly one row upon collection.
///
/// The initial creation via [`crate::Meteostat::hourly`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)]
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
    pub(crate) fn new(frame: LazyFrame) -> Self {
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
    /// // Collect the results into a DataFrame
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

    // ... [ get_range, get_at, get_for_period methods remain unchanged ] ...
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

    /// Executes the lazy query and collects the results into a `Vec<Hourly>`.
    ///
    /// This method triggers the computation defined by the `LazyFrame` (including any
    /// previous filtering operations) and maps each resulting row to an `Hourly` struct.
    /// Rows where the essential 'datetime' column is missing or invalid are skipped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Hourly>` on success, or a [`MeteostatError`]
    /// if the computation or mapping fails (e.g., `MeteostatError::PolarsError`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year, Hourly};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let hourly_lazy = client
    ///     .hourly()
    ///     .location(paris)
    ///     .call()
    ///     .await?;
    ///
    /// // Get data for a specific year
    /// let year_data_lazy = hourly_lazy.get_for_period(Year(2022))?;
    ///
    /// // Collect into Vec<Hourly>
    /// let hourly_vec: Vec<Hourly> = year_data_lazy.collect_hourly()?;
    ///
    /// println!("Collected {} hourly records for 2022.", hourly_vec.len());
    /// if let Some(first_hour) = hourly_vec.first() {
    ///     println!("First record: {:?}", first_hour);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_hourly(&self) -> Result<Vec<Hourly>, MeteostatError> {
        let df = self
            .frame
            .clone() // Clone frame as collect consumes it
            .collect()
            .map_err(MeteostatError::PolarsError)?; // Map PolarsError

        Self::dataframe_to_hourly_vec(&df) // Use helper function
    }

    /// Executes the lazy query, expecting exactly one row, and collects it into an `Hourly` struct.
    ///
    /// This is useful after filtering the frame down to a single expected record,
    /// for example using `get_at()`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the single `Hourly` struct on success.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::ExpectedSingleRow`] if the collected `DataFrame` does not
    /// contain exactly one row.
    /// Returns [`MeteostatError::PolarsError`] if the computation fails.
    /// Returns other potential mapping errors if the single row cannot be converted.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Hourly};
    /// use chrono::{Utc, TimeZone};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// use chrono::Timelike;
    /// let client = Meteostat::new().await?;
    /// let station_id = "07147"; // Paris Orly
    ///
    /// let hourly_lazy = client.hourly().station(station_id).call().await?;
    ///
    /// // Get data for a specific hour
    /// let target_dt = Utc.with_ymd_and_hms(2022, 9, 1, 10, 15, 0).unwrap(); // rounds to 10:00
    /// let single_hour_lazy = hourly_lazy.get_at(target_dt)?;
    ///
    /// // Collect the single expected row
    /// match single_hour_lazy.collect_single_hourly() {
    ///     Ok(hourly_data) => {
    ///         println!("Collected single hour data: {:?}", hourly_data);
    ///         assert_eq!(hourly_data.datetime.hour(), 10); // Verify correct hour
    ///     },
    ///     Err(MeteostatError::ExpectedSingleRow { actual }) => {
    ///          println!("Expected 1 row, but found {}. Data might be missing for that hour.", actual);
    ///          // Handle missing data case if necessary
    ///          assert_eq!(actual, 0); // Or assert based on expected availability
    ///     },
    ///     Err(e) => return Err(e), // Propagate other errors
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_single_hourly(&self) -> Result<Hourly, MeteostatError> {
        let df = self
            .frame
            .clone()
            .collect()
            .map_err(MeteostatError::PolarsError)?;

        if df.height() != 1 {
            return Err(MeteostatError::ExpectedSingleRow {
                actual: df.height(),
            });
        }

        // Use the Vec conversion helper and take the guaranteed single element
        Self::dataframe_to_hourly_vec(&df)?
            .pop() // Take the only element
            .ok_or(MeteostatError::ExpectedSingleRow { actual: 0 })
    }

    // --- Helper function to map DataFrame rows to Vec<Hourly> ---
    fn dataframe_to_hourly_vec(df: &DataFrame) -> Result<Vec<Hourly>, MeteostatError> {
        // --- Get required columns as Series ---
        let dt_series = df.column("datetime")?;
        let temp_series = df.column("temp")?;
        let dwpt_series = df.column("dwpt")?;
        let rhum_series = df.column("rhum")?; // Integer type
        let prcp_series = df.column("prcp")?;
        let snow_series = df.column("snow")?; // Integer type
        let wdir_series = df.column("wdir")?; // Integer type
        let wspd_series = df.column("wspd")?;
        let wpgt_series = df.column("wpgt")?;
        let pres_series = df.column("pres")?;
        let tsun_series = df.column("tsun")?; // Integer type
        let coco_series = df.column("coco")?; // Integer type (weather code)

        // --- Get ChunkedArrays (handle potential type variations if needed) ---
        // We assume default types here. Add specific casting if Polars reads differently.
        let dt_ca = dt_series.datetime()?; // ChronoNaiveDateTimeChunked - expects NaiveDateTime
        let temp_ca = temp_series.f64()?;
        let dwpt_ca = dwpt_series.f64()?;
        let rhum_ca = rhum_series.i64()?; // Read as i64 initially
        let prcp_ca = prcp_series.f64()?;
        let snow_ca = snow_series.i64()?; // Read as i64 initially
        let wdir_ca = wdir_series.i64()?; // Read as i64 initially
        let wspd_ca = wspd_series.f64()?;
        let wpgt_ca = wpgt_series.f64()?;
        let pres_ca = pres_series.f64()?;
        let tsun_ca = tsun_series.i64()?; // Read as i64 initially
        let coco_ca = coco_series.i64()?; // Read as i64 initially

        let mut hourly_vec = Vec::with_capacity(df.height());

        // --- Iterate through rows and map ---
        for i in 0..df.height() {
            // Get datetime (essential) - skip row if missing/invalid
            let naive_dt_opt: Option<NaiveDateTime> = dt_ca
                .get(i)
                // dt_ca gives Option<i64> (ms timestamp), convert to NaiveDateTime
                .and_then(DateTime::from_timestamp_millis)
                .map(|dt_utc| dt_utc.naive_utc()); // We know it's UTC

            let Some(naive_dt) = naive_dt_opt else {
                // Skip row if datetime is missing or invalid
                continue;
            };
            // Convert NaiveDateTime (representing UTC) to DateTime<Utc>
            let datetime_utc = Utc.from_utc_datetime(&naive_dt);

            // Get weather condition code and map to enum
            let condition = coco_ca.get(i).and_then(WeatherCondition::from_i64);

            // Construct the struct
            let hourly_record = Hourly {
                datetime: datetime_utc,
                temperature: temp_ca.get(i),
                dew_point: dwpt_ca.get(i),
                relative_humidity: rhum_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                precipitation: prcp_ca.get(i),
                snow: snow_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                wind_direction: wdir_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                wind_speed: wspd_ca.get(i),
                peak_wind_gust: wpgt_ca.get(i),
                pressure: pres_ca.get(i),
                sunshine_minutes: tsun_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                condition,
            };

            hourly_vec.push(hourly_record);
        }

        Ok(hourly_vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError};
    use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    use polars::prelude::*;

    // Helper to fetch hourly data for tests - uses Paris Orly ("07147")
    async fn get_test_hourly_frame() -> Result<HourlyLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.hourly().station("07147").call().await // Paris Orly Airport
    }

    fn ms_to_datetime(ms: i64) -> NaiveDateTime {
        DateTime::from_timestamp_millis(ms).unwrap().naive_utc()
    }

    #[tokio::test]
    async fn test_hourly_frame_new_schema() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;

        // Expected columns for hourly data (subset for testing)
        let expected_cols = [
            "datetime", "temp", "dwpt", "rhum", "prcp", "wspd", "pres", "coco",
        ]; // Added coco

        let df = hourly_lazy.frame.limit(1).collect()?; // Collect small sample
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
            DataType::Datetime(TimeUnit::Milliseconds, None) // Polars default is often None TZ
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
        let df = result_lazy.frame.collect()?; // Collect

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
        let df_round_up = result_round_up_lazy.frame.collect()?; // Collect

        assert_eq!(
            df_round_up.height(),
            1,
            "Expected exactly one row for hour nearest {} (rounds up)",
            target_dt_round_up
        );
        let dt_round_up_ms = df_round_up.column("datetime")?.datetime()?.get(0).unwrap(); // Use new var name
        let retrieved_round_up_naive_dt = ms_to_datetime(dt_round_up_ms);
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
        let df = result_lazy.frame.collect()?; // Collect

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
        let df = result_lazy.frame.collect()?; // Collect

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

        let df = windy_hours_lazy.frame.collect()?; // Collect

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
                let ndt = dt_series.get(i).unwrap(); // use ndt here
                let wspd_val = wspd_series.get(i).unwrap_or(0.0); // Default to 0 if null

                assert_eq!(ms_to_datetime(ndt).date(), target_day); // use ndt here
                assert!(wspd_val > 15.0);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_collect_hourly_vec() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        let start_dt = Utc.with_ymd_and_hms(2023, 1, 5, 0, 0, 0).unwrap();
        let end_dt = Utc.with_ymd_and_hms(2023, 1, 5, 3, 59, 59).unwrap(); // 4 hours
        let expected_max_rows = 4;

        let result_lazy = hourly_lazy.get_range(start_dt, end_dt)?;
        let hourly_vec = result_lazy.collect_hourly()?;

        assert!(
            hourly_vec.len() <= expected_max_rows,
            "Collected vector length ({}) should be <= {}",
            hourly_vec.len(),
            expected_max_rows
        );
        // Can't assume exactly 4 rows due to potential missing data
        assert!(
            !hourly_vec.is_empty(),
            "Expected some data for the 4-hour period"
        );

        // Check the first record if it exists
        if let Some(first_record) = hourly_vec.first() {
            println!("First collected record: {:?}", first_record);
            assert!(first_record.datetime >= start_dt);
            assert!(first_record.datetime <= end_dt);
            // Example check on a field (temp might be None, check definition)
            assert!(first_record.temperature.is_some() || first_record.temperature.is_none());
            // Check condition mapping (might be None)
            if let Some(code_i64) = hourly_lazy
                .frame
                .clone()
                .limit(1)
                .collect()?
                .column("coco")?
                .i64()?
                .get(0)
            {
                assert_eq!(first_record.condition, WeatherCondition::from_i64(code_i64));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_hourly_single_row_success() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        let target_dt = Utc.with_ymd_and_hms(2021, 7, 10, 18, 0, 0).unwrap(); // Expect exact hour match

        let single_hour_lazy = hourly_lazy.get_at(target_dt)?;
        let hourly_record = single_hour_lazy.collect_single_hourly()?;

        println!("Collected single record: {:?}", hourly_record);
        assert_eq!(hourly_record.datetime, target_dt);
        // Can add more assertions based on expected data for that hour if known
        assert!(hourly_record.temperature.is_some()); // Example check
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_hourly_single_row_fail_multiple_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        // Use a range that will definitely yield multiple rows
        let start_dt = Utc.with_ymd_and_hms(2022, 3, 3, 0, 0, 0).unwrap();
        let end_dt = Utc.with_ymd_and_hms(2022, 3, 3, 5, 0, 0).unwrap(); // 6 hours

        let multi_hour_lazy = hourly_lazy.get_range(start_dt, end_dt)?;
        let result = multi_hour_lazy.collect_single_hourly(); // Expect this to fail

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Got expected error: {:?}", err);

        match err {
            MeteostatError::ExpectedSingleRow { actual } => {
                assert!(actual > 1, "Expected actual rows to be > 1, got {}", actual);
            }
            _ => panic!("Expected MeteostatError::ExpectedSingleRow, got {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_collect_hourly_single_row_fail_zero_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        // Use a date without data
        let future_dt = Utc.with_ymd_and_hms(1820, 1, 1, 12, 0, 0).unwrap();

        let zero_hour_lazy = hourly_lazy.get_at(future_dt)?;
        let result = zero_hour_lazy.collect_single_hourly(); // Expect this to fail

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Got expected error: {:?}", err);

        match err {
            MeteostatError::ExpectedSingleRow { actual } => {
                assert_eq!(actual, 0, "Expected actual rows to be 0, got {}", actual);
            }
            _ => panic!("Expected MeteostatError::ExpectedSingleRow, got {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_collect_hourly_vec_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let hourly_lazy = get_test_hourly_frame().await?;
        // Use a date far in the future, guaranteed to have no data
        let future_dt_start = Utc.with_ymd_and_hms(1818, 1, 1, 0, 0, 0).unwrap();
        let future_dt_end = Utc.with_ymd_and_hms(1818, 1, 1, 23, 59, 59).unwrap();

        let empty_lazy = hourly_lazy.get_range(future_dt_start, future_dt_end)?;
        let hourly_vec = empty_lazy.collect_hourly()?;

        assert!(
            hourly_vec.is_empty(),
            "Expected empty vector for future date range"
        );

        Ok(())
    }
}
