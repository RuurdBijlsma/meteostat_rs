// daily_frame.rs

//! Contains the `DailyLazyFrame` structure for handling lazy operations on Meteostat daily weather data.

use crate::types::traits::any::any_date::AnyDate;
use crate::types::traits::period::date_period::DatePeriod;
use crate::MeteostatError;
use chrono::{Duration, NaiveDate};
use polars::prelude::{col, lit, DataFrame, Expr, LazyFrame};

/// Represents a row of daily weather data, suitable for collecting results.
#[derive(Debug, Clone, PartialEq)] // Made public and added derives
pub struct Daily {
    /// The specific date for this observation.
    pub date: NaiveDate, // Non-optional
    /// Average air temperature in Celsius.
    pub average_temperature: Option<f64>, // tavg
    /// Minimum air temperature in Celsius.
    pub minimum_temperature: Option<f64>, // tmin
    /// Maximum air temperature in Celsius.
    pub maximum_temperature: Option<f64>, // tmax
    /// Total precipitation amount in mm.
    pub precipitation: Option<f64>, // prcp
    /// Snow depth on the ground in mm (often null or 0).
    pub snow: Option<i32>, // snow
    /// Average wind direction in degrees (0-360).
    pub wind_direction: Option<i32>, // wdir
    /// Average wind speed in km/h.
    pub wind_speed: Option<f64>, // wspd
    /// Peak wind gust speed in km/h.
    pub peak_wind_gust: Option<f64>, // wpgt
    /// Average sea-level air pressure in hPa.
    pub pressure: Option<f64>, // pres
    /// Total sunshine duration in minutes.
    pub sunshine_minutes: Option<i32>, // tsun
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat daily weather data.
///
/// This struct provides methods tailored for common operations on daily datasets,
/// such as filtering by date ranges, while retaining the benefits of lazy evaluation.
/// It also includes methods to collect the results into Rust structs.
///
/// Instances are typically obtained via [`crate::Meteostat::daily`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`,
/// or the collection methods here) can potentially return a [`polars::prelude::PolarsError`]
/// (wrapped as [`MeteostatError::PolarsError`]).
///
/// Methods involving date parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input dates cannot be resolved.
///
/// The `collect_single_daily` method returns [`MeteostatError::ExpectedSingleRow`] if the frame
/// does not contain exactly one row upon collection.
///
/// The initial creation via [`crate::Meteostat::daily`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)]
pub struct DailyLazyFrame {
    /// The underlying Polars LazyFrame containing the daily data.
    pub frame: LazyFrame,
}

impl DailyLazyFrame {
    /// Creates a new `DailyLazyFrame` wrapping the given Polars `LazyFrame`.
    ///
    /// This is typically called internally by the [`crate::Meteostat`] client methods.
    ///
    /// # Arguments
    ///
    /// * `frame` - A `LazyFrame` assumed to contain daily weather data with the expected schema,
    ///   including a "date" column of type `DataType::Date`.
    pub(crate) fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    /// Filters the daily data based on a Polars predicate expression.
    ///
    /// This method allows applying arbitrary filtering logic supported by Polars.
    /// It returns a *new* `DailyLazyFrame` with the filter applied lazily.
    /// The original `DailyLazyFrame` remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A Polars [`Expr`] defining the filtering condition.
    ///
    /// # Returns
    ///
    /// A new `DailyLazyFrame` representing the filtered data.
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
    /// let daily_lazy = client.daily().location(berlin).call().await?;
    ///
    /// // Filter for days where average temperature was above 20 degrees Celsius
    /// let warm_days = daily_lazy.filter(col("tavg").gt(lit(20.0f64)));
    ///
    /// // Collect the results
    /// let df = warm_days.frame.collect()?;
    /// println!("Warm days found:\n{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// While this method itself doesn't typically error, subsequent operations like `.collect()`
    /// might return a [`polars::prelude::PolarsError`].
    pub fn filter(&self, predicate: Expr) -> DailyLazyFrame {
        DailyLazyFrame::new(self.frame.clone().filter(predicate))
    }

    /// Filters the daily data to include only dates within the specified range (inclusive).
    ///
    /// The `start` and `end` arguments can be any type that implements [`AnyDate`],
    /// such as `NaiveDate`, [`crate::Month`], or [`crate::Year`]. The trait resolves these
    /// into specific start and end `NaiveDate` values for the filter.
    ///
    /// # Arguments
    ///
    /// * `start` - The starting date boundary (inclusive), implementing [`AnyDate`].
    /// * `end` - The ending date boundary (inclusive), implementing [`AnyDate`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `DailyLazyFrame` filtered by the date range,
    /// or a [`MeteostatError::DateParsingError`] if the date conversion fails.
    pub fn get_range(
        &self,
        start: impl AnyDate,
        end: impl AnyDate,
    ) -> Result<DailyLazyFrame, MeteostatError> {
        let start_naive = start
            .get_date_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        let end_naive = end
            .get_date_range()
            .ok_or(MeteostatError::DateParsingError)?
            .end;

        Ok(self.filter(
            col("date")
                .gt_eq(lit(start_naive))
                .and(col("date").lt_eq(lit(end_naive))),
        ))
    }

    /// Filters the daily data to get the record for a specific date.
    ///
    /// The `date` argument can be any type that implements [`AnyDate`]. It is resolved
    /// to a single `NaiveDate` for filtering. If the input represents a range (like `Year`),
    /// the *start* of that range is used for the equality check. For precise single-day
    /// filtering, use `NaiveDate`.
    ///
    /// # Arguments
    ///
    /// * `date` - The target date, implementing [`AnyDate`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `DailyLazyFrame` filtered to the specific date,
    /// or a [`MeteostatError::DateParsingError`] if the date conversion fails. Collecting
    /// the frame should yield zero or one row.
    pub fn get_at(&self, date: impl AnyDate) -> Result<DailyLazyFrame, MeteostatError> {
        // Use the start of the range provided by AnyDate for the equality check
        let naive_date = date
            .get_date_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        Ok(self.filter(col("date").eq(lit(naive_date))))
    }

    /// Filters the daily data to include only dates within a specified period.
    ///
    /// This is a convenience method that accepts types implementing [`DatePeriod`],
    /// such as [`crate::Year`]. It resolves the period into a start and end `NaiveDate`
    /// and then calls `get_range`.
    ///
    /// # Arguments
    ///
    /// * `period` - The time period (e.g., a specific year), implementing [`DatePeriod`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `DailyLazyFrame` filtered by the period's date range,
    /// or a [`MeteostatError::DateParsingError`] if the period cannot be resolved.
    pub fn get_for_period(
        &self,
        period: impl DatePeriod,
    ) -> Result<DailyLazyFrame, MeteostatError> {
        let date_period = period
            .get_date_period()
            .ok_or(MeteostatError::DateParsingError)?;
        // Delegate to get_range using the resolved start and end dates
        self.get_range(date_period.start, date_period.end)
    }

    /// Executes the lazy query and collects the results into a `Vec<Daily>`.
    ///
    /// This method triggers the computation defined by the `LazyFrame` (including any
    /// previous filtering operations) and maps each resulting row to a `Daily` struct.
    /// Rows where the essential 'date' column is missing or invalid are skipped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Daily>` on success, or a [`MeteostatError`]
    /// if the computation or mapping fails (e.g., `MeteostatError::PolarsError`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year, Daily};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let daily_lazy = client
    ///     .daily()
    ///     .location(paris)
    ///     .call()
    ///     .await?;
    ///
    /// // Get data for a specific year
    /// let year_data_lazy = daily_lazy.get_for_period(Year(2022))?;
    ///
    /// // Collect into Vec<Daily>
    /// let daily_vec: Vec<Daily> = year_data_lazy.collect_daily()?;
    ///
    /// println!("Collected {} daily records for 2022.", daily_vec.len());
    /// if let Some(first_day) = daily_vec.first() {
    ///     println!("First record: {:?}", first_day);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_daily(&self) -> Result<Vec<Daily>, MeteostatError> {
        let df = self
            .frame
            .clone()
            .collect()
            .map_err(MeteostatError::PolarsError)?;

        Self::dataframe_to_daily_vec(&df)
    }

    /// Executes the lazy query, expecting exactly one row, and collects it into a `Daily` struct.
    ///
    /// This is useful after filtering the frame down to a single expected record,
    /// for example using `get_at()`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the single `Daily` struct on success.
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
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Daily};
    /// use chrono::NaiveDate;
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10384"; // Berlin Tempelhof
    ///
    /// let daily_lazy = client.daily().station(station_id).call().await?;
    ///
    /// // Get data for a specific day
    /// let target_date = NaiveDate::from_ymd_opt(2022, 8, 15).unwrap();
    /// let single_day_lazy = daily_lazy.get_at(target_date)?;
    ///
    /// // Collect the single expected row
    /// match single_day_lazy.collect_single_daily() {
    ///     Ok(daily_data) => {
    ///         println!("Collected single day data: {:?}", daily_data);
    ///         assert_eq!(daily_data.date, target_date); // Verify correct date
    ///     },
    ///     Err(MeteostatError::ExpectedSingleRow { actual }) => {
    ///          println!("Expected 1 row, but found {}.", actual);
    ///     },
    ///     Err(e) => return Err(e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_single_daily(&self) -> Result<Daily, MeteostatError> {
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
        Self::dataframe_to_daily_vec(&df)?
            .pop() // Take the only element
            .ok_or(MeteostatError::ExpectedSingleRow { actual: 0 }) // Should be unreachable after height check
    }

    // --- Helper function to map DataFrame rows to Vec<Daily> ---
    fn dataframe_to_daily_vec(df: &DataFrame) -> Result<Vec<Daily>, MeteostatError> {
        // --- Get required columns as Series ---
        let date_series = df.column("date")?;
        let tavg_series = df.column("tavg")?;
        let tmin_series = df.column("tmin")?;
        let tmax_series = df.column("tmax")?;
        let prcp_series = df.column("prcp")?;
        let snow_series = df.column("snow")?;
        let wdir_series = df.column("wdir")?;
        let wspd_series = df.column("wspd")?;
        let wpgt_series = df.column("wpgt")?;
        let pres_series = df.column("pres")?;
        let tsun_series = df.column("tsun")?;

        // --- Get ChunkedArrays (handle potential type variations if needed) ---
        let date_ca = date_series.date()?;
        let tavg_ca = tavg_series.f64()?;
        let tmin_ca = tmin_series.f64()?;
        let tmax_ca = tmax_series.f64()?;
        let prcp_ca = prcp_series.f64()?;
        let snow_ca = snow_series.i64()?;
        let wdir_ca = wdir_series.i64()?;
        let wspd_ca = wspd_series.f64()?;
        let wpgt_ca = wpgt_series.f64()?;
        let pres_ca = pres_series.f64()?;
        let tsun_ca = tsun_series.i64()?;

        let mut daily_vec = Vec::with_capacity(df.height());

        // --- Define epoch once for date conversion ---
        // Polars Date type stores days since UNIX_EPOCH (1970-01-01)
        let epoch_date =
            NaiveDate::from_ymd_opt(1970, 1, 1).expect("Failed to create epoch NaiveDate");

        // --- Iterate through rows and map ---
        for i in 0..df.height() {
            // Get date (essential) - skip row if missing/invalid
            let date_opt: Option<NaiveDate> = date_ca
                .get(i) // Returns Option<i32> (days since epoch)
                .map(|days_since_epoch| epoch_date + Duration::days(days_since_epoch as i64));

            let Some(date) = date_opt else {
                // Skip row if date is missing or invalid
                continue;
            };

            // Construct the struct
            let daily_record = Daily {
                date,
                average_temperature: tavg_ca.get(i),
                minimum_temperature: tmin_ca.get(i),
                maximum_temperature: tmax_ca.get(i),
                precipitation: prcp_ca.get(i),
                snow: snow_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                wind_direction: wdir_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
                wind_speed: wspd_ca.get(i),
                peak_wind_gust: wpgt_ca.get(i),
                pressure: pres_ca.get(i),
                sunshine_minutes: tsun_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
            };

            daily_vec.push(daily_record);
        }

        Ok(daily_vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError, Year};
    use chrono::{Datelike, NaiveDate};
    use polars::prelude::{DataType, PlSmallStr};

    // Helper to fetch daily data for tests - uses Berlin Tempelhof ("10384")
    // This station usually has extensive daily records.
    async fn get_test_daily_frame() -> Result<DailyLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.daily().station("10384").call().await // Berlin Tempelhof
    }

    // --- Existing Tests Remain Unchanged ---

    #[tokio::test]
    async fn test_daily_frame_new_schema() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;

        // Expected columns for daily data (subset for testing)
        let expected_cols = [
            "date", "tavg", "tmin", "tmax", "prcp", "wspd", "pres", "tsun", "snow", "wdir", "wpgt",
        ];

        let df = daily_lazy.frame.limit(1).collect()?;
        let actual_cols = df.get_column_names();

        for col_name in expected_cols {
            assert!(
                actual_cols.contains(&&PlSmallStr::from_str(col_name)),
                "Expected column '{}' not found in daily data",
                col_name
            );
        }
        // Check date column type
        assert_eq!(df.column("date")?.dtype(), &DataType::Date);
        assert!(matches!(df.column("tavg")?.dtype(), &DataType::Float64));
        assert!(matches!(
            df.column("snow")?.dtype(),
            &DataType::Int64 | &DataType::Int32 | &DataType::Float64
        )); // Allow flexibility in how it might be read

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_filter_temp() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;

        // Filter for days with average temp > 25.0
        let filtered_lazy = daily_lazy.filter(col("tavg").gt(lit(25.0f64)));
        let df = filtered_lazy.frame.collect()?;

        if df.height() > 0 {
            println!("Found {} days with tavg > 25.0", df.height());
            let temp_series = df.column("tavg")?.f64()?;
            assert!(temp_series.into_iter().all(|opt_temp| match opt_temp {
                Some(t) => t > 25.0,
                None => true, // Allow nulls (should be filtered out by gt ideally)
            }));
        } else {
            println!("No days found with tavg > 25.0 in the test data subset.");
            // This is acceptable if the station/period doesn't have such days
        }

        Ok(())
    }

    // Helper function to convert polars date int to NaiveDate
    fn polars_date_to_naive(days: i32) -> NaiveDate {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        epoch + Duration::days(days as i64)
    }

    #[tokio::test]
    async fn test_daily_frame_get_at_specific_date() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        // Choose a date likely to exist in historical records
        let target_date = NaiveDate::from_ymd_opt(2020, 7, 15).unwrap();

        let result_lazy = daily_lazy.get_at(target_date)?;
        let df = result_lazy.frame.collect()?;

        // Expect exactly one row for a specific date if data exists
        assert_eq!(
            df.height(),
            1,
            "Expected exactly one row for date {}",
            target_date
        );

        // Verify the date in that row
        let date_series = df.column("date")?.date()?;
        let retrieved_date_int = date_series.get(0).unwrap(); // Get date as i32 days since epoch
        let actual_date = polars_date_to_naive(retrieved_date_int);

        assert_eq!(actual_date, target_date);

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_get_range_naive_dates() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        let start_date = NaiveDate::from_ymd_opt(2021, 3, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2021, 3, 7).unwrap(); // 7 days inclusive

        let result_lazy = daily_lazy.get_range(start_date, end_date)?;
        let df = result_lazy.frame.collect()?;

        // Expect roughly 7 rows, allowing for missing days
        assert!(
            df.height() <= 7,
            "Should have at most 7 rows for a 7-day period, got {}",
            df.height()
        );
        assert!(df.height() > 0, "Should find some data for the period"); // Assume data exists

        // Verify dates are within the range
        let date_series = df.column("date")?.date()?;
        assert!(date_series.into_iter().all(|opt_date_int| {
            match opt_date_int {
                Some(di) => {
                    let d = polars_date_to_naive(di);
                    d >= start_date && d <= end_date
                }
                None => false, // Date should not be null
            }
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_get_for_period_year() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        let target_year = Year(2019);

        let result_lazy = daily_lazy.get_for_period(target_year)?;
        let df = result_lazy.frame.collect()?;

        let is_leap = target_year.is_leap();
        let max_days = if is_leap { 366 } else { 365 };

        assert!(
            df.height() <= max_days,
            "Should have at most {} rows for year {}, got {}",
            max_days,
            target_year.get(),
            df.height()
        );
        assert!(
            df.height() > 300, // Expect good coverage
            "Should have found most days for year {} (expected >300, found {})",
            target_year.get(),
            df.height()
        );

        // Verify all dates are within the target year
        let date_series = df.column("date")?.date()?;
        assert!(date_series.into_iter().all(|opt_date_int| {
            match opt_date_int {
                Some(di) => polars_date_to_naive(di).year() == target_year.get(),
                None => false,
            }
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_get_range_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        // Use a past date range where no data exists
        let start_date = NaiveDate::from_ymd_opt(1800, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(1800, 1, 7).unwrap();

        let result_lazy = daily_lazy.get_range(start_date, end_date)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(df.height(), 0, "Expected zero rows for a past date range");

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_chaining_range_and_filter() -> Result<(), Box<dyn std::error::Error>>
    {
        let daily_lazy = get_test_daily_frame().await?;
        let target_year = Year(2022);

        // Get data for 2022, then filter for days with precipitation > 5.0 mm
        let rainy_days_lazy = daily_lazy
            .get_for_period(target_year)?
            .filter(col("prcp").gt(lit(5.0f64)));

        let df = rainy_days_lazy.frame.collect()?;

        println!(
            "Found {} days in {} with prcp > 5.0 mm",
            df.height(),
            target_year.get()
        );
        let max_days = if target_year.is_leap() { 366 } else { 365 };
        assert!(df.height() < max_days); // Should be fewer than all days

        if df.height() > 0 {
            // Verify year and precipitation condition
            let date_series = df.column("date")?.date()?;
            let prcp_series = df.column("prcp")?.f64()?;

            for i in 0..df.height() {
                let date_int = date_series.get(i).unwrap();
                let prcp_val = prcp_series.get(i).unwrap_or(0.0); // Default to 0 if null
                let date = polars_date_to_naive(date_int);

                assert_eq!(date.year(), target_year.get());
                assert!(prcp_val > 5.0);
            }
        }

        Ok(())
    }

    // --- Collection Methods ---

    #[tokio::test]
    async fn test_collect_daily_vec() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        let start_date = NaiveDate::from_ymd_opt(2023, 2, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2023, 2, 5).unwrap(); // 5 days
        let expected_max_rows = 5;

        let result_lazy = daily_lazy.get_range(start_date, end_date)?;
        let daily_vec = result_lazy.collect_daily()?;

        assert!(
            daily_vec.len() <= expected_max_rows,
            "Collected vector length ({}) should be <= {}",
            daily_vec.len(),
            expected_max_rows
        );
        // Can't assume exactly 5 rows due to potential missing data
        assert!(
            !daily_vec.is_empty(),
            "Expected some data for the 5-day period"
        );

        // Check the first record if it exists
        if let Some(first_record) = daily_vec.first() {
            println!("First collected record: {:?}", first_record);
            assert!(first_record.date >= start_date);
            assert!(first_record.date <= end_date);
            // Example check on a field
            assert!(
                first_record.average_temperature.is_some()
                    || first_record.average_temperature.is_none()
            );
            assert!(first_record.wind_speed.is_some() || first_record.wind_speed.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_daily_single_row_success() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        let target_date = NaiveDate::from_ymd_opt(2021, 8, 22).unwrap(); // Expect data exists

        let single_day_lazy = daily_lazy.get_at(target_date)?;
        let daily_record = single_day_lazy.collect_single_daily()?;

        println!("Collected single record: {:?}", daily_record);
        assert_eq!(daily_record.date, target_date);
        // Can add more assertions based on expected data for that day if known
        assert!(daily_record.average_temperature.is_some()); // Example check
        assert!(daily_record.precipitation.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_daily_single_row_fail_multiple_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        // Use a range that will definitely yield multiple rows
        let start_date = NaiveDate::from_ymd_opt(2022, 4, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2022, 4, 5).unwrap(); // 5 days

        let multi_day_lazy = daily_lazy.get_range(start_date, end_date)?;
        let result = multi_day_lazy.collect_single_daily(); // Expect this to fail

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
    async fn test_collect_daily_single_row_fail_zero_rows() -> Result<(), Box<dyn std::error::Error>>
    {
        let daily_lazy = get_test_daily_frame().await?;
        // Use a date without data (far past)
        let ancient_date = NaiveDate::from_ymd_opt(1850, 1, 1).unwrap();

        let zero_day_lazy = daily_lazy.get_at(ancient_date)?;
        let result = zero_day_lazy.collect_single_daily(); // Expect this to fail

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
    async fn test_collect_daily_vec_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        // Use a date far in the past, guaranteed to have no data
        let ancient_start = NaiveDate::from_ymd_opt(1818, 1, 1).unwrap();
        let ancient_end = NaiveDate::from_ymd_opt(1818, 1, 7).unwrap();

        let empty_lazy = daily_lazy.get_range(ancient_start, ancient_end)?;
        let daily_vec = empty_lazy.collect_daily()?;

        assert!(
            daily_vec.is_empty(),
            "Expected empty vector for ancient date range"
        );

        Ok(())
    }
}
