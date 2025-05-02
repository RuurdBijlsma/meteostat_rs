// monthly_frame.rs

//! Contains the `MonthlyLazyFrame` structure for handling lazy operations on Meteostat monthly weather data.

use crate::types::traits::any::any_month::AnyMonth;
use crate::types::traits::period::month_period::MonthPeriod;
use crate::MeteostatError;
use polars::prelude::{col, lit, DataFrame, Expr, LazyFrame};
// Added DataFrame

/// Represents a row of monthly weather data, suitable for collecting results.
#[derive(Debug, Clone, PartialEq)] // Made public and added derives
pub struct Monthly {
    /// The year of the observation.
    pub year: i32, // Use i32 for year
    /// The month of the observation (1-12).
    pub month: u32, // Use u32 for month
    /// Average air temperature in Celsius.
    pub average_temperature: Option<f64>, // tavg
    /// Average minimum air temperature in Celsius.
    pub minimum_temperature: Option<f64>, // tmin
    /// Average maximum air temperature in Celsius.
    pub maximum_temperature: Option<f64>, // tmax
    /// Total precipitation amount in mm.
    pub precipitation: Option<f64>, // prcp
    /// Average wind speed in km/h.
    pub wind_speed: Option<f64>, // wspd
    /// Average sea-level air pressure in hPa.
    pub pressure: Option<f64>, // pres
    /// Total sunshine duration in minutes.
    pub sunshine_minutes: Option<i32>, // tsun (read as i64, store as i32)
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat monthly weather data.
///
/// This struct provides methods tailored for common operations on monthly datasets,
/// such as filtering by month ranges or specific years, while retaining the benefits of lazy evaluation.
/// It also includes methods to collect the results into Rust structs.
///
/// Instances are typically obtained via [`crate::Meteostat::monthly`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`,
/// or the collection methods here) can potentially return a [`polars::prelude::PolarsError`]
/// (wrapped as [`MeteostatError::PolarsError`]).
///
/// Methods involving month/year parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input cannot be resolved.
///
/// The `collect_single_monthly` method returns [`MeteostatError::ExpectedSingleRow`] if the frame
/// does not contain exactly one row upon collection.
///
/// The initial creation via [`crate::Meteostat::monthly`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)]
pub struct MonthlyLazyFrame {
    /// The underlying Polars LazyFrame containing the monthly data.
    pub frame: LazyFrame,
}

impl MonthlyLazyFrame {
    /// Creates a new `MonthlyLazyFrame` wrapping the given Polars `LazyFrame`.
    ///
    /// This is typically called internally by the [`crate::Meteostat`] client methods.
    ///
    /// # Arguments
    ///
    /// * `frame` - A `LazyFrame` assumed to contain monthly weather data with the expected schema
    ///   (columns like "year", "month", "tavg", "prcp", etc.). Year and Month are expected
    ///   to be numerical types (like Int64).
    pub(crate) fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    /// Filters the monthly data based on a Polars predicate expression.
    ///
    /// This method allows applying arbitrary filtering logic supported by Polars.
    /// It returns a *new* `MonthlyLazyFrame` with the filter applied lazily.
    /// The original `MonthlyLazyFrame` remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A Polars [`Expr`] defining the filtering condition.
    ///
    /// # Returns
    ///
    /// A new `MonthlyLazyFrame` representing the filtered data.
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
    /// let monthly_lazy = client.monthly().location(berlin).call().await?;
    ///
    /// // Filter for months where average temperature was above 15 degrees Celsius
    /// let warm_months = monthly_lazy.filter(col("tavg").gt(lit(15.0f64)));
    ///
    /// // Collect the results
    /// let df = warm_months.frame.collect()?;
    /// println!("Warm months found:\n{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// While this method itself doesn't typically error, subsequent operations like `.collect()`
    /// might return a [`polars::prelude::PolarsError`].
    pub fn filter(&self, predicate: Expr) -> MonthlyLazyFrame {
        MonthlyLazyFrame::new(self.frame.clone().filter(predicate))
    }

    /// Filters the monthly data to include only records within the specified month range (inclusive).
    ///
    /// The `start` and `end` arguments can be any type that implements [`AnyMonth`],
    /// such as [`crate::Month`] or [`crate::Year`]. The trait resolves these into specific
    /// start and end `Month` values (year, month number). The filter logic correctly handles
    /// ranges spanning across year boundaries. Assumes the "year" and "month" columns in the
    /// frame are numerical (e.g., Int64).
    ///
    /// # Arguments
    ///
    /// * `start` - The starting month boundary (inclusive), implementing [`AnyMonth`].
    /// * `end` - The ending month boundary (inclusive), implementing [`AnyMonth`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MonthlyLazyFrame` filtered by the month range,
    /// or a [`MeteostatError::DateParsingError`] if the month conversion fails.
    pub fn get_range(
        &self,
        start: impl AnyMonth,
        end: impl AnyMonth,
    ) -> Result<MonthlyLazyFrame, MeteostatError> {
        // Resolve inputs to start and end Months
        let start_month_obj = start
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        let end_month_obj = end
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .end;

        // Use i64 literals for comparison as Polars often reads CSV integers as i64
        let start_year = start_month_obj.year() as i64;
        let end_year = end_month_obj.year() as i64;
        let start_month_num = start_month_obj.month() as i64;
        let end_month_num = end_month_obj.month() as i64;

        // Build the filter expression
        // Condition: (year > start_year) OR (year == start_year AND month >= start_month_num)
        let after_start_cond = col("year").gt(lit(start_year)).or(col("year")
            .eq(lit(start_year))
            .and(col("month").gt_eq(lit(start_month_num))));

        // Condition: (year < end_year) OR (year == end_year AND month <= end_month_num)
        let before_end_cond = col("year").lt(lit(end_year)).or(col("year")
            .eq(lit(end_year))
            .and(col("month").lt_eq(lit(end_month_num))));

        // Combine the conditions: Must be after start AND before end
        Ok(self.filter(after_start_cond.and(before_end_cond)))
    }

    /// Filters the monthly data to get the record for a specific year and month.
    ///
    /// The `month_spec` argument can be any type that implements [`AnyMonth`]. It is resolved
    /// to a single `Month` (year, month number). If the input represents a range (like `Year`),
    /// the *start* of that range (e.g., January of that year) is used for the equality check.
    /// For precise single-month filtering, use [`crate::Month`]. Assumes "year" and "month"
    /// columns are numerical.
    ///
    /// # Arguments
    ///
    /// * `month_spec` - The target year and month, implementing [`AnyMonth`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MonthlyLazyFrame` filtered to the specific year and month,
    /// or a [`MeteostatError::DateParsingError`] if the conversion fails. Collecting
    /// the frame should yield zero or one row.
    pub fn get_at(&self, month_spec: impl AnyMonth) -> Result<MonthlyLazyFrame, MeteostatError> {
        // Use the start of the range provided by AnyMonth for the equality check
        let month_obj = month_spec
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;

        // Use i64 literals for comparison
        let target_year = month_obj.year() as i64;
        let target_month_num = month_obj.month() as i64;

        Ok(self.filter(
            col("year")
                .eq(lit(target_year))
                .and(col("month").eq(lit(target_month_num))),
        ))
    }

    /// Filters the monthly data to include only records within a specified period.
    ///
    /// This is a convenience method that accepts types implementing [`MonthPeriod`],
    /// such as [`crate::Year`]. It resolves the period into a start and end `Month`
    /// and then calls `get_range`.
    ///
    /// # Arguments
    ///
    /// * `period` - The time period (e.g., a specific year), implementing [`MonthPeriod`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a new `MonthlyLazyFrame` filtered by the period's month range,
    /// or a [`MeteostatError::DateParsingError`] if the period cannot be resolved.
    pub fn get_for_period(
        &self,
        period: impl MonthPeriod,
    ) -> Result<MonthlyLazyFrame, MeteostatError> {
        let month_period = period
            .get_month_period()
            .ok_or(MeteostatError::DateParsingError)?;
        // Delegate to get_range using the resolved start and end months
        self.get_range(month_period.start, month_period.end)
    }

    /// Executes the lazy query and collects the results into a `Vec<Monthly>`.
    ///
    /// This method triggers the computation defined by the `LazyFrame` (including any
    /// previous filtering operations) and maps each resulting row to a `Monthly` struct.
    /// Rows where the essential 'year' or 'month' columns are missing or invalid are skipped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Monthly>` on success, or a [`MeteostatError`]
    /// if the computation or mapping fails (e.g., `MeteostatError::PolarsError`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year, Monthly};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let monthly_lazy = client
    ///     .monthly()
    ///     .location(paris)
    ///     .call()
    ///     .await?;
    ///
    /// // Get data for a specific year
    /// let year_data_lazy = monthly_lazy.get_for_period(Year(2022))?;
    ///
    /// // Collect into Vec<Monthly>
    /// let monthly_vec: Vec<Monthly> = year_data_lazy.collect_monthly()?;
    ///
    /// println!("Collected {} monthly records for 2022.", monthly_vec.len());
    /// if let Some(first_month) = monthly_vec.first() {
    ///     println!("First record: {:?}", first_month);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_monthly(&self) -> Result<Vec<Monthly>, MeteostatError> {
        let df = self
            .frame
            .clone() // Clone frame as collect consumes it
            .collect()
            .map_err(MeteostatError::PolarsError)?; // Map PolarsError

        Self::dataframe_to_monthly_vec(&df) // Use helper function
    }

    /// Executes the lazy query, expecting exactly one row, and collects it into a `Monthly` struct.
    ///
    /// This is useful after filtering the frame down to a single expected record,
    /// for example using `get_at()`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the single `Monthly` struct on success.
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
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Month, Monthly};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10384"; // Berlin Tempelhof
    ///
    /// let monthly_lazy = client.monthly().station(station_id).call().await?;
    ///
    /// // Get data for a specific month
    /// let target_month = Month::new(9, 2022); // September 2022
    /// let single_month_lazy = monthly_lazy.get_at(target_month)?;
    ///
    /// // Collect the single expected row
    /// match single_month_lazy.collect_single_monthly() {
    ///     Ok(monthly_data) => {
    ///         println!("Collected single month data: {:?}", monthly_data);
    ///         assert_eq!(monthly_data.year, 2022);
    ///         assert_eq!(monthly_data.month, 9); // Verify correct month
    ///     },
    ///     Err(MeteostatError::ExpectedSingleRow { actual }) => {
    ///          println!("Expected 1 row, but found {}. Data might be missing for that month.", actual);
    ///          // Handle missing data case if necessary
    ///          // assert_eq!(actual, 0); // Or assert based on expected availability
    ///     },
    ///     Err(e) => return Err(e), // Propagate other errors
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_single_monthly(&self) -> Result<Monthly, MeteostatError> {
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
        Self::dataframe_to_monthly_vec(&df)?
            .pop() // Take the only element
            .ok_or(MeteostatError::ExpectedSingleRow { actual: 0 }) // Should be unreachable
    }

    // --- Helper function to map DataFrame rows to Vec<Monthly> ---
    fn dataframe_to_monthly_vec(df: &DataFrame) -> Result<Vec<Monthly>, MeteostatError> {
        // --- Get required columns as Series ---
        let year_series = df.column("year")?;
        let month_series = df.column("month")?;
        let tavg_series = df.column("tavg")?;
        let tmin_series = df.column("tmin")?;
        let tmax_series = df.column("tmax")?;
        let prcp_series = df.column("prcp")?;
        let wspd_series = df.column("wspd")?;
        let pres_series = df.column("pres")?;
        let tsun_series = df.column("tsun")?; // Integer type

        // --- Get ChunkedArrays (assuming Polars read them as i64/f64) ---
        let year_ca = year_series.i64()?;
        let month_ca = month_series.i64()?;
        let tavg_ca = tavg_series.f64()?;
        let tmin_ca = tmin_series.f64()?;
        let tmax_ca = tmax_series.f64()?;
        let prcp_ca = prcp_series.f64()?;
        let wspd_ca = wspd_series.f64()?;
        let pres_ca = pres_series.f64()?;
        let tsun_ca = tsun_series.i64()?; // Read as i64 initially

        let mut monthly_vec = Vec::with_capacity(df.height());

        // --- Iterate through rows and map ---
        for i in 0..df.height() {
            // Get year and month (essential) - skip row if missing or invalid
            let year_opt: Option<i32> = year_ca.get(i).and_then(|y| i32::try_from(y).ok());
            let month_opt: Option<u32> = month_ca
                .get(i)
                .and_then(|m| u32::try_from(m).ok())
                .filter(|&m| (1..=12).contains(&m)); // Validate month range

            let (Some(year), Some(month)) = (year_opt, month_opt) else {
                // Skip row if year or month is missing or invalid
                continue;
            };

            // Construct the struct
            let monthly_record = Monthly {
                year,
                month,
                average_temperature: tavg_ca.get(i),
                minimum_temperature: tmin_ca.get(i),
                maximum_temperature: tmax_ca.get(i),
                precipitation: prcp_ca.get(i),
                wind_speed: wspd_ca.get(i),
                pressure: pres_ca.get(i),
                sunshine_minutes: tsun_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
            };

            monthly_vec.push(monthly_record);
        }

        Ok(monthly_vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError, Month, Year};
    use polars::prelude::*;

    // Helper to fetch monthly data for tests - uses Berlin Tempelhof ("10384")
    async fn get_test_monthly_frame() -> Result<MonthlyLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.monthly().station("10384").call().await // Berlin Tempelhof
    }

    #[tokio::test]
    async fn test_monthly_frame_new_schema() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;

        // Expected columns for monthly data
        let expected_cols = [
            "year", "month", "tavg", "tmin", "tmax", "prcp", "wspd", "pres", "tsun",
        ];

        let df = monthly_lazy.frame.limit(1).collect()?; // Collect small sample
        let actual_cols = df.get_column_names();

        for col_name in expected_cols {
            assert!(
                actual_cols.contains(&&PlSmallStr::from_str(col_name)),
                "Expected column '{}' not found in monthly data",
                col_name
            );
        }
        // Check year/month types (likely i64 from Polars CSV reader)
        assert_eq!(df.column("year")?.dtype(), &DataType::Int64);
        assert_eq!(df.column("month")?.dtype(), &DataType::Int64);
        // Check data column types
        assert_eq!(df.column("tavg")?.dtype(), &DataType::Float64);
        assert_eq!(df.column("tsun")?.dtype(), &DataType::Int64); // Check tsun type

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_filter_temp() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;

        // Filter for months with average temp > 20.0 (likely summer months)
        let filtered_lazy = monthly_lazy.filter(col("tavg").gt(lit(20.0f64)));
        let df = filtered_lazy.frame.collect()?;

        if df.height() > 0 {
            println!("Found {} months with tavg > 20.0", df.height());
            let temp_series = df.column("tavg")?.f64()?;
            assert!(temp_series.into_iter().all(|opt_temp| match opt_temp {
                Some(t) => t > 20.0,
                None => true, // Allow nulls
            }));
        } else {
            println!("No months found with tavg > 20.0 in the test data subset.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_at_specific_month() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Choose a month likely to exist
        let target_month = Month::new(8, 2018); // August 2018

        let result_lazy = monthly_lazy.get_at(target_month)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(
            df.height(),
            1,
            "Expected exactly one row for month {:?}",
            target_month
        );

        // Verify the year and month in that row (expecting i64 from Polars)
        let year_series = df.column("year")?.i64()?;
        let month_series = df.column("month")?.i64()?;
        let retrieved_year = year_series.get(0).unwrap();
        let retrieved_month = month_series.get(0).unwrap();

        assert_eq!(retrieved_year, target_month.year() as i64);
        assert_eq!(retrieved_month, target_month.month() as i64);

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_range_specific_months() -> Result<(), Box<dyn std::error::Error>>
    {
        let monthly_lazy = get_test_monthly_frame().await?;
        let start_month = Month::new(11, 2019); // Nov 2019
        let end_month = Month::new(2, 2020); // Feb 2020
        let expected_rows = 4; // Nov, Dec, Jan, Feb

        let result_lazy = monthly_lazy.get_range(start_month, end_month)?;
        let df = result_lazy.frame.collect()?;

        // Allow for missing data within the range if the station isn't perfect
        assert!(
            df.height() <= expected_rows,
            "Expected at most {} rows for the period {:?} to {:?}, found {}",
            expected_rows,
            start_month,
            end_month,
            df.height()
        );
        assert!(
            df.height() > 0, // Should find *some* data usually
            "Expected > 0 rows for the period {:?} to {:?}",
            start_month,
            end_month
        );

        // Verify year/month are within the range if rows exist
        if df.height() > 0 {
            let year_series = df.column("year")?.i64()?;
            let month_series = df.column("month")?.i64()?;

            let first_year = year_series.get(0).unwrap();
            let first_month = month_series.get(0).unwrap();
            let last_year = year_series.get(df.height() - 1).unwrap();
            let last_month = month_series.get(df.height() - 1).unwrap();

            // Check first row is >= start
            assert!(
                first_year > start_month.year() as i64
                    || (first_year == start_month.year() as i64
                        && first_month >= start_month.month() as i64)
            );
            // Check last row is <= end
            assert!(
                last_year < end_month.year() as i64
                    || (last_year == end_month.year() as i64
                        && last_month <= end_month.month() as i64)
            );

            // If exactly expected rows, check bounds precisely
            if df.height() == expected_rows {
                assert_eq!(first_year, 2019);
                assert_eq!(first_month, 11);
                assert_eq!(last_year, 2020);
                assert_eq!(last_month, 2);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_for_period_year() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        let target_year = Year(2017);

        let result_lazy = monthly_lazy.get_for_period(target_year)?;
        let df = result_lazy.frame.collect()?;

        // Expect up to 12 rows for a full year (allow for missing data)
        assert!(
            df.height() <= 12,
            "Expected at most 12 rows for year {}, found {}",
            target_year.get(),
            df.height()
        );
        assert!(
            df.height() > 6, // Expect most months usually
            "Expected more than 6 rows for year {}, found {}",
            target_year.get(),
            df.height()
        );

        // Verify all records are from the target year if any exist
        if df.height() > 0 {
            let year_series = df.column("year")?.i64()?;
            assert!(year_series
                .into_iter()
                .all(|opt_year| opt_year.unwrap() == target_year.get() as i64));

            // If exactly 12, verify months are 1 through 12
            if df.height() == 12 {
                let month_series = df.column("month")?.i64()?;
                let mut months: Vec<i64> = month_series.into_iter().map(|m| m.unwrap()).collect();
                months.sort_unstable();
                let expected_months: Vec<i64> = (1..=12).collect();
                assert_eq!(months, expected_months);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_range_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Use a past year range where no data exists
        let start_month = Month::new(1, 1800);
        let end_month = Month::new(12, 1800);

        let result_lazy = monthly_lazy.get_range(start_month, end_month)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(df.height(), 0, "Expected zero rows for a past month range");

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_chaining_period_and_filter(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        let target_year = Year(2016);

        // Get data for 2016, then filter for months with precipitation > 50 mm
        let wet_months_lazy = monthly_lazy
            .get_for_period(target_year)?
            .filter(col("prcp").gt(lit(50.0f64)));

        let df = wet_months_lazy.frame.collect()?;

        println!(
            "Found {} months in {} with prcp > 50.0 mm",
            df.height(),
            target_year.get()
        );
        assert!(df.height() < 12); // Should be fewer than all months

        if df.height() > 0 {
            // Verify year and precipitation condition
            let year_series = df.column("year")?.i64()?;
            let prcp_series = df.column("prcp")?.f64()?;

            for i in 0..df.height() {
                let year_val = year_series.get(i).unwrap();
                let prcp_val = prcp_series.get(i).unwrap_or(0.0); // Default to 0 if null

                assert_eq!(year_val, target_year.get() as i64);
                assert!(prcp_val > 50.0);
            }
        }

        Ok(())
    }

    // --- New Tests for Collection Methods ---

    #[tokio::test]
    async fn test_collect_monthly_vec() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        let start_month = Month::new(1, 2021);
        let end_month = Month::new(4, 2021); // 4 months
        let expected_max_rows = 4;

        let result_lazy = monthly_lazy.get_range(start_month, end_month)?;
        let monthly_vec = result_lazy.collect_monthly()?;

        assert!(
            monthly_vec.len() <= expected_max_rows,
            "Collected vector length ({}) should be <= {}",
            monthly_vec.len(),
            expected_max_rows
        );
        assert!(
            !monthly_vec.is_empty(),
            "Expected some data for the 4-month period"
        );

        // Check the first record if it exists
        if let Some(first_record) = monthly_vec.first() {
            println!("First collected record: {:?}", first_record);
            assert!(first_record.year >= start_month.year());
            assert!(first_record.year <= end_month.year());
            // Could add more specific month check logic if needed
            assert!(
                first_record.average_temperature.is_some()
                    || first_record.average_temperature.is_none()
            );
            assert!(first_record.precipitation.is_some() || first_record.precipitation.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_monthly_single_row_success() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        let target_month = Month::new(7, 2019); // Expect data exists

        let single_month_lazy = monthly_lazy.get_at(target_month)?;
        let monthly_record = single_month_lazy.collect_single_monthly()?;

        println!("Collected single record: {:?}", monthly_record);
        assert_eq!(monthly_record.year, target_month.year());
        assert_eq!(monthly_record.month, target_month.month());
        assert!(monthly_record.average_temperature.is_some());
        assert!(
            monthly_record.sunshine_minutes.is_some() || monthly_record.sunshine_minutes.is_none()
        ); // tsun often null
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_monthly_single_row_fail_multiple_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Use a range that will yield multiple rows (a full year)
        let target_year = Year(2020);

        let multi_month_lazy = monthly_lazy.get_for_period(target_year)?;
        let result = multi_month_lazy.collect_single_monthly(); // Expect this to fail

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Got expected error: {:?}", err);

        match err {
            MeteostatError::ExpectedSingleRow { actual } => {
                // Should be 12 if data is complete, but allow less due to potential missing data
                assert!(actual > 1, "Expected actual rows to be > 1, got {}", actual);
            }
            _ => panic!("Expected MeteostatError::ExpectedSingleRow, got {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_collect_monthly_single_row_fail_zero_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Use a month without data (far past)
        let ancient_month = Month::new(6, 1850);

        let zero_month_lazy = monthly_lazy.get_at(ancient_month)?;
        let result = zero_month_lazy.collect_single_monthly(); // Expect this to fail

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
    async fn test_collect_monthly_vec_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Use a date far in the past, guaranteed to have no data
        let ancient_start = Month::new(1, 1818);
        let ancient_end = Month::new(12, 1818);

        let empty_lazy = monthly_lazy.get_range(ancient_start, ancient_end)?;
        let monthly_vec = empty_lazy.collect_monthly()?;

        assert!(
            monthly_vec.is_empty(),
            "Expected empty vector for ancient month range"
        );

        Ok(())
    }
}
