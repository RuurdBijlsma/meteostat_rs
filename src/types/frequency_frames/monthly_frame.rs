// monthly_frame.rs

//! Contains the `MonthlyLazyFrame` structure for handling lazy operations on Meteostat monthly weather data.

use crate::types::traits::any::any_month::AnyMonth;
use crate::types::traits::period::month_period::MonthPeriod;
use crate::MeteostatError;
use polars::prelude::{col, lit, Expr, LazyFrame};

/// Represents a row of monthly weather data.
/// (Note: This struct is currently not directly used in the lazy frame processing pipeline,
/// but represents the expected structure of a collected row).
#[allow(dead_code)]
struct Monthly {
    year: i32,
    month: u32,                       // Month should be u32 (1-12)
    average_temperature: Option<f64>, // Use Option<f64> for potentially missing float values
    minimum_temperature: Option<f64>,
    maximum_temperature: Option<f64>,
    precipitation: Option<f64>,    // Precipitation sum
    wind_speed: Option<f64>,       // Average wind speed
    pressure: Option<f64>,         // Average pressure
    sunshine_minutes: Option<i32>, // Total sunshine duration (might be integer)
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat monthly weather data.
///
/// This struct provides methods tailored for common operations on monthly datasets,
/// such as filtering by month ranges or specific years, while retaining the benefits of lazy evaluation.
///
/// Instances are typically obtained via [`crate::Meteostat::monthly`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`)
/// can potentially return a [`polars::prelude::PolarsError`].
///
/// Methods involving month/year parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input cannot be resolved.
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
    ///   (columns like "year", "month", "tavg", "prcp", etc.).
    pub fn new(frame: LazyFrame) -> Self {
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
    /// ranges spanning across year boundaries.
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Month, Year};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let london = LatLon(51.50, -0.12);
    ///
    /// let monthly_lazy = client.monthly().location(london).call().await?;
    ///
    /// // Get data from June 2022 to May 2023 (inclusive)
    /// let start_month = Month::new(6, 2022);
    /// let end_month = Month::new(5, 2023);
    ///
    /// let period_data_lazy = monthly_lazy.get_range(start_month, end_month)?;
    ///
    /// // Collect the result (expecting 12 rows)
    /// let df = period_data_lazy.frame.collect()?;
    /// println!("Data for {} to {}:\n{}", start_month, end_month, df); // Month needs Display impl
    /// assert_eq!(df.height(), 12);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `start` or `end` cannot be resolved to a `Month`.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
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

        let start_year = start_month_obj.year();
        let end_year = end_month_obj.year();
        let start_month_num = start_month_obj.month(); // u32
        let end_month_num = end_month_obj.month(); // u32

        // Build the filter expression
        // Condition: (year > start_year) OR (year == start_year AND month >= start_month_num)
        let after_start_cond = col("year").gt(lit(start_year)).or(col("year")
            .eq(lit(start_year))
            // Cast month column to u32 if needed, or ensure literal is correct type
            .and(col("month").gt_eq(lit(start_month_num))));

        // Condition: (year < end_year) OR (year == end_year AND month <= end_month_num)
        let before_end_cond = col("year").lt(lit(end_year)).or(col("year")
            .eq(lit(end_year))
            // Cast month column to u32 if needed, or ensure literal is correct type
            .and(col("month").lt_eq(lit(end_month_num))));

        // Combine the conditions: Must be after start AND before end
        Ok(self.filter(after_start_cond.and(before_end_cond)))
    }

    /// Filters the monthly data to get the record for a specific year and month.
    ///
    /// The `month_spec` argument can be any type that implements [`AnyMonth`]. It is resolved
    /// to a single `Month` (year, month number). If the input represents a range (like `Year`),
    /// the *start* of that range (e.g., January of that year) is used for the equality check.
    /// For precise single-month filtering, use [`crate::Month`].
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Month};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let monthly_lazy = client.monthly().location(paris).call().await?;
    ///
    /// // Get data for July 2021
    /// let target_month = Month::new(7, 2021);
    ///
    /// let july_data_lazy = monthly_lazy.get_at(target_month)?;
    ///
    /// // Collect the result (should be one row if data exists)
    /// let df = july_data_lazy.frame.collect()?;
    /// if df.height() == 1 {
    ///     println!("Data for {}:\n{}", target_month, df); // Month needs Display
    /// } else {
    ///     println!("No data found for {} at the nearest station.", target_month); // Month needs Display
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `month_spec` cannot be resolved to a `Month`.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
    pub fn get_at(&self, month_spec: impl AnyMonth) -> Result<MonthlyLazyFrame, MeteostatError> {
        // Use the start of the range provided by AnyMonth for the equality check
        let month_obj = month_spec
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;

        Ok(self.filter(
            col("year")
                .eq(lit(month_obj.year()))
                // Ensure types match for month comparison (column vs literal)
                .and(col("month").eq(lit(month_obj.month()))),
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let rome = LatLon(41.90, 12.50);
    ///
    /// let monthly_lazy = client.monthly().location(rome).call().await?;
    ///
    /// // Get all available monthly data for the year 2021
    /// let year_2021_data_lazy = monthly_lazy.get_for_period(Year(2021))?;
    ///
    /// // Collect the result (expecting up to 12 rows)
    /// let df = year_2021_data_lazy.frame.collect()?;
    /// println!("Data for 2021 ({} months found):\n{}", df.height(), df);
    /// assert!(df.height() <= 12);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `period` cannot be resolved to a month range.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
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

        // Expected columns for monthly data (subset for testing)
        let expected_cols = ["year", "month", "tavg", "tmin", "tmax", "prcp", "wspd"];

        let df = monthly_lazy.frame.collect()?; // Collect to check schema
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
        // Check a data column type (likely Float64)
        assert_eq!(df.column("tavg")?.dtype(), &DataType::Float64);

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
            "Expected exactly one row for month {:?}", // Use Debug for Month
            target_month
        );

        // Verify the year and month in that row (expecting i64 from Polars)
        let year_series = df.column("year")?.i64()?;
        let month_series = df.column("month")?.i64()?;
        let retrieved_year = year_series.get(0).unwrap();
        let retrieved_month = month_series.get(0).unwrap();

        assert_eq!(retrieved_year, target_month.year() as i64);
        assert_eq!(retrieved_month, target_month.month() as i64); // Month is u32

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

        assert_eq!(
            df.height(),
            expected_rows,
            "Expected {} rows for the period {:?} to {:?}",
            expected_rows,
            start_month,
            end_month
        );

        // Verify year/month are within the range
        let year_series = df.column("year")?.i64()?;
        let month_series = df.column("month")?.i64()?;

        // Check first row (Nov 2019)
        assert_eq!(year_series.get(0).unwrap(), 2019);
        assert_eq!(month_series.get(0).unwrap(), 11);
        // Check last row (Feb 2020)
        assert_eq!(year_series.get(expected_rows - 1).unwrap(), 2020);
        assert_eq!(month_series.get(expected_rows - 1).unwrap(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_for_period_year() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        let target_year = Year(2017);

        let result_lazy = monthly_lazy.get_for_period(target_year)?;
        let df = result_lazy.frame.collect()?;

        // Expect exactly 12 rows for a full year (assuming complete data)
        assert_eq!(
            df.height(),
            12,
            "Expected 12 rows for year {}",
            target_year.get()
        );

        // Verify all records are from the target year
        let year_series = df.column("year")?.i64()?;
        assert!(year_series
            .into_iter()
            .all(|opt_year| opt_year.unwrap() == target_year.get() as i64));

        // Verify months are 1 through 12
        let month_series = df.column("month")?.i64()?;
        let mut months: Vec<i64> = month_series.into_iter().map(|m| m.unwrap()).collect();
        months.sort_unstable();
        let expected_months: Vec<i64> = (1..=12).collect();
        assert_eq!(months, expected_months);

        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_frame_get_range_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let monthly_lazy = get_test_monthly_frame().await?;
        // Use a future year range
        let start_month = Month::new(1, 2300);
        let end_month = Month::new(12, 2300);

        let result_lazy = monthly_lazy.get_range(start_month, end_month)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(
            df.height(),
            0,
            "Expected zero rows for a future month range"
        );

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
}
