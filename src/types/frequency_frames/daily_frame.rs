// daily_frame.rs

//! Contains the `DailyLazyFrame` structure for handling lazy operations on Meteostat daily weather data.

use crate::types::traits::any::any_date::AnyDate;
use crate::types::traits::period::date_period::DatePeriod;
use crate::MeteostatError;
use chrono::NaiveDate;
use polars::prelude::{col, lit, Expr, LazyFrame};

/// Represents a row of daily weather data.
/// (Note: This struct is currently not directly used in the lazy frame processing pipeline,
/// but represents the expected structure of a collected row).
#[allow(dead_code)]
struct Daily {
    date: NaiveDate,
    average_temperature: Option<f64>, // Use Option for potentially missing values
    minimum_temperature: Option<f64>,
    maximum_temperature: Option<f64>,
    precipitation: Option<f64>,
    snow: Option<i32>,
    wind_direction: Option<i32>,
    wind_speed: Option<f64>,
    peak_wind_gust: Option<f64>,
    pressure: Option<f64>,
    sunshine_minutes: Option<i32>,
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat daily weather data.
///
/// This struct provides methods tailored for common operations on daily datasets,
/// such as filtering by date ranges, while retaining the benefits of lazy evaluation.
///
/// Instances are typically obtained via [`crate::Meteostat::daily`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`)
/// can potentially return a [`polars::prelude::PolarsError`] if the computation fails.
///
/// Methods involving date parsing or range generation (`get_range`, `get_at`, `get_for_period`)
/// can return [`MeteostatError::DateParsingError`] if the input dates cannot be resolved.
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
    /// * `frame` - A `LazyFrame` assumed to contain daily weather data with the expected schema.
    pub fn new(frame: LazyFrame) -> Self {
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// use chrono::NaiveDate;
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let london = LatLon(51.50, -0.12); // London location
    ///
    /// let daily_lazy = client.daily().location(london).call().await?;
    ///
    /// // Get data for a specific week in 2023
    /// let start_date = NaiveDate::from_ymd_opt(2023, 7, 1).unwrap();
    /// let end_date = NaiveDate::from_ymd_opt(2023, 7, 7).unwrap();
    ///
    /// let week_data_lazy = daily_lazy.get_range(start_date, end_date)?;
    ///
    /// // Collect the result
    /// let df = week_data_lazy.frame.collect()?;
    /// println!("Data for first week of July 2023:\n{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `start` or `end` cannot be resolved to a `NaiveDate`.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// use chrono::NaiveDate;
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let daily_lazy = client.daily().location(paris).call().await?;
    ///
    /// // Get data for New Year's Day 2022
    /// let target_date = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    ///
    /// let new_year_data_lazy = daily_lazy.get_at(target_date)?;
    ///
    /// // Collect the result (should be one row if data exists)
    /// let df = new_year_data_lazy.frame.collect()?;
    /// if df.height() == 1 {
    ///     println!("Data for 2022-01-01:\n{}", df);
    /// } else {
    ///     println!("No data found for 2022-01-01 at the nearest station.");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `date` cannot be resolved to a `NaiveDate`.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
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
    /// let daily_lazy = client.daily().location(rome).call().await?;
    ///
    /// // Get all available data for the year 2021
    /// let year_2021_data_lazy = daily_lazy.get_for_period(Year(2021))?;
    ///
    /// // Collect the result
    /// let df = year_2021_data_lazy.frame.collect()?;
    /// println!("Data for 2021 ({} days found):\n{}", df.height(), df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::DateParsingError`] if `period` cannot be resolved to a date range.
    /// Subsequent `.collect()` calls might return a [`polars::prelude::PolarsError`].
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError, Year};
    use chrono::{Datelike, NaiveDate};
    use polars::prelude::*;

    // Helper to fetch daily data for tests - uses Berlin Tempelhof ("10384")
    // This station usually has extensive daily records.
    async fn get_test_daily_frame() -> Result<DailyLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.daily().station("10384").call().await // Berlin Tempelhof
    }

    #[tokio::test]
    async fn test_daily_frame_new_schema() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;

        // Expected columns for daily data (subset for testing)
        let expected_cols = ["date", "tavg", "tmin", "tmax", "prcp", "wspd"];

        let df = daily_lazy.frame.collect()?; // Collect to check schema on DataFrame
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

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_filter_temp() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;

        // Filter for days with average temp > 25.0
        let filtered_lazy = daily_lazy.filter(col("tavg").gt(lit(25.0f64)));
        let df = filtered_lazy.frame.collect()?;

        // We expect *some* hot days, but can't guarantee how many.
        // Just check the frame is not empty if results are found, and the condition holds.
        if df.height() > 0 {
            println!("Found {} days with tavg > 25.0", df.height());
            let temp_series = df.column("tavg")?.f64()?;
            assert!(temp_series.into_iter().all(|opt_temp| match opt_temp {
                Some(t) => t > 25.0,
                None => true, // Allow nulls (though filter should exclude them here)
            }));
        } else {
            println!("No days found with tavg > 25.0 in the test data subset.");
            // This is acceptable if the station/period doesn't have such days
        }

        Ok(())
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
        let retrieved_date = date_series.get(0).unwrap(); // Get date as i32 days since epoch
                                                          // Convert back to NaiveDate for comparison
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let actual_date = epoch + chrono::Duration::days(retrieved_date as i64);

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
            "Should have at most 7 rows for a 7-day period"
        );
        assert!(df.height() > 0, "Should find some data for the period"); // Assume data exists

        // Verify dates are within the range
        let date_series = df.column("date")?.date()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert!(date_series.into_iter().all(|opt_date_int| {
            match opt_date_int {
                Some(di) => {
                    let d = epoch + chrono::Duration::days(di as i64);
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

        // Expect around 365 rows for a full year (allow for missing data)
        assert!(
            df.height() <= 366,
            "Should have at most 366 rows for year {}",
            target_year.get()
        ); // Leap year check
        assert!(
            df.height() > 300,
            "Should have found most days for year {}",
            target_year.get()
        ); // Expect good coverage

        // Verify all dates are within the target year
        let date_series = df.column("date")?.date()?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert!(date_series.into_iter().all(|opt_date_int| {
            match opt_date_int {
                Some(di) => (epoch + chrono::Duration::days(di as i64)).year() == target_year.get(),
                None => false,
            }
        }));

        Ok(())
    }

    #[tokio::test]
    async fn test_daily_frame_get_range_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let daily_lazy = get_test_daily_frame().await?;
        // Use a future date range where no data exists
        let start_date = NaiveDate::from_ymd_opt(2200, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2200, 1, 7).unwrap();

        let result_lazy = daily_lazy.get_range(start_date, end_date)?;
        let df = result_lazy.frame.collect()?;

        assert_eq!(df.height(), 0, "Expected zero rows for a future date range");

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
        assert!(df.height() < 366); // Should be fewer than all days

        if df.height() > 0 {
            // Verify year and precipitation condition
            let date_series = df.column("date")?.date()?;
            let prcp_series = df.column("prcp")?.f64()?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

            for i in 0..df.height() {
                let date_int = date_series.get(i).unwrap();
                let prcp_val = prcp_series.get(i).unwrap_or(0.0); // Default to 0 if null
                let date = epoch + chrono::Duration::days(date_int as i64);

                assert_eq!(date.year(), target_year.get());
                assert!(prcp_val > 5.0);
            }
        }

        Ok(())
    }
}
