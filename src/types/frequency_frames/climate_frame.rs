// climate_frame.rs

//! Contains the `ClimateLazyFrame` structure for handling lazy operations on Meteostat climate data.

use crate::Year;
// Added Meteostat and MeteostatError for tests
use polars::prelude::{col, lit, Expr, LazyFrame};
// Added IntoLazy, PolarsError for tests/docs

/// Represents a row of climate normals data.
/// (Note: This struct is currently not directly used in the lazy frame processing pipeline,
/// but represents the expected structure of a collected row).
#[allow(dead_code)]
struct Climate {
    start_year: i32,
    end_year: i32,
    month: u32,
    minimum_temperature: Option<f64>, // Use Option for potentially missing values
    maximum_temperature: Option<f64>,
    precipitation: Option<f64>,
    wind_speed: Option<f64>,
    pressure: Option<f64>,
    sunshine_minutes: Option<i32>, // Sunshine might be integer minutes or similar
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat climate data.
///
/// This struct provides methods tailored for common operations on climate normals datasets,
/// while retaining the benefits of lazy evaluation provided by Polars.
///
/// Instances are typically obtained via [`Meteostat::climate`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`)
/// can potentially return a [`PolarsError`] if the computation fails (e.g., due to type mismatches
/// or invalid operations).
///
/// The initial creation via [`Meteostat::climate`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)]
pub struct ClimateLazyFrame {
    /// The underlying Polars LazyFrame containing the climate data.
    pub frame: LazyFrame,
}

impl ClimateLazyFrame {
    /// Creates a new `ClimateLazyFrame` wrapping the given Polars `LazyFrame`.
    ///
    /// This is typically called internally by the [`Meteostat`] client methods.
    ///
    /// # Arguments
    ///
    /// * `frame` - A `LazyFrame` assumed to contain climate data with the expected schema.
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    /// Filters the climate data based on a Polars predicate expression.
    ///
    /// This method allows applying arbitrary filtering logic supported by Polars.
    /// It returns a *new* `ClimateLazyFrame` with the filter applied lazily.
    /// The original `ClimateLazyFrame` remains unchanged.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A Polars [`Expr`] defining the filtering condition.
    ///
    /// # Returns
    ///
    /// A new `ClimateLazyFrame` representing the filtered data.
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
    /// let climate_lazy = client.climate().location(berlin).call().await?;
    ///
    /// // Filter for months in the second half of the year (July - December)
    /// let summer_autumn_climate = climate_lazy.filter(col("month").gt(lit(6u32)));
    ///
    /// // Collect the results to see the data
    /// let df = summer_autumn_climate.frame.collect()?;
    /// println!("{}", df);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// While this method itself doesn't typically error (it just builds the query plan),
    /// subsequent operations like `.collect()` on the returned `frame` might return a
    /// [`PolarsError`] if the expression is invalid or encounters issues during execution.
    pub fn filter(&self, predicate: Expr) -> ClimateLazyFrame {
        ClimateLazyFrame {
            // Need to clone the frame to apply the filter without modifying the original
            frame: self.frame.clone().filter(predicate),
        }
    }

    /// Filters the climate data to get the normals for a specific period and month.
    ///
    /// This is a convenience method that filters the data based on the `start_year`,
    /// `end_year`, and `month` columns, which are standard in Meteostat climate normals data.
    /// It assumes a standard climate normals period (like 1991-2020).
    ///
    /// # Arguments
    ///
    /// * `start_year` - The starting year of the climate normal period (e.g., `Year(1991)`).
    /// * `end_year` - The ending year of the climate normal period (e.g., `Year(2020)`).
    /// * `month` - The month number (1-12).
    ///
    /// # Returns
    ///
    /// A new `ClimateLazyFrame` filtered to the specified period and month. Typically,
    /// collecting this frame should result in zero or one row.
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
    /// let london = LatLon(51.50, -0.12); // London location
    ///
    /// let climate_lazy = client.climate().location(london).call().await?;
    ///
    /// // Get climate normals for July for the 1991-2020 period
    /// let july_normals_lazy = climate_lazy.get_at(Year(1991), Year(2020), 7);
    ///
    /// // Collect the result (should be one row if data exists)
    /// let df = july_normals_lazy.frame.collect()?;
    /// if df.height() == 1 {
    ///     println!("July 1991-2020 Normals:\n{}", df);
    /// } else {
    ///     println!("No 1991-2020 climate normals found for July at the nearest station.");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Similar to [`filter`], this method modifies the lazy query plan. Errors ([`PolarsError`])
    /// may occur during subsequent computation (e.g., `.collect()`).
    pub fn get_at(&self, start_year: Year, end_year: Year, month: u32) -> ClimateLazyFrame {
        self.filter(
            col("start_year")
                .eq(lit(start_year.get()))
                .and(col("end_year").eq(lit(end_year.get())))
                .and(col("month").eq(lit(month))), // Ensure month is u32 literal
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError};
    use polars::prelude::*;

    // Helper to fetch climate data for tests - reduces repetition
    // Uses a station known to often have climate normals (Berlin Tempelhof)
    async fn get_test_climate_frame() -> Result<ClimateLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.climate().station("10384").await // Berlin Tempelhof
    }

    #[tokio::test]
    async fn test_climate_frame_new() -> Result<(), MeteostatError> {
        let climate_lazy = get_test_climate_frame().await?;
        // Basic check: the frame exists and has the expected columns
        let expected_cols = [
            "start_year",
            "end_year",
            "month",
            "tmin",
            "tmax",
            "prcp",
            "wspd",
            "pres",
            "tsun",
        ];
        let df = climate_lazy.frame.collect()?;
        let actual_cols = df.get_column_names();
        for col_name in expected_cols {
            assert!(
                actual_cols.contains(&&PlSmallStr::from_str(col_name)),
                "Expected column '{}' not found",
                col_name
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_climate_frame_get_at_specific_month() -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;

        // Get data for July (month 7) for the common 1991-2020 period
        let start_yr = Year(1991);
        let end_yr = Year(2020);
        let target_month = 7u32;

        let specific_month_lazy = climate_lazy.get_at(start_yr, end_yr, target_month);
        let df = specific_month_lazy.frame.collect()?;

        // Expect exactly one row for a specific climate normal period/month
        assert_eq!(
            df.height(),
            1,
            "Expected exactly one row for 1991-2020 July normals"
        );

        // Verify the values in that row

        // Extract values by column name
        let row_start_year =df.column("start_year")?.i64()?.get(0).unwrap();
        let row_end_year = df.column("end_year")?.i64()?.get(0).unwrap();
        let row_month = df.column("month")?.i64()?.get(0).unwrap();

        dbg!(row_start_year, row_end_year, row_month);

        assert_eq!(row_start_year, start_yr.get() as i64);
        assert_eq!(row_end_year, end_yr.get() as i64);
        assert_eq!(row_month, target_month as i64);

        Ok(())
    }

    #[tokio::test]
    async fn test_climate_frame_get_at_no_results() -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;

        // Try to get data for a non-existent period or month
        let start_yr = Year(1800); // Unlikely period
        let end_yr = Year(1830);
        let target_month = 13u32; // Invalid month

        let no_results_lazy_period = climate_lazy.clone().get_at(start_yr, end_yr, 1); // Valid month, invalid period
        let no_results_lazy_month = climate_lazy.get_at(Year(1991), Year(2020), target_month); // Valid period, invalid month

        let df_period = no_results_lazy_period.frame.collect()?;
        let df_month = no_results_lazy_month.frame.collect()?;

        assert_eq!(
            df_period.height(),
            0,
            "Expected zero rows for non-existent climate period"
        );
        assert_eq!(
            df_month.height(),
            0,
            "Expected zero rows for invalid month (13)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_climate_frame_chaining_filters() -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;

        // Get July 1991-2020 data, then filter where max temp > 20.0
        let july_normals_lazy = climate_lazy
            .get_at(Year(1991), Year(2020), 7)
            .filter(col("tmax").gt(lit(20.0f64))); // Add filter for max temp

        let df = july_normals_lazy.frame.collect()?;

        // It should still be one row if July's max temp is > 20 (likely in Berlin)
        // If the condition isn't met, it will be 0 rows.
        if df.height() == 1 {
            let tmax_val: f64 = df.column("tmax")?.f64()?.get(0).unwrap();
            assert!(tmax_val > 20.0, "Max temp should be > 20.0");
            println!("Filtered July (tmax > 20.0) data found: {}", df);
        } else {
            // This case is also valid if the condition wasn't met
            println!("No July 1991-2020 data found with tmax > 20.0");
            assert_eq!(df.height(), 0);
        }

        Ok(())
    }
}
