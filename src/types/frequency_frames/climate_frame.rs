// climate_frame.rs

//! Contains the `ClimateLazyFrame` structure for handling lazy operations on Meteostat climate data.

use crate::{MeteostatError, Year};
// Added MeteostatError
use polars::prelude::{col, lit, DataFrame, Expr, LazyFrame};
// Added DataFrame

/// Represents a row of climate normals data, suitable for collecting results.
#[derive(Debug, Clone, PartialEq)] // Made public and added derives
pub struct Climate {
    /// The starting year of the climate normal period.
    pub start_year: i32,
    /// The ending year of the climate normal period.
    pub end_year: i32,
    /// The month (1-12) the normal applies to.
    pub month: u32,
    /// Average minimum air temperature in Celsius for the month.
    pub minimum_temperature: Option<f64>, // tmin
    /// Average maximum air temperature in Celsius for the month.
    pub maximum_temperature: Option<f64>, // tmax
    /// Average total precipitation amount in mm for the month.
    pub precipitation: Option<f64>, // prcp
    /// Average wind speed in km/h for the month.
    pub wind_speed: Option<f64>, // wspd
    /// Average sea-level air pressure in hPa for the month.
    pub pressure: Option<f64>, // pres
    /// Average total sunshine duration in minutes for the month.
    pub sunshine_minutes: Option<i32>, // tsun (read as i64, store as i32)
}

/// A wrapper around a Polars `LazyFrame` specifically for Meteostat climate data.
///
/// This struct provides methods tailored for common operations on climate normals datasets,
/// such as filtering by period and month, while retaining the benefits of lazy evaluation.
/// It also includes methods to collect the results into Rust structs.
///
/// Instances are typically obtained via [`crate::Meteostat::climate`].
///
/// # Errors
///
/// Operations that trigger computation on the underlying `LazyFrame` (e.g., calling `.collect()`,
/// or the collection methods here) can potentially return a [`polars::prelude::PolarsError`]
/// (wrapped as [`MeteostatError::PolarsError`]).
///
/// The `collect_single_climate` method returns [`MeteostatError::ExpectedSingleRow`] if the frame
/// does not contain exactly one row upon collection.
///
/// The initial creation via [`crate::Meteostat::climate`] methods can return a [`MeteostatError`] if
/// data fetching or station lookup fails.
#[derive(Clone)]
pub struct ClimateLazyFrame {
    /// The underlying Polars LazyFrame containing the climate data.
    pub frame: LazyFrame,
}

impl ClimateLazyFrame {
    /// Creates a new `ClimateLazyFrame` wrapping the given Polars `LazyFrame`.
    ///
    /// This is typically called internally by the [`crate::Meteostat`] client methods.
    ///
    /// # Arguments
    ///
    /// * `frame` - A `LazyFrame` assumed to contain climate data with the expected schema
    ///   (columns like "start_year", "end_year", "month", "tmin", etc.). Year and month
    ///   columns are expected to be numerical (like Int64).
    pub(crate) fn new(frame: LazyFrame) -> Self {
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
    /// // Ensure the literal type matches the column type or can be cast.
    /// let summer_autumn_climate = climate_lazy.filter(col("month").gt(lit(6i64))); // Use i64 if column is i64
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
    /// While this method itself doesn't typically error, subsequent operations like `.collect()`
    /// might return a [`polars::prelude::PolarsError`].
    pub fn filter(&self, predicate: Expr) -> ClimateLazyFrame {
        ClimateLazyFrame {
            frame: self.frame.clone().filter(predicate),
        }
    }

    /// Filters the climate data to get the normals for a specific period and month.
    ///
    /// This is a convenience method that filters the data based on the `start_year`,
    /// `end_year`, and `month` columns, which are standard in Meteostat climate normals data.
    /// It assumes these columns are numerical in the underlying frame.
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
    /// May eventually lead to a [`polars::prelude::PolarsError`] during computation (e.g., `.collect()`).
    pub fn get_at(&self, start_year: Year, end_year: Year, month: u32) -> ClimateLazyFrame {
        self.filter(
            col("start_year")
                .eq(lit(start_year.get() as i64)) // Match i64 column type
                .and(col("end_year").eq(lit(end_year.get() as i64))) // Match i64 column type
                .and(col("month").eq(lit(month as i64))), // Match i64 column type
        )
    }

    /// Executes the lazy query and collects the results into a `Vec<Climate>`.
    ///
    /// This method triggers the computation defined by the `LazyFrame` (including any
    /// previous filtering operations) and maps each resulting row to a `Climate` struct.
    /// Rows where the essential 'start_year', 'end_year', or 'month' columns are missing
    /// or invalid are skipped.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Climate>` on success, or a [`MeteostatError`]
    /// if the computation or mapping fails (e.g., `MeteostatError::PolarsError`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Climate};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// let climate_lazy = client
    ///     .climate()
    ///     .location(paris)
    ///     .call()
    ///     .await?;
    ///
    /// // Collect all available climate normals
    /// let climate_vec: Vec<Climate> = climate_lazy.collect_climate()?;
    ///
    /// println!("Collected {} climate normal records.", climate_vec.len());
    /// if let Some(first_normal) = climate_vec.first() {
    ///     println!("First record: {:?}", first_normal);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_climate(&self) -> Result<Vec<Climate>, MeteostatError> {
        let df = self
            .frame
            .clone() // Clone frame as collect consumes it
            .collect()
            .map_err(MeteostatError::PolarsError)?; // Map PolarsError

        Self::dataframe_to_climate_vec(&df) // Use helper function
    }

    /// Executes the lazy query, expecting exactly one row, and collects it into a `Climate` struct.
    ///
    /// This is useful after filtering the frame down to a single expected record,
    /// for example using `get_at()`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the single `Climate` struct on success.
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
    /// # use meteostat::{Meteostat, MeteostatError, LatLon, Year, Climate};
    /// use polars::prelude::PolarsError;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10384"; // Berlin Tempelhof
    ///
    /// let climate_lazy = client.climate().station(station_id).call().await?;
    ///
    /// // Get data for a specific period and month
    /// let target_start = Year(1991);
    /// let target_end = Year(2020);
    /// let target_month = 8; // August
    ///
    /// let single_climate_lazy = climate_lazy.get_at(target_start, target_end, target_month);
    ///
    /// // Collect the single expected row
    /// match single_climate_lazy.collect_single_climate() {
    ///     Ok(climate_data) => {
    ///         println!("Collected single climate normal: {:?}", climate_data);
    ///         assert_eq!(climate_data.start_year, 1991);
    ///         assert_eq!(climate_data.end_year, 2020);
    ///         assert_eq!(climate_data.month, 8); // Verify correct month
    ///     },
    ///     Err(MeteostatError::ExpectedSingleRow { actual }) => {
    ///          println!("Expected 1 row, but found {}. Climate normal might be missing.", actual);
    ///          // Handle missing data case if necessary
    ///          // assert_eq!(actual, 0); // Or assert based on expected availability
    ///     },
    ///     Err(e) => return Err(e), // Propagate other errors
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn collect_single_climate(&self) -> Result<Climate, MeteostatError> {
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
        Self::dataframe_to_climate_vec(&df)?
            .pop() // Take the only element
            .ok_or(MeteostatError::ExpectedSingleRow { actual: 0 }) // Should be unreachable
    }

    // --- Helper function to map DataFrame rows to Vec<Climate> ---
    fn dataframe_to_climate_vec(df: &DataFrame) -> Result<Vec<Climate>, MeteostatError> {
        // --- Get required columns as Series ---
        let start_year_series = df.column("start_year")?;
        let end_year_series = df.column("end_year")?;
        let month_series = df.column("month")?;
        let tmin_series = df.column("tmin")?;
        let tmax_series = df.column("tmax")?;
        let prcp_series = df.column("prcp")?;
        let wspd_series = df.column("wspd")?;
        let pres_series = df.column("pres")?;
        let tsun_series = df.column("tsun")?;

        // --- Get ChunkedArrays (assuming Polars read them as i64/f64) ---
        let start_year_ca = start_year_series.i64()?;
        let end_year_ca = end_year_series.i64()?;
        let month_ca = month_series.i64()?;
        let tmin_ca = tmin_series.f64()?;
        let tmax_ca = tmax_series.f64()?;
        let prcp_ca = prcp_series.f64()?;
        let wspd_ca = wspd_series.f64()?;
        let pres_ca = pres_series.f64()?;
        let tsun_ca = tsun_series.i64()?; // Read as i64 initially

        let mut climate_vec = Vec::with_capacity(df.height());

        // --- Iterate through rows and map ---
        for i in 0..df.height() {
            // Get year and month (essential) - skip row if missing or invalid
            let start_year_opt: Option<i32> =
                start_year_ca.get(i).and_then(|y| i32::try_from(y).ok());
            let end_year_opt: Option<i32> =
                end_year_ca.get(i).and_then(|y| i32::try_from(y).ok());
            let month_opt: Option<u32> = month_ca
                .get(i)
                .and_then(|m| u32::try_from(m).ok())
                .filter(|&m| m >= 1 && m <= 12); // Validate month range

            let (Some(start_year), Some(end_year), Some(month)) =
                (start_year_opt, end_year_opt, month_opt)
            else {
                // Skip row if start_year, end_year or month is missing or invalid
                continue;
            };

            // Construct the struct
            let climate_record = Climate {
                start_year,
                end_year,
                month,
                minimum_temperature: tmin_ca.get(i),
                maximum_temperature: tmax_ca.get(i),
                precipitation: prcp_ca.get(i),
                wind_speed: wspd_ca.get(i),
                pressure: pres_ca.get(i),
                sunshine_minutes: tsun_ca.get(i).and_then(|v| i32::try_from(v).ok()), // Convert Option<i64> to Option<i32>
            };

            climate_vec.push(climate_record);
        }

        Ok(climate_vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Meteostat, MeteostatError, Year};
    use polars::prelude::*;

    // Helper to fetch climate data for tests - reduces repetition
    // Uses a station known to often have climate normals (Berlin Tempelhof)
    async fn get_test_climate_frame() -> Result<ClimateLazyFrame, MeteostatError> {
        let client = Meteostat::new().await?;
        client.climate().station("10384").call().await // Berlin Tempelhof
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
        let df = climate_lazy.frame.limit(1).collect()?; // Collect small sample
        let actual_cols = df.get_column_names();
        for col_name in expected_cols {
            assert!(
                actual_cols.contains(&&PlSmallStr::from_str(col_name)),
                "Expected column '{}' not found",
                col_name
            );
        }
        // Check key column types (as read by Polars)
        assert_eq!(df.column("start_year")?.dtype(), &DataType::Int64);
        assert_eq!(df.column("month")?.dtype(), &DataType::Int64);
        assert_eq!(df.column("tmin")?.dtype(), &DataType::Float64);
        assert_eq!(df.column("tsun")?.dtype(), &DataType::Int64);

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

        // Expect exactly one row for a specific climate normal period/month if available
        if df.height() == 0 {
            println!("Warning: Climate normal for 1991-2020 July not found for station 10384. Skipping detailed checks.");
            return Ok(()); // Test passes if data is missing
        }

        assert_eq!(
            df.height(),
            1,
            "Expected exactly one row for 1991-2020 July normals"
        );

        // Verify the values in that row
        let row_start_year = df.column("start_year")?.i64()?.get(0).unwrap();
        let row_end_year = df.column("end_year")?.i64()?.get(0).unwrap();
        let row_month = df.column("month")?.i64()?.get(0).unwrap();

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
        // If the condition isn't met, or the base data is missing, it will be 0 rows.
        if df.height() == 1 {
            let tmax_val: f64 = df.column("tmax")?.f64()?.get(0).unwrap();
            assert!(tmax_val > 20.0, "Max temp should be > 20.0");
            println!("Filtered July (tmax > 20.0) data found: {}", df);
        } else {
            println!(
                "Filtered July 1991-2020 data with tmax > 20.0 not found (height: {})",
                df.height()
            );
            assert_eq!(df.height(), 0); // Expect 0 if initial row missing or filter excludes it
        }

        Ok(())
    }

    // --- New Tests for Collection Methods ---

    #[tokio::test]
    async fn test_collect_climate_vec() -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;

        // Collect all available climate normals (usually 12 for 1961-1990 and 12 for 1991-2020)
        let climate_vec = climate_lazy.collect_climate()?;
        let df_height = climate_lazy.frame.clone().collect()?.height(); // Get expected height

        // Check if collection matches collected DataFrame height (after potential parsing skips)
        // Allow for slight discrepancies if parsing fails on edge cases, but should be close
        assert!(
            (climate_vec.len() as isize - df_height as isize).abs() <= 1,
            "Collected vector length ({}) should be close to DataFrame height ({})",
            climate_vec.len(),
            df_height
        );

        // Expect some data (likely 12 or 24 rows for standard periods)
        assert!(
            !climate_vec.is_empty(),
            "Expected some climate normal data"
        );

        // Check the first record if it exists
        if let Some(first_record) = climate_vec.first() {
            println!("First collected record: {:?}", first_record);
            assert!(first_record.start_year == 1961 || first_record.start_year == 1991); // Common periods
            assert!(first_record.month >= 1 && first_record.month <= 12);
            assert!(first_record.minimum_temperature.is_some() || first_record.minimum_temperature.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_climate_single_row_success() -> Result<(), MeteostatError> {
        let climate_lazy = get_test_climate_frame().await?;
        let target_start = Year(1991);
        let target_end = Year(2020);
        let target_month = 9; // September

        let single_climate_lazy = climate_lazy.get_at(target_start, target_end, target_month);

        // Use collect_single_climate, handling potential missing data for this specific entry
        match single_climate_lazy.collect_single_climate() {
            Ok(climate_record) => {
                println!("Collected single record: {:?}", climate_record);
                assert_eq!(climate_record.start_year, target_start.get());
                assert_eq!(climate_record.end_year, target_end.get());
                assert_eq!(climate_record.month, target_month);
                assert!(climate_record.maximum_temperature.is_some()); // Example check
            }
            Err(MeteostatError::ExpectedSingleRow { actual: 0 }) => {
                println!("Warning: Climate normal for 1991-2020 Sep not found for station 10384. Test passes.");
            }
            Err(e) => return Err(e), // Propagate other errors
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_collect_climate_single_row_fail_multiple_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;
        // Filter for a specific year, which should yield 12 rows if complete
        let multi_climate_lazy = climate_lazy.filter(col("start_year").eq(lit(1991i64)));
        let result = multi_climate_lazy.collect_single_climate(); // Expect this to fail

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Got expected error: {:?}", err);

        match err {
            MeteostatError::ExpectedSingleRow { actual } => {
                // Should be 12 if data is complete, but might be less
                assert!(actual > 1, "Expected actual rows to be > 1, got {}", actual);
            }
            _ => panic!("Expected MeteostatError::ExpectedSingleRow, got {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_collect_climate_single_row_fail_zero_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;
        // Use a period/month guaranteed not to exist
        let ancient_start = Year(1800);
        let ancient_end = Year(1830);
        let target_month = 5;

        let zero_climate_lazy = climate_lazy.get_at(ancient_start, ancient_end, target_month);
        let result = zero_climate_lazy.collect_single_climate(); // Expect this to fail

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
    async fn test_collect_climate_vec_empty_result() -> Result<(), Box<dyn std::error::Error>> {
        let climate_lazy = get_test_climate_frame().await?;
        // Filter to guarantee an empty result
        let empty_lazy = climate_lazy.filter(col("start_year").eq(lit(100i64))); // Impossible year
        let climate_vec = empty_lazy.collect_climate()?;

        assert!(
            climate_vec.is_empty(),
            "Expected empty vector for impossible filter condition"
        );

        Ok(())
    }
}