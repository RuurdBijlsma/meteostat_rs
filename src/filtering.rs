use crate::types::into_utc_trait::IntoUtcDateTime;
use chrono::{NaiveDate};
use polars::prelude::{
    col, duration, lit, DataType, DurationArgs, LazyFrame, StrptimeOptions, TimeUnit,
};

pub trait MeteostatFrameFilterExt {
    /// Filters an hourly LazyFrame by a UTC datetime range (inclusive).
    /// Assumes the 'date' column is 'YYYY-MM-DD' and 'hour' column is integer hour.
    ///
    /// # Arguments
    /// * `start`: The start DateTime (inclusive).
    /// * `end`: The end DateTime (inclusive).
    ///
    /// # Returns
    /// A new `LazyFrame` with the filter applied. Potential parsing errors
    /// occur during execution (e.g., `collect`).
    fn filter_hourly(
        self,
        start: impl IntoUtcDateTime + Clone,
        end: impl IntoUtcDateTime + Clone,
    ) -> LazyFrame;

    /// Filters a daily LazyFrame by a NaiveDate range (inclusive).
    /// Assumes the 'date' column is 'YYYY-MM-DD'.
    ///
    /// # Arguments
    /// * `start_date`: The start NaiveDate (inclusive).
    /// * `end_date`: The end NaiveDate (inclusive).
    ///
    /// # Returns
    /// A new `LazyFrame` with the filter applied. Potential parsing errors
    /// occur during execution (e.g., `collect`).
    fn filter_daily(self, start_date: NaiveDate, end_date: NaiveDate) -> LazyFrame; // Changed return type - error handling happens on collect

    /// Filters a monthly LazyFrame by a year range (inclusive).
    /// Assumes 'year' and 'month' columns exist.
    ///
    /// # Arguments
    /// * `start_year`: The start year (inclusive).
    /// * `end_year`: The end year (inclusive).
    ///
    /// # Returns
    /// A new `LazyFrame` with the filter applied.
    fn filter_monthly(self, start_year: i32, end_year: i32) -> LazyFrame;

    /// Filters a climate LazyFrame by a year range (inclusive).
    /// Filters records where the record's period [record_start_year, record_end_year]
    /// is fully contained within the provided `start_year` and `end_year`.
    /// Assumes 'start_year' and 'end_year' columns exist.
    ///
    /// # Arguments
    /// * `start_year`: The start year of the desired range (inclusive).
    /// * `end_year`: The end year of the desired range (inclusive).
    ///
    /// # Returns
    /// A new `LazyFrame` with the filter applied.
    fn filter_climate(self, start_year: i32, end_year: i32) -> LazyFrame;
}

impl MeteostatFrameFilterExt for LazyFrame {
    fn filter_hourly(
        self,
        start: impl IntoUtcDateTime + Clone,
        end: impl IntoUtcDateTime + Clone,
    ) -> LazyFrame {
        let start_utc = start.into_utc();
        let end_utc = end.into_utc();
        let start_naive = start_utc.naive_utc();
        let end_naive = end_utc.naive_utc();

        // Construct a NaiveDateTime expression from 'date' and 'hour' columns
        // 1. Parse 'date' string to Date type.
        // 2. Cast Date to Datetime (at time 00:00:00).
        // 3. Add 'hour' hours to the Datetime.
        let datetime_expr = col("date")
            .str()
            .strptime(
                DataType::Date, // Target type
                StrptimeOptions {
                    format: Some("%Y-%m-%d".into()),
                    strict: true,  // Must match format exactly
                    exact: true,   // Must consume entire string
                    cache: true,
                },
                lit("raise"),
            )
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            + duration(DurationArgs::new().with_hours(col("hour")));

        // Since strptime no longer returns Result, the Ok() wrapper is removed.
        // The function now returns LazyFrame directly.
        self.filter(
            datetime_expr
                .clone()
                .gt_eq(lit(start_naive))
                .and(datetime_expr.lt_eq(lit(end_naive))),
        )
    }

    fn filter_daily(self, start_date: NaiveDate, end_date: NaiveDate) -> LazyFrame {
        // Changed return type
        // Parse the 'date' string column to Polars Date type
        let date_expr = col("date").str().strptime(
            DataType::Date,
            StrptimeOptions {
                format: Some("%Y-%m-%d".into()),
                strict: true,
                exact: true,
                cache: true,
            },
            lit("raise"),
        );

        // Since strptime no longer returns Result, the Ok() wrapper is removed.
        // The function now returns LazyFrame directly.
        self.filter(
            date_expr
                .clone()
                .gt_eq(lit(start_date)) // Greater than or equal to start
                .and(date_expr.lt_eq(lit(end_date))), // Less than or equal to end
        )
    }

    fn filter_monthly(self, start_year: i32, end_year: i32) -> LazyFrame {
        // Filter directly on the 'year' column
        self.filter(
            col("year")
                .gt_eq(lit(start_year as i64)) // Greater than or equal to start year
                .and(col("year").lt_eq(lit(end_year as i64))), // Less than or equal to end year
        )
    }

    fn filter_climate(self, start_year: i32, end_year: i32) -> LazyFrame {
        // Filter rows where the climate period [record_start_year, record_end_year]
        // is fully contained within [start_year, end_year].
        self.filter(
            col("start_year")
                .gt_eq(lit(start_year as i64))
                .and(col("end_year").lt_eq(lit(end_year as i64))),
        )
    }
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MeteostatError;
    use crate::meteostat::{LatLon, Meteostat};
    use crate::types::data_source::Frequency;
    use chrono::{NaiveDate, TimeZone, Utc};
    use polars::prelude::df;

    #[tokio::test]
    async fn test_get_hourly_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        // 1. Arrange: Fetch the base LazyFrame for hourly data
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        // 2. Arrange: Define a specific UTC time range (e.g., one day)
        // Use a past date where data is likely available in the cache/source
        let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
        let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap(); // Inclusive end

        // 3. Act: Apply the hourly filter
        let filtered_lazy_frame = lazy_frame.filter_hourly(start_utc, end_utc);

        // 4. Assert: Collect the results and verify
        let frame_result = filtered_lazy_frame.collect();
        assert!(
            frame_result.is_ok(),
            "Collecting filtered hourly data failed: {:?}",
            frame_result.err()
        );
        let frame = frame_result?;

        dbg!(&frame);

        let shape = frame.shape();

        // Assertions:
        // Expect at most 24 rows for a single day. Could be less if data is missing.
        assert!(
            shape.0 <= 24,
            "Expected max 24 hourly records for 2023-10-26, got {}",
            shape.0
        );

        // Only check column count and content if rows were actually returned
        if shape.0 > 0 {
            // Expect the standard 13 columns for hourly data
            assert_eq!(shape.1, 13, "Expected 13 columns for hourly data");
            let date_col = frame.column("date")?.str()?;
            let hour_col = frame.column("hour")?.i64()?; // Use i64 if that's the type

            let start_naive = start_utc.naive_utc();
            let end_naive = end_utc.naive_utc();

            for (date_str_opt, hour_opt) in date_col.into_iter().zip(hour_col.into_iter()) {
                match (date_str_opt, hour_opt) {
                    (Some(date_str), Some(hour)) => {
                        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                            .expect("Failed to parse date string in collected frame");
                        // Safely cast hour to u32 for NaiveDate::and_hms_opt
                        let hour_u32 =
                            u32::try_from(hour).expect("Hour value out of range for u32");
                        let record_naive_dt = date
                            .and_hms_opt(hour_u32, 0, 0)
                            .expect("Failed to create NaiveDateTime from date and hour");

                        assert!(
                            record_naive_dt >= start_naive && record_naive_dt <= end_naive,
                            "Record timestamp {} is outside the requested range [{}, {}]",
                            record_naive_dt,
                            start_naive,
                            end_naive
                        );
                    }
                    _ => panic!("Null value found in date or hour column after filtering"),
                }
            }
        } else {
            // It's possible no data exists for this specific day in the cache/source.
            // The test still passes because the filtering logic itself didn't error.
            println!("Warning: No hourly data found for station 10637 on 2023-10-26. Filter test passed structurally.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Daily)
            .call()
            .await?;

        // Define the date range: Year 2023
        let start_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

        // Apply the filter - returns LazyFrame directly now
        let filtered_lazy_frame = lazy_frame.filter_daily(start_date, end_date);
        let daily_frame_result = filtered_lazy_frame.collect();

        assert!(
            daily_frame_result.is_ok(),
            "Collecting daily data failed: {:?}",
            daily_frame_result.err()
        );
        let daily_frame = daily_frame_result?;

        dbg!(&daily_frame);

        let shape = daily_frame.shape();
        // Expect 365 days for 2023 (not a leap year)
        // Allow for slight variations if data source is missing a day
        assert!(
            shape.0 >= 360 && shape.0 <= 365,
            "Expected around 365 days for 2023, got {}",
            shape.0
        );
        assert_eq!(shape.1, 11);

        assert_eq!(
            daily_frame.get_column_names(),
            [
                "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres",
                "tsun",
            ]
        );

        // Optional: Verify dates are within range (more robust test)
        let date_col = daily_frame.column("date")?.str()?;
        for date_str_opt in date_col.into_iter() {
            if let Some(date_str) = date_str_opt {
                let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();
                assert!(
                    date >= start_date && date <= end_date,
                    "Date {} out of range",
                    date
                );
            } else {
                panic!("Date column contains nulls");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Monthly)
            .call()
            .await?;

        // Filter for years 2020 to 2022 inclusive
        let filtered_lazy_frame = lazy_frame.filter_monthly(2020, 2022);

        // collect() handles potential errors
        let monthly_frame = filtered_lazy_frame.collect()?;
        dbg!(&monthly_frame);

        let shape = monthly_frame.shape();
        // Expect 3 years * 12 months = 36 records
        // Allow for variations if data isn't complete up to current month end
        assert!(
            shape.0 >= 30 && shape.0 <= 36,
            "Expected around 36 months for 2020-2022, got {}",
            shape.0
        );
        assert_eq!(shape.1, 9); // Check number of columns based on your debug output

        // Optional: Verify years
        let year_col = monthly_frame.column("year")?.i64()?;
        for year_opt in year_col.into_iter() {
            let year = year_opt.unwrap();
            assert!(year >= 2020 && year <= 2022, "Year {} out of range", year);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let lazy_frame = meteostat
            .from_station()
            .station("10637") // Climate data often available for major stations
            .frequency(Frequency::Climate)
            .call()
            .await?;

        // Filter for the 1991-2020 climate period records specifically
        let filtered_lazy_frame = lazy_frame.filter_climate(1991, 2020);

        // collect() handles potential errors
        let climate_frame = filtered_lazy_frame.collect()?;
        dbg!(&climate_frame);

        let shape = climate_frame.shape();
        // Expect 12 months for the single period 1991-2020
        assert_eq!(
            shape.0, 12,
            "Expected 12 months for the 1991-2020 climate period"
        );
        assert_eq!(shape.1, 9); // Based on your dbg! output

        // Verify start/end years
        let start_year_col = climate_frame.column("start_year")?.i64()?;
        let end_year_col = climate_frame.column("end_year")?.i64()?;
        for (start_opt, end_opt) in start_year_col.into_iter().zip(end_year_col.into_iter()) {
            assert_eq!(start_opt.unwrap(), 1991);
            assert_eq!(end_opt.unwrap(), 2020);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_hourly_location_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let lazy_frame = meteostat
            .from_location()
            .location(LatLon {
                lat: 52.520008,
                lon: 13.404954,
            }) // Berlin center
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        // Define a specific time range, e.g., a single day in UTC
        let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
        let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap(); // Inclusive end

        // Apply filter - returns LazyFrame directly now
        let filtered_lazy_frame = lazy_frame.filter_hourly(start_utc, end_utc); // No ? needed

        // Collect will now return the potential parsing error
        let frame_result = filtered_lazy_frame.collect();
        assert!(
            frame_result.is_ok(),
            "Collecting hourly location data failed: {:?}",
            frame_result.err()
        );
        let frame = frame_result?;

        dbg!(&frame);

        let shape = frame.shape();
        // Expect up to 24 records for one day
        assert!(
            shape.0 <= 24,
            "Expected max 24 hourly records for one day, got {}",
            shape.0
        );
        // If data exists, expect 13 columns
        if shape.0 > 0 {
            assert_eq!(shape.1, 13);
        }

        // Optional: More robust check on the timestamps if needed after collection

        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily_invalid_date_format() -> Result<(), MeteostatError> {
        // No need for real Meteostat here
        // let meteostat = get_meteostat().await?;

        // Create a fake LazyFrame with an invalid date format
        use polars::prelude::*;
        let df = df! {
            "date" => &["26/10/2023", "27/10/2023"], // Invalid format for "%Y-%m-%d"
            "tavg" => &[10.0, 11.0]
        }?;
        let lazy_frame = df.lazy();

        // Define the date range
        let start_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

        // Apply the filter - this succeeds as it only builds the plan
        let filtered_lazy_frame = lazy_frame.filter_daily(start_date, end_date);

        // Attempt to collect - THIS is where the error should occur
        let result = filtered_lazy_frame.collect();

        assert!(
            result.is_err(),
            "Expected a PolarsError during collect due to invalid date format"
        );
        if let Err(e) = result {
            println!("Successfully caught expected error during collect: {}", e);
            // Check if the error is InvalidOperation, which Polars raises for format mismatches here.
            assert!(
                matches!(e, PolarsError::InvalidOperation(_)),
                "Expected InvalidOperation error due to parsing failure, got {:?}",
                e
            );
        } else {
            panic!("Expected an error during collect, but got Ok");
        }

        Ok(())
    }
}
