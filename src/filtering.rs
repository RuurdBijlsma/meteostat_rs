use crate::types::into_utc_trait::IntoUtcDateTime;
use chrono::{Duration, NaiveDate, Timelike};
use polars::prelude::{col, lit, DataType, LazyFrame};

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

    /// Gets a single row from an hourly LazyFrame matching the hour closest
    /// to the specific UTC datetime provided.
    /// Rounds the input datetime to the nearest hour (>= 30 minutes rounds up).
    ///
    /// # Arguments
    /// * `datetime`: The UTC DateTime to find the nearest hourly record for.
    ///
    /// # Returns
    /// A new `LazyFrame` containing the matching row (or zero rows if not found).
    fn get_hourly_row(self, datetime: impl IntoUtcDateTime + Clone) -> LazyFrame;

    /// Gets a single row from a daily LazyFrame matching a specific NaiveDate.
    ///
    /// # Arguments
    /// * `date`: The exact NaiveDate to match.
    ///
    /// # Returns
    /// A new `LazyFrame` containing the matching row (or zero rows if not found).
    fn get_daily_row(self, date: NaiveDate) -> LazyFrame;

    /// Gets a single row from a monthly LazyFrame matching a specific year and month.
    ///
    /// # Arguments
    /// * `year`: The exact year to match.
    /// * `month`: The exact month (1-12) to match.
    ///
    /// # Returns
    /// A new `LazyFrame` containing the matching row (or zero rows if not found).
    fn get_monthly_row(self, year: i32, month: u32) -> LazyFrame;

    /// Gets a single row from a climate LazyFrame matching a specific climate period and month.
    ///
    /// # Arguments
    /// * `start_year`: The start year of the climate period to match.
    /// * `end_year`: The end year of the climate period to match.
    /// * `month`: The exact month (1-12) within the period to match.
    ///
    /// # Returns
    /// A new `LazyFrame` containing the matching row (or zero rows if not found).
    fn get_climate_row(self, start_year: i32, end_year: i32, month: u32) -> LazyFrame;
}

impl MeteostatFrameFilterExt for LazyFrame {
    fn filter_hourly(
        self,
        start: impl IntoUtcDateTime + Clone,
        end: impl IntoUtcDateTime + Clone,
    ) -> LazyFrame {
        let start_naive = start.into_utc().naive_utc();
        let end_naive = end.into_utc().naive_utc();

        self.filter(
            col("datetime")
                .gt_eq(lit(start_naive)) // No cast
                .and(
                    col("datetime").lt_eq(lit(end_naive)), // No cast
                ),
        )
    }

    fn filter_daily(self, start_date: NaiveDate, end_date: NaiveDate) -> LazyFrame {
        // Filter directly on the pre-parsed 'date' column (which is now DataType::Date)
        self.filter(
            col("date")
                .cast(DataType::Date) // Ensure correct type for comparison
                .gt_eq(lit(start_date))
                .and(
                    col("date")
                        .cast(DataType::Date) // Ensure correct type
                        .lt_eq(lit(end_date)),
                ),
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

    fn get_hourly_row(self, datetime: impl IntoUtcDateTime + Clone) -> LazyFrame {
        let input_utc = datetime.into_utc();

        // Round to the nearest hour
        let rounded_hour_start_utc = if input_utc.minute() >= 30 {
            // Round up: Add an hour, then truncate minutes/seconds/nanos
            // Using Duration::hours handles potential day/month/year rollovers
            (input_utc + Duration::hours(1))
                .with_minute(0)
                .and_then(|dt| dt.with_second(0))
                .and_then(|dt| dt.with_nanosecond(0))
                // Setting 0 for minute/second/nanosecond is always valid
                .expect("Truncating to start of hour after adding hour failed unexpectedly")
        } else {
            // Round down: Truncate minutes/seconds/nanos
            input_utc
                .with_minute(0)
                .and_then(|dt| dt.with_second(0))
                .and_then(|dt| dt.with_nanosecond(0))
                // Setting 0 for minute/second/nanosecond is always valid
                .expect("Truncating to start of hour failed unexpectedly")
        };

        // Convert the target UTC datetime (start of the hour) to NaiveDateTime for Polars comparison
        let target_naive = rounded_hour_start_utc.naive_utc();

        // Filter the LazyFrame for the exact start of the rounded hour
        self.filter(col("datetime").eq(lit(target_naive)))
    }

    fn get_daily_row(self, date: NaiveDate) -> LazyFrame {
        self.filter(col("date").cast(DataType::Date).eq(lit(date))) // Use exact equality
    }

    fn get_monthly_row(self, year: i32, month: u32) -> LazyFrame {
        self.filter(
            col("year")
                .eq(lit(year as i64))
                .and(col("month").eq(lit(month as i64))), // Match both year and month
        )
    }

    fn get_climate_row(self, start_year: i32, end_year: i32, month: u32) -> LazyFrame {
        self.filter(
            col("start_year")
                .eq(lit(start_year as i64))
                .and(col("end_year").eq(lit(end_year as i64)))
                .and(col("month").eq(lit(month as i64))), // Match start_year, end_year, and month
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
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use polars::prelude::{DateChunked, DatetimeChunked, TimeUnit};

    #[tokio::test]
    async fn test_get_hourly_filtered() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
        let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap();

        let filtered_lazy_frame = lazy_frame.filter_hourly(start_utc, end_utc);

        let frame = filtered_lazy_frame.collect()?;
        dbg!(&frame);

        let shape = frame.shape();
        assert!(
            shape.0 <= 24,
            "Expected max 24 hourly records for 2023-10-26, got {}",
            shape.0
        );

        if shape.0 > 0 {
            assert_eq!(
                shape.1, 14,
                "Expected 14 columns for hourly data (incl. datetime)"
            );

            // Verify filtering using the 'datetime' column
            let dt_col: &DatetimeChunked = frame
                .column("datetime")?
                .datetime() // Access as DatetimeChunked
                .map_err(|e| MeteostatError::PolarsError(e))?; // Handle potential type mismatch

            // Check the time unit of the column
            let time_unit = match dt_col.time_unit() {
                TimeUnit::Milliseconds => TimeUnit::Milliseconds,
                TimeUnit::Nanoseconds => TimeUnit::Nanoseconds, // Handle nanoseconds if used
                TimeUnit::Microseconds => TimeUnit::Microseconds, // Handle microseconds if used
            };

            let start_naive = start_utc.naive_utc();
            let end_naive = end_utc.naive_utc();

            // Iterate over the underlying integer values (timestamps)
            for timestamp_opt in dt_col.into_iter() {
                match timestamp_opt {
                    Some(timestamp) => {
                        // Convert the timestamp integer to NaiveDateTime based on the column's TimeUnit
                        let record_naive_dt = match time_unit {
                            TimeUnit::Milliseconds => {
                                DateTime::<Utc>::from_timestamp_millis(timestamp)
                            }
                            TimeUnit::Microseconds => {
                                DateTime::<Utc>::from_timestamp_micros(timestamp)
                            }
                            TimeUnit::Nanoseconds => {
                                Some(DateTime::<Utc>::from_timestamp_nanos(timestamp))
                            }
                        }
                        .expect("Invalid timestamp conversion in datetime column")
                        .naive_utc();

                        assert!(
                            record_naive_dt >= start_naive && record_naive_dt <= end_naive,
                            "Record timestamp {} ({}) is outside the requested range [{}, {}]",
                            record_naive_dt,
                            timestamp,
                            start_naive,
                            end_naive
                        );
                    }
                    None => panic!("Null value found in datetime column after filtering"),
                }
            }
        } else {
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

        let start_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end_date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

        let filtered_lazy_frame = lazy_frame.filter_daily(start_date, end_date);
        let daily_frame = filtered_lazy_frame.collect()?;
        dbg!(&daily_frame); // Check the schema and types here! ('date' should be Date type)

        let shape = daily_frame.shape();
        assert!(
            shape.0 >= 360 && shape.0 <= 365,
            "Expected around 365 days for 2023, got {}",
            shape.0
        );
        assert_eq!(shape.1, 11, "Expected 11 columns for daily data"); // Count should be unchanged

        // Verify dates are within range using the pre-parsed 'date' column
        let date_col: &DateChunked = daily_frame
            .column("date")?
            .date() // Access as DateChunked
            .map_err(|e| MeteostatError::PolarsError(e))?; // Handle potential type mismatch

        for date_opt in date_col.into_iter() {
            if let Some(date_int) = date_opt {
                // Convert days since epoch to NaiveDate
                let date = NaiveDate::from_num_days_from_ce_opt(date_int + 719_163) // Polars Date is days since 1970-01-01, NaiveDate::from_num_days_from_ce_opt needs days since 0001-01-01
                    .expect("Invalid date integer encountered");
                assert!(
                    date >= start_date && date <= end_date,
                    "Date {} out of range [{}, {}]",
                    date,
                    start_date,
                    end_date
                );
            } else {
                panic!("Date column contains nulls");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly_filtered() -> Result<(), MeteostatError> {
        // No changes needed here as monthly data format was not modified
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Monthly)
            .call()
            .await?;
        let filtered_lazy_frame = lazy_frame.filter_monthly(2020, 2022);
        let monthly_frame = filtered_lazy_frame.collect()?;
        dbg!(&monthly_frame);
        let shape = monthly_frame.shape();
        assert!(
            shape.0 >= 30 && shape.0 <= 36,
            "Expected around 36 months for 2020-2022, got {}",
            shape.0
        );
        assert_eq!(shape.1, 9);
        let year_col = monthly_frame.column("year")?.i64()?;
        for year_opt in year_col.into_iter() {
            let year = year_opt.unwrap();
            assert!(year >= 2020 && year <= 2022, "Year {} out of range", year);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate_filtered() -> Result<(), MeteostatError> {
        // No changes needed here as climate data format was not modified
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Climate)
            .call()
            .await?;
        let filtered_lazy_frame = lazy_frame.filter_climate(1991, 2020);
        let climate_frame = filtered_lazy_frame.collect()?;
        dbg!(&climate_frame);
        let shape = climate_frame.shape();
        assert_eq!(
            shape.0, 12,
            "Expected 12 months for the 1991-2020 climate period"
        );
        assert_eq!(shape.1, 9);
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
            })
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
        let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap();

        let filtered_lazy_frame = lazy_frame.filter_hourly(start_utc, end_utc);
        let frame = filtered_lazy_frame.collect()?;
        dbg!(&frame);

        let shape = frame.shape();
        assert!(
            shape.0 <= 24,
            "Expected max 24 hourly records for one day, got {}",
            shape.0
        );

        if shape.0 > 0 {
            assert_eq!(
                shape.1, 14,
                "Expected 14 columns for hourly data (incl. datetime)"
            );

            // Verify filtering using the 'datetime' column
            let dt_col: &DatetimeChunked = frame
                .column("datetime")?
                .datetime()
                .map_err(|e| MeteostatError::PolarsError(e))?;

            // Check the time unit of the column
            let time_unit = match dt_col.time_unit() {
                TimeUnit::Milliseconds => TimeUnit::Milliseconds,
                TimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
                TimeUnit::Microseconds => TimeUnit::Microseconds,
            };

            let start_naive = start_utc.naive_utc();
            let end_naive = end_utc.naive_utc();

            // Iterate over the underlying integer values (timestamps)
            for timestamp_opt in dt_col.into_iter() {
                match timestamp_opt {
                    Some(timestamp) => {
                        // Convert the timestamp integer to NaiveDateTime based on the column's TimeUnit
                        let record_naive_dt = match time_unit {
                            TimeUnit::Milliseconds => {
                                DateTime::<Utc>::from_timestamp_millis(timestamp)
                            }
                            TimeUnit::Microseconds => {
                                DateTime::<Utc>::from_timestamp_micros(timestamp)
                            }
                            TimeUnit::Nanoseconds => {
                                Some(DateTime::<Utc>::from_timestamp_nanos(timestamp))
                            }
                        }
                        .expect("Invalid timestamp conversion in datetime column") // Unwrap the Option<DateTime<Utc>>
                        .naive_utc(); // Convert DateTime<Utc> to NaiveDateTime

                        assert!(
                            record_naive_dt >= start_naive && record_naive_dt <= end_naive,
                            "Record timestamp {} ({}) is outside the requested range [{}, {}]",
                            record_naive_dt,
                            timestamp,
                            start_naive,
                            end_naive
                        );
                    }
                    None => panic!("Null value found in datetime column after filtering"),
                }
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_get_hourly_row_exists() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637") // Use a known station
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        // Pick a specific datetime likely to exist (e.g., noon on a recent day)
        // Let's try getting the first available row from a known range instead
        let start_range = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
        let end_range = Utc.with_ymd_and_hms(2023, 10, 26, 5, 0, 0).unwrap(); // Small range
        let initial_rows = lazy_frame
            .clone()
            .filter_hourly(start_range, end_range)
            .limit(1) // Get just the first one
            .collect()?;

        if initial_rows.height() == 0 {
            println!("Warning: No hourly data found for station 10637 around 2023-10-26 00:00 to test get_hourly_row. Test skipped.");
            return Ok(());
        }

        let dt_series = initial_rows.column("datetime")?.datetime()?;
        let first_ts = dt_series.get(0).expect("Should have one timestamp");
        let first_naive_dt = match dt_series.time_unit() {
            TimeUnit::Milliseconds => DateTime::<Utc>::from_timestamp_millis(first_ts),
            TimeUnit::Microseconds => DateTime::<Utc>::from_timestamp_micros(first_ts),
            TimeUnit::Nanoseconds => Some(DateTime::<Utc>::from_timestamp_nanos(first_ts)),
        }
        .unwrap()
        .naive_utc();

        // Now try to get exactly that row
        let target_dt = Utc.from_utc_datetime(&first_naive_dt);
        let single_row_lazy = lazy_frame.get_hourly_row(target_dt);
        let single_row_frame = single_row_lazy.collect()?;

        assert_eq!(
            single_row_frame.shape().0,
            1,
            "Expected exactly one row for datetime {}",
            target_dt
        );
        assert_eq!(single_row_frame.shape().1, 14, "Expected 14 columns");

        // Verify the datetime column value
        let result_dt_series = single_row_frame.column("datetime")?.datetime()?;
        let result_ts = result_dt_series.get(0).unwrap();
        let result_naive_dt = match result_dt_series.time_unit() {
            TimeUnit::Milliseconds => DateTime::<Utc>::from_timestamp_millis(result_ts),
            TimeUnit::Microseconds => DateTime::<Utc>::from_timestamp_micros(result_ts),
            TimeUnit::Nanoseconds => Some(DateTime::<Utc>::from_timestamp_nanos(result_ts)),
        }
        .unwrap()
        .naive_utc();

        assert_eq!(
            result_naive_dt, first_naive_dt,
            "The retrieved row's datetime does not match the target"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_hourly_row_not_exists() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        // Pick a datetime highly unlikely to exist (e.g., ancient date or future date)
        let non_existent_dt = Utc.with_ymd_and_hms(1800, 1, 1, 12, 0, 0).unwrap();

        let single_row_lazy = lazy_frame.get_hourly_row(non_existent_dt);
        let single_row_frame = single_row_lazy.collect()?;

        assert_eq!(
            single_row_frame.shape().0,
            0,
            "Expected zero rows for non-existent datetime {}",
            non_existent_dt
        );
        // Column count might be 0 or the original count depending on Polars optimization
        // assert_eq!(single_row_frame.shape().1, 14); // This might fail if no rows are returned

        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily_row_exists() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Daily)
            .call()
            .await?;

        // Find the first available date in a known range
        let start_range = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let end_range = NaiveDate::from_ymd_opt(2023, 1, 10).unwrap();
        let initial_rows = lazy_frame
            .clone()
            .filter_daily(start_range, end_range)
            .limit(1)
            .collect()?;

        if initial_rows.height() == 0 {
            println!("Warning: No daily data found for station 10637 around 2023-01-01 to test get_daily_row. Test skipped.");
            return Ok(());
        }

        let date_series = initial_rows.column("date")?.date()?;
        let first_date_int = date_series.get(0).expect("Should have one date");
        let first_date = NaiveDate::from_num_days_from_ce_opt(first_date_int + 719_163).unwrap();

        // Now try to get exactly that row
        let target_date = first_date;
        let single_row_lazy = lazy_frame.get_daily_row(target_date);
        let single_row_frame = single_row_lazy.collect()?;

        assert_eq!(
            single_row_frame.shape().0,
            1,
            "Expected exactly one row for date {}",
            target_date
        );
        assert_eq!(single_row_frame.shape().1, 11, "Expected 11 columns");

        // Verify the date column value
        let result_date_series = single_row_frame.column("date")?.date()?;
        let result_date_int = result_date_series.get(0).unwrap();
        let result_date = NaiveDate::from_num_days_from_ce_opt(result_date_int + 719_163).unwrap();

        assert_eq!(
            result_date, target_date,
            "The retrieved row's date does not match the target"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly_row_exists() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Monthly)
            .call()
            .await?;

        // Find the first available month in a known range
        let start_year = 2022;
        let end_year = 2022;
        let initial_rows = lazy_frame
            .clone()
            .filter_monthly(start_year, end_year)
            .limit(1)
            .sort(["year", "month"], Default::default()) // Sort to get a consistent first month
            .collect()?;

        if initial_rows.height() == 0 {
            println!("Warning: No monthly data found for station 10637 in {} to test get_monthly_row. Test skipped.", start_year);
            return Ok(());
        }

        let year_series = initial_rows.column("year")?.i64()?;
        let month_series = initial_rows.column("month")?.i64()?;
        let target_year = year_series.get(0).unwrap() as i32;
        let target_month = month_series.get(0).unwrap() as u32;

        // Now try to get exactly that row
        let single_row_lazy = lazy_frame.get_monthly_row(target_year, target_month);
        let single_row_frame = single_row_lazy.collect()?;

        assert_eq!(
            single_row_frame.shape().0,
            1,
            "Expected exactly one row for year {}, month {}",
            target_year,
            target_month
        );
        assert_eq!(single_row_frame.shape().1, 9, "Expected 9 columns");

        // Verify the year and month column values
        let result_year = single_row_frame.column("year")?.i64()?.get(0).unwrap() as i32;
        let result_month = single_row_frame.column("month")?.i64()?.get(0).unwrap() as u32;

        assert_eq!(
            result_year, target_year,
            "The retrieved row's year does not match the target"
        );
        assert_eq!(
            result_month, target_month,
            "The retrieved row's month does not match the target"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate_row_exists() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let lazy_frame = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Climate)
            .call()
            .await?;

        // Assume the standard 1991-2020 period exists and pick a month (e.g., January)
        let target_start_year = 1991;
        let target_end_year = 2020;
        let target_month = 1; // January

        let single_row_lazy =
            lazy_frame.get_climate_row(target_start_year, target_end_year, target_month);
        let single_row_frame = single_row_lazy.collect()?;

        // Check if the climate period exists at all first
        let period_exists = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Climate)
            .call()
            .await?
            .filter_climate(target_start_year, target_end_year)
            .collect()?
            .height()
            > 0;

        if !period_exists {
            println!("Warning: Climate period {}-{} not found for station 10637. Cannot test get_climate_row reliably. Test skipped.", target_start_year, target_end_year);
            assert_eq!(single_row_frame.height(), 0); // Should return 0 rows if period doesn't exist
            return Ok(());
        }

        assert_eq!(
            single_row_frame.shape().0,
            1,
            "Expected exactly one row for climate period {}-{}, month {}",
            target_start_year,
            target_end_year,
            target_month
        );
        assert_eq!(single_row_frame.shape().1, 9, "Expected 9 columns");

        // Verify the start_year, end_year, and month column values
        let result_start_year = single_row_frame
            .column("start_year")?
            .i64()?
            .get(0)
            .unwrap() as i32;
        let result_end_year = single_row_frame.column("end_year")?.i64()?.get(0).unwrap() as i32;
        let result_month = single_row_frame.column("month")?.i64()?.get(0).unwrap() as u32;

        assert_eq!(
            result_start_year, target_start_year,
            "The retrieved row's start_year does not match the target"
        );
        assert_eq!(
            result_end_year, target_end_year,
            "The retrieved row's end_year does not match the target"
        );
        assert_eq!(
            result_month, target_month,
            "The retrieved row's month does not match the target"
        );

        Ok(())
    }
}
