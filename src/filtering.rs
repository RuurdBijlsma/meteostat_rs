use crate::types::into_utc_trait::IntoUtcDateTime;
use chrono::NaiveDate;
use polars::prelude::{col, lit, DataType, LazyFrame, TimeUnit};

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
        let start_naive = start.into_utc().naive_utc();
        let end_naive = end.into_utc().naive_utc();

        // Filter directly on the pre-computed 'datetime' column
        self.filter(
            col("datetime") // Use the pre-computed datetime column
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None)) // Ensure correct type for comparison
                .gt_eq(lit(start_naive))
                .and(
                    col("datetime")
                        .cast(DataType::Datetime(TimeUnit::Milliseconds, None)) // Ensure correct type
                        .lt_eq(lit(end_naive)),
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
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MeteostatError;
    use crate::meteostat::{LatLon, Meteostat};
    use crate::types::data_source::Frequency;
    use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
    use polars::prelude::{
        DateChunked, DatetimeChunked, TimeUnit,
    };

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
                                NaiveDateTime::from_timestamp_millis(timestamp)
                            }
                            TimeUnit::Microseconds => {
                                NaiveDateTime::from_timestamp_micros(timestamp)
                            }
                            TimeUnit::Nanoseconds => {
                                // NaiveDateTime::from_timestamp_nanos exists from chrono 0.4.31+
                                // If using older chrono, need division:
                                // NaiveDateTime::from_timestamp_opt(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32)
                                NaiveDateTime::from_timestamp_nanos(timestamp) // Use this if chrono >= 0.4.31
                            }
                        }
                        .expect("Invalid timestamp conversion in datetime column");

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
                                NaiveDateTime::from_timestamp_millis(timestamp)
                            }
                            TimeUnit::Microseconds => {
                                NaiveDateTime::from_timestamp_micros(timestamp)
                            }
                            TimeUnit::Nanoseconds => NaiveDateTime::from_timestamp_nanos(timestamp), // Use this if chrono >= 0.4.31
                        }
                        .expect("Invalid timestamp conversion in datetime column");

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
}
