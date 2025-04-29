use crate::types::traits::any::any_datetime::AnyDateTime;
use crate::types::traits::period::datetime_period::DateTimePeriod;
use crate::{MeteostatError, WeatherCondition};
use chrono::{DateTime, Duration, Timelike, Utc};
use polars::prelude::{col, lit, Expr, LazyFrame};

#[allow(dead_code)]
pub struct Hourly {
    datetime: DateTime<Utc>,
    temperature: f64,
    dew_point: f64,
    relative_humidity: i32,
    precipitation: f64,
    snow: i32,
    wind_direction: i32,
    wind_speed: f64,
    peak_wind_gust: f64,
    pressure: f64,
    sunshine_minutes: i32,
    condition: WeatherCondition,
}

pub struct HourlyLazyFrame {
    pub frame: LazyFrame,
}

impl HourlyLazyFrame {
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    pub fn filter(&self, predicate: Expr) -> HourlyLazyFrame {
        HourlyLazyFrame::new(self.frame.clone().filter(predicate))
    }

    pub fn get_range(
        &self,
        start: impl AnyDateTime,
        end: impl AnyDateTime,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        let start_utc = start
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        let end_utc = end
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .end;
        let start_naive = start_utc.naive_utc();
        let end_naive = end_utc.naive_utc();

        Ok(self.filter(
            col("datetime")
                .gt_eq(lit(start_naive))
                .and(col("datetime").lt_eq(lit(end_naive))),
        ))
    }

    pub fn get_at(&self, date: impl AnyDateTime) -> Result<HourlyLazyFrame, MeteostatError> {
        let date_utc = date
            .get_datetime_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
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

        // Filter the LazyFrame for the exact start of the rounded hour
        Ok(self.filter(col("datetime").eq(lit(rounded_hour_start_utc.naive_utc()))))
    }

    pub fn get_for_period(
        &self,
        period: impl DateTimePeriod,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        let period = period
            .get_datetime_period()
            .ok_or(MeteostatError::DateParsingError)?;
        self.get_range(period.start, period.end)
    }
}
