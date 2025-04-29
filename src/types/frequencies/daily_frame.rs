use crate::types::traits::any::any_date::AnyDate;
use crate::types::traits::period::date_period::DatePeriod;
use crate::MeteostatError;
use chrono::NaiveDate;
use polars::prelude::{col, lit, Expr, LazyFrame};

#[allow(dead_code)]
pub struct Daily {
    date: NaiveDate,
    average_temperature: f64,
    minimum_temperature: f64,
    maximum_temperature: f64,
    precipitation: f64,
    snow: i32,
    wind_direction: i32,
    wind_speed: f64,
    peak_wind_gust: f64,
    pressure: f64,
    sunshine_minutes: i32,
}

pub struct DailyLazyFrame {
    pub frame: LazyFrame,
}

impl DailyLazyFrame {
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    pub fn filter(&self, predicate: Expr) -> DailyLazyFrame {
        DailyLazyFrame::new(self.frame.clone().filter(predicate))
    }

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

    pub fn get_at(&self, date: impl AnyDate) -> Result<DailyLazyFrame, MeteostatError> {
        let naive_date = date
            .get_date_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        Ok(self.filter(col("date").eq(lit(naive_date))))
    }

    pub fn get_for_period(&self, period: impl DatePeriod) -> Result<DailyLazyFrame, MeteostatError> {
        let period = period
            .get_date_period()
            .ok_or(MeteostatError::DateParsingError)?;
        self.get_range(period.start, period.end)
    }
}