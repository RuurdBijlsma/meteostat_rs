use crate::types::traits::any::any_month::AnyMonth;
use crate::types::traits::period::month_period::MonthPeriod;
use crate::MeteostatError;
use polars::prelude::{col, lit, Expr, LazyFrame};

pub struct Monthly {
    year: i32,
    month: i32,
    average_temperature: i32,
    minimum_temperature: i32,
    maximum_temperature: i32,
    precipitation: i32,
    wind_speed: i32,
    pressure: i32,
    sunshine_minutes: i32,
}

pub struct MonthlyFrame {
    frame: LazyFrame,
}

impl MonthlyFrame {
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    pub fn filter(&self, predicate: Expr) -> MonthlyFrame {
        MonthlyFrame::new(self.frame.clone().filter(predicate))
    }

    pub fn get_range(
        &self,
        start: impl AnyMonth,
        end: impl AnyMonth,
    ) -> Result<MonthlyFrame, MeteostatError> {
        let (start_year, start_month) = start
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        let (end_year, end_month) = end
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .end;

        let start_year = start_year.get();
        let end_year = end_year.get();
        let start_month = start_month.get();
        let end_month = end_month.get();

        // Condition: (year > start_year) OR (year == start_year AND month >= start_month)
        let after_start_cond = col("year").gt(lit(start_year)).or(col("year")
            .eq(lit(start_year))
            .and(col("month").gt_eq(lit(start_month))));

        // Condition: (year < end_year) OR (year == end_year AND month <= end_month)
        let before_end_cond = col("year").lt(lit(end_year)).or(col("year")
            .eq(lit(end_year))
            .and(col("month").lt_eq(lit(end_month))));

        // Combine the conditions: Must be after start AND before end
        Ok(self.filter(after_start_cond.and(before_end_cond)))
    }

    pub fn get_at(&self, date: impl AnyMonth) -> Result<MonthlyFrame, MeteostatError> {
        let (year, month) = date
            .get_month_range()
            .ok_or(MeteostatError::DateParsingError)?
            .start;
        Ok(self.filter(
            col("year")
                .eq(lit(year.get()))
                .and(col("month").eq(lit(month.get()))),
        ))
    }

    pub fn get_for_period(&self, period: impl MonthPeriod) -> Result<MonthlyFrame, MeteostatError> {
        let period = period
            .get_month_period()
            .ok_or(MeteostatError::DateParsingError)?;
        self.get_range(period.start, period.end)
    }
}
