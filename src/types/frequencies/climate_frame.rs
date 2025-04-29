use crate::types::traits::types::Year;
use polars::prelude::{col, lit, Expr, LazyFrame};

#[allow(dead_code)]
pub struct Climate {
    start_year: i32,
    end_year: i32,
    month: u32,
    minimum_temperature: f64,
    maximum_temperature: f64,
    precipitation: f64,
    wind_speed: f64,
    pressure: f64,
    sunshine_minutes: i32,
}

pub struct ClimateLazyFrame {
    pub frame: LazyFrame,
}

impl ClimateLazyFrame {
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    pub fn filter(&self, predicate: Expr) -> ClimateLazyFrame {
        ClimateLazyFrame {
            frame: self.frame.clone().filter(predicate),
        }
    }

    pub fn get_at(&self, start_year: Year, end_year: Year, month: u32) -> ClimateLazyFrame {
        self.filter(
            col("start_year")
                .eq(lit(start_year.get()))
                .and(col("end_year").eq(lit(end_year.get())))
                .and(col("month").eq(lit(month))),
        )
    }
}
