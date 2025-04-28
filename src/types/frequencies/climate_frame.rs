use crate::types::traits::types::{Month, Year};
use polars::prelude::{col, lit, Expr, LazyFrame};

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

pub struct ClimateFrame {
    frame: LazyFrame,
}

impl ClimateFrame {
    pub fn new(frame: LazyFrame) -> Self {
        Self { frame }
    }

    pub fn filter(&self, predicate: Expr) -> ClimateFrame {
        ClimateFrame::new(self.frame.clone().filter(predicate))
    }

    pub fn get_at(&self, start_year: Year, end_year: Year, month: Month) -> ClimateFrame {
        self.filter(
            col("start_year")
                .eq(lit(start_year.get()))
                .and(col("end_year").eq(lit(end_year.get())))
                .and(col("month").eq(lit(month.get()))),
        )
    }
}
