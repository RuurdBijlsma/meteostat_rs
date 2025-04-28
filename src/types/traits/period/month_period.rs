use crate::types::traits::types::{Month, StartEndMonth, Year};

pub trait MonthPeriod {
    fn get_month_period(self) -> Option<StartEndMonth>;
}

impl MonthPeriod for Year {
    fn get_month_period(self) -> Option<StartEndMonth> {
        Some(StartEndMonth {
            start: (self, Month(1)),
            end: (self, Month(12))
        })
    }
}
