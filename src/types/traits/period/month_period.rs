use crate::types::traits::types::{Month, StartEndMonth, Year};

pub trait MonthPeriod {
    fn get_month_period(self) -> Option<StartEndMonth>;
}

impl MonthPeriod for Year {
    fn get_month_period(self) -> Option<StartEndMonth> {
        Some(StartEndMonth {
            start: Month(self.0, 1),
            end: Month(self.0, 12),
        })
    }
}
