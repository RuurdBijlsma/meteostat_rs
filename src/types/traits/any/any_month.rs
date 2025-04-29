use crate::types::traits::types::{Month, StartEndMonth, Year};

pub trait AnyMonth {
    fn get_month_range(self) -> Option<StartEndMonth>;
}

impl AnyMonth for Month {
    fn get_month_range(self) -> Option<StartEndMonth> {
        Some(StartEndMonth {
            start: self,
            end: self,
        })
    }
}

impl AnyMonth for Year {
    fn get_month_range(self) -> Option<StartEndMonth> {
        Some(StartEndMonth {
            start: Month(self.0, 1),
            end: Month(self.0, 12),
        })
    }
}
