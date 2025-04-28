use crate::types::traits::types::{Month, StartEndDate, Year};
use crate::types::traits::utils::days_in_month;
use chrono::NaiveDate;

pub trait DatePeriod {
    fn get_date_period(self) -> Option<StartEndDate>;
}

impl DatePeriod for Year {
    fn get_date_period(self) -> Option<StartEndDate> {
        Some(StartEndDate {
            start: NaiveDate::from_ymd_opt(self.0, 1, 1)?,
            end: NaiveDate::from_ymd_opt(self.0, 12, 31)?,
        })
    }
}

impl DatePeriod for (Year, Month) {
    fn get_date_period(self) -> Option<StartEndDate> {
        let year = self.0.get();
        let month = self.1.get();
        Some(StartEndDate {
            start: NaiveDate::from_ymd_opt(year, month, 1)?,
            end: NaiveDate::from_ymd_opt(year, month, days_in_month(year, month)?)?,
        })
    }
}
