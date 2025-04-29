use crate::types::traits::types::{Month, StartEndDate, Year};
use crate::types::traits::utils::days_in_month;
use chrono::NaiveDate;

pub trait AnyDate {
    fn get_date_range(self) -> Option<StartEndDate>;
}

impl AnyDate for NaiveDate {
    fn get_date_range(self) -> Option<StartEndDate> {
        Some(StartEndDate {
            start: self,
            end: self,
        })
    }
}

impl AnyDate for &str {
    fn get_date_range(self) -> Option<StartEndDate> {
        self.to_string().get_date_range()
    }
}

impl AnyDate for String {
    fn get_date_range(self) -> Option<StartEndDate> {
        // Try naive date
        if let Ok(naive_date) = NaiveDate::parse_from_str(&self, "%Y-%m-%d") {
            return naive_date.get_date_range();
        }
        None
    }
}

impl AnyDate for Year {
    fn get_date_range(self) -> Option<StartEndDate> {
        Some(StartEndDate {
            start: NaiveDate::from_ymd_opt(self.0, 1, 1)?,
            end: NaiveDate::from_ymd_opt(self.0, 12, 31)?,
        })
    }
}

impl AnyDate for Month {
    fn get_date_range(self) -> Option<StartEndDate> {
        let year = self.year();
        let month = self.month();
        Some(StartEndDate {
            start: NaiveDate::from_ymd_opt(year, month, 1)?,
            end: NaiveDate::from_ymd_opt(year, month, days_in_month(year, month)?)?,
        })
    }
}
