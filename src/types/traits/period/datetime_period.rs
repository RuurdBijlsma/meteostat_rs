use crate::types::traits::types::{Month, StartEndDateTime, Year};
use crate::types::traits::utils::days_in_month;
use chrono::{DateTime, NaiveDate, Utc};

pub trait DateTimePeriod {
    fn get_datetime_period(self) -> Option<StartEndDateTime>;
}

impl DateTimePeriod for NaiveDate {
    fn get_datetime_period(self) -> Option<StartEndDateTime> {
        let Some(start) = self
            .and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        else {
            return None;
        };
        let Some(end) = self
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        else {
            return None;
        };
        Some(StartEndDateTime { start, end })
    }
}

impl DateTimePeriod for String {
    fn get_datetime_period(self) -> Option<StartEndDateTime> {
        if let Ok(naive_date) = NaiveDate::parse_from_str(&self, "%Y-%m-%d") {
            return naive_date.get_datetime_period()
        }
        None
    }
}

impl DateTimePeriod for &str {
    fn get_datetime_period(self) -> Option<StartEndDateTime> {
        self.to_string().get_datetime_period()
    }
}

impl DateTimePeriod for Year {
    fn get_datetime_period(self) -> Option<StartEndDateTime> {
        Some(StartEndDateTime {
            start: NaiveDate::from_ymd_opt(self.0, 1, 1)?.get_datetime_period()?.start,
            end: NaiveDate::from_ymd_opt(self.0, 12, 31)?.get_datetime_period()?.end,
        })
    }
}

impl DateTimePeriod for Month {
    fn get_datetime_period(self) -> Option<StartEndDateTime> {
        let year = self.year();
        let month = self.month();
        Some(StartEndDateTime {
            start: NaiveDate::from_ymd_opt(year, month, 1)?.get_datetime_period()?.start,
            end: NaiveDate::from_ymd_opt(year, month, days_in_month(year, month)?)?.get_datetime_period()?.end,
        })
    }
}
