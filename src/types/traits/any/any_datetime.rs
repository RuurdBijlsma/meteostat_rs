use crate::types::traits::types::{Month, StartEndDateTime, Year};
use crate::types::traits::utils::days_in_month;
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};

pub trait AnyDateTime {
    fn get_datetime_range(self) -> Option<StartEndDateTime>;
}

impl AnyDateTime for NaiveDateTime {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let dt = Utc.from_utc_datetime(&self);
        Some(StartEndDateTime { start: dt, end: dt })
    }
}

impl AnyDateTime for DateTime<Local> {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let dt = self.with_timezone(&Utc);
        Some(StartEndDateTime { start: dt, end: dt })
    }
}

impl AnyDateTime for DateTime<Utc> {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        Some(StartEndDateTime {
            start: self,
            end: self,
        })
    }
}

impl AnyDateTime for DateTime<FixedOffset> {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let dt = self.with_timezone(&Utc);
        Some(StartEndDateTime { start: dt, end: dt })
    }
}

impl AnyDateTime for NaiveDate {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let start = self
            .and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))?;
        let end = self
            .and_hms_micro_opt(23, 59, 59, 999_999)
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))?;
        Some(StartEndDateTime { start, end })
    }
}

impl AnyDateTime for &str {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        self.to_string().get_datetime_range()
    }
}

impl AnyDateTime for String {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        // Try full UTC parse
        if let Ok(dt) = self.parse::<DateTime<Utc>>() {
            return dt.get_datetime_range();
        }
        // Try fixed offset (e.g., +02:00)
        if let Ok(dt) = self.parse::<DateTime<FixedOffset>>() {
            return dt.get_datetime_range();
        }
        // Try naive datetime
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(&self, "%Y-%m-%d %H:%M:%S") {
            return naive_dt.get_datetime_range();
        }
        // Try naive date
        if let Ok(naive_date) = NaiveDate::parse_from_str(&self, "%Y-%m-%d") {
            return naive_date.get_datetime_range();
        }
        None
    }
}

impl AnyDateTime for Year {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let start_naive = NaiveDate::from_ymd_opt(self.0, 1, 1)?.and_hms_opt(0, 0, 0)?;
        let start = DateTime::<Utc>::from_naive_utc_and_offset(start_naive, Utc);
        let end_naive =
            NaiveDate::from_ymd_opt(self.0, 12, 31)?.and_hms_micro_opt(23, 59, 59, 999_999)?;
        let end = DateTime::<Utc>::from_naive_utc_and_offset(end_naive, Utc);
        Some(StartEndDateTime { start, end })
    }
}

impl AnyDateTime for Month {
    fn get_datetime_range(self) -> Option<StartEndDateTime> {
        let year = self.year();
        let month = self.month();

        let start_naive = NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)?;
        let start = DateTime::<Utc>::from_naive_utc_and_offset(start_naive, Utc);

        let end_naive = NaiveDate::from_ymd_opt(year, month, days_in_month(year, month)?)?
            .and_hms_micro_opt(23, 59, 59, 999_999)?;
        let end = DateTime::<Utc>::from_naive_utc_and_offset(end_naive, Utc);

        Some(StartEndDateTime { start, end })
    }
}
