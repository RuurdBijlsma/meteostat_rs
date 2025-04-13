use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc, Local, TimeZone};

pub trait IntoUtcDateTime {
    fn into_utc(self) -> DateTime<Utc>;
}

impl IntoUtcDateTime for NaiveDate {
    fn into_utc(self) -> DateTime<Utc> {
        Utc.from_utc_datetime(&self.and_hms_opt(0, 0, 0).unwrap())
    }
}

impl IntoUtcDateTime for NaiveDateTime {
    fn into_utc(self) -> DateTime<Utc> {
        Utc.from_utc_datetime(&self)
    }
}

impl IntoUtcDateTime for DateTime<Local> {
    fn into_utc(self) -> DateTime<Utc> {
        self.with_timezone(&Utc)
    }
}

impl IntoUtcDateTime for DateTime<Utc> {
    fn into_utc(self) -> DateTime<Utc> {
        self
    }
}