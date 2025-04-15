use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, TimeZone, Utc};

pub trait IntoUtcDateTime {
    fn into_utc(self) -> DateTime<Utc>;
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

impl IntoUtcDateTime for DateTime<FixedOffset> {
    fn into_utc(self) -> DateTime<Utc> {
        self.with_timezone(&Utc)
    }
}