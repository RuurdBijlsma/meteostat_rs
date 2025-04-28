use chrono::{DateTime, NaiveDate, Utc};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Year(pub i32);
impl Year {
    pub fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Month(pub u32);
impl Month {
    pub fn get(self) -> u32 {
        self.0
    }
}

pub struct StartEndDateTime {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

pub struct StartEndDate {
    pub start: NaiveDate,
    pub end: NaiveDate,
}

pub struct StartEndMonth {
    pub start: (Year, Month),
    pub end: (Year, Month),
}
