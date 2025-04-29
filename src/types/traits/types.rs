use chrono::{DateTime, NaiveDate, Utc};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Year(pub i32);
impl Year {
    pub fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Month(pub i32, pub u32);
impl Month {
    pub fn year(self) -> i32 {
        self.0
    }
    pub fn month(self) -> u32 {
        self.1
    }
    pub fn new(month: u32, year: i32) -> Self {
        Self(year, month)
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
    pub start: Month,
    pub end: Month,
}
