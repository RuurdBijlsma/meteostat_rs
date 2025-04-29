use chrono::{DateTime, NaiveDate, Utc};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Year(pub i32);
impl Year {
    pub fn get(self) -> i32 {
        self.0
    }
}

impl Display for Year {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}", self.0)
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

impl Display for Month {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.0, self.1)
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
