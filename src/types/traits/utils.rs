use chrono::{Datelike, Duration, NaiveDate};

pub(crate) fn days_in_month(year: i32, month: u32) -> Option<u32> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let (next_month_year, next_month) = if month == 12 {
        (year.checked_add(1)?, 1)
    } else {
        (year, month + 1)
    };
    let first_day_of_next_month = NaiveDate::from_ymd_opt(next_month_year, next_month, 1)?;
    let last_day_of_current_month = first_day_of_next_month - Duration::days(1);
    Some(last_day_of_current_month.day())
}
