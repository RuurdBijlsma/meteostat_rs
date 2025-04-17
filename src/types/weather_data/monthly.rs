use crate::types::bitflags::monthly::RequiredMonthlyField; // Assuming the bitflags are here

#[derive(Debug, PartialEq, Clone)]
pub struct MonthlyWeatherInfo {
    pub year: i32,                        // year
    pub month: u32,                       // month
    pub temp_avg: Option<f64>,            // tavg
    pub temp_min_avg: Option<f64>,        // tmin (avg daily min)
    pub temp_max_avg: Option<f64>,        // tmax (avg daily max)
    pub precipitation_total: Option<f64>, // prcp (total mm)
    pub wind_speed_avg: Option<f64>,      // wspd (avg km/h)
    pub pressure_avg: Option<f64>,        // pres (avg hPa)
    pub sunshine_total: Option<i32>,      // tsun (total minutes)
}


impl MonthlyWeatherInfo {
    /// Merges data from `other` into `self`, filling in `None` fields.
    pub fn merge_from(&mut self, other: &Self) {
        if self.temp_avg.is_none() { self.temp_avg = other.temp_avg; }
        if self.temp_min_avg.is_none() { self.temp_min_avg = other.temp_min_avg; }
        if self.temp_max_avg.is_none() { self.temp_max_avg = other.temp_max_avg; }
        if self.precipitation_total.is_none() { self.precipitation_total = other.precipitation_total; }
        if self.wind_speed_avg.is_none() { self.wind_speed_avg = other.wind_speed_avg; }
        if self.pressure_avg.is_none() { self.pressure_avg = other.pressure_avg; }
        if self.sunshine_total.is_none() { self.sunshine_total = other.sunshine_total; }
    }

    /// Checks if all fields specified in `required` are `Some`.
    pub fn has_required_fields(&self, required: RequiredMonthlyField) -> bool {
        if required.contains(RequiredMonthlyField::TEMP_AVG) && self.temp_avg.is_none() { return false; }
        if required.contains(RequiredMonthlyField::TEMP_MIN_AVG) && self.temp_min_avg.is_none() { return false; }
        if required.contains(RequiredMonthlyField::TEMP_MAX_AVG) && self.temp_max_avg.is_none() { return false; }
        if required.contains(RequiredMonthlyField::PRECIPITATION_TOTAL) && self.precipitation_total.is_none() { return false; }
        if required.contains(RequiredMonthlyField::WIND_SPEED_AVG) && self.wind_speed_avg.is_none() { return false; }
        if required.contains(RequiredMonthlyField::PRESSURE_AVG) && self.pressure_avg.is_none() { return false; }
        if required.contains(RequiredMonthlyField::SUNSHINE_TOTAL) && self.sunshine_total.is_none() { return false; }
        true // All required fields were present
    }
}