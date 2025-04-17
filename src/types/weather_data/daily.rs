use chrono::NaiveDate;
use crate::types::bitflags::daily::RequiredDailyField;

#[derive(Debug, PartialEq, Clone)]
pub struct DailyWeatherInfo {
    pub date: NaiveDate,                 // date
    pub temp_avg: Option<f64>,           // tavg
    pub temp_min: Option<f64>,           // tmin
    pub temp_max: Option<f64>,           // tmax
    pub precipitation: Option<f64>,      // prcp (total mm)
    pub snow_depth: Option<i32>,         // snow (max depth mm)
    pub wind_direction_avg: Option<i32>, // wdir (avg degrees)
    pub wind_speed_avg: Option<f64>,     // wspd (avg km/h)
    pub peak_wind_gust: Option<f64>,     // wpgt (km/h)
    pub pressure_avg: Option<f64>,       // pres (avg hPa)
    pub sunshine_total: Option<i32>,     // tsun (total minutes)
}

impl DailyWeatherInfo {
    /// Merges data from `other` into `self`, filling in `None` fields.
    pub fn merge_from(&mut self, other: &Self) {
        if self.temp_avg.is_none() { self.temp_avg = other.temp_avg; }
        if self.temp_min.is_none() { self.temp_min = other.temp_min; }
        if self.temp_max.is_none() { self.temp_max = other.temp_max; }
        if self.precipitation.is_none() { self.precipitation = other.precipitation; }
        if self.snow_depth.is_none() { self.snow_depth = other.snow_depth; }
        if self.wind_direction_avg.is_none() { self.wind_direction_avg = other.wind_direction_avg; }
        if self.wind_speed_avg.is_none() { self.wind_speed_avg = other.wind_speed_avg; }
        if self.peak_wind_gust.is_none() { self.peak_wind_gust = other.peak_wind_gust; }
        if self.pressure_avg.is_none() { self.pressure_avg = other.pressure_avg; }
        if self.sunshine_total.is_none() { self.sunshine_total = other.sunshine_total; }
    }

    /// Checks if all fields specified in `required` are `Some`.
    pub fn has_required_fields(&self, required: RequiredDailyField) -> bool {
        if required.contains(RequiredDailyField::TEMP_AVG) && self.temp_avg.is_none() { return false; }
        if required.contains(RequiredDailyField::TEMP_MIN) && self.temp_min.is_none() { return false; }
        if required.contains(RequiredDailyField::TEMP_MAX) && self.temp_max.is_none() { return false; }
        if required.contains(RequiredDailyField::PRECIPITATION) && self.precipitation.is_none() { return false; }
        if required.contains(RequiredDailyField::SNOW_DEPTH) && self.snow_depth.is_none() { return false; }
        if required.contains(RequiredDailyField::WIND_DIRECTION_AVG) && self.wind_direction_avg.is_none() { return false; }
        if required.contains(RequiredDailyField::WIND_SPEED_AVG) && self.wind_speed_avg.is_none() { return false; }
        if required.contains(RequiredDailyField::PEAK_WIND_GUST) && self.peak_wind_gust.is_none() { return false; }
        if required.contains(RequiredDailyField::PRESSURE_AVG) && self.pressure_avg.is_none() { return false; }
        if required.contains(RequiredDailyField::SUNSHINE_TOTAL) && self.sunshine_total.is_none() { return false; }
        true // All required fields were present
    }
}