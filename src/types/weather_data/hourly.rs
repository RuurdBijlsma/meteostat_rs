// In the file where HourlyWeatherInfo is defined
use chrono::NaiveDate;
use crate::types::bitflags::hourly::RequiredHourlyField;
use crate::types::weather_condition::WeatherCondition;

#[derive(Debug, PartialEq, Clone)]
pub struct HourlyWeatherInfo {
    pub date: NaiveDate,
    pub hour: u32,
    pub temperature: Option<f64>,
    pub dew_point: Option<f64>,
    pub relative_humidity: Option<i32>,
    pub precipitation: Option<f64>,
    pub snow: Option<i32>,
    pub wind_direction: Option<i32>,
    pub wind_speed: Option<f64>,
    pub peak_wind_gust: Option<f64>,
    pub pressure: Option<f64>,
    pub sunshine: Option<u32>,
    pub condition: Option<WeatherCondition>,
}

impl HourlyWeatherInfo {
    /// Merges data from `other` into `self`, filling in `None` fields.
    pub fn merge_from(&mut self, other: &Self) {
        if self.temperature.is_none() { self.temperature = other.temperature; }
        if self.dew_point.is_none() { self.dew_point = other.dew_point; }
        if self.relative_humidity.is_none() { self.relative_humidity = other.relative_humidity; }
        if self.precipitation.is_none() { self.precipitation = other.precipitation; }
        if self.snow.is_none() { self.snow = other.snow; }
        if self.wind_direction.is_none() { self.wind_direction = other.wind_direction; }
        if self.wind_speed.is_none() { self.wind_speed = other.wind_speed; }
        if self.peak_wind_gust.is_none() { self.peak_wind_gust = other.peak_wind_gust; }
        if self.pressure.is_none() { self.pressure = other.pressure; }
        if self.sunshine.is_none() { self.sunshine = other.sunshine; }
        if self.condition.is_none() { self.condition = other.condition.clone(); }
    }

    /// Checks if all fields specified in `required` are `Some`.
    pub fn has_required_fields(&self, required: RequiredHourlyField) -> bool {
        if required.contains(RequiredHourlyField::TEMPERATURE) && self.temperature.is_none() { return false; }
        if required.contains(RequiredHourlyField::DEW_POINT) && self.dew_point.is_none() { return false; }
        if required.contains(RequiredHourlyField::RELATIVE_HUMIDITY) && self.relative_humidity.is_none() { return false; }
        if required.contains(RequiredHourlyField::PRECIPITATION) && self.precipitation.is_none() { return false; }
        if required.contains(RequiredHourlyField::SNOW) && self.snow.is_none() { return false; } // Adjust if snow=None is acceptable
        if required.contains(RequiredHourlyField::WIND_DIRECTION) && self.wind_direction.is_none() { return false; }
        if required.contains(RequiredHourlyField::WIND_SPEED) && self.wind_speed.is_none() { return false; }
        if required.contains(RequiredHourlyField::PEAK_WIND_GUST) && self.peak_wind_gust.is_none() { return false; }
        if required.contains(RequiredHourlyField::PRESSURE) && self.pressure.is_none() { return false; }
        if required.contains(RequiredHourlyField::SUNSHINE) && self.sunshine.is_none() { return false; }
        if required.contains(RequiredHourlyField::CONDITION) && self.condition.is_none() { return false; }
        true // All required fields were present
    }
}