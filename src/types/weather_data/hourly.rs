use crate::types::weather_condition::WeatherCondition;
use chrono::NaiveDate;

#[derive(Debug, PartialEq, Clone)] // Added Clone
pub struct HourlyWeatherInfo {
    pub date: NaiveDate,
    pub hour: u32,
    pub temperature: Option<f64>,            // temp (celsius)
    pub dew_point: Option<f64>,              // dwpt (celsius)
    pub relative_humidity: Option<i32>,      // rhum (percentage)
    pub precipitation: Option<f64>,          // prcp (mm)
    pub snow: Option<i32>,                   // snow depth (mm)
    pub wind_direction: Option<i32>,         // wdir (degrees)
    pub wind_speed: Option<f64>,             // wspd (km/h)
    pub peak_wind_gust: Option<f64>,         // wpgt (km/h)
    pub pressure: Option<f64>,               // pres (hPa)
    pub sunshine: Option<u32>,               // tsun (sunshine minutes in the hour)
    pub condition: Option<WeatherCondition>, // coco
}
