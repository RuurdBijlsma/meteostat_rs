use chrono::NaiveDate;

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