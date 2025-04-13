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
