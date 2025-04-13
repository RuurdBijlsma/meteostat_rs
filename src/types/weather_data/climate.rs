#[derive(Debug, PartialEq, Clone)]
pub struct ClimateNormalInfo {
    pub start_year: i32,                // start_year
    pub end_year: i32,                  // end_year
    pub month: u32,                     // month
    pub temp_min_avg: Option<f64>,      // tmin (avg daily min)
    pub temp_max_avg: Option<f64>,      // tmax (avg daily max)
    pub precipitation_avg: Option<f64>, // prcp (avg monthly total mm)
    pub wind_speed_avg: Option<f64>,    // wspd (avg km/h)
    pub pressure_avg: Option<f64>,      // pres (avg hPa)
    pub sunshine_avg: Option<i32>,      // tsun (avg monthly total minutes)
}
