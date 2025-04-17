use crate::types::bitflags::climate::RequiredClimateField;

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

impl ClimateNormalInfo {
    /// Merges data from `other` into `self`, filling in `None` fields.
    /// Note: This might be less common for climate normals unless combining different periods.
    pub fn merge_from(&mut self, other: &Self) {
        // Non-optional fields like year/month/period shouldn't typically be merged
        // unless the logic specifically handles combining different records.
        if self.temp_min_avg.is_none() { self.temp_min_avg = other.temp_min_avg; }
        if self.temp_max_avg.is_none() { self.temp_max_avg = other.temp_max_avg; }
        if self.precipitation_avg.is_none() { self.precipitation_avg = other.precipitation_avg; }
        if self.wind_speed_avg.is_none() { self.wind_speed_avg = other.wind_speed_avg; }
        if self.pressure_avg.is_none() { self.pressure_avg = other.pressure_avg; }
        if self.sunshine_avg.is_none() { self.sunshine_avg = other.sunshine_avg; }
    }

    /// Checks if all fields specified in `required` are `Some`.
    pub fn has_required_fields(&self, required: RequiredClimateField) -> bool {
        if required.contains(RequiredClimateField::TEMP_MIN_AVG) && self.temp_min_avg.is_none() { return false; }
        if required.contains(RequiredClimateField::TEMP_MAX_AVG) && self.temp_max_avg.is_none() { return false; }
        if required.contains(RequiredClimateField::PRECIPITATION_AVG) && self.precipitation_avg.is_none() { return false; }
        if required.contains(RequiredClimateField::WIND_SPEED_AVG) && self.wind_speed_avg.is_none() { return false; }
        if required.contains(RequiredClimateField::PRESSURE_AVG) && self.pressure_avg.is_none() { return false; }
        if required.contains(RequiredClimateField::SUNSHINE_AVG) && self.sunshine_avg.is_none() { return false; }
        true // All required fields were present
    }
}