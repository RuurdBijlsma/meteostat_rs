use std::collections::HashMap;
use chrono::NaiveDate;
use rstar::{PointDistance, RTreeObject, AABB};
use serde::{Deserialize, Serialize};

// --- Data Structures ---
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Station {
    pub id: String,
    pub country: String,
    pub region: Option<String>,
    pub timezone: Option<String>,
    pub name: HashMap<String, String>,
    pub identifiers: Identifiers,
    pub location: Location,
    pub inventory: Inventory,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Inventory {
    pub daily: DateRange,
    pub hourly: DateRange,
    pub model: DateRange,
    pub monthly: YearRange,
    pub normals: YearRange,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateRange {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YearRange {
    pub start: Option<i32>,
    pub end: Option<i32>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Identifiers {
    pub national: Option<String>,
    pub wmo: Option<String>,
    pub icao: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Location {
    pub latitude: f64,
    pub longitude: f64,
    pub elevation: Option<i32>,
}

// --- R-Tree Implementations ---
impl RTreeObject for Station {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.location.latitude, self.location.longitude])
    }
}

impl PointDistance for Station {
    // R*Tree uses squared Euclidean distance for performance in its algorithms.
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        // point[0] = query latitude, point[1] = query longitude
        let dx = self.location.latitude - point[0];
        let dy = self.location.longitude - point[1];
        dx * dx + dy * dy
    }
}