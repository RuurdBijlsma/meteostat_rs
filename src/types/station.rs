//! Defines the data structures representing Meteostat weather stations and their metadata,
//! including inventory, location, and identifiers. Also includes implementations
//! necessary for spatial indexing using the `rstar` crate.

use chrono::NaiveDate;
use rstar::{PointDistance, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Data Structures ---

/// Represents a single Meteostat weather station and its associated metadata.
///
/// This structure holds information retrieved from the Meteostat stations metadata,
/// often corresponding to fields in their JSON format. It includes identification,
/// geographical location, and data availability (inventory).
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Station {
    /// The unique Meteostat station identifier (e.g., "10637").
    pub id: String,
    /// The country code where the station is located (e.g., "NL", "DE").
    pub country: String,
    /// The region code (state, province, etc.), if available.
    pub region: Option<String>,
    /// The IANA timezone name for the station's location (e.g., "Europe/Amsterdam"), if available.
    pub timezone: Option<String>,
    /// A map of station names in different languages (e.g., {"en": "Amsterdam Airport Schiphol"}).
    pub name: HashMap<String, String>,
    /// Other known identifiers for the station.
    pub identifiers: Identifiers,
    /// Geographical location details (latitude, longitude, elevation).
    pub location: Location,
    /// Information about the availability periods for different data frequencies.
    pub inventory: Inventory,
}

/// Stores the data availability ranges for different [`Frequency`] types for a station.
///
/// Indicates the approximate start and end dates/years for which data is expected
/// to be available according to Meteostat's metadata. Note that gaps might exist
/// within these ranges.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Inventory {
    /// The reported start and end dates for daily data.
    pub daily: DateRange,
    /// The reported start and end dates for hourly data.
    pub hourly: DateRange,
    /// The reported start and end dates for model data (if applicable, often mirrors hourly).
    pub model: DateRange,
    /// The reported start and end years for monthly data.
    pub monthly: YearRange,
    /// The reported start and end years for climate normals data.
    pub normals: YearRange,
}

/// Represents a date range with optional start and end dates.
///
/// Used within [`Inventory`] for frequencies where day-level precision is relevant (daily, hourly).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DateRange {
    /// The earliest date for which data is reported available, if known.
    pub start: Option<NaiveDate>,
    /// The latest date for which data is reported available, if known.
    pub end: Option<NaiveDate>,
}

/// Represents a year range with optional start and end years.
///
/// Used within [`Inventory`] for frequencies where year-level precision is sufficient (monthly, climate normals).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YearRange {
    /// The earliest year for which data is reported available, if known.
    pub start: Option<i32>,
    /// The latest year for which data is reported available, if known.
    pub end: Option<i32>,
}

/// Holds various alternative identifiers that might be associated with a weather station.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Identifiers {
    /// National station identifier, if available.
    pub national: Option<String>,
    /// World Meteorological Organization (WMO) identifier, if available.
    pub wmo: Option<String>,
    /// International Civil Aviation Organization (ICAO) airport code, if the station is at an airport.
    pub icao: Option<String>,
}

/// Represents the geographical location of a weather station.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Location {
    /// Latitude in decimal degrees (positive for North, negative for South).
    pub latitude: f64,
    /// Longitude in decimal degrees (positive for East, negative for West).
    pub longitude: f64,
    /// Elevation above sea level in meters, if available.
    pub elevation: Option<i32>,
}

// --- R-Tree Implementations ---

/// Implementation required by `rstar` to treat a `Station` as an object within an R-Tree.
///
/// This allows for efficient spatial indexing and searching of stations based on their location.
impl RTreeObject for Station {
    /// The envelope type is an Axis-Aligned Bounding Box (AABB) in 2D space (latitude, longitude).
    type Envelope = AABB<[f64; 2]>;

    /// Returns the spatial envelope (bounding box) for the station.
    ///
    /// Since a station is conceptually a point, its envelope is a degenerate AABB
    /// containing only that single point (latitude, longitude).
    fn envelope(&self) -> Self::Envelope {
        // Create an AABB that represents the single point location of the station.
        AABB::from_point([self.location.latitude, self.location.longitude])
    }
}

/// Implementation required by `rstar` to calculate distances between stations (points) and query points.
impl PointDistance for Station {
    /// Calculates the squared Euclidean distance between the station's location
    /// and a given 2D query point (latitude, longitude).
    ///
    /// R-Tree algorithms often use squared distance internally for performance reasons,
    /// avoiding the computationally more expensive square root operation.
    ///
    /// # Arguments
    ///
    /// * `point`: A 2-element array `[query_latitude, query_longitude]`.
    ///
    /// # Returns
    ///
    /// The squared Euclidean distance: `(station_lat - query_lat)^2 + (station_lon - query_lon)^2`.
    /// Note: This calculation treats latitude and longitude as Cartesian coordinates, which is an
    /// approximation suitable for small distances but less accurate over large distances compared
    /// to Haversine distance. However, it's standard for R-Tree nearest neighbor searches based on points.
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        // point[0] = query latitude, point[1] = query longitude
        let dx = self.location.latitude - point[0];
        let dy = self.location.longitude - point[1];
        // Returns the squared Euclidean distance
        dx * dx + dy * dy
    }
}
