//! This module provides the main entry point for interacting with the Meteostat API client.
//! It allows fetching weather data either by specifying a station ID or by providing
//! geographical coordinates (latitude/longitude).

use crate::error::MeteostatError;
use crate::stations::locate_station::StationLocator;
use crate::types::data_source::{Frequency, RequiredData};
use crate::types::station::Station;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use crate::weather_data::frame_fetcher::FrameFetcher;
use bon::bon;
use polars::prelude::LazyFrame;
use std::path::PathBuf;

/// Represents a geographical coordinate using latitude and longitude.
///
/// Latitude is the first element (index 0), and longitude is the second (index 1).
/// Both values are represented as `f64`.
///
/// # Examples
///
/// ```
/// use meteostat::LatLon;
///
/// let berlin_center = LatLon(52.5200, 13.4050);
/// assert_eq!(berlin_center.0, 52.5200); // Latitude
/// assert_eq!(berlin_center.1, 13.4050); // Longitude
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon(pub f64, pub f64);

/// Specifies criteria for checking station data availability (inventory).
///
/// Used in conjunction with [`Meteostat::find_stations`] to filter stations
/// based on whether they have data for a specific frequency ([`Frequency`])
/// and time period ([`RequiredData`]).
///
/// # Examples
///
/// ```
/// use meteostat::InventoryRequest;
/// use meteostat::{Frequency, RequiredData};
/// use chrono::NaiveDate;
///
/// // Request stations with *any* hourly data
/// let req_any_hourly = InventoryRequest::new(Frequency::Hourly, RequiredData::Any);
///
/// // Request stations with daily covering the year 2023
/// let req_daily_2023 = InventoryRequest::new(Frequency::Daily, RequiredData::Year(2023));
///
/// // Request stations with monthly data covering a specific range
/// let start_date = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
/// let end_date = NaiveDate::from_ymd_opt(2020, 6, 31).unwrap();
/// let req_monthly_range = InventoryRequest::new(
///     Frequency::Monthly,
///     RequiredData::DateRange { start: start_date, end: end_date }
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InventoryRequest {
    frequency: Frequency,
    required_data: RequiredData,
}

impl InventoryRequest {
    /// Creates a new `InventoryRequest`.
    ///
    /// # Arguments
    ///
    /// * `frequency` - The [`Frequency`] of data required (e.g., Hourly, Daily).
    /// * `required_data` - The specific time criteria ([`RequiredData`]) the station's inventory must meet.
    ///
    /// # Returns
    ///
    /// A new `InventoryRequest` instance.
    pub fn new(frequency: Frequency, required_data: RequiredData) -> Self {
        Self {
            frequency,
            required_data,
        }
    }
}

/// The main client struct for accessing Meteostat data.
///
/// This struct handles fetching weather data (`LazyFrame`s from Polars) and
/// finding weather stations based on location and data availability.
/// It manages internal caching of downloaded data to speed up subsequent requests.
///
/// Create an instance using [`Meteostat::new()`] for default behavior (using a
/// standard cache directory) or [`Meteostat::with_cache_folder()`] for custom cache locations.
///
/// # Examples
///
/// ```rust
/// # use meteostat::Meteostat;
/// # use meteostat::MeteostatError;
/// # async fn run() -> Result<(), MeteostatError> {
/// // Create a client using the default cache directory
/// let client = Meteostat::new().await?;
/// // Now you can use the client to fetch data or find stations
/// # Ok(())
/// # }
/// ```
pub struct Meteostat {
    fetcher: FrameFetcher,
    station_locator: StationLocator,
}

#[bon]
impl Meteostat {
    /// Creates a new `Meteostat` client instance with a specified cache directory.
    ///
    /// Use this if you need to control where the downloaded station lists and
    /// weather data files are stored.
    ///
    /// # Arguments
    ///
    /// * `cache_folder` - A `PathBuf` pointing to the directory to use for caching.
    ///                    The directory will be created if it doesn't exist.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Meteostat` client on success, or a [`MeteostatError`]
    /// if the cache directory cannot be determined or created, or if loading station data fails.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::CacheDirCreation`] if the specified directory cannot be created.
    /// Returns [`MeteostatError::LocateStation`] variants if loading or parsing the station metadata fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use meteostat::Meteostat;
    /// # use meteostat::MeteostatError;
    /// # use std::path::{Path, PathBuf};
    /// # async fn run() -> Result<(), MeteostatError> {
    /// let cache_path = Path::new("/home/user/.cache").to_path_buf();
    /// let client = Meteostat::with_cache_folder(cache_path).await?;
    /// // ... use client ...
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_cache_folder(cache_folder: PathBuf) -> Result<Self, MeteostatError> {
        ensure_cache_dir_exists(&cache_folder)
            .await
            .map_err(|e| MeteostatError::CacheDirCreation(cache_folder.clone(), e))?;
        Ok(Self {
            station_locator: StationLocator::new(&cache_folder)
                .await
                .map_err(MeteostatError::from)?,
            fetcher: FrameFetcher::new(&cache_folder),
        })
    }

    /// Creates a new `Meteostat` client instance using the default cache directory.
    ///
    /// This is the simplest way to get started. The default cache directory is
    /// determined using the `dirs` crate, typically located in the user's
    /// cache directory (e.g., `~/.cache/meteostat_rs` on Linux).
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Meteostat` client on success, or a [`MeteostatError`]
    /// if the default cache directory cannot be resolved or created, or if loading station data fails.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::CacheDirResolution`] if the default cache directory cannot be found.
    /// Returns [`MeteostatError::CacheDirCreation`] if the default cache directory cannot be created.
    /// Returns [`MeteostatError::LocateStation`] variants if loading or parsing the station metadata fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use meteostat::Meteostat;
    /// # use meteostat::MeteostatError;
    /// # async fn run() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// // ... use client ...
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new() -> Result<Self, MeteostatError> {
        let cache_folder = get_cache_dir().map_err(MeteostatError::CacheDirResolution)?;
        Self::with_cache_folder(cache_folder).await
    }

    /// Finds weather stations near a given geographical location.
    ///
    /// This method searches the Meteostat station database for stations within a specified
    /// radius of the `location`. Results can be optionally filtered by data availability
    /// using `inventory_request` and limited by the number of stations returned (`station_limit`).
    /// Stations are returned sorted by distance, closest first.
    ///
    /// This method uses a builder pattern.
    ///
    /// # Arguments
    ///
    /// * `.location(LatLon)`: **Required.** The geographical coordinates ([`LatLon`]) around which to search.
    /// * `.inventory_request(Option<InventoryRequest>)`: Optional. Filters stations based on data availability ([`InventoryRequest`]). If `None`, no inventory check is performed.
    /// * `.max_distance_km(Option<f64>)`: Optional. The maximum search radius in kilometers. Defaults to `50.0`.
    /// * `.station_limit(Option<usize>)`: Optional. The maximum number of stations to return. Defaults to `5`.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Station>` on success, sorted by distance (closest first),
    /// or a [`MeteostatError`] on failure. The vector might be empty if no stations match the criteria.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::LocateStation`] variants if querying the station data fails internally.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use meteostat::{Meteostat, LatLon, InventoryRequest};
    /// # use meteostat::{Frequency, RequiredData};
    /// # use meteostat::MeteostatError;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let berlin_center = LatLon(52.52, 13.40);
    ///
    /// // Find the 5 closest stations within 50km (defaults)
    /// let nearby_stations = client
    ///     .find_stations()
    ///     .location(berlin_center)
    ///     .call()
    ///     .await?;
    /// assert!(nearby_stations.len() <= 5);
    /// println!("Found {} stations near Berlin (default settings).", nearby_stations.len());
    ///
    /// // Find up to 10 stations within 100km that have *any* hourly data
    /// let hourly_stations = client
    ///     .find_stations()
    ///     .location(berlin_center)
    ///     .max_distance_km(100.0)
    ///     .station_limit(10)
    ///     .inventory_request(InventoryRequest::new(Frequency::Hourly, RequiredData::Any))
    ///     .call()
    ///     .await?;
    /// assert!(hourly_stations.len() <= 10);
    /// println!("Found {} stations with hourly data within 100km (limit 10).", hourly_stations.len());
    ///
    /// # Ok(())
    /// # }
    /// ```
    #[builder]
    pub async fn find_stations(
        &self,
        location: LatLon,
        inventory_request: Option<InventoryRequest>,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
    ) -> Result<Vec<Station>, MeteostatError> {
        // Note: The defaults below are applied *if* the corresponding builder method was not called.
        let max_distance_km = max_distance_km.unwrap_or(50.0);
        let stations_limit = station_limit.unwrap_or(5); // Default limit for find_stations

        let (freq_option, date_option) = inventory_request
            .map(|req| (Some(req.frequency), Some(req.required_data))) // Pass Some when inventory_request is Some
            .unwrap_or((None, None)); // Pass None otherwise

        let stations_with_distance = self.station_locator.query(
            location.0,
            location.1,
            stations_limit,
            max_distance_km,
            freq_option,
            date_option,
        );

        Ok(stations_with_distance
            .into_iter()
            .map(|(station, _distance)| station) // Discard the distance for the return type
            .collect())
    }

    /// Fetches weather data for a specific Meteostat station ID.
    ///
    /// Retrieves data for the given station ID and data frequency (e.g., hourly, daily).
    /// This function first checks the local cache. If the data is not cached or is outdated,
    /// it downloads the data from the Meteostat servers and stores it in the cache.
    ///
    /// Data is returned as a Polars `LazyFrame` for efficient subsequent processing.
    ///
    /// This method uses a builder pattern.
    ///
    /// # Arguments
    ///
    /// * `.station(&str)`: **Required.** The Meteostat station ID (e.g., "10637" for Schiphol).
    /// * `.frequency(Frequency)`: **Required.** The desired data frequency ([`Frequency`]).
    ///
    /// # Returns
    ///
    /// A `Result` containing a Polars `LazyFrame` on success, or a [`MeteostatError`] on failure.
    /// The `LazyFrame` allows you to perform filtering, aggregation, and other operations
    /// before collecting the data into memory.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::WeatherData`] variants for issues like:
    ///   - Network errors during download.
    ///   - Errors reading from or writing to the cache.
    ///   - Errors parsing the downloaded CSV data.
    ///   - Data for the station/frequency not being available (e.g., 404 Not Found).
    /// Returns [`MeteostatError::PolarsError`] if Polars encounters an issue creating the `LazyFrame`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use meteostat::Meteostat;
    /// # use meteostat::{Frequency, MeteostatFrameFilterExt};
    /// # use meteostat::MeteostatError;
    /// # use polars::prelude::*;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10637"; // Amsterdam Schiphol
    ///
    /// // Get hourly data for Schiphol
    /// let lazy_hourly_data = client
    ///     .from_station()
    ///     .station(station_id)
    ///     .frequency(Frequency::Hourly)
    ///     .call()
    ///     .await?
    ///     .collect()?;
    ///
    /// println!("Collected hourly data for {}:\n{}", station_id, lazy_hourly_data);
    ///
    /// // Get daily data for the same station
    /// let lazy_daily_data = client
    ///     .from_station()
    ///     .station(station_id)
    ///     .frequency(Frequency::Daily)
    ///     .call()
    ///     .await?;
    ///
    /// let daily_df = lazy_daily_data.collect()?;
    /// println!("\nCollected daily data for {}:\n{}", station_id, daily_df.head(Some(5)));
    ///
    /// # Ok(())
    /// # }
    /// ```
    #[builder]
    pub async fn from_station(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<LazyFrame, MeteostatError> {
        self.fetcher
            .get_cache_lazyframe(station, frequency)
            .await
            .map_err(MeteostatError::from) // Convert WeatherDataError to MeteostatError
    }

    /// Fetches weather data for the closest available station near a geographical location.
    ///
    /// This method first finds nearby stations (within `max_distance_km`, up to `station_limit` stations,
    /// optionally matching `required_data`). It then attempts to fetch the data for these stations,
    /// starting with the closest one. It returns the data for the *first* station for which
    /// the data fetch is successful.
    ///
    /// This is useful when you don't know the exact station ID but have a location of interest.
    ///
    /// Data is returned as a Polars `LazyFrame`.
    ///
    /// This method uses a builder pattern.
    ///
    /// # Arguments
    ///
    /// * `.location(LatLon)`: **Required.** The geographical coordinates ([`LatLon`]) for the search center.
    /// * `.frequency(Frequency)`: **Required.** The desired data frequency ([`Frequency`]).
    /// * `.max_distance_km(Option<f64>)`: Optional. The maximum search radius in kilometers. Defaults to `50.0`.
    /// * `.station_limit(Option<usize>)`: Optional. The maximum number of candidate stations to find and attempt to fetch data from. Defaults to `1`. Note: It tries these stations sequentially and returns data from the first successful fetch. Increasing this may help if the absolute closest station has no data.
    /// * `.required_data(Option<RequiredData>)`: Optional. Filters the candidate stations based on data availability ([`RequiredData`]). If `None`, no inventory check is performed before attempting download.
    ///
    /// # Returns
    ///
    /// A `Result` containing a Polars `LazyFrame` from the closest successful station on success,
    /// or a [`MeteostatError`] on failure.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::NoStationWithinRadius`] if no stations are found within the specified `max_distance_km` and matching `required_data` (if provided).
    /// Returns [`MeteostatError::NoDataFoundForNearbyStations`] if stations are found, but fetching data fails for all attempted stations (up to `station_limit`). This error includes the last encountered fetch error.
    /// Returns [`MeteostatError::WeatherData`] variants for issues during the fetch attempt of the chosen station (network, cache, parse errors).
    /// Returns [`MeteostatError::PolarsError`] if Polars encounters an issue creating the `LazyFrame`.
    /// Returns [`MeteostatError::LocateStation`] variants if querying the station data fails internally during the station search phase.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use meteostat::{Meteostat, LatLon};
    /// # use meteostat::{Frequency, MeteostatFrameFilterExt};
    /// # use meteostat::MeteostatError;
    /// # use polars::prelude::*;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let brandenburg_gate = LatLon(52.5163, 13.3777);
    ///
    /// // Get hourly data for the closest station near Brandenburg Gate (defaults: 50km, try 1 station)
    /// let hourly_data_df = client
    ///     .from_location()
    ///     .location(brandenburg_gate)
    ///     .frequency(Frequency::Hourly)
    ///     .call()
    ///     .await?
    ///     .collect()?;
    ///
    /// println!("Hourly data near Brandenburg Gate:\n{}", hourly_data_df);
    ///
    /// // Get daily data, increasing search radius and stations to try
    /// let daily_data_wider_search_df = client
    ///     .from_location()
    ///     .location(brandenburg_gate)
    ///     .frequency(Frequency::Daily)
    ///     .max_distance_km(100.0) // Search up to 100 km
    ///     .station_limit(3)      // Try up to 3 closest stations if needed
    ///     .call()
    ///     .await?
    ///     .collect()?;
    ///
    /// println!("\nDaily data near Brandenburg Gate (wider search):\n{}", daily_data_wider_search_df);
    ///
    /// # Ok(())
    /// # }
    /// ```
    #[builder]
    pub async fn from_location(
        &self,
        location: LatLon,
        frequency: Frequency,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<LazyFrame, MeteostatError> {
        // Note: Defaults applied here if builder methods not called.
        let max_distance_km = max_distance_km.unwrap_or(50.0);
        // Default limit for *candidate stations to try* in from_location is 1.
        let stations_limit = station_limit.unwrap_or(1);

        // Query for candidate stations
        let stations = self.station_locator.query(
            location.0,
            location.1,
            stations_limit, // Limit the number of candidates fetched
            max_distance_km,
            Some(frequency), // Always filter by frequency for from_location
            required_data,   // Apply optional date/inventory filter
        );

        // Handle case where no stations are found matching the criteria
        if stations.is_empty() {
            return Err(MeteostatError::NoStationWithinRadius {
                radius: max_distance_km,
                lat: location.0,
                lon: location.1,
            });
        }

        let mut last_error: Option<MeteostatError> = None;

        // Iterate through the found stations (sorted by distance) and try to fetch data
        for (station, _) in stations.iter() {
            match self
                .fetcher
                .get_cache_lazyframe(&station.id, frequency)
                .await
            {
                Ok(lazy_frame) => {
                    // Successfully fetched data, return it immediately
                    return Ok(lazy_frame);
                }
                Err(e) => {
                    // Convert specific WeatherDataError to the general MeteostatError
                    let current_error = MeteostatError::from(e);
                    last_error = Some(current_error);
                }
            }
        }

        // If the loop finishes without returning, it means all attempts failed.
        Err(MeteostatError::NoDataFoundForNearbyStations {
            radius: max_distance_km,
            lat: location.0,
            lon: location.1,
            stations_tried: stations.len(), // Report how many unique stations were attempted
            last_error: last_error.map(Box::new), // Include the last error encountered
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::MeteostatError;
    use crate::meteostat::{InventoryRequest, LatLon, Meteostat};
    use crate::types::data_source::{Frequency, RequiredData};

    #[tokio::test]
    async fn test_get_hourly() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let data = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let hourly_frame = data.collect()?;

        let shape = hourly_frame.shape();
        assert!(shape.0 >= 719_681);
        assert_eq!(shape.1, 14);

        let columns = hourly_frame.get_column_names();
        assert_eq!(
            columns,
            [
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco", "datetime"
            ]
        );

        dbg!(&hourly_frame);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let data = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Daily)
            .call()
            .await?;

        let daily_frame = data.collect()?;

        let shape = daily_frame.shape();
        assert!(shape.0 >= 32_221);
        assert_eq!(shape.1, 11);

        assert_eq!(
            daily_frame.get_column_names(),
            [
                "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres",
                "tsun",
            ]
        );

        dbg!(&daily_frame);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let data = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Monthly)
            .call()
            .await?;

        let monthly_frame = data.collect()?;
        dbg!(&monthly_frame);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let data = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Climate)
            .call()
            .await?;

        let climate_frame = data.collect()?;
        dbg!(&climate_frame);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_hourly_location() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let hourly_data = meteostat
            .from_location()
            .location(LatLon(52.520008, 13.404954))
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let frame = hourly_data.collect()?;

        let shape = frame.shape();
        assert_eq!(shape.1, 14);

        let columns = frame.get_column_names();
        assert_eq!(
            columns,
            [
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco", "datetime"
            ]
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_find_stations_basic() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let stations = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            // Using defaults: limit=5, distance=50.0, no inventory filter
            .call()
            .await?;

        dbg!(&stations); // See which stations are found

        assert!(
            !stations.is_empty(),
            "Expected to find stations near Berlin"
        );
        assert!(
            stations.len() <= 5, // Default limit is 5
            "Expected at most 5 stations with default limit, found {}",
            stations.len()
        );
        // Optional: Check if a known Berlin station ID is potentially included
        // assert!(stations.iter().any(|s| s.id == "10382")); // Example: Berlin-Brandenburg

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_limit() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        // Test with limit = 1
        let stations_limit_1 = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            .station_limit(1)
            .call()
            .await?;

        assert_eq!(
            stations_limit_1.len(),
            1,
            "Expected exactly 1 station with limit=1"
        );

        // Test with limit = 10
        let stations_limit_10 = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            .station_limit(10)
            .call()
            .await?;

        assert!(
            stations_limit_10.len() > 1, // Should find more than 1 near Berlin
            "Expected more than 1 station with limit=10 near Berlin"
        );
        assert!(
            stations_limit_10.len() <= 10,
            "Expected at most 10 stations with limit=10, found {}",
            stations_limit_10.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_distance() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let known_station_id = "06240"; // Schiphol weather station in Amsterdam

        // Test with very small distance (should ideally find only the airport station if close enough)
        let stations_dist_1 = meteostat
            .find_stations()
            .location(LatLon(52.3, 4.7667)) // Schiphol airport
            .max_distance_km(1.0) // 1 km radius
            .station_limit(5) // Keep default limit
            .call()
            .await?;

        dbg!(&stations_dist_1);
        assert!(
            !stations_dist_1.is_empty(),
            "Expected stations within 5km of Schiphol"
        );
        // Check if the specific known station is found within this small radius
        assert!(
            stations_dist_1.iter().any(|s| s.id == known_station_id),
            "Expected to find station {} within 5km",
            known_station_id
        );

        // Test with larger distance (should find more stations than the 5km test, up to the limit)
        let stations_dist_100 = meteostat
            .find_stations()
            .location(LatLon(52.36, 13.50))
            .max_distance_km(100.0) // 100 km radius
            .station_limit(5) // Keep default limit
            .call()
            .await?;

        dbg!(&stations_dist_100);
        assert!(
            stations_dist_100.len() >= stations_dist_1.len(),
            "Expected more or equal stations with 100km radius compared to 5km"
        );
        assert!(
            stations_dist_100.len() <= 5, // Respecting the default limit
            "Expected max 5 stations even with 100km radius due to default limit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_inventory_request() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let stations_hourly = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            .inventory_request(InventoryRequest::new(Frequency::Hourly, RequiredData::Any))
            .station_limit(5)
            .call()
            .await?;

        dbg!(&stations_hourly);
        assert!(
            !stations_hourly.is_empty(),
            "Expected to find stations near Berlin with *any* hourly data"
        );
        // Note: Verifying they *actually* have hourly data requires checking metadata,
        // this test mainly ensures the filter is applied in the query.

        let stations_daily_2023 = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            .inventory_request(InventoryRequest::new(
                Frequency::Daily,
                RequiredData::Year(2023),
            ))
            .station_limit(5)
            .call()
            .await?;

        dbg!(&stations_daily_2023);
        // We can't be certain stations *will* exist for this specific filter,
        // but the call should succeed. Check if it returns potentially fewer stations
        // than the basic query (though not a strict requirement).
        println!(
            "Found {} stations with Daily data for 2023 near Berlin.",
            stations_daily_2023.len()
        );
        assert!(
            stations_daily_2023.len() <= 5,
            "Should respect station limit even with inventory filter"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_no_stations_found_remote() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        // Location in the middle of the Pacific Ocean
        let remote_location = LatLon(0.0, 160.0);

        let stations = meteostat
            .find_stations()
            .location(remote_location)
            .call()
            .await?;

        assert!(
            stations.is_empty(),
            "Expected no stations in the middle of the Pacific"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_no_stations_found_restrictive_distance(
    ) -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;
        let berlin = LatLon(52.52, 13.40);

        // Use an extremely small radius unlikely to contain any station
        let stations = meteostat
            .find_stations()
            .location(berlin)
            .max_distance_km(0.1) // 100 meters
            .call()
            .await?;

        assert!(
            stations.is_empty(),
            "Expected no stations within 0.1km of Berlin center"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_combined_filters() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let stations = meteostat
            .find_stations()
            .location(LatLon(52.52, 13.40))
            .max_distance_km(25.0) // Limit distance
            .station_limit(3) // Limit number
            .inventory_request(InventoryRequest::new(
                Frequency::Hourly,
                RequiredData::Year(2020),
            )) // Filter by hourly data
            .call()
            .await?;

        dbg!(&stations);
        assert!(
            stations.len() <= 3, // Must respect station_limit
            "Expected max 3 stations with combined filters, found {}",
            stations.len()
        );
        // Further assertions could check distances if Station struct contained distance info

        Ok(())
    }
}
