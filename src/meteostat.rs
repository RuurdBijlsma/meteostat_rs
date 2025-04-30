//! The main entry point for interacting with the Meteostat API client.
//!
//! Provides methods to configure the client (e.g., cache location) and access
//! different types of weather data (hourly, daily, monthly, climate normals)
//! either by station ID or by geographical location.

use crate::stations::locate_station::{StationLocator, BINCODE_CACHE_FILE_NAME};
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use crate::weather_data::frame_fetcher::FrameFetcher;
use crate::{
    ClimateClient, DailyClient, Frequency, HourlyClient, MeteostatError, MonthlyClient,
    RequiredData, Station,
};
use bon::bon;
use polars::prelude::LazyFrame;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;

/// Represents a geographical coordinate using Latitude and Longitude.
///
/// Used for querying weather stations or data based on location.
///
/// # Fields
///
/// * `0` - Latitude in decimal degrees.
/// * `1` - Longitude in decimal degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon(pub f64, pub f64);

/// Represents criteria for filtering weather stations based on their data inventory.
///
/// Used in conjunction with [`Meteostat::find_stations`] to find stations that
/// report having data for a specific frequency and meeting certain data coverage requirements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InventoryRequest {
    /// The required data frequency (e.g., Hourly, Daily).
    frequency: Frequency,
    /// The specific data coverage requirement (e.g., Any, FullYear(2023)).
    required_data: RequiredData,
}

impl InventoryRequest {
    /// Creates a new inventory request.
    ///
    /// # Arguments
    ///
    /// * `frequency` - The desired data [`Frequency`].
    /// * `required_data` - The [`RequiredData`] criteria for data coverage.
    pub fn new(frequency: Frequency, required_data: RequiredData) -> Self {
        Self {
            frequency,
            required_data,
        }
    }
}

/// The main client struct for accessing Meteostat data.
///
/// Provides methods to fetch weather data (hourly, daily, monthly, climate)
/// and find weather stations. Handles data caching internally.
///
/// Create instances using [`Meteostat::new`] or [`Meteostat::with_cache_folder`].
pub struct Meteostat {
    fetcher: FrameFetcher,
    station_locator: StationLocator,
    cache_folder: PathBuf,
}

#[bon]
impl Meteostat {
    /// Creates a new `Meteostat` client using a specific cache folder.
    ///
    /// Initializes the station locator and frame fetcher, ensuring the specified
    /// cache directory exists.
    ///
    /// # Arguments
    ///
    /// * `cache_folder` - A `PathBuf` representing the directory to use for caching
    ///   station metadata and downloaded weather data.
    ///
    /// # Returns
    ///
    /// A `Result` containing the initialized `Meteostat` client or a `MeteostatError`
    /// if initialization fails.
    ///
    /// # Errors
    ///
    /// This function can return errors if:
    /// - The cache directory cannot be created ([`MeteostatError::CacheDirCreation`]).
    /// - Loading or initializing station data fails (propagated from `StationLocator::new`,
    ///   resulting in [`MeteostatError::LocateStation`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError};
    /// use std::path::PathBuf;
    /// use tempfile::tempdir; // For example purposes
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let temp_dir = tempdir()?; // Create a temporary directory
    /// let cache_path = temp_dir.path().join("my_meteostat_cache");
    ///
    /// // Create a client with a custom cache location
    /// let client = Meteostat::with_cache_folder(cache_path).await?;
    ///
    /// println!("Meteostat client initialized with custom cache.");
    /// // Use the client...
    ///
    /// temp_dir.close()?; // Clean up temp directory
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_cache_folder(cache_folder: PathBuf) -> Result<Self, MeteostatError> {
        // Ensure the directory exists
        ensure_cache_dir_exists(&cache_folder)
            .await
            .map_err(|e| MeteostatError::CacheDirCreation(cache_folder.clone(), e))?;

        // Initialize components
        Ok(Self {
            station_locator: StationLocator::new(&cache_folder)
                .await
                .map_err(MeteostatError::from)?, // Converts LocateStationError
            fetcher: FrameFetcher::new(&cache_folder),
            cache_folder,
        })
    }

    /// Creates a new `Meteostat` client using the default cache folder location.
    ///
    /// The default location is platform-dependent (e.g., `~/.cache/meteostat-rs` on Linux).
    /// Initializes the station locator and frame fetcher, ensuring the default
    /// cache directory exists.
    ///
    /// # Returns
    ///
    /// A `Result` containing the initialized `Meteostat` client or a `MeteostatError`
    /// if initialization fails.
    ///
    /// # Errors
    ///
    /// This function can return errors if:
    /// - The default cache directory path cannot be determined ([`MeteostatError::CacheDirResolution`]).
    /// - The default cache directory cannot be created ([`MeteostatError::CacheDirCreation`]).
    /// - Loading or initializing station data fails (propagated from `StationLocator::new`,
    ///   resulting in [`MeteostatError::LocateStation`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// // Create a client with the default cache location
    /// let client = Meteostat::new().await?;
    ///
    /// println!("Meteostat client initialized with default cache.");
    /// // Use the client...
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new() -> Result<Self, MeteostatError> {
        let cache_folder = get_cache_dir().map_err(MeteostatError::CacheDirResolution)?;
        Self::with_cache_folder(cache_folder).await
    }

    /// Prepares a request builder for fetching hourly weather data.
    ///
    /// Returns an [`HourlyClient`] which allows specifying a station ID or location
    /// and optional parameters before executing the request.
    ///
    /// # Returns
    ///
    /// An [`HourlyClient`] associated with this `Meteostat` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let berlin = LatLon(52.52, 13.40);
    ///
    /// // Get hourly data for Berlin (nearest station)
    /// let hourly_data = client.hourly().location(berlin).call().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn hourly(&self) -> HourlyClient<'_> {
        HourlyClient::new(self)
    }

    /// Prepares a request builder for fetching daily weather data.
    ///
    /// Returns a [`DailyClient`] which allows specifying a station ID or location
    /// and optional parameters before executing the request.
    ///
    /// # Returns
    ///
    /// A [`DailyClient`] associated with this `Meteostat` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let paris = LatLon(48.85, 2.35);
    ///
    /// // Get daily data for station "07150" (Paris-Montsouris)
    /// let daily_data = client.daily().station("07150").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn daily(&self) -> DailyClient<'_> {
        DailyClient::new(self)
    }

    /// Prepares a request builder for fetching monthly weather data.
    ///
    /// Returns a [`MonthlyClient`] which allows specifying a station ID or location
    /// and optional parameters before executing the request.
    ///
    /// # Returns
    ///
    /// A [`MonthlyClient`] associated with this `Meteostat` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let london = LatLon(51.50, -0.12);
    ///
    /// // Get monthly data for London (nearest station)
    /// let monthly_data = client.monthly().location(london).call().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn monthly(&self) -> MonthlyClient<'_> {
        MonthlyClient::new(self)
    }

    /// Prepares a request builder for fetching climate normals data.
    ///
    /// Returns a [`ClimateClient`] which allows specifying a station ID or location
    /// and optional parameters before executing the request.
    ///
    /// # Returns
    ///
    /// A [`ClimateClient`] associated with this `Meteostat` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, LatLon};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    ///
    /// // Get climate normals for station "10382" (Berlin-Tegel)
    /// let climate_data = client.climate().station("10382").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn climate(&self) -> ClimateClient<'_> {
        ClimateClient::new(self)
    }

    /// Finds weather stations near a given geographical location.
    ///
    /// Allows filtering by maximum distance, number of stations, and data inventory requirements.
    /// Uses a builder pattern for optional parameters.
    ///
    /// # Arguments (Builder Methods)
    ///
    /// * `.location(LatLon)`: **Required.** The geographical coordinate [`LatLon`] around which to search.
    /// * `.inventory_request(InventoryRequest)`: *Optional.* Filters stations based on reported data availability using an [`InventoryRequest`].
    /// * `.max_distance_km(f64)`: *Optional.* The maximum search radius in kilometers. Defaults to `50.0`.
    /// * `.station_limit(usize)`: *Optional.* The maximum number of stations to return, sorted by distance. Defaults to `5`.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<Station>` of found stations (sorted by distance, closest first),
    /// or a `MeteostatError` if the search fails.
    ///
    /// # Errors
    ///
    /// Can return errors propagated from the underlying station search mechanism
    /// ([`MeteostatError::LocateStation`]). Note that finding *no* stations within
    /// the criteria is **not** considered an error for this method; it will return an empty `Vec`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError, LatLon, InventoryRequest, Frequency, RequiredData};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let nyc = LatLon(40.7128, -74.0060);
    ///
    /// // Find the 3 closest stations within 100km of NYC
    /// // that have reported *any* Daily data.
    /// let inventory_req = InventoryRequest::new(Frequency::Daily, RequiredData::Any);
    ///
    /// let stations = client.find_stations()
    ///     .location(nyc)
    ///     .max_distance_km(100.0)
    ///     .station_limit(3)
    ///     .inventory_request(inventory_req)
    ///     .call()
    ///     .await?;
    ///
    /// println!("Found {} stations near NYC matching criteria:", stations.len());
    /// for station in stations {
    ///     println!("  - ID: {}, Name: {:?}", station.id, station.name.get("en"));
    /// }
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

        // Perform the query using the station locator
        let stations_with_distance = self.station_locator.query(
            location.0,
            location.1,
            stations_limit,
            max_distance_km,
            freq_option,
            date_option,
        );

        // Extract stations and discard distances
        Ok(stations_with_distance
            .into_iter()
            .map(|(station, _distance)| station) // Discard the distance for the return type
            .collect())
    }

    /// **Internal:** Fetches a lazy frame for a specific station and frequency.
    ///
    /// Handles cache lookup and potential downloads via `FrameFetcher`.
    /// This method is intended for internal use by the frequency-specific clients
    /// (`HourlyClient`, `DailyClient`, etc.).
    ///
    /// # Arguments
    ///
    /// * `station` - The ID of the weather station.
    /// * `frequency` - The desired data [`Frequency`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a Polars `LazyFrame` for the requested data,
    /// or a `MeteostatError` if fetching fails.
    ///
    /// # Errors
    ///
    /// Can return [`MeteostatError::WeatherData`] if fetching/parsing the data fails
    /// (e.g., network error, file not found, CSV parsing error).
    #[builder]
    pub(crate) async fn data_from_station(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<LazyFrame, MeteostatError> {
        self.fetcher
            .get_cache_lazyframe(station, frequency)
            .await
            .map_err(MeteostatError::from) // Converts WeatherDataError
    }

    /// **Internal:** Fetches a lazy frame for the nearest suitable station to a location.
    ///
    /// Finds nearby stations matching the criteria, then attempts to fetch data
    /// from them sequentially (closest first) until successful.
    /// This method is intended for internal use by the frequency-specific clients.
    ///
    /// # Arguments
    ///
    /// * `location` - The target [`LatLon`].
    /// * `frequency` - The desired data [`Frequency`].
    /// * `max_distance_km` - *Optional.* Max search radius. Defaults to `50.0`.
    /// * `station_limit` - *Optional.* Max number of *candidate stations* to query. Defaults to `1`.
    /// * `required_data` - *Optional.* Filter candidate stations by [`RequiredData`].
    ///
    /// # Returns
    ///
    /// A `Result` containing a Polars `LazyFrame` for the first successful station,
    /// or a `MeteostatError` if no suitable station is found or data fetching fails for all candidates.
    ///
    /// # Errors
    ///
    /// Can return:
    /// - [`MeteostatError::NoStationWithinRadius`]: If the initial station query finds no candidates matching the criteria.
    /// - [`MeteostatError::NoDataFoundForNearbyStations`]: If candidate stations were found, but fetching data failed for all of them. Includes the last encountered `WeatherData` error.
    /// - [`MeteostatError::LocateStation`]: If the station query itself fails.
    /// - [`MeteostatError::WeatherData`]: Encapsulated within `NoDataFoundForNearbyStations` if fetching fails for a candidate.
    #[builder]
    pub(crate) async fn data_from_location(
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
                    // Continue to the next station
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

    /// Clears the cached station list file (`stations_lite.bin`).
    ///
    /// This removes the locally stored station metadata. This function doesn't
    /// clear the in-memory tree of stations. To clear that, use [`Meteostat::rebuild_station_list_cache`].
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::CacheDeletionError`] if the file cannot be removed
    /// (e.g., due to permissions issues or if the file doesn't exist).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::Meteostat;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// // ... use client ...
    ///
    /// client.clear_station_list_cache().await?;
    /// println!("Station list cache cleared.");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_station_list_cache(&self) -> Result<(), MeteostatError> {
        let stations_file = self.cache_folder.join(BINCODE_CACHE_FILE_NAME);
        match tokio::fs::remove_file(&stations_file).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()), // Not an error if already gone
            Err(e) => Err(MeteostatError::CacheDeletionError(stations_file.clone(), e)),
        }
    }

    /// Forces a rebuild of the station list cache.
    ///
    /// This method will delete the existing station cache file (if any)
    /// and then immediately download and process the latest station metadata
    /// from Meteostat, storing it in the cache.
    ///
    /// Note: This requires mutable access (`&mut self`) because it modifies the
    /// internal `StationLocator` state.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Can return errors related to:
    /// - Deleting the old cache file ([`MeteostatError::CacheDeletionError`]).
    /// - Downloading or processing the new station data (propagated as [`MeteostatError::LocateStation`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::Meteostat;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = Meteostat::new().await?;
    /// // ... use client ...
    ///
    /// // Ensure the station cache is up-to-date
    /// client.rebuild_station_list_cache().await?;
    /// println!("Station list cache rebuilt.");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rebuild_station_list_cache(&mut self) -> Result<(), MeteostatError> {
        // Delegate the actual rebuilding (which includes clearing) to the locator
        self.station_locator
            .rebuild_cache(&self.cache_folder)
            .await
            .map_err(MeteostatError::from) // Convert LocateStationError
    }

    /// Clears the cached weather data file(s) for a specific station and frequency.
    ///
    /// Removes the `.parquet` file associated with the given station ID and data frequency
    /// from the cache directory. Also clears any in-memory cache associated with this
    /// specific data in the `FrameFetcher`.
    ///
    /// # Arguments
    ///
    /// * `station` - The ID of the station whose cache should be cleared.
    /// * `frequency` - The [`Frequency`] of the data cache to clear (e.g., Hourly, Daily).
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::CacheDeletionError`] if the parquet file cannot be removed.
    /// Returns [`MeteostatError::WeatherData`] if clearing the internal `FrameFetcher` cache fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, Frequency};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10382"; // Example: Berlin-Tegel
    ///
    /// // Fetch some data first to ensure it's cached
    /// let _ = client.hourly().station(station_id).await?;
    ///
    /// // Clear only the hourly cache for this station
    /// client.clear_weather_data_cache_per_station(station_id, Frequency::Hourly).await?;
    /// println!("Hourly cache for station {} cleared.", station_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_weather_data_cache_per_station(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<(), MeteostatError> {
        let file =
            self.cache_folder
                .join(format!("{}-{}.parquet", frequency.path_segment(), station));
        match tokio::fs::remove_file(&file).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {} // Not an error if already gone
            Err(e) => return Err(MeteostatError::CacheDeletionError(file.clone(), e)),
        }
        // Also clear from the in-memory FrameFetcher cache
        self.fetcher
            .clear_cache(station, frequency)
            .await
            .map_err(MeteostatError::from) // Convert WeatherDataError
    }

    /// Clears all cached weather data files (`.parquet` files).
    ///
    /// Iterates through the cache directory and removes all files ending with the
    /// `.parquet` extension. This effectively deletes all cached hourly, daily, monthly,
    /// and climate normal data. The station list cache (`stations_lite.bin`) is **not** removed
    /// by this method. Also clears the in-memory cache of the `FrameFetcher`.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns [`MeteostatError::CacheDeletionError`] if removing any specific parquet file fails.
    /// Returns [`MeteostatError::WeatherData`] if clearing the internal `FrameFetcher` cache fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::Meteostat;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// // ... fetch some data ...
    ///
    /// // Clear all downloaded weather data
    /// client.clear_weather_data_cache().await?;
    /// println!("All weather data cache cleared.");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_weather_data_cache(&self) -> Result<(), MeteostatError> {
        let mut entries = tokio::fs::read_dir(&self.cache_folder)
            .await
            .map_err(|e| MeteostatError::CacheDeletionError(self.cache_folder.clone(), e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| MeteostatError::CacheDeletionError(self.cache_folder.clone(), e))?
        {
            let file_path = entry.path();
            if file_path.is_file() {
                if let Some(extension) = file_path.extension() {
                    if extension == OsStr::new("parquet") {
                        match tokio::fs::remove_file(&file_path).await {
                            Ok(_) => {}
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {} // Ignore if already gone
                            Err(e) => {
                                return Err(MeteostatError::CacheDeletionError(
                                    file_path.clone(),
                                    e,
                                ))
                            }
                        }
                    }
                }
            }
        }
        // Also clear the FrameFetcher's internal cache
        self.fetcher
            .clear_cache_all()
            .await
            .map_err(MeteostatError::from)?;
        Ok(())
    }

    /// Clears the entire cache directory.
    ///
    /// This removes both the cached station list (`stations_lite.bin`) and all
    /// cached weather data files (`.parquet` files). It effectively combines
    /// [`Meteostat::clear_station_list_cache`] and [`Meteostat::clear_weather_data_cache`].
    ///
    /// **Note**: this retains the in-memory`StationLocator` state, to clear that as well
    /// you have to use [`Meteostat::clear_cache_and_rebuild`].
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Can return errors from either [`Meteostat::clear_station_list_cache`] or [`Meteostat::clear_weather_data_cache`].
    /// See their documentation for specific error types ([`MeteostatError::CacheDeletionError`], [`MeteostatError::WeatherData`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::Meteostat;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Meteostat::new().await?;
    /// // ... fetch data ...
    ///
    /// // Remove all cached files
    /// client.clear_cache().await?;
    /// println!("Complete cache cleared.");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_cache(&self) -> Result<(), MeteostatError> {
        // Clear station list first
        self.clear_station_list_cache().await?;
        // Then clear weather data
        self.clear_weather_data_cache().await?;
        Ok(())
    }

    /// Clears the entire cache directory and then rebuilds the station list cache.
    ///
    /// This first removes all cached files (station list and weather data) and then
    /// immediately downloads and rebuilds the station list cache. It's useful for
    /// ensuring a completely fresh start while pre-populating the essential station metadata.
    ///
    /// Note: This requires mutable access (`&mut self`) because it modifies the
    /// internal `StationLocator` state during the rebuild step.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Can return errors from [`Meteostat::clear_cache`] or [`Meteostat::rebuild_station_list_cache`]. See their
    /// documentation for specific error types ([`MeteostatError::CacheDeletionError`],
    /// [`MeteostatError::WeatherData`], [`MeteostatError::LocateStation`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::Meteostat;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = Meteostat::new().await?;
    /// // ... potentially use client ...
    ///
    /// // Clear everything and ensure station list is immediately available again
    /// client.clear_cache_and_rebuild().await?;
    /// println!("Cache cleared and station list rebuilt.");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_cache_and_rebuild(&mut self) -> Result<(), MeteostatError> {
        self.clear_cache().await?;
        self.rebuild_station_list_cache().await?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
    }

    /// Helper function to check if a cache file exists
    async fn cache_file_exists(cache_dir: &Path, station: &str, frequency: Frequency) -> bool {
        let file = cache_dir.join(format!("{}-{}.parquet", frequency.path_segment(), station));
        file.exists()
    }

    #[tokio::test]
    async fn test_clear_weather_data_cache_per_station() -> Result<(), MeteostatError> {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();
        let client = Meteostat::with_cache_folder(cache_path.clone()).await?;

        // Ensure station cache exists
        let berlin = berlin_location();
        let stations = client.find_stations().location(berlin).call().await?;
        let station_id = &stations[0].id;
        let _lf = client.hourly().station(station_id).await?;
        println!("Found station ID: {}", station_id);
        assert!(cache_file_exists(&cache_path, station_id, Frequency::Hourly).await);

        // Clear cache for this station's hourly data
        client
            .clear_weather_data_cache_per_station(station_id, Frequency::Hourly)
            .await?;

        // Verify cache file is gone
        assert!(!cache_file_exists(&cache_path, station_id, Frequency::Hourly).await);

        temp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_weather_data_cache() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();
        let client = Meteostat::with_cache_folder(cache_path.clone()).await?;

        // Get some data of different types to populate cache
        let berlin = berlin_location();
        let _ = client.hourly().location(berlin).call().await?;
        let _ = client.daily().location(berlin).call().await?;

        // --- Optional: Sanity check before clearing (sync version) ---
        let initial_file_count = fs::read_dir(&cache_path)?
            .filter_map(Result::ok) // Ignore errors reading entries
            .filter(|entry| entry.path().is_file()) // Keep only files
            .count();
        // Ensure more than just the stations file exists (assuming it's created early)
        assert!(
            initial_file_count > 1,
            "Expected more than one file before clearing cache."
        );

        // --- Clear all cache (async operation) ---
        client.clear_weather_data_cache().await?;

        // --- Verify directory is empty except for stations file (sync version) ---
        let mut file_count = 0;
        let mut stations_file_found = false;
        let stations_filename = OsStr::new(BINCODE_CACHE_FILE_NAME); // Define expected filename

        // Iterate through the directory synchronously
        for entry_result in fs::read_dir(&cache_path)? {
            let entry = entry_result?; // Propagate IO errors
            let path = entry.path();

            if path.is_file() {
                file_count += 1;
                if entry.file_name() == stations_filename {
                    stations_file_found = true;
                }
                println!("Found file after clear: {:?}", path); // Debug print
            }
        }

        // --- Assertions ---
        assert_eq!(
            file_count, 1,
            "Expected exactly one file to remain after clearing."
        );
        assert!(
            stations_file_found,
            "The remaining file should be 'stations_lite.bin'."
        );

        // Optional: Double check the path directly (redundant but sometimes helpful)
        // let stations_path = cache_path.join(stations_filename);
        // assert!(stations_path.exists() && stations_path.is_file());

        temp_dir.close()?; // Clean up the temp directory
        Ok(())
    }

    // --- Constructor Tests ---

    #[tokio::test]
    async fn test_new_constructor_succeeds() {
        // This test assumes default cache dir resolution works
        let result = Meteostat::new().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_with_cache_folder_constructor_succeeds() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempdir()?;
        let cache_path = temp_dir.path().to_path_buf();

        // Ensure the directory exists before calling (though the function does this too)
        tokio::fs::create_dir_all(&cache_path).await?;

        let result = Meteostat::with_cache_folder(cache_path.clone()).await;
        assert!(result.is_ok());

        temp_dir.close()?; // Clean up the temp directory
        Ok(())
    }

    // --- find_stations Tests ---

    #[tokio::test]
    async fn test_find_stations_basic() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let location = berlin_location();

        let stations = client.find_stations().location(location).call().await?;

        assert!(!stations.is_empty(), "Should find stations near Berlin");
        // Default limit is 5
        assert!(
            stations.len() <= 5,
            "Should return at most the default limit (5)"
        );
        println!("Found {} stations (default limit 5):", stations.len());
        for station in stations.iter().take(5) {
            println!("  - {} ({:?})", station.id, station.name.get("en"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_limit() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let location = berlin_location();
        let limit = 3;

        let stations = client
            .find_stations()
            .location(location)
            .station_limit(limit)
            .call()
            .await?;

        assert!(!stations.is_empty(), "Should find stations near Berlin");
        assert!(
            stations.len() <= limit,
            "Should return at most {} stations",
            limit
        );
        println!("Found {} stations (limit {}):", stations.len(), limit);
        for station in &stations {
            println!("  - {} ({:?})", station.id, station.name.get("en"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_max_distance() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let location = berlin_location();
        let max_dist = 15.0; // Smaller radius

        let stations = client
            .find_stations()
            .location(location)
            .max_distance_km(max_dist)
            .call()
            .await?;

        // Might still find stations, but possibly fewer than default distance
        println!("Found {} stations within {} km:", stations.len(), max_dist);
        for station in &stations {
            println!("  - {} ({:?})", station.id, station.name.get("en"));
        }
        // We can't easily assert the *exact* number, just that the call works.
        assert!(stations.len() <= 5); // Still respects default limit
        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_with_inventory_request() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let location = berlin_location();

        // Request stations with Daily data covering the year 2022
        let inventory_req = InventoryRequest::new(Frequency::Daily, RequiredData::FullYear(2022));

        let stations = client
            .find_stations()
            .location(location)
            .inventory_request(inventory_req)
            .call()
            .await?;

        assert!(
            !stations.is_empty(),
            "Should find stations near Berlin with daily data for 2022"
        );
        assert!(stations.len() <= 5); // Default limit
        println!(
            "Found {} stations with Daily data for 2022:",
            stations.len()
        );
        for station in &stations {
            println!("  - {} ({:?})", station.id, station.name.get("en"));
        }
        // A more robust test would involve checking the inventory details of the returned stations,
        // but that requires parsing the station metadata more deeply.
        Ok(())
    }

    #[tokio::test]
    async fn test_find_stations_no_results_far_away() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        // A location likely far from any station
        let location = LatLon(0.0, 0.0); // Middle of the Atlantic
        let max_dist = 5.0; // Very small radius

        let stations = client
            .find_stations()
            .location(location)
            .max_distance_km(max_dist)
            .call()
            .await?;

        assert!(
            stations.is_empty(),
            "Should find no stations in the middle of the ocean with small radius"
        );
        Ok(())
    }

    // --- Error Handling Tests ---

    #[tokio::test]
    async fn test_data_from_location_no_station_within_radius() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let location = LatLon(0.0, 0.0); // Middle of Atlantic
        let small_radius = 1.0;

        let result = client
            .hourly() // Any frequency works here
            .location(location)
            .max_distance_km(small_radius)
            .call()
            .await;

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Expected error: {:?}", err); // See the error details

        // Use matches macro if added to dev-dependencies:
        // use matches::assert_matches;
        // assert_matches!(err, MeteostatError::NoStationWithinRadius { radius, lat, lon }
        //     if radius == small_radius && lat == location.0 && lon == location.1
        // );

        // Manual check without `matches` macro:
        match err {
            MeteostatError::NoStationWithinRadius { radius, lat, lon } => {
                assert_eq!(radius, small_radius);
                assert_eq!(lat, location.0);
                assert_eq!(lon, location.1);
            }
            _ => panic!("Expected NoStationWithinRadius error, got {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_data_from_location_station_found_but_no_data_error() -> Result<(), MeteostatError>
    {
        // This test is harder to make deterministic without knowing specific station data gaps
        // or using mocking. We'll try requesting data for a likely available station
        // but with a RequiredData filter that *might* not be met by the *closest* station,
        // potentially forcing it to try others and maybe fail.

        let client = Meteostat::new().await?;
        let location = berlin_location(); // Berlin

        // Request hourly data, but require it covers a specific *future* year
        // This data *won't* exist yet.
        let future_req = RequiredData::FullYear(2100);

        let result = client
            .hourly()
            .location(location)
            // Add the requirement for non-existent data
            .required_data(future_req)
            // Try only the very closest station(s)
            .station_limit(1) // limit candidates checked by data_from_location
            .call()
            .await;

        // We expect this to fail because although stations are nearby,
        // none will satisfy the RequiredData filter for year 2100.
        // The specific error could be NoStationWithinRadius (if the filter eliminates all candidates)
        // OR NoDataFoundForNearbyStations (if candidates exist but data fetch fails for other reasons
        // after filtering, though this is less likely path for *this* specific setup).
        // The most likely outcome of the `station_locator.query` with an impossible date requirement
        // is that it returns *no* stations matching the criteria, leading to NoStationWithinRadius.

        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Expected error for future data requirement: {:?}", err);

        match err {
            // This is the most likely error because the query itself finds no stations meeting the criteria
            MeteostatError::NoStationWithinRadius { .. } => {
                // Test passed - expected this error type
                println!("Got expected NoStationWithinRadius error due to impossible filter.")
            }
            // This might occur if the station query *did* return stations (e.g., if filtering logic changes)
            // but the subsequent data fetch failed.
            MeteostatError::NoDataFoundForNearbyStations { .. } => {
                // Test passed - also an acceptable error type in this scenario
                println!("Got NoDataFoundForNearbyStations error - filter might have passed but fetch failed.")
            }
            _ => panic!(
                "Expected NoStationWithinRadius or NoDataFoundForNearbyStations error, got {:?}",
                err
            ),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_data_from_invalid_station_id() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let invalid_station_id = "INVALID_STATION_ID_123"; // Make it less likely to exist by chance

        let result = client
            .hourly() // Any frequency
            .station(invalid_station_id)
            .await; // Directly call on the client returned by .station()

        // This should ideally result in an error related to fetching/finding data for that ID.
        // The exact error might depend on FrameFetcher's implementation (e.g., file not found, download error).
        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Error fetching data for invalid station ID: {:?}", err);

        // The error should originate from the data fetching layer
        assert!(
            matches!(err, MeteostatError::WeatherData(_)),
            "Expected a WeatherData error variant, got {:?}",
            err
        );

        Ok(())
    }
}
