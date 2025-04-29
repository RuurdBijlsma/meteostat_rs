use crate::stations::locate_station::StationLocator;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use crate::weather_data::frame_fetcher::FrameFetcher;
use crate::{
    ClimateClient, DailyClient, Frequency, HourlyClient, MeteostatError, MonthlyClient,
    RequiredData, Station,
};
use bon::bon;
use polars::prelude::LazyFrame;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon(pub f64, pub f64);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InventoryRequest {
    frequency: Frequency,
    required_data: RequiredData,
}

impl InventoryRequest {
    pub fn new(frequency: Frequency, required_data: RequiredData) -> Self {
        Self {
            frequency,
            required_data,
        }
    }
}

pub struct Meteostat {
    fetcher: FrameFetcher,
    station_locator: StationLocator,
}

#[bon]
impl Meteostat {
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

    pub async fn new() -> Result<Self, MeteostatError> {
        let cache_folder = get_cache_dir().map_err(MeteostatError::CacheDirResolution)?;
        Self::with_cache_folder(cache_folder).await
    }

    /// Prepares a request for hourly data.
    pub fn hourly(&self) -> HourlyClient<'_> {
        HourlyClient::new(self)
    }

    /// Prepares a request for daily data.
    pub fn daily(&self) -> DailyClient<'_> {
        DailyClient::new(self)
    }

    /// Prepares a request for monthly data.
    pub fn monthly(&self) -> MonthlyClient<'_> {
        MonthlyClient::new(self)
    }

    /// Prepares a request for climate data.
    pub fn climate(&self) -> ClimateClient<'_> {
        ClimateClient::new(self)
    }

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

    #[builder]
    pub(crate) async fn data_from_station(
        &self,
        station: &str,
        frequency: Frequency,
    ) -> Result<LazyFrame, MeteostatError> {
        self.fetcher
            .get_cache_lazyframe(station, frequency)
            .await
            .map_err(MeteostatError::from)
    }
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
    use super::*;
    use crate::{Month, Year};
    use chrono::NaiveDate;
    use tempfile::tempdir;

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
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

    // --- Data Fetching Tests (via Clients) ---
    // These also implicitly test data_from_station and data_from_location

    // HOURLY (Existing tests are good, maybe add one for specific date)
    #[tokio::test]
    async fn test_hourly_from_station_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .hourly()
            .station("06240") // Station near Paris
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some hourly data for 2023");
        // dbg!(&data.head(Some(5))); // Optional: print head
        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_from_station_at_specific_datetime() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .hourly()
            .station("06240") // Station near Paris
            .await?
            // Use a type that implements AnyDateTime, like chrono::DateTime<Utc>
            .get_at(chrono::DateTime::parse_from_rfc3339("2023-07-15T12:00:00Z").unwrap())?
            .frame
            .collect()?;
        // get_at should return 0 or 1 row for hourly data
        assert!(data.height() <= 1, "Expected 0 or 1 row for specific hour");
        // dbg!(&data);
        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_from_location_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .hourly()
            .location(berlin_location())
            // .max_distance_km(50.0) // Default is 50km anyway
            .call() // Call finishes the builder
            .await?
            .get_for_period(Month(2023, 7))? // Get July 2023
            .frame
            .collect()?;
        assert!(
            data.height() > 0,
            "Expected some hourly data for Berlin area in July 2023"
        );
        // dbg!(&data.head(Some(5)));
        Ok(())
    }

    // DAILY
    #[tokio::test]
    async fn test_daily_from_station_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .station("06240") // Station near Paris
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some daily data for 2023");
        // dbg!(&data.head(Some(5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_daily_from_station_at_specific_date() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .station("06240") // Station near Paris
            .await?
            .get_at(NaiveDate::from_ymd_opt(2023, 7, 15).unwrap())? // Use Day, Month, Year or NaiveDate
            .frame
            .collect()?;
        assert!(data.height() <= 1, "Expected 0 or 1 row for specific day");
        // dbg!(&data);
        Ok(())
    }

    #[tokio::test]
    async fn test_daily_from_location_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .location(berlin_location())
            .call() // Call finishes the builder
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        assert!(
            data.height() > 0,
            "Expected some daily data for Berlin area in 2023"
        );
        // dbg!(&data.head(Some(5)));
        Ok(())
    }

    // MONTHLY
    #[tokio::test]
    async fn test_monthly_from_station() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .monthly()
            .station("06240") // Station near Paris
            .await?
            // Monthly data often doesn't have per-period filters in the same way,
            // just collect the whole frame for the test.
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some monthly data");
        // dbg!(&data.head(Some(5)));
        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_from_location() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .monthly()
            .location(berlin_location())
            .call() // Call finishes the builder
            .await?
            .frame
            .collect()?;
        assert!(
            data.height() > 0,
            "Expected some monthly data for Berlin area"
        );
        // dbg!(&data.head(Some(5)));
        Ok(())
    }

    // CLIMATE
    #[tokio::test]
    async fn test_climate_from_station() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        // Climate normals station (e.g., Berlin-Tegel if available)
        // Using 10382 as an example which often has normals
        let data = client.climate().station("10382").await?.frame.collect()?;
        assert!(!data.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_climate_from_location() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .climate()
            .location(berlin_location())
            .call() 
            .await?
            .frame
            .collect()?;
        assert!(!data.is_empty(), "Expected climate normals for Berlin area");
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
            .station_limit(1)
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
        let invalid_station_id = "INVALID";

        let result = client
            .hourly() // Any frequency
            .station(invalid_station_id)
            .await;

        // This should ideally result in an error related to fetching/finding data for that ID.
        // The exact error might depend on FrameFetcher's implementation (e.g., file not found, download error).
        assert!(result.is_err());
        let err = result.err().unwrap();
        println!("Error fetching data for invalid station ID: {:?}", err);
        // Example check (might need adjustment based on actual error type)
        assert!(matches!(err, MeteostatError::WeatherData(_)));

        Ok(())
    }
}
