use crate::error::MeteostatError;
use crate::stations::locate_station::StationLocator;
use crate::types::data_source::{Frequency, RequiredData};
use crate::types::station::Station;
use crate::utils::get_cache_dir;
use crate::weather_data::frame_fetcher::FrameFetcher;
use bon::bon;
use polars::prelude::LazyFrame;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon(pub f64, pub f64);

pub struct Meteostat {
    fetcher: FrameFetcher,
    station_locator: StationLocator,
}

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

#[bon]
impl Meteostat {
    pub async fn with_cache_folder(cache_folder: PathBuf) -> Result<Self, MeteostatError> {
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

    #[builder]
    pub async fn find_stations(
        &self,
        location: LatLon,
        inventory_request: Option<InventoryRequest>,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
    ) -> Result<Vec<Station>, MeteostatError> {
        let max_distance_km = max_distance_km.unwrap_or(50.0);
        let stations_limit = station_limit.unwrap_or(5);

        let (freq_option, date_option) = inventory_request
            .map(|req| (req.frequency, req.required_data))
            .unzip();

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
            .map(|(station, _distance)| station)
            .collect())
    }

    #[builder]
    pub async fn from_station(
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
    pub async fn from_location(
        &self,
        location: LatLon,
        frequency: Frequency,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<LazyFrame, MeteostatError> {
        let max_distance_km = max_distance_km.unwrap_or(50.0);
        let stations_limit = station_limit.unwrap_or(1);

        // Query for multiple stations
        let stations = self.station_locator.query(
            location.0,
            location.1,
            stations_limit,
            max_distance_km,
            Some(frequency),
            required_data,
        );

        // Handle case where no stations are found within the radius
        if stations.is_empty() {
            return Err(MeteostatError::NoStationWithinRadius {
                radius: max_distance_km,
                lat: location.0,
                lon: location.1,
            });
        }

        let mut last_error: Option<MeteostatError> = None;

        // Iterate through the found stations (which are sorted by distance)
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
                    let current_error = MeteostatError::from(e);
                    // Store the error and continue to the next station
                    last_error = Some(current_error);
                }
            }
        }

        // Return the specific error indicating failure across all tried stations
        Err(MeteostatError::NoDataFoundForNearbyStations {
            radius: max_distance_km,
            lat: location.0,
            lon: location.1,
            stations_tried: stations.len(),
            last_error: last_error.map(Box::new),
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
