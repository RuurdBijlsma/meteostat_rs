use crate::error::MeteostatError;
use crate::stations::locate_station::StationLocator;
use crate::types::data_source::Frequency;
use crate::utils::get_cache_dir;
use crate::weather_data::frame_fetcher::FrameFetcher;
use bon::bon;
use polars::prelude::LazyFrame;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLon {
    pub lat: f64,
    pub lon: f64,
}

pub struct Meteostat {
    fetcher: FrameFetcher,
    station_locator: StationLocator,
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
        stations_to_try: Option<usize>,
    ) -> Result<LazyFrame, MeteostatError> {
        let max_distance_km = max_distance_km.unwrap_or(50.0);
        // How many nearby stations to attempt (e.g., 5)
        let stations_limit = stations_to_try.unwrap_or(5);

        // Query for multiple stations
        let stations =
            self.station_locator
                .query(location.lat, location.lon, stations_limit, max_distance_km);

        // Handle case where no stations are found within the radius
        if stations.is_empty() {
            return Err(MeteostatError::NoStationWithinRadius {
                radius: max_distance_km,
                lat: location.lat,
                lon: location.lon,
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
            lat: location.lat,
            lon: location.lon,
            stations_tried: stations.len(),
            last_error: last_error.map(Box::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::MeteostatError;
    use crate::meteostat::{LatLon, Meteostat};
    use crate::types::data_source::Frequency;

    #[tokio::test]
    async fn test_get_hourly() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let data = meteostat
            .from_station()
            .station("10637")
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let hourly_frame = data.collect().unwrap();

        let shape = hourly_frame.shape();
        assert!(shape.0 >= 719_681);
        assert_eq!(shape.1, 13);

        let columns = hourly_frame.get_column_names();
        assert_eq!(
            columns,
            [
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco",
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

        let daily_frame = data.collect().unwrap();

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

        let monthly_frame = data.collect().unwrap();
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

        let climate_frame = data.collect().unwrap();
        dbg!(&climate_frame);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_hourly_location() -> Result<(), MeteostatError> {
        let meteostat = Meteostat::new().await?;

        let hourly_data = meteostat
            .from_location()
            .location(LatLon {
                lat: 52.520008,
                lon: 13.404954,
            })
            .frequency(Frequency::Hourly)
            .call()
            .await?;

        let frame = hourly_data.collect().unwrap();

        let shape = frame.shape();
        assert_eq!(shape.1, 13);

        let columns = frame.get_column_names();
        assert_eq!(
            columns,
            [
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco",
            ]
        );

        Ok(())
    }
}
