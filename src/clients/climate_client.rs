use crate::{ClimateLazyFrame, Frequency, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

pub struct ClimateClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> ClimateClient<'a> {
    pub fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    pub async fn station(&self, station: &str) -> Result<ClimateLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .frequency(Frequency::Climate)
            .call()
            .await?;
        Ok(ClimateLazyFrame::new(frame))
    }

    #[builder(start_fn = location)]
    #[doc(hidden)]
    pub async fn build_location(
        &self,
        #[builder(start_fn)] coordinate: LatLon,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<ClimateLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_location()
            .location(coordinate)
            .maybe_max_distance_km(max_distance_km)
            .maybe_station_limit(station_limit)
            .maybe_required_data(required_data)
            .frequency(Frequency::Climate)
            .call()
            .await?;
        Ok(ClimateLazyFrame::new(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
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
}
