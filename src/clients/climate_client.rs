use crate::types::frequencies::climate_frame::ClimateLazyFrame;
use crate::{Frequency, LatLon, Meteostat, MeteostatError, RequiredData};
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
        #[builder(start_fn)]
        coordinate: LatLon,
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
