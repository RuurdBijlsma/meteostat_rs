use crate::{Frequency, LatLon, Meteostat, MeteostatError, MonthlyLazyFrame, RequiredData};
use bon::bon;

pub struct MonthlyClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> MonthlyClient<'a> {
    pub fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    pub async fn station(&self, station: &str) -> Result<MonthlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .frequency(Frequency::Monthly)
            .call()
            .await?;
        Ok(MonthlyLazyFrame::new(frame))
    }

    #[builder(start_fn = location)]
    #[doc(hidden)]
    pub async fn build_location(
        &self,
        #[builder(start_fn)] coordinate: LatLon,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<MonthlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_location()
            .location(coordinate)
            .maybe_max_distance_km(max_distance_km)
            .maybe_station_limit(station_limit)
            .maybe_required_data(required_data)
            .frequency(Frequency::Monthly)
            .call()
            .await?;
        Ok(MonthlyLazyFrame::new(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
    }

    // MONTHLY
    #[tokio::test]
    async fn test_monthly_from_station() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .monthly()
            .station("06240") // Schiphol
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
}
