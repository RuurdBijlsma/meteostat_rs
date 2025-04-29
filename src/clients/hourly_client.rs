use crate::{Frequency, HourlyLazyFrame, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

pub struct HourlyClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> HourlyClient<'a> {
    pub fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    pub async fn station(&self, station: &str) -> Result<HourlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .frequency(Frequency::Hourly)
            .call()
            .await?;
        Ok(HourlyLazyFrame::new(frame))
    }

    #[builder(start_fn = location)]
    #[doc(hidden)]
    pub async fn build_location(
        &self,
        #[builder(start_fn)] coordinate: LatLon,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_location()
            .location(coordinate)
            .maybe_max_distance_km(max_distance_km)
            .maybe_station_limit(station_limit)
            .maybe_required_data(required_data)
            .frequency(Frequency::Hourly)
            .call()
            .await?;
        Ok(HourlyLazyFrame::new(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Month, Year};

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
    }

    // HOURLY (Existing tests are good, maybe add one for specific date)
    #[tokio::test]
    async fn test_hourly_from_station_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .hourly()
            .station("06240") // Schiphol
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
            .station("06240") // Schiphol
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
}
