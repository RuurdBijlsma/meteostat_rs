use crate::{DailyLazyFrame, Frequency, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

pub struct DailyClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> DailyClient<'a> {
    pub fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    pub async fn station(&self, station: &str) -> Result<DailyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .frequency(Frequency::Daily)
            .call()
            .await?;
        Ok(DailyLazyFrame::new(frame))
    }

    #[builder(start_fn = location)]
    #[doc(hidden)]
    pub async fn build_location(
        &self,
        #[builder(start_fn)] coordinate: LatLon,
        max_distance_km: Option<f64>,
        station_limit: Option<usize>,
        required_data: Option<RequiredData>,
    ) -> Result<DailyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_location()
            .location(coordinate)
            .maybe_max_distance_km(max_distance_km)
            .maybe_station_limit(station_limit)
            .maybe_required_data(required_data)
            .frequency(Frequency::Daily)
            .call()
            .await?;
        Ok(DailyLazyFrame::new(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Year;
    use chrono::NaiveDate;

    // Helper to create a known location (Berlin Mitte)
    fn berlin_location() -> LatLon {
        LatLon(52.520008, 13.404954)
    }

    // DAILY
    #[tokio::test]
    async fn test_daily_from_station_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .station("06240") // Schiphol
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
            .station("06240") // Schiphol
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
}
