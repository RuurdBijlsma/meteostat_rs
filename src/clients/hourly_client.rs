//! Provides the `HourlyClient` for initiating requests for Meteostat hourly weather data.
//!
//! This client acts as an intermediate builder, obtained via [`Meteostat::hourly()`],
//! allowing the user to specify the data source (station ID or location) before
//! executing the request to fetch hour-by-hour weather observations.

use crate::{Frequency, HourlyLazyFrame, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

/// A client builder specifically for fetching hourly weather data.
///
/// Instances are created by calling [`Meteostat::hourly()`]. Methods on this struct
/// allow specifying the target (a specific station ID or a geographical location)
/// and optional parameters for location-based searches.
///
/// Calling `.station()` or `.location().call()` executes the request and returns a
/// [`Result<HourlyLazyFrame, MeteostatError>`].
pub struct HourlyClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> HourlyClient<'a> {
    /// Creates a new `HourlyClient`.
    ///
    /// This is typically called internally by [`Meteostat::hourly()`] and not directly by users.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to the configured `Meteostat` instance.
    pub(crate) fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    /// Fetches hourly weather data for a specific weather station ID.
    ///
    /// # Arguments
    ///
    /// * `station` - The unique identifier string of the weather station (e.g., "06240").
    ///
    /// # Returns
    ///
    /// A `Result` containing an [`HourlyLazyFrame`] on success, allowing further
    /// processing or collection of the data (e.g., filtering by datetime range). Returns a
    /// [`MeteostatError`] if the data cannot be fetched.
    ///
    /// # Errors
    ///
    /// Can return [`MeteostatError::WeatherData`] if fetching or parsing the underlying
    /// data file fails (e.g., network error, file not found, CSV parse error).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError};
    /// use chrono::{Utc, TimeZone};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "06240"; // Amsterdam Schiphol
    ///
    /// // Fetch hourly data for the specified station
    /// let hourly_lazy = client.hourly().station(station_id).call().await?;
    ///
    /// // Filter for a specific time range and collect
    /// let start_dt = Utc.with_ymd_and_hms(2023, 1, 1, 6, 0, 0).unwrap();
    /// let end_dt = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
    /// let morning_df = hourly_lazy.get_range(start_dt, end_dt)?.frame.collect()?;
    ///
    /// println!("Hourly data for station {} on morning of 2023-01-01:\n{}", station_id, morning_df);
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = station)]
    #[doc(hidden)]
    pub async fn build_station(
        &self,
        #[builder(start_fn)] station: &str,
        required_data: Option<RequiredData>,
    ) -> Result<HourlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .maybe_required_data(required_data)
            .frequency(Frequency::Hourly)
            .call()
            .await?;
        Ok(HourlyLazyFrame::new(frame))
    }

    /// Initiates a request to fetch hourly weather data for the nearest suitable station to a given location.
    ///
    /// This method starts a builder pattern. You must provide the location.
    /// You can optionally specify:
    /// *   `.max_distance_km(f64)`: Maximum search radius (default: 50.0 km).
    /// *   `.station_limit(usize)`: Max number of candidate stations to *consider* (default: 1). It will return data for the *first* successful one found.
    /// *   `.required_data(RequiredData)`: Filter candidate stations based on their reported data inventory (e.g., `RequiredData::FullYear(2023)`).
    ///
    /// Finally, call `.call().await` on the builder to execute the search and data fetch.
    ///
    /// # Arguments (Initial Builder Method)
    ///
    /// * `coordinate` - The [`LatLon`] representing the geographical point of interest.
    ///
    /// # Returns
    ///
    /// After calling `.call().await`, returns a `Result` containing an [`HourlyLazyFrame`]
    /// for the nearest suitable station found, or a [`MeteostatError`] if no suitable station
    /// is found within the radius or if data fetching fails for all candidates.
    ///
    /// # Errors
    ///
    /// Can return:
    /// *   [`MeteostatError::NoStationWithinRadius`]: If the initial station search finds no candidates matching the criteria.
    /// *   [`MeteostatError::NoDataFoundForNearbyStations`]: If candidate stations were found, but fetching hourly data failed for all attempted stations.
    /// *   [`MeteostatError::LocateStation`]: If the underlying station search mechanism fails.
    /// *   [`MeteostatError::WeatherData`]: Encapsulated within `NoDataFoundForNearbyStations` if fetching fails for a candidate.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError, LatLon, RequiredData, Frequency};
    /// use chrono::{Utc, TimeZone};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let berlin_center = LatLon(52.52, 13.40);
    ///
    /// // Fetch hourly data near Berlin, checking stations have data for 2023
    /// let hourly_lazy = client
    ///     .hourly()
    ///     .location(berlin_center) // Required: Start builder with location
    ///     .required_data(RequiredData::FullYear(2023)) // Optional: Filter candidates
    ///     .call()                  // Required: Execute the search & fetch
    ///     .await?;                 // -> Result<HourlyLazyFrame, MeteostatError>
    ///
    /// // Filter the resulting frame for a specific time range and collect
    /// let start_dt = Utc.with_ymd_and_hms(2023, 8, 15, 0, 0, 0).unwrap();
    /// let end_dt = Utc.with_ymd_and_hms(2023, 8, 15, 23, 59, 59).unwrap();
    /// let day_df = hourly_lazy.get_range(start_dt, end_dt)?.frame.collect()?;
    ///
    /// println!("Hourly data near {:?} for 2023-08-15:\n{}", berlin_center, day_df.head(Some(6)));
    /// # Ok(())
    /// # }
    /// ```
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
            .call()
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some hourly data for 2023");
        Ok(())
    }

    #[tokio::test]
    async fn test_hourly_from_station_at_specific_datetime() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .hourly()
            .station("06240") // Schiphol
            .call()
            .await?
            // Use a type that implements AnyDateTime, like chrono::DateTime<Utc>
            .get_at(chrono::DateTime::parse_from_rfc3339("2023-07-15T12:00:00Z").unwrap())?
            .frame
            .collect()?;
        // get_at should return 0 or 1 row for hourly data
        assert!(data.height() <= 1, "Expected 0 or 1 row for specific hour");
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
        Ok(())
    }
}
