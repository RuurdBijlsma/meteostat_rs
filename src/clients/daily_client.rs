//! Provides the `DailyClient` for initiating requests for Meteostat daily weather data.
//!
//! This client acts as an intermediate builder, obtained via [`Meteostat::daily()`],
//! allowing the user to specify the data source (station ID or location) before
//! executing the request to fetch daily aggregated data.

use crate::{DailyLazyFrame, Frequency, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

/// A client builder specifically for fetching daily weather data.
///
/// Instances are created by calling [`Meteostat::daily()`]. Methods on this struct
/// allow specifying the target (a specific station ID or a geographical location)
/// and optional parameters for location-based searches.
///
/// Calling `.station()` or `.location().call()` executes the request and returns a
/// [`Result<DailyLazyFrame, MeteostatError>`].
pub struct DailyClient<'a> {
    /// A reference to the main Meteostat client instance.
    client: &'a Meteostat,
}

#[bon] // Enables the builder pattern, especially for `location()`
impl<'a> DailyClient<'a> {
    /// Creates a new `DailyClient`.
    ///
    /// This is typically called internally by [`Meteostat::daily()`] and not directly by users.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to the configured `Meteostat` instance.
    pub(crate) fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    /// Fetches daily weather data for a specific weather station ID.
    ///
    /// # Arguments
    ///
    /// * `station` - The unique identifier string of the weather station (e.g., "06240").
    ///
    /// # Returns
    ///
    /// A `Result` containing a [`DailyLazyFrame`] on success, allowing further
    /// processing or collection of the data (e.g., filtering by date). Returns a
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
    /// # use meteostat::{Meteostat, MeteostatError, Year};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "06240"; // Amsterdam Schiphol
    ///
    /// // Fetch daily data for the specified station
    /// let daily_lazy = client.daily().station(station_id).call().await?;
    ///
    /// // Filter for a specific year and collect
    /// let daily_2023_df = daily_lazy.get_for_period(Year(2023))?.frame.collect()?;
    /// println!("Daily data for station {} in 2023:\n{}", station_id, daily_2023_df.head(Some(5)));
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = station)] // Define 'location' as the entry point for the builder
    #[doc(hidden)] // Hide the internal implementation detail `build_location` from docs
    pub async fn build_station(
        &self,
        #[builder(start_fn)] station: &str,
        required_data: Option<RequiredData>,
    ) -> Result<DailyLazyFrame, MeteostatError> {
        // Internal call to the main client's data fetching logic for a specific station
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .maybe_required_data(required_data)
            .frequency(Frequency::Daily)
            .call()
            .await?;
        // Wrap the resulting LazyFrame in the specific DailyLazyFrame type
        Ok(DailyLazyFrame::new(frame))
    }

    /// Initiates a request to fetch daily weather data for the nearest suitable station to a given location.
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
    /// After calling `.call().await`, returns a `Result` containing a [`DailyLazyFrame`]
    /// for the nearest suitable station found, or a [`MeteostatError`] if no suitable station
    /// is found within the radius or if data fetching fails for all candidates.
    ///
    /// # Errors
    ///
    /// Can return:
    /// *   [`MeteostatError::NoStationWithinRadius`]: If the initial station search finds no candidates matching the criteria.
    /// *   [`MeteostatError::NoDataFoundForNearbyStations`]: If candidate stations were found, but fetching daily data failed for all attempted stations.
    /// *   [`MeteostatError::LocateStation`]: If the underlying station search mechanism fails.
    /// *   [`MeteostatError::WeatherData`]: Encapsulated within `NoDataFoundForNearbyStations` if fetching fails for a candidate.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError, LatLon, Year, RequiredData, Frequency};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let berlin_center = LatLon(52.52, 13.40);
    ///
    /// // Fetch daily data near Berlin for 2023, ensuring the station reported *any* daily data.
    /// let daily_lazy = client
    ///     .daily()
    ///     .location(berlin_center)      // Required: Start builder with location
    ///     .required_data(RequiredData::Any) // Optional: Filter candidates
    ///     .call()                       // Required: Execute the search & fetch
    ///     .await?;                      // -> Result<DailyLazyFrame, MeteostatError>
    ///
    /// // Filter the resulting frame for the specific year and collect
    /// let daily_2023_df = daily_lazy.get_for_period(Year(2023))?.frame.collect()?;
    ///
    /// println!("Daily data near {:?} for 2023:\n{}", berlin_center, daily_2023_df.head(Some(5)));
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = location)] // Define 'location' as the entry point for the builder
    #[doc(hidden)] // Hide the internal implementation detail `build_location` from docs
    pub async fn build_location(
        &self,
        #[builder(start_fn)] coordinate: LatLon, // The required LatLon arg for the 'location' start fn
        max_distance_km: Option<f64>,            // Optional builder arg
        station_limit: Option<usize>,            // Optional builder arg
        required_data: Option<RequiredData>,     // Optional builder arg
    ) -> Result<DailyLazyFrame, MeteostatError> {
        // Internal call to the main client's data fetching logic for a location
        let frame = self
            .client
            .data_from_location()
            .location(coordinate) // Pass the location
            .maybe_max_distance_km(max_distance_km) // Pass optional distance
            .maybe_station_limit(station_limit) // Pass optional station limit
            .maybe_required_data(required_data) // Pass optional inventory requirement
            .frequency(Frequency::Daily) // Specify we want daily data
            .call() // Execute the internal builder
            .await?;
        // Wrap the resulting LazyFrame
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

    #[tokio::test]
    async fn test_daily_from_station_for_period() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .station("06240") // Schiphol
            .call()
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some daily data for 2023");
        Ok(())
    }

    #[tokio::test]
    async fn test_daily_from_station_at_specific_date() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .daily()
            .station("06240") // Schiphol
            .call()
            .await?
            .get_at(NaiveDate::from_ymd_opt(2023, 7, 15).unwrap())? // Use Day, Month, Year or NaiveDate
            .frame
            .collect()?;
        assert!(data.height() <= 1, "Expected 0 or 1 row for specific day");
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
        Ok(())
    }
}
