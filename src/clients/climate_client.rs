//! Provides the `ClimateClient` for initiating requests for Meteostat climate normals data.
//!
//! This client acts as an intermediate builder, obtained via [`Meteostat::climate()`],
//! allowing the user to specify the data source (station ID or location) before
//! executing the request to fetch climate data.

use crate::{ClimateLazyFrame, Frequency, LatLon, Meteostat, MeteostatError, RequiredData};
use bon::bon;

/// A client builder specifically for fetching climate normals data.
///
/// Instances are created by calling [`Meteostat::climate()`]. Methods on this struct
/// allow specifying the target (a specific station ID or a geographical location)
/// and optional parameters for location-based searches.
///
/// Calling `.station()` or `.location().call()` executes the request and returns a
/// [`Result<ClimateLazyFrame, MeteostatError>`].
pub struct ClimateClient<'a> {
    /// A reference to the main Meteostat client instance.
    client: &'a Meteostat,
}

#[bon]
impl<'a> ClimateClient<'a> {
    /// Creates a new `ClimateClient`.
    ///
    /// This is typically called internally by [`Meteostat::climate()`] and not directly by users.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to the configured `Meteostat` instance.
    pub(crate) fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    /// Initiates a builder to fetch climate normals data for a specific weather station ID.
    ///
    /// This method sets the target station ID for the request.
    /// You can optionally specify `.required_data(RequiredData)` to apply an inventory filter
    /// (though less common for direct station ID climate lookups).
    ///
    /// Finally, call `.call().await` on the resulting builder object to execute the
    /// data fetch.
    ///
    /// # Arguments (Initial Builder Method)
    ///
    /// * `station` - The unique identifier string of the weather station (e.g., "10382")
    ///   passed to the initial `.station()` call.
    ///
    /// # Optional Builder Methods
    ///
    /// * `.required_data(RequiredData)`: Filters the request based on the station's
    ///   advertised data inventory. If the station doesn't meet the criteria according
    ///   to the inventory (even if the data *might* exist), the fetch might fail early
    ///   or return an error depending on the internal implementation. Defaults to `None`
    ///   (no specific inventory requirement beyond needing climate data).
    ///
    /// # Returns
    ///
    /// After calling `.call().await`, returns a `Result` containing a [`ClimateLazyFrame`]
    /// on success, allowing further processing or collection of the data. Returns a
    /// [`MeteostatError`] if the data cannot be fetched (e.g., network error,
    /// station data file not found, parsing error, or unmet `required_data` filter).
    ///
    /// # Errors
    ///
    /// Can return [`MeteostatError::WeatherData`] if fetching or parsing the underlying
    /// data file fails, potentially influenced by the `required_data` filter if set.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, RequiredData};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "10382"; // Berlin-Tegel
    ///
    /// // Fetch climate normals for the specified station
    /// let climate_lazy = client
    ///     .climate()
    ///     .station(station_id) // Required: Start builder with station ID
    ///     // .required_data(RequiredData::Any) // Optional: Add inventory filter if needed
    ///     .call()              // Required: Execute the fetch
    ///     .await?;             // -> Result<ClimateLazyFrame, MeteostatError>
    ///
    /// // Collect the data into a DataFrame
    /// let climate_df = climate_lazy.frame.collect()?;
    /// println!("Climate normals for station {}:\n{}", station_id, climate_df);
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = station)]
    #[doc(hidden)]
    pub async fn build_station(
        &self,
        #[builder(start_fn)] station: &str,
        required_data: Option<RequiredData>,
    ) -> Result<ClimateLazyFrame, MeteostatError> {
        // Internal call to the main client's data fetching logic for a specific station
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .maybe_required_data(required_data)
            .frequency(Frequency::Climate) // Specify we want climate data
            .call() // Execute the internal builder
            .await?;
        // Wrap the resulting LazyFrame in the specific ClimateLazyFrame type
        Ok(ClimateLazyFrame::new(frame))
    }

    /// Initiates a request to fetch climate normals data for the nearest suitable station to a given location.
    ///
    /// This method starts a builder pattern. You must provide the location.
    /// You can optionally specify:
    /// *   `.max_distance_km(f64)`: Maximum search radius (default: 50.0 km).
    /// *   `.station_limit(usize)`: Max number of candidate stations to *consider* (default: 1). Note: It will still only return data for the *first* successful one found.
    /// *   `.required_data(RequiredData)`: Filter candidate stations based on their reported data inventory (e.g., `RequiredData::Any`). By default, no inventory filter is applied specifically for climate data location searches beyond the implicit check during data fetching.
    ///
    /// Finally, call `.call().await` on the builder to execute the search and data fetch.
    ///
    /// # Arguments (Initial Builder Method)
    ///
    /// * `coordinate` - The [`LatLon`] representing the geographical point of interest.
    ///
    /// # Returns
    ///
    /// After calling `.call().await`, returns a `Result` containing a [`ClimateLazyFrame`]
    /// for the nearest suitable station found, or a [`MeteostatError`] if no suitable station
    /// is found within the radius or if data fetching fails for all candidates.
    ///
    /// # Errors
    ///
    /// Can return:
    /// *   [`MeteostatError::NoStationWithinRadius`]: If the initial station search finds no candidates matching the criteria.
    /// *   [`MeteostatError::NoDataFoundForNearbyStations`]: If candidate stations were found, but fetching climate data failed for all attempted stations.
    /// *   [`MeteostatError::LocateStation`]: If the underlying station search mechanism fails.
    /// *   [`MeteostatError::WeatherData`]: Encapsulated within `NoDataFoundForNearbyStations` if fetching fails for a candidate.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use meteostat::{Meteostat, MeteostatError, LatLon, RequiredData};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let berlin_center = LatLon(52.52, 13.40);
    ///
    /// // Fetch climate data for the location, searching up to 100km
    /// // and considering up to 3 candidate stations if the first fails.
    /// let climate_lazy = client
    ///     .climate()
    ///     .location(berlin_center) // Required: Start builder with location
    ///     .max_distance_km(100.0)   // Optional: Set search radius
    ///     .station_limit(3)         // Optional: Consider up to 3 stations
    ///     // .required_data(RequiredData::Any) // Optional: Add inventory filter if needed
    ///     .call()                   // Required: Execute the search & fetch
    ///     .await?;                  // -> Result<ClimateLazyFrame, MeteostatError>
    ///
    /// let climate_df = climate_lazy.frame.collect()?;
    /// println!("Climate data near {:?}:\n{}", berlin_center, climate_df);
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = location)] // Define 'location' as the entry point for the builder
    #[doc(hidden)] // Hide the internal implementation detail `build_location` from docs
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
        let data = client
            .climate()
            .station("10382")
            .call()
            .await?
            .frame
            .collect()?;
        assert!(!data.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_climate_from_station_with_filter() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        // Using a station known to exist and likely have climate data
        let data = client
            .climate()
            .station("10382") // Berlin-Tegel
            .required_data(RequiredData::FullYear(2023))
            .call()
            .await?
            .frame
            .collect()?;
        assert!(
            !data.is_empty(),
            "Expected climate data even with RequiredData filter"
        );
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
