//! Provides the `MonthlyClient` for initiating requests for Meteostat monthly weather data.
//!
//! This client acts as an intermediate builder, obtained via [`Meteostat::monthly()`],
//! allowing the user to specify the data source (station ID or location) before
//! executing the request to fetch monthly aggregated data.

use crate::{Frequency, LatLon, Meteostat, MeteostatError, MonthlyLazyFrame, RequiredData};
use bon::bon;

/// A client builder specifically for fetching monthly weather data.
///
/// Instances are created by calling [`Meteostat::monthly()`]. Methods on this struct
/// allow specifying the target (a specific station ID or a geographical location)
/// and optional parameters for location-based searches.
///
/// Calling `.station()` or `.location().call()` executes the request and returns a
/// [`Result<MonthlyLazyFrame, MeteostatError>`].
pub struct MonthlyClient<'a> {
    client: &'a Meteostat,
}

#[bon]
impl<'a> MonthlyClient<'a> {
    /// Creates a new `MonthlyClient`.
    ///
    /// This is typically called internally by [`Meteostat::monthly()`] and not directly by users.
    ///
    /// # Arguments
    ///
    /// * `client` - A reference to the configured `Meteostat` instance.
    pub(crate) fn new(client: &'a Meteostat) -> Self {
        Self { client }
    }

    /// Initiates a builder to fetch monthly weather data for a specific weather station ID.
    ///
    /// This method sets the target station ID for the request.
    /// You can optionally specify `.required_data(RequiredData)` to apply an inventory filter,
    /// ensuring the station is suitable based on its reported data availability (e.g.,
    /// requiring data for a specific year using `RequiredData::FullYear(2023)`).
    ///
    /// Finally, call `.call().await` on the resulting builder object to execute the
    /// data fetch.
    ///
    /// # Arguments (Initial Builder Method)
    ///
    /// * `station` - The unique identifier string of the weather station (e.g., "06240")
    ///   passed to the initial `.station()` call.
    ///
    /// # Optional Builder Methods
    ///
    /// * `.required_data(RequiredData)`: Filters the request based on the station's
    ///   advertised data inventory. This is useful for ensuring the station is likely
    ///   to have the monthly data you need (e.g., for a specific year) before attempting
    ///   the download. For example, `RequiredData::FullYear(2023)` would check
    ///   if the station inventory indicates monthly data for the full year 2023. If the filter
    ///   isn't met, the fetch might fail early or return an error. Defaults to `None`
    ///   (no inventory pre-filtering).
    ///
    /// # Returns
    ///
    /// After calling `.call().await`, returns a `Result` containing a [`MonthlyLazyFrame`]
    /// on success. This lazy frame holds all available monthly data for the station, which
    /// can then be further filtered (e.g., by year/month range) or collected. Returns a
    /// [`MeteostatError`] if the data cannot be fetched or if the `required_data`
    /// filter is not met according to the station inventory.
    ///
    /// # Errors
    ///
    /// Can return:
    /// *   [`MeteostatError::WeatherData`]: If fetching or parsing the underlying data file fails
    ///     (e.g., network error, file not found, CSV parse error).
    /// *   Could also potentially return an error related to unmet `required_data` criteria
    ///     if the inventory check fails before attempting the fetch (depends on internal logic).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use meteostat::{Meteostat, MeteostatError, Month, Year, RequiredData};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), MeteostatError> {
    /// let client = Meteostat::new().await?;
    /// let station_id = "06240"; // Amsterdam Schiphol
    ///
    /// // Fetch monthly data for the specified station, requiring inventory for 2023
    /// let monthly_lazy = client
    ///     .monthly()
    ///     .station(station_id)                // Required: Start builder with station ID
    ///     .required_data(RequiredData::FullYear(2023)) // Optional: Filter by inventory
    ///     .call()                             // Required: Execute the fetch
    ///     .await?;                            // -> Result<MonthlyLazyFrame, MeteostatError>
    ///
    /// // Filter the result for a specific month (e.g., July 2023) and collect
    /// let july_2023_df = monthly_lazy.get_at(Month::new(7, 2023))?.frame.collect()?;
    /// println!("Monthly data for station {} in July 2023:\n{}", station_id, july_2023_df);
    /// # Ok(())
    /// # }
    /// ```
    #[builder(start_fn = station)]
    #[doc(hidden)]
    pub async fn build_station(
        &self,
        #[builder(start_fn)] station: &str,
        required_data: Option<RequiredData>,
    ) -> Result<MonthlyLazyFrame, MeteostatError> {
        let frame = self
            .client
            .data_from_station()
            .station(station)
            .maybe_required_data(required_data)
            .frequency(Frequency::Monthly)
            .call()
            .await?;
        Ok(MonthlyLazyFrame::new(frame))
    }

    /// Initiates a request to fetch monthly weather data for the nearest suitable station to a given location.
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
    /// After calling `.call().await`, returns a `Result` containing a [`MonthlyLazyFrame`]
    /// for the nearest suitable station found, or a [`MeteostatError`] if no suitable station
    /// is found within the radius or if data fetching fails for all candidates.
    ///
    /// # Errors
    ///
    /// Can return:
    /// *   [`MeteostatError::NoStationWithinRadius`]: If the initial station search finds no candidates matching the criteria.
    /// *   [`MeteostatError::NoDataFoundForNearbyStations`]: If candidate stations were found, but fetching monthly data failed for all attempted stations.
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
    /// // Fetch monthly data near Berlin, checking stations have data for 2022
    /// let monthly_lazy = client
    ///     .monthly()
    ///     .location(berlin_center) // Required: Start builder with location
    ///     .required_data(RequiredData::FullYear(2022)) // Optional: Filter candidates
    ///     .call()                  // Required: Execute the search & fetch
    ///     .await?;                 // -> Result<MonthlyLazyFrame, MeteostatError>
    ///
    /// // Collect the full monthly data frame for the found station
    /// let monthly_df = monthly_lazy.frame.collect()?;
    ///
    /// println!("Monthly data near {:?} (station inventory checked for 2022):\n{}", berlin_center, monthly_df.head(Some(5)));
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
    use crate::{RequiredData, Year}; // Import for tests

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
            .call()
            .await?
            // Monthly data often doesn't have per-period filters in the same way,
            // just collect the whole frame for the test.
            .frame
            .collect()?;
        assert!(data.height() > 0, "Expected some monthly data");
        Ok(())
    }

    #[tokio::test]
    async fn test_monthly_from_station_with_filter() -> Result<(), MeteostatError> {
        let client = Meteostat::new().await?;
        let data = client
            .monthly()
            .station("10382") // Schiphol
            // Require inventory to show data for 2023
            .required_data(RequiredData::FullYear(2023))
            .call()
            .await?
            .get_for_period(Year(2023))?
            .frame
            .collect()?;
        // If the filter passes based on inventory, we should get data.
        // The number of rows might vary, but should be > 0.
        // Checking for exactly 12 might be too strict if inventory/data isn't perfect.
        assert!(
            data.height() > 0,
            "Expected > 0 rows of monthly data for 2023 after inventory filter"
        );
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
        Ok(())
    }
}
