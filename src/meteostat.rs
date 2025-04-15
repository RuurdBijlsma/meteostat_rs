use crate::error::MeteostatError;
use crate::stations::locate_station::StationLocator;
use crate::types::bitflags::hourly::RequiredWeatherField;
use crate::types::data_source::DataSource;
use crate::types::into_utc_trait::IntoUtcDateTime;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use crate::weather_data::error::WeatherDataError;
use crate::weather_data::fetcher::WeatherFetcher;
use std::path::PathBuf;

pub struct Meteostat {
    station_locator: StationLocator,
    weather_fetcher: WeatherFetcher,
    max_distance_km: f64,
    station_check_limit: usize,
}

impl Meteostat {
    /// Retrieves hourly weather information from the *first* nearby station
    /// that provides data for the given time.
    ///
    /// This function queries stations within the configured `max_distance_km`
    /// and `station_check_limit`. It returns the `HourlyWeatherInfo` from the
    /// first station that successfully responds with data for the specified
    /// `datetime`. It does *not* combine data from multiple sources.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `datetime_input`: The target date and time (converted to UTC).
    ///
    /// # Returns
    ///
    /// Returns `Ok(HourlyWeatherInfo)` from the first suitable station found.
    /// Returns `Err(MeteostatError::NoDataFound)` if no data could be found for the specified
    /// time at any suitable station within the limits.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_hourly(
        &self,
        latitude: f64,
        longitude: f64,
        datetime_input: impl IntoUtcDateTime + Clone,
    ) -> Result<HourlyWeatherInfo, MeteostatError> {
        let utc_datetime = datetime_input.clone().into_utc();

        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        for (station, _) in nearest_stations {
            match self.weather_fetcher.hourly(&station.id, utc_datetime).await {
                Ok(weather_info) => {
                    // Found data, return immediately
                    return Ok(weather_info);
                }
                Err(WeatherDataError::DataNotFound { .. }) => {
                    // Data not found for *this* station, try the next one
                    continue;
                }
                Err(e) => {
                    // Any other fetch error is terminal for this request
                    return Err(MeteostatError::WeatherData(e));
                }
            }
        }

        // If the loop finishes, no station provided data
        Err(MeteostatError::NoDataFound {
            granularity: DataSource::Hourly,
            latitude,
            longitude,
            datetime: utc_datetime.to_string(),
        })
    }

    /// Retrieves hourly weather information, combining data from nearby stations
    /// to fulfill the specified `required_fields`.
    ///
    /// This function queries stations within the configured `max_distance_km`
    /// and `station_check_limit`. It attempts to merge data from these stations
    /// until an `HourlyWeatherInfo` record is created containing `Some` values for
    /// all fields specified in `required_fields`.
    ///
    /// If a complete record (based on `required_fields`) is formed, it returns early.
    /// If the station limit is reached, it returns the best combined record found,
    /// even if it doesn't satisfy all `required_fields`.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `datetime_input`: The target date and time (converted to UTC).
    /// * `required_fields`: A set of flags indicating which weather data fields are essential.
    ///                      Use `RequiredWeatherField::ALL` to require everything.
    ///
    /// # Returns
    ///
    /// Returns `Ok(HourlyWeatherInfo)` containing the combined data. This record is
    /// guaranteed to have `Some` for the `required_fields` if possible within the
    /// checked stations. It may contain additional fields if they were available.
    /// Returns `Err(MeteostatError::NoDataFound)` if *no* data could be found from *any*
    /// checked station for the specified time.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_hourly_combined(
        &self,
        latitude: f64,
        longitude: f64,
        datetime_input: impl IntoUtcDateTime + Clone,
        required_fields: RequiredWeatherField,
    ) -> Result<HourlyWeatherInfo, MeteostatError> {
        let utc_datetime = datetime_input.clone().into_utc();

        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        let mut combined_info: Option<HourlyWeatherInfo> = None;

        for (station, _) in nearest_stations {
            match self.weather_fetcher.hourly(&station.id, utc_datetime).await {
                Ok(current_info) => {
                    match combined_info.as_mut() {
                        Some(existing_info) => {
                            // Merge new data into the existing combined record
                            existing_info.merge_from(&current_info);
                            // Check if we now satisfy the requirements
                            if existing_info.has_required_fields(required_fields) {
                                return Ok(existing_info.clone()); // Done!
                            }
                            // Otherwise, continue loop to potentially find more data
                        }
                        None => {
                            // This is the first piece of data found
                            if current_info.has_required_fields(required_fields) {
                                // If it satisfies requirements right away, return it
                                return Ok(current_info);
                            } else {
                                // Otherwise, store it and keep searching to fill gaps
                                combined_info = Some(current_info);
                            }
                        }
                    }
                }
                Err(WeatherDataError::DataNotFound { .. }) => {
                    // Data not found for *this* station, try the next one
                    continue;
                }
                Err(e) => {
                    // Any other fetch error is terminal for this request
                    return Err(MeteostatError::WeatherData(e));
                }
            }
        }

        // Loop finished. Check if we managed to gather *any* data.
        if let Some(final_info) = combined_info {
            // Return the best we could do, even if requirements aren't fully met
            Ok(final_info)
        } else {
            // No data found from any station
            Err(MeteostatError::NoDataFound {
                granularity: DataSource::Hourly,
                latitude,
                longitude,
                datetime: utc_datetime.to_string(),
            })
        }
    }
}

pub struct MeteostatBuilder {
    max_distance_km: f64,
    cache_folder: Option<PathBuf>,
    station_check_limit: usize,
}

impl Default for MeteostatBuilder {
    fn default() -> Self {
        Self {
            max_distance_km: 30.0,
            cache_folder: None,
            station_check_limit: 5,
        }
    }
}

impl MeteostatBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_station_check_limit(mut self, limit: usize) -> Self {
        self.station_check_limit = limit;
        self
    }

    pub fn with_max_distance_km(mut self, km: f64) -> Self {
        self.max_distance_km = km;
        self
    }

    pub fn with_cache_folder<P: Into<PathBuf>>(mut self, folder: P) -> Self {
        self.cache_folder = Some(folder.into());
        self
    }

    pub async fn build(&self) -> Result<Meteostat, MeteostatError> {
        let cache_folder = match &self.cache_folder {
            Some(p) => p.clone(),
            None => get_cache_dir().map_err(MeteostatError::CacheDirResolution)?,
        };
        ensure_cache_dir_exists(&cache_folder)
            .await
            .map_err(|e| MeteostatError::CacheDirCreation(cache_folder.clone(), e))?;

        let station_locator = StationLocator::new(&cache_folder).await?;
        let fetcher = WeatherFetcher::new(&cache_folder);

        Ok(Meteostat {
            station_locator,
            weather_fetcher: fetcher,
            station_check_limit: self.station_check_limit,
            max_distance_km: self.max_distance_km,
        })
    }
}
