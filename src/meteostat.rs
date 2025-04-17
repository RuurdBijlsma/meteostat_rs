use crate::error::MeteostatError;
use crate::stations::locate_station::StationLocator;
use crate::types::bitflags::daily::RequiredDailyField;
use crate::types::bitflags::hourly::RequiredHourlyField;
use crate::types::bitflags::monthly::RequiredMonthlyField;
use crate::types::data_source::DataSource;
use crate::types::into_utc_trait::IntoUtcDateTime;
use crate::types::weather_data::daily::DailyWeatherInfo;
use crate::types::weather_data::hourly::HourlyWeatherInfo;
use crate::types::weather_data::monthly::MonthlyWeatherInfo;
use crate::utils::{ensure_cache_dir_exists, get_cache_dir};
use crate::weather_data::error::WeatherDataError;
use crate::weather_data::fetcher::WeatherFetcher;
use chrono::NaiveDate;
use std::path::PathBuf;
use crate::types::bitflags::climate::RequiredClimateField;
use crate::types::weather_data::climate::ClimateNormalInfo;

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
        required_fields: RequiredHourlyField,
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

    /// Retrieves daily weather information from the *first* nearby station
    /// that provides data for the given date.
    ///
    /// Similar to `get_hourly`, but fetches daily summaries.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `date`: The target date.
    ///
    /// # Returns
    ///
    /// Returns `Ok(DailyWeatherInfo)` from the first suitable station found.
    /// Returns `Err(MeteostatError::NoDataFound)` if no data could be found for the specified
    /// date at any suitable station within the limits.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_daily(
        &self,
        latitude: f64,
        longitude: f64,
        date: NaiveDate,
    ) -> Result<DailyWeatherInfo, MeteostatError> {
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        for (station, _) in nearest_stations {
            match self.weather_fetcher.daily(&station.id, date).await {
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
            granularity: DataSource::Daily, // Changed granularity
            latitude,
            longitude,
            datetime: date.to_string(), // Changed to date
        })
    }

    /// Retrieves daily weather information, combining data from nearby stations
    /// to fulfill the specified `required_fields`.
    ///
    /// Similar to `get_hourly_combined`, but fetches and merges daily summaries.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `date`: The target date.
    /// * `required_fields`: A set of flags indicating which daily data fields are essential.
    ///                      Use `RequiredDailyField::ALL` to require everything.
    ///
    /// # Returns
    ///
    /// Returns `Ok(DailyWeatherInfo)` containing the combined data.
    /// Returns `Err(MeteostatError::NoDataFound)` if *no* data could be found from *any*
    /// checked station for the specified date.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_daily_combined(
        &self,
        latitude: f64,
        longitude: f64,
        date: NaiveDate,
        required_fields: RequiredDailyField, // Changed to daily bitflags
    ) -> Result<DailyWeatherInfo, MeteostatError> {
        // Changed return type
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        let mut combined_info: Option<DailyWeatherInfo> = None; // Changed type

        for (station, _) in nearest_stations {
            // Call the daily fetcher method
            match self.weather_fetcher.daily(&station.id, date).await {
                Ok(current_info) => {
                    match combined_info.as_mut() {
                        Some(existing_info) => {
                            // Merge using DailyWeatherInfo's merge_from
                            existing_info.merge_from(&current_info);
                            // Check using DailyWeatherInfo's has_required_fields
                            if existing_info.has_required_fields(required_fields) {
                                return Ok(existing_info.clone()); // Done!
                            }
                        }
                        None => {
                            // Check using DailyWeatherInfo's has_required_fields
                            if current_info.has_required_fields(required_fields) {
                                return Ok(current_info);
                            } else {
                                combined_info = Some(current_info); // Store DailyWeatherInfo
                            }
                        }
                    }
                }
                Err(WeatherDataError::DataNotFound { .. }) => {
                    continue;
                }
                Err(e) => {
                    return Err(MeteostatError::WeatherData(e));
                }
            }
        }

        if let Some(final_info) = combined_info {
            Ok(final_info) // Return DailyWeatherInfo
        } else {
            Err(MeteostatError::NoDataFound {
                granularity: DataSource::Daily, // Changed granularity
                latitude,
                longitude,
                datetime: date.to_string(), // Changed to date
            })
        }
    }

    /// Retrieves monthly weather information from the *first* nearby station
    /// that provides data for the given year and month.
    ///
    /// Similar to `get_hourly` and `get_daily`, but fetches monthly summaries.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `year`: The target year.
    /// * `month`: The target month (1-12).
    ///
    /// # Returns
    ///
    /// Returns `Ok(MonthlyWeatherInfo)` from the first suitable station found.
    /// Returns `Err(MeteostatError::NoDataFound)` if no data could be found for the specified
    /// month at any suitable station within the limits.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_monthly(
        &self,
        latitude: f64,
        longitude: f64,
        year: i32,
        month: u32,
    ) -> Result<MonthlyWeatherInfo, MeteostatError> {
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        // Create a representative string for the month (e.g., "2023-10") for error reporting
        let year_month_str = format!("{}-{:02}", year, month);

        for (station, _) in nearest_stations {
            match self.weather_fetcher.monthly(&station.id, year, month).await {
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
            granularity: DataSource::Monthly,
            latitude,
            longitude,
            datetime: year_month_str,
        })
    }

    /// Retrieves monthly weather information, combining data from nearby stations
    /// to fulfill the specified `required_fields`.
    ///
    /// Similar to `get_hourly_combined` and `get_daily_combined`, but fetches and merges monthly summaries.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `year`: The target year.
    /// * `month`: The target month (1-12).
    /// * `required_fields`: A set of flags indicating which monthly data fields are essential.
    ///                      Use `RequiredMonthlyField::ALL` to require everything.
    ///
    /// # Returns
    ///
    /// Returns `Ok(MonthlyWeatherInfo)` containing the combined data.
    /// Returns `Err(MeteostatError::NoDataFound)` if *no* data could be found from *any*
    /// checked station for the specified month.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_monthly_combined(
        &self,
        latitude: f64,
        longitude: f64,
        year: i32,
        month: u32,
        required_fields: RequiredMonthlyField,
    ) -> Result<MonthlyWeatherInfo, MeteostatError> {
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        let mut combined_info: Option<MonthlyWeatherInfo> = None; // Changed type

        // Create a representative string for the month (e.g., "2023-10") for error reporting
        let year_month_str = format!("{}-{:02}", year, month);

        for (station, _) in nearest_stations {
            // Call the monthly fetcher method
            match self.weather_fetcher.monthly(&station.id, year, month).await {
                Ok(current_info) => {
                    match combined_info.as_mut() {
                        Some(existing_info) => {
                            // Merge using MonthlyWeatherInfo's merge_from
                            existing_info.merge_from(&current_info);
                            // Check using MonthlyWeatherInfo's has_required_fields
                            if existing_info.has_required_fields(required_fields) {
                                return Ok(existing_info.clone()); // Done!
                            }
                        }
                        None => {
                            // Check using MonthlyWeatherInfo's has_required_fields
                            if current_info.has_required_fields(required_fields) {
                                return Ok(current_info);
                            } else {
                                combined_info = Some(current_info); // Store MonthlyWeatherInfo
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
        combined_info.ok_or_else(|| MeteostatError::NoDataFound {
            granularity: DataSource::Monthly,
            latitude,
            longitude,
            datetime: year_month_str,
        })
    }

    /// Retrieves climate normal data from the *first* nearby station
    /// that provides data for the specified period and month.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `start_year`: The start year of the normal period.
    /// * `end_year`: The end year of the normal period.
    /// * `month`: The target month (1-12).
    ///
    /// # Returns
    ///
    /// Returns `Ok(ClimateNormalInfo)` from the first suitable station found.
    /// Returns `Err(MeteostatError::NoDataFound)` if no data could be found for the specified
    /// period and month at any suitable station within the limits.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_climate_normals(
        &self,
        latitude: f64,
        longitude: f64,
        start_year: i32,
        end_year: i32,
        month: u32,
    ) -> Result<ClimateNormalInfo, MeteostatError> {
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        // Create a representative string for the period/month (e.g., "1991-2020-10") for error reporting
        let period_month_str = format!("{}-{}-{:02}", start_year, end_year, month);

        for (station, _) in nearest_stations {
            match self.weather_fetcher.climate_normals(&station.id, start_year, end_year, month).await {
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
            granularity: DataSource::Normals, // Changed granularity
            latitude,
            longitude,
            datetime: period_month_str, // Use the period-month string
        })
    }

    /// Retrieves climate normal data, combining data from nearby stations
    /// to fulfill the specified `required_fields`.
    ///
    /// # Arguments
    ///
    /// * `latitude`: Target latitude.
    /// * `longitude`: Target longitude.
    /// * `start_year`: The start year of the normal period.
    /// * `end_year`: The end year of the normal period.
    /// * `month`: The target month (1-12).
    /// * `required_fields`: A set of flags indicating which climate data fields are essential.
    ///                      Use `RequiredClimateField::ALL` to require everything.
    ///
    /// # Returns
    ///
    /// Returns `Ok(ClimateNormalInfo)` containing the combined data.
    /// Returns `Err(MeteostatError::NoDataFound)` if *no* data could be found from *any*
    /// checked station for the specified period and month.
    /// Returns `Err(MeteostatError::WeatherData)` if a fetching error occurs.
    pub async fn get_climate_normals_combined(
        &self,
        latitude: f64,
        longitude: f64,
        start_year: i32,
        end_year: i32,
        month: u32,
        required_fields: RequiredClimateField, // Changed to climate bitflags
    ) -> Result<ClimateNormalInfo, MeteostatError> { // Changed return type
        let nearest_stations = self.station_locator.query(
            latitude,
            longitude,
            self.station_check_limit,
            self.max_distance_km,
        );

        let mut combined_info: Option<ClimateNormalInfo> = None; // Changed type

        // Create a representative string for the period/month (e.g., "1991-2020-10") for error reporting
        let period_month_str = format!("{}-{}-{:02}", start_year, end_year, month);

        for (station, _) in nearest_stations {
            // Call the climate normals fetcher method
            match self.weather_fetcher.climate_normals(&station.id, start_year, end_year, month).await {
                Ok(current_info) => {
                    // Ensure the fetched data matches the requested period and month before merging
                    if current_info.start_year == start_year && current_info.end_year == end_year && current_info.month == month {
                        match combined_info.as_mut() {
                            Some(existing_info) => {
                                // Merge using ClimateNormalInfo's merge_from
                                existing_info.merge_from(&current_info);
                                // Check using ClimateNormalInfo's has_required_fields
                                if existing_info.has_required_fields(required_fields) {
                                    return Ok(existing_info.clone()); // Done!
                                }
                            }
                            None => {
                                // Check using ClimateNormalInfo's has_required_fields
                                if current_info.has_required_fields(required_fields) {
                                    return Ok(current_info);
                                } else {
                                    combined_info = Some(current_info); // Store ClimateNormalInfo
                                }
                            }
                        }
                    } else {
                        // Data found, but for a different period/month (should ideally not happen if extractor is correct)
                        // Log or handle this case if necessary, but continue searching for the correct data.
                        log::warn!("Station {} returned climate normals for {}-{} month {} instead of requested {}-{} month {}",
                                   station.id, current_info.start_year, current_info.end_year, current_info.month,
                                   start_year, end_year, month);
                        continue;
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

        // Loop finished. Check if we managed to gather *any* data matching the criteria.
        combined_info.ok_or_else(|| MeteostatError::NoDataFound {
            granularity: DataSource::Normals, // Changed granularity
            latitude,
            longitude,
            datetime: period_month_str, // Use the period-month string
        })
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


#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};
    use crate::error::MeteostatError;
    use crate::meteostat::{Meteostat, MeteostatBuilder};
    use crate::types::bitflags::climate::RequiredClimateField;
    use crate::types::bitflags::daily::RequiredDailyField;
    use crate::types::bitflags::hourly::RequiredHourlyField;
    use crate::types::bitflags::monthly::RequiredMonthlyField;
    use crate::utils::get_cache_dir;

    // Helper to build Meteostat instance for tests, using a temporary cache dir
    async fn build_test_meteostat() -> Result<Meteostat, MeteostatError> {
        let cache_dir = get_cache_dir()?;
        Ok(MeteostatBuilder::new()
            .with_cache_folder(&cache_dir)
            .with_max_distance_km(75.0)
            .with_station_check_limit(10)
            .build()
            .await
            .expect("Failed to build Meteostat instance for testing"))
    }

    // --- Test Coordinates and Times (Near London Heathrow - LHR) ---
    const TEST_LAT: f64 = 51.47; // LHR Latitude
    const TEST_LON: f64 = -0.45; // LHR Longitude
    const TEST_YEAR: i32 = 2023;
    const TEST_MONTH: u32 = 7; // July
    const TEST_DAY: u32 = 15;
    const TEST_HOUR: u32 = 12; // Noon UTC

    // --- Standard Method Tests ---

    #[tokio::test]
    async fn test_get_hourly_integration()-> Result<(), MeteostatError> {
        let meteostat = build_test_meteostat().await?;
        let test_datetime = Utc
            .with_ymd_and_hms(TEST_YEAR, TEST_MONTH, TEST_DAY, TEST_HOUR, 0, 0)
            .unwrap();

        let result = meteostat
            .get_hourly(TEST_LAT, TEST_LON, test_datetime)
            .await;

        println!("Hourly Result: {:?}", result); // Print for debugging if it fails
        assert!(result.is_ok(), "Expected Ok result for get_hourly");
        // Basic check: Ensure some core data field might be populated
        // Note: specific values aren't checked due to data variability
        assert!(
            result?.temperature.is_some(),
            "Expected temperature to be Some"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily_integration() -> Result<(), MeteostatError>{
        let meteostat = build_test_meteostat().await?;
        let test_date = NaiveDate::from_ymd_opt(TEST_YEAR, TEST_MONTH, TEST_DAY).unwrap();

        let result = meteostat.get_daily(TEST_LAT, TEST_LON, test_date).await;

        println!("Daily Result: {:?}", result);
        assert!(result.is_ok(), "Expected Ok result for get_daily");
        // Basic check
        assert!(
            result?.temp_avg.is_some(),
            "Expected average temperature to be Some"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly_integration()-> Result<(), MeteostatError> {
        let meteostat = build_test_meteostat().await?;

        let result = meteostat
            .get_monthly(TEST_LAT, TEST_LON, TEST_YEAR, TEST_MONTH)
            .await;

        println!("Monthly Result: {:?}", result);
        assert!(result.is_ok(), "Expected Ok result for get_monthly");
        // Basic check
        assert!(
            result?.temp_avg.is_some(),
            "Expected average temperature to be Some"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate_normals_integration()-> Result<(), MeteostatError> {
        let meteostat = build_test_meteostat().await?;
        let start_year = 1991; // Standard climate normal period
        let end_year = 2020;

        let result = meteostat
            .get_climate_normals(TEST_LAT, TEST_LON, start_year, end_year, TEST_MONTH)
            .await;

        println!("Climate Normals Result: {:?}", result);
        assert!(result.is_ok(), "Expected Ok result for get_climate_normals");
        // Basic check
        assert!(
            result?.temp_min_avg.is_some(),
            "Expected average min temperature to be Some"
        );
        Ok(())
    }

    // --- Combined Method Tests ---

    #[tokio::test]
    async fn test_get_hourly_combined_integration() -> Result<(), MeteostatError>{
        let meteostat = build_test_meteostat().await?;
        let test_datetime = Utc
            .with_ymd_and_hms(TEST_YEAR, TEST_MONTH, TEST_DAY, TEST_HOUR, 0, 0)
            .unwrap();
        // Request specific fields
        let required = RequiredHourlyField::TEMPERATURE | RequiredHourlyField::PRECIPITATION;

        let result = meteostat
            .get_hourly_combined(TEST_LAT, TEST_LON, test_datetime, required)
            .await;

        println!("Hourly Combined Result: {:?}", result);
        assert!(result.is_ok(), "Expected Ok result for get_hourly_combined");
        let data = result?;
        // Check if the *requested* fields were filled (assuming data exists nearby)
        assert!(
            data.temperature.is_some(),
            "Expected combined temperature to be Some"
        );
        // Other fields might be None if not requested or not available
        Ok(())
    }

    #[tokio::test]
    async fn test_get_daily_combined_integration() -> Result<(), MeteostatError>{
        let meteostat = build_test_meteostat().await?;
        let test_date = NaiveDate::from_ymd_opt(TEST_YEAR, TEST_MONTH, TEST_DAY).unwrap();
        let required = RequiredDailyField::TEMP_AVG | RequiredDailyField::PRECIPITATION;

        let result = meteostat
            .get_daily_combined(TEST_LAT, TEST_LON, test_date, required)
            .await;

        println!("Daily Combined Result: {:?}", result);
        assert!(result.is_ok(), "Expected Ok result for get_daily_combined");
        let data = result?;
        assert!(
            data.temp_avg.is_some(),
            "Expected combined avg temperature to be Some"
        );
        assert!(
            data.precipitation.is_some(),
            "Expected combined precipitation to be Some"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_monthly_combined_integration()-> Result<(), MeteostatError> {
        let meteostat = build_test_meteostat().await?;
        let required = RequiredMonthlyField::TEMP_AVG | RequiredMonthlyField::PRECIPITATION_TOTAL;

        let result = meteostat
            .get_monthly_combined(TEST_LAT, TEST_LON, TEST_YEAR, TEST_MONTH, required)
            .await;

        println!("Monthly Combined Result: {:?}", result);
        assert!(
            result.is_ok(),
            "Expected Ok result for get_monthly_combined"
        );
        let data = result?;
        assert!(
            data.temp_avg.is_some(),
            "Expected combined avg temperature to be Some"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_climate_normals_combined_integration() -> Result<(), MeteostatError>{
        let meteostat = build_test_meteostat().await?;
        let start_year = 1991;
        let end_year = 2020;
        let required = RequiredClimateField::TEMP_MIN_AVG | RequiredClimateField::PRECIPITATION_AVG;

        let result = meteostat
            .get_climate_normals_combined(
                TEST_LAT, TEST_LON, start_year, end_year, TEST_MONTH, required,
            )
            .await;

        println!("Climate Normals Combined Result: {:?}", result);
        assert!(
            result.is_ok(),
            "Expected Ok result for get_climate_normals_combined"
        );
        let data = result?;
        assert!(
            data.temp_min_avg.is_some(),
            "Expected combined avg min temperature to be Some"
        );
        assert!(
            data.precipitation_avg.is_some(),
            "Expected combined avg precipitation to be Some"
        );
        Ok(())
    }
}
