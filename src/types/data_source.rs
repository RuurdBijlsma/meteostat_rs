//! Defines types related to the frequency of Meteostat data and requirements
//! for checking station data availability (inventory).

use chrono::NaiveDate;
use std::fmt;

/// Represents the time frequency or granularity of Meteostat weather data.
///
/// Used to specify the type of data to fetch (e.g., hourly temperature vs. daily average)
/// and influences the expected data schema and available date ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Frequency {
    /// Data recorded for each hour. Typically includes temperature, precipitation, wind, etc.
    Hourly,
    /// Aggregated or summarized data for each day. Often includes average/min/max temperature, daily precipitation sum, etc.
    Daily,
    /// Aggregated or summarized data for each month.
    Monthly,
    /// Long-term climate normals, usually calculated over a 30-year period (e.g., 1991-2020) for each month.
    Climate,
}

impl Frequency {
    pub(crate) fn path_segment(&self) -> &'static str {
        match self {
            Frequency::Hourly => "hourly",
            Frequency::Daily => "daily",
            Frequency::Monthly => "monthly",
            Frequency::Climate => "normals",
        }
    }

    pub(crate) fn cache_file_prefix(&self) -> String {
        format!("{}-", self.path_segment())
    }

    pub(crate) fn get_schema_column_names(&self) -> Vec<&'static str> {
        match self {
            Frequency::Hourly => vec![
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco",
            ],
            Frequency::Daily => vec![
                "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres",
                "tsun",
            ],
            Frequency::Monthly => vec![
                "year", "month", "tavg", "tmin", "tmax", "prcp", "wspd", "pres", "tsun",
            ],
            Frequency::Climate => vec![
                "start_year", // Note: Corresponds to 'start' in Meteostat JSON
                "end_year",   // Note: Corresponds to 'end' in Meteostat JSON
                "month",
                "tmin",
                "tmax",
                "prcp",
                "wspd",
                "pres",
                "tsun",
            ],
        }
    }
}

/// Allows formatting a `Frequency` variant using its `path_segment`.
///
/// # Examples
///
/// ```
/// use meteostat::Frequency;
///
/// assert_eq!(format!("{}", Frequency::Hourly), "hourly");
/// assert_eq!(Frequency::Daily.to_string(), "daily");
/// ```
impl fmt::Display for Frequency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path_segment())
    }
}

/// Specifies the criteria for checking if a weather station has the necessary
/// data inventory when searching for stations (e.g., using [`crate::Meteostat::find_stations`]).
///
/// This is used within an [`crate::InventoryRequest`] to filter stations based on
/// their reported data availability ranges. Note that these checks rely on the
/// station metadata provided by Meteostat and don't guarantee that *every single datapoint*
/// within a reported range actually exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequiredData {
    /// Checks only if the station's metadata indicates *any* data coverage
    /// (i.e., start and end dates/years are listed) for the requested [`Frequency`].
    /// This is the least restrictive check.
    ///
    /// **Example Use:** Find stations that have *ever* reported hourly data,
    /// regardless of the specific time period.
    Any,

    /// Checks if the station's reported data availability range includes a specific `NaiveDate`.
    /// Applicable primarily for [`Frequency::Daily`] and [`Frequency::Hourly`].
    ///
    /// **Example Use:** Find stations that reported daily data on 2023-10-26.
    SpecificDate(NaiveDate),

    /// Checks if the station's reported data availability range fully encompasses
    /// the specified date range (inclusive start, inclusive end). Applicable primarily
    /// for [`Frequency::Daily`] and [`Frequency::Hourly`].
    ///
    /// **Example Use:** Find stations that have daily data covering the entire period
    /// from 2023-01-01 to 2023-12-31.
    DateRange {
        /// The required start date (inclusive).
        start: NaiveDate,
        /// The required end date (inclusive).
        end: NaiveDate,
    },

    /// Checks if the station's reported data availability range covers the entire
    /// specified calendar year. This is conceptually shorthand for a `DateRange`
    /// covering January 1st to December 31st of that year. Applicable for
    /// [`Frequency::Daily`] and [`Frequency::Hourly`], and can also be used conceptually
    /// for [`Frequency::Monthly`] (checking if the year is within the monthly start/end years).
    ///
    /// **Example Use:** Find stations reporting daily data for the full year 2022.
    Year(i32),
}