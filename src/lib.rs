//! Provides a Rust interface for accessing historical weather and climate data
//! from [Meteostat](https://meteostat.net/), using their free bulk data interface.
//!
//! This crate allows fetching hourly, daily, monthly, and climate normal data
//! for thousands of weather stations worldwide, either by station ID or by
//! geographical coordinates (latitude/longitude). Data is returned efficiently
//! as Polars `LazyFrame` wrappers, enabling powerful filtering and analysis
//! before loading data into memory. Automatic caching of station metadata and
//! weather data files minimizes redundant downloads.
//!
//! # Key Features
//!
//! *   **Fetch by Station ID or Location:** Initiate requests via frequency-specific clients
//!     (`client.hourly()`, `client.daily()`, etc.) and then specify either `.station("ID")`
//!     or `.location(LatLon)`.
//! *   **Find Nearby Stations:** Search for stations near coordinates using `client.find_stations()`,
//!     optionally filtering by distance and required data availability (inventory).
//! *   **Multiple Frequencies:** Supports Hourly, Daily, Monthly, and Climate Normals data.
//! *   **Efficient Data Handling:** Returns data as wrappers around Polars `LazyFrame`s
//!     (e.g., [`HourlyLazyFrame`]), allowing for powerful, memory-efficient filtering
//!     and manipulation *before* collecting results.
//! *   **Convenient Filtering:** Frame wrappers provide methods for easy filtering by date,
//!     year, month, or datetime ranges (e.g., `daily_lazy.get_for_period(Year(2023))`).
//! *   **Automatic Caching:** Downloads and caches station metadata and weather data files
//!     locally to speed up subsequent requests.
//! *   **Asynchronous:** Built with `tokio` for non-blocking I/O.
//!
//! # Basic Usage
//!
//! ```no_run
//! use meteostat::{Meteostat, LatLon, MeteostatError, Year, DailyLazyFrame, HourlyLazyFrame};
//! use polars::prelude::*;
//! use chrono::{DateTime, Utc, TimeZone}; // For Hourly/DateTime filtering
//!
//! #[tokio::main]
//! async fn main() -> Result<(), MeteostatError> {
//!     // 1. Initialize the client (uses default cache directory)
//! let client = Meteostat::new().await?;
//!
//!     // 2. Define location or station
//!     let berlin_center = LatLon(52.52, 13.40);
//!     let station_id = "10382"; // Berlin-Tegel
//!
//!     // --- Example: Get Daily data for a station ID for a specific year ---
//!
//!     // 3. Select client and specify source
//!     let daily_lazy: DailyLazyFrame = client // Start with the main client
//!         .daily()                 // Select the daily data client
//!         .station(station_id)     // Specify station ID
//!         .await?;                 // -> Result<DailyLazyFrame, MeteostatError>
//!
//!     // 4. Filter the data using methods on the frame wrapper
//!     let daily_2023_lazy = daily_lazy.get_for_period(Year(2023))?;
//!
//!     // 5. Access the underlying LazyFrame and collect results
//!     let daily_2023_df = daily_2023_lazy.frame.collect()?;
//!
//!     println!(
//!         "Daily data for Station {} in {}:\n{}",
//!         station_id,
//!         2023,
//!         daily_2023_df.head(Some(5))
//!     );
//!
//!     // --- Example: Get Hourly data for a location ---
//!     let hourly_lazy: HourlyLazyFrame = client
//!         .hourly()
//!         .location(berlin_center)
//!         .call() // .call() needed for location builder
//!         .await?;
//!
//!     // Filter for a specific time range
//!     let start_dt = Utc.with_ymd_and_hms(2022, 1, 10, 0, 0, 0).unwrap();
//!     let end_dt = Utc.with_ymd_and_hms(2022, 1, 10, 5, 59, 59).unwrap();
//!     let specific_hours_df = hourly_lazy
//!         .get_range(start_dt, end_dt)?
//!         .frame
//!         .collect()?;
//!
//!     println!(
//!         "\nHourly data near Berlin for 2022-01-10 00:00-05:59:\n{}",
//!         specific_hours_df.head(Some(6))
//!     );
//!
//!     Ok(())
//! }
//! ```
//!
//! # Core Concepts
//!
//! *   **[`Meteostat`]:** The main entry point client struct. Created via [`Meteostat::new`] or [`Meteostat::with_cache_folder`].
//! *   **Frequency Clients:** Accessed via methods on `Meteostat` (e.g., [`Meteostat::hourly`], [`Meteostat::daily`]). These return builders.
//! *   **Source Specification:** Use `.station("ID")` or `.location(LatLon)` on the frequency client builders.
//! *   **LazyFrame Wrappers:** Fetching data returns structs like [`HourlyLazyFrame`], [`DailyLazyFrame`], [`MonthlyLazyFrame`], [`ClimateLazyFrame`] which contain a Polars `LazyFrame` and provide convenience filtering methods.
//! *   **Filtering:** Use methods like `get_range`, `get_at`, `get_for_period` on the frame wrappers, or access `.frame` for advanced Polars operations.
//! *   **Collecting:** Call `.frame.collect()?` on the frame wrappers to execute the query and get a `DataFrame`.
//! *   **Finding Stations:** Use [`Meteostat::find_stations`] to search for [`Station`] objects near a [`LatLon`], optionally filtering by [`InventoryRequest`] criteria.
//!
//! # Data Source and Attribution
//!
//! *   All weather data is sourced from **[Meteostat](https://meteostat.net/)**.
//! *   This crate uses Meteostat's **free bulk data interface**. No API key is required. Please consider supporting them if you find their service useful.

// Module structure
mod clients;
mod error;
mod meteostat;
mod stations;
mod types;
mod utils;
mod weather_data;

// --- Core Exports ---
pub use error::MeteostatError;
pub use meteostat::{InventoryRequest, LatLon, Meteostat};

// --- Client Builders (Accessed via Meteostat methods) ---
// These are the types returned by `meteostat.hourly()`, etc., before specifying source.
// Users don't typically need to import them directly.
pub use clients::climate_client::*;
pub use clients::daily_client::*;
pub use clients::hourly_client::*;
pub use clients::monthly_client::*;

// --- Data Types & Enums ---
pub use types::frequency::{Frequency, RequiredData};
pub use types::station::Station;
pub use types::weather_condition::WeatherCondition;

// --- Time/Date Trait Exports (for filtering convenience) ---
// Traits implemented by chrono types, Year, Month for easy filtering
pub use types::traits::any::any_date::AnyDate;
pub use types::traits::any::any_datetime::AnyDateTime;
pub use types::traits::any::any_month::AnyMonth;
pub use types::traits::period::date_period::DatePeriod;
pub use types::traits::period::datetime_period::DateTimePeriod;
pub use types::traits::period::month_period::MonthPeriod;
// Concrete time period types
pub use types::traits::types::{Month, Year};

// --- LazyFrame Wrapper Exports ---
// These are the types returned *after* fetching data (e.g., from `client.daily().station().await?`)
pub use types::frequency_frames::climate_frame::ClimateLazyFrame;
pub use types::frequency_frames::daily_frame::DailyLazyFrame;
pub use types::frequency_frames::hourly_frame::HourlyLazyFrame;
pub use types::frequency_frames::monthly_frame::MonthlyLazyFrame;

// --- Sub-Error Type Exports (useful for specific error matching) ---
pub use stations::error::LocateStationError;
pub use weather_data::error::WeatherDataError;
