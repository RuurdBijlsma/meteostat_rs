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
//! # Features
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
//! *   **Flexible Collection:** Collect results either as a Polars `DataFrame` (`.frame.collect()`)
//!     or directly into Rust structs (`.collect_hourly()`, `.collect_single_daily()`, etc.)
//!     using methods on the frame wrappers.
//! *   **Automatic Caching:** Downloads and caches station metadata and weather data files
//!     locally to speed up subsequent requests.
//! *   **Asynchronous:** Built with `tokio` for non-blocking I/O.
//!
//! # Basic Usage
//!
//! ```no_run
//! use meteostat::{Meteostat, LatLon, MeteostatError, Year};
//! use polars::prelude::*;
//! use chrono::{NaiveDate};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), MeteostatError> {
//!     let client = Meteostat::new().await?;
//!     // Period for which we want hourly data:
//!     let period = NaiveDate::from_ymd_opt(2023, 9, 1).unwrap();
//!
//!     // --- Example 1: Collect 24 hourly data points into `Vec<Hourly>` ---
//!     let hourly_vec = client
//!         .hourly()
//!         .location(LatLon(52.0836403, 5.1257283))
//!         .call()
//!         .await? // `HourlyLazyFrame`
//!         .get_for_period(period)? // `HourlyLazyFrame` with filter plan
//!         .collect_hourly()?; // `Vec<Hourly>`
//!
//!     // Do something with the hourly data...
//!
//!     // --- Example 2: Collect daily data from 2023 into a `DataFrame` ---
//!     let daily_df = client
//!         .daily()
//!         .location(LatLon(52.0836403, 5.1257283))
//!         .call()
//!         .await? // `DailyLazyFrame`
//!         .get_for_period(Year(2023))? // `DailyLazyFrame` with filter plan
//!         .frame // `LazyFrame` with filter plan
//!         .collect()?; // `DataFrame`
//!
//!     // Do something with the daily data...
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
//! *   **LazyFrame Wrappers:** Fetching data returns structs like [`HourlyLazyFrame`], [`DailyLazyFrame`], [`MonthlyLazyFrame`], [`ClimateLazyFrame`] which contain a Polars `LazyFrame` and provide convenience filtering and collection methods.
//! *   **Filtering:** Use methods like `get_range`, `get_at`, `get_for_period` on the frame wrappers, or access `.frame` for advanced Polars operations.
//! *   **Collecting:** Call `.frame.collect()?` on the frame wrappers to execute the query and get a `DataFrame`, OR use specific methods like `.collect_daily()`, `.collect_single_hourly()`, etc., to get results directly as Rust structs (e.g., `Vec<Daily>`, `Hourly`).
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

// --- Data Types & Enums ---
pub use types::frequency::{Frequency, RequiredData};
pub use types::station::Station;
pub use types::weather_condition::WeatherCondition;

// --- Time/Date Trait Exports (for filtering convenience) ---
pub use types::traits::any::any_date::AnyDate;
pub use types::traits::any::any_datetime::AnyDateTime;
pub use types::traits::any::any_month::AnyMonth;
pub use types::traits::period::date_period::DatePeriod;
pub use types::traits::period::datetime_period::DateTimePeriod;
pub use types::traits::period::month_period::MonthPeriod;
// Concrete time period types
pub use types::traits::types::{Month, Year};

// --- Clients ---
pub use clients::climate_client::ClimateClient;
pub use clients::daily_client::DailyClient;
pub use clients::hourly_client::HourlyClient;
pub use clients::monthly_client::MonthlyClient;

// --- Result Struct Exports (Needed for Vec<Struct> collection) ---
// These are the structs returned by collect_daily(), collect_hourly(), etc.
pub use types::frequency_frames::climate_frame::Climate;
pub use types::frequency_frames::daily_frame::Daily;
pub use types::frequency_frames::hourly_frame::Hourly;
pub use types::frequency_frames::monthly_frame::Monthly;

// --- LazyFrame Wrapper Exports ---
// These are the types returned *after* fetching data (e.g., from `client.daily().station().await?`)
pub use types::frequency_frames::climate_frame::ClimateLazyFrame;
pub use types::frequency_frames::daily_frame::DailyLazyFrame;
pub use types::frequency_frames::hourly_frame::HourlyLazyFrame;
pub use types::frequency_frames::monthly_frame::MonthlyLazyFrame;

// --- Sub-Error Type Exports (useful for specific error matching) ---
pub use stations::error::LocateStationError;
pub use weather_data::error::WeatherDataError;
