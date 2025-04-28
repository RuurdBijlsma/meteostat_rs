//! # Meteostat Client
//!
//! This crate provides a convenient Rust interface for accessing historical weather
//! and climate data from [Meteostat](https://meteostat.net/), leveraging their
//! publicly available bulk data. It allows fetching data for thousands of weather
//! stations worldwide.
//!
//! ## Features
//!
//! *   Fetch weather data by specific **station ID**.
//! *   Fetch weather data for the **closest station** to a given **latitude/longitude**.
//! *   **Find nearby stations** based on coordinates, with optional filtering by distance and data availability.
//! *   Supports multiple data frequencies: **Hourly**, **Daily**, **Monthly**, and **Climate Normals**.
//! *   **Automatic caching** of downloaded data (station metadata and weather records) to speed up subsequent requests and reduce load on data sources.
//! *   Data is returned as efficient [Polars](https://pola.rs/) **`LazyFrame`s**, allowing for powerful filtering and manipulation before collecting into memory.
//! *   Asynchronous API using `tokio`.
//!
//! ## Usage Example
//!
//! ```rust
//! use meteostat::{Meteostat, LatLon, Frequency, MeteostatError, MeteostatFrameFilterExt};
//! use polars::prelude::*;
//! use chrono::NaiveDate;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), MeteostatError> {
//!     use std::str::FromStr;
//! // Initialize the client
//!     use chrono::{DateTime, NaiveDateTime, Utc};
//! let client = Meteostat::new().await?;
//!
//!     // --- Example 1: Get data for a known station ID ---
//!     let schiphol_id = "06240"; // Amsterdam Schiphol Airport station ID
//!     println!("Fetching daily data for station {}...", schiphol_id);
//!     let daily_lazy = client
//!         .from_station()
//!         .station(schiphol_id)
//!         .frequency(Frequency::Daily)
//!         .call()
//!         .await?;
//!
//!     // Filter the LazyFrame (e.g., for a specific year) before collecting
//!     let daily_2023 = daily_lazy
//!         .filter_daily_by_year(2023)?
//!         .collect()?;
//!
//!     println!("Daily data for Schiphol (2023):\n{}", daily_2023.head(Some(5)));
//!
//!     // --- Example 2: Get data for a location ---
//!     let berlin_center = LatLon(52.52, 13.40); // Berlin center coordinates
//!     println!("\nFetching hourly data near Berlin ({}, {})...", berlin_center.0, berlin_center.1);
//!     let hourly_lazy = client
//!         .from_location()
//!         .location(berlin_center)
//!         .frequency(Frequency::Hourly)
//!         // Optionally refine search: .max_distance_km(25.0).station_limit(3)
//!         .call()
//!         .await?;
//!
//!     // Filter for a specific date
//!     let start_datetime = DateTime::<Utc>::from_str("2022-01-10T00:00:00Z").unwrap();// Jan 10 2022 00:00:00
//!     let end_datetime = DateTime::<Utc>::from_str("2022-01-10T23:59:59Z").unwrap();// Jan 10 2022 23:59:59
//!     let specific_day_hourly = hourly_lazy
//!         .filter_hourly(start_datetime, end_datetime)
//!         .collect()?;
//!
//!     println!("Hourly data near Berlin:\n{}", specific_day_hourly);
//!
//!     // --- Example 3: Find nearby stations ---
//!     println!("\nFinding stations near Berlin...");
//!     let nearby_stations = client
//!         .find_stations()
//!         .location(berlin_center)
//!         .station_limit(3) // Find the closest 3 stations
//!         .call()
//!         .await?;
//!
//!     println!("Found {} stations near Berlin:", nearby_stations.len());
//!     for station in nearby_stations {
//!         println!(
//!             "ID: {}, Name: {:?}",
//!             station.id,
//!             station.name.get("en")
//!         );
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Key Components
//!
//! *   [`Meteostat`]: The main client struct to interact with the API.
//! *   [`LatLon`]: Represents geographical coordinates.
//! *   [`Frequency`]: Enum for data granularity (Hourly, Daily, Monthly, Climate).
//! *   [`Station`]: Struct representing a weather station's metadata.
//! *   [`MeteostatError`]: The primary error type for the crate.
//! *   [`polars::prelude::LazyFrame`]: Data is returned in this Polars type for efficient processing.
//! *   [`MeteostatFrameFilterExt`]: Trait providing convenient filtering methods on `LazyFrame`s returned by this crate.
//!
//! Check the specific function/method documentation for details on arguments, return types, and potential errors.

mod error;
mod filtering;
mod meteostat;
mod stations;
mod types;
mod utils;
mod weather_data;

pub use meteostat::*;

pub use types::data_source::{Frequency, RequiredData};
pub use types::into_utc_trait::IntoUtcDateTime;
pub use types::station::Station;
pub use types::weather_condition::WeatherCondition;

pub use error::MeteostatError;

pub use filtering::MeteostatFrameFilterExt;
