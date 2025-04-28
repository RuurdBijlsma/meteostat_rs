# Meteostat for Rust

[![Crates.io](https://img.shields.io/crates/v/meteostat.svg)](https://crates.io/crates/meteostat)
[![Docs.rs](https://docs.rs/meteostat/badge.svg)](https://docs.rs/meteostat)
[![License](https://img.shields.io/crates/l/meteostat.svg)](https://github.com/RuurdBijlsma/meteostat_rs/blob/main/LICENSE)
[![Repository](https://img.shields.io/badge/GitHub-Repo-blue)](https://github.com/RuurdBijlsma/meteostat_rs)
[![Build Status](https://github.com/RuurdBijlsma/meteostat_rs/actions/workflows/ci.yml/badge.svg)](https://github.com/RuurdBijlsma/meteostat_rs/actions/workflows/ci.yml)

**The Weather's Record Keeper - In Rust!**

![Example Plot showing Daily Temperature Averages, Min, and Max for De Bilt (2023).](https://raw.githubusercontent.com/RuurdBijlsma/meteostat_rs/main/.github/screenshots/temp_plot.png)

This crate provides a convenient asynchronous Rust interface for accessing historical weather and climate data
from [Meteostat](https://meteostat.net/), leveraging their publicly available **bulk data interface**. It allows
fetching data for thousands of weather stations worldwide.

> **Meteostat is a free and open provider of weather & climate data.** They do the hard work of collecting, processing,
> and providing the data. This crate is simply a Rust client for their bulk API. Please consider supporting Meteostat if
> you find their data useful: [**Donate to Meteostat**](https://meteostat.net/en/patrons).

Take a look at yesterday's temperatures or discover the weather hundreds of years ago, right from your Rust application.

## Key Features

* **Fetch by Station ID:** Get weather data for a
  specific Meteostat station ID.
* **Fetch by Location:** Get weather data for a location, specified by a latitude and longitude.
* **Find Nearby Stations:** Search for stations near coordinates, optionally filtering by distance and required data
  availability (inventory).
* **Multiple Frequencies:** Supports [**Hourly**](https://dev.meteostat.net/bulk/hourly.html#endpoints),
[**Daily**](https://dev.meteostat.net/bulk/daily.html), [**Monthly**](https://dev.meteostat.net/bulk/monthly.html), and 
[**Climate Normals**](https://dev.meteostat.net/bulk/normals.html) data.
* **Efficient Data Handling:** Returns data as [Polars](https://pola.rs/) **`LazyFrame`s**, allowing for powerful,
  memory-efficient filtering and manipulation *before* collecting results.
* **Convenient Filtering:** Includes a `MeteostatFrameFilterExt` trait for easy filtering of `LazyFrame`s by date, year,
  or datetime ranges.
* **Automatic Caching:** Downloads and caches station metadata and weather data files locally to speed up subsequent
  requests and reduce load on Meteostat's servers.
* **Asynchronous:** Built with `tokio` for non-blocking I/O.

## Installation

Add `meteostat` to your `Cargo.toml` dependencies:

```bash
cargo add meteostat
```

## Basic Usage

Here's a quick example demonstrating fetching data by station ID and by location:

```rust
use meteostat::{Meteostat, LatLon, Frequency, MeteostatError, MeteostatFrameFilterExt};
use polars::prelude::*;
use chrono::{DateTime, NaiveDate, Utc};
use std::str::FromStr; // For parsing DateTime<Utc>

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    // Initialize the client (uses default cache directory)
    let client = Meteostat::new().await?;

    // --- Example: Get data for a location ---
    let berlin_center = LatLon(52.52, 13.40);
    let hourly_lazy = client
        .from_location()
        .location(berlin_center)
        .frequency(Frequency::Hourly)
        .call()
        .await?;

    // Filter for a specific date
    let start_datetime = DateTime::<Utc>::from_str("2022-01-10T00:00:00Z").unwrap(); // Jan 10 2022 00:00:00 UTC
    let end_datetime = DateTime::<Utc>::from_str("2022-01-10T23:59:59Z").unwrap(); // Jan 10 2022 23:59:59 UTC
    let specific_day_hourly = hourly_lazy
        .filter_hourly(start_datetime, end_datetime)
        .collect()?;

    println!("Hourly data near Berlin for 2022-01-10:\n{}", specific_day_hourly.head(Some(5)));
    Ok(())
}
```

*(See more examples in the [examples directory](https://github.com/RuurdBijlsma/meteostat_rs/tree/main/examples))*

## Data Handling

### Polars `LazyFrame`

All weather data fetching methods (`from_station`, `from_location`) return a Polars `LazyFrame`. This allows you to:

1. **Chain operations:** Apply filters, selections, aggregations, etc., without loading the entire dataset into memory
   immediately.
2. **Optimize queries:** Polars optimizes the plan before execution.
3. **Collect when ready:** Use `.collect()?` to execute the plan and get a `DataFrame` in memory.

This is particularly beneficial when dealing with potentially large historical datasets.

### Caching

The crate automatically caches downloaded data to avoid redundant downloads and respect Meteostat's resources:

* **Station Metadata:** The list of all stations (`stations/lite.json.gz`) is downloaded once and cached.
* **Weather Data:** Individual station data files (e.g., `hourly/10637.csv.gz`) are downloaded and cached per station
  and frequency.

By default, cache files are stored in your system's standard cache directory (e.g., `~/.cache/meteostat_rs` on Linux,
`%LOCALAPPDATA%/meteostat_rs_cache` on Windows).
You can specify a custom cache location using `Meteostat::with_cache_folder(path)`.

## Filtering Data Frames

The [`MeteostatFrameFilterExt` trait](https://docs.rs/meteostat/latest/meteostat/trait.MeteostatFrameFilterExt.html)
provides convenient methods for filtering the `LazyFrame`s returned by this crate based on date and time.

```rust
use meteostat::{Meteostat, Frequency, MeteostatError, MeteostatFrameFilterExt};
use polars::prelude::*;
use chrono::{NaiveDate, Utc, TimeZone};

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let client = Meteostat::new().await?;
    let station_id = "10637"; // Schiphol

    // --- Filter Daily Data by Year ---
    let daily_lazy = client.from_station().station(station_id).frequency(Frequency::Daily).call().await?;
    let daily_2022 = daily_lazy.filter_daily_by_year(2022)?.collect()?;
    println!("Daily data for 2022:\n{}", daily_2022.head(Some(3)));

    // --- Filter Hourly Data by Datetime Range ---
    let hourly_lazy = client.from_station().station(station_id).frequency(Frequency::Hourly).call().await?;
    let start_dt = Utc.with_ymd_and_hms(2023, 5, 1, 6, 0, 0).unwrap(); // May 1st 2023, 06:00 UTC
    let end_dt = Utc.with_ymd_and_hms(2023, 5, 1, 12, 0, 0).unwrap(); // May 1st 2023, 12:00 UTC
    let hourly_morning = hourly_lazy.filter_hourly(start_dt, end_dt).collect()?;
    println!("\nHourly data for morning of 2023-05-01:\n{}", hourly_morning.head(Some(3)));

    // --- Get a Single Daily Row ---
    let daily_lazy_again = client.from_station().station(station_id).frequency(Frequency::Daily).call().await?;
    let specific_date = NaiveDate::from_ymd_opt(2023, 10, 26).unwrap();
    let single_day = daily_lazy_again.get_daily_row(specific_date).collect()?;
    println!("\nDaily data for {}:\n{}", specific_date, single_day);

    Ok(())
}
```

See the [
`MeteostatFrameFilterExt` documentation](https://docs.rs/meteostat/latest/meteostat/trait.MeteostatFrameFilterExt.html)
for all available filtering methods (`filter_daily`, `filter_monthly`, `filter_climate`, `get_hourly_row`, etc.).

## Data Source and Attribution

* All weather data is sourced from **[Meteostat](https://meteostat.net/)**.
* This crate uses Meteostat's **free bulk data interface**. No API key is required.

## API Documentation

Full API documentation is available on [docs.rs](https://docs.rs/meteostat).

## Example: Plotting Data

You can easily use the `DataFrame` output with plotting libraries like `plotlars`.

```rust
// Requires the 'examples' feature: cargo run --example graph_data --features examples
use std::error::Error;

use meteostat::{Frequency, LatLon, Meteostat, MeteostatFrameFilterExt};
use plotlars::{Line, LinePlot, Plot, Rgb, Text};
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let meteostat = Meteostat::new().await?;
  let location = LatLon(52.118641, 5.185589); // De Bilt, Netherlands

  let weather_data: DataFrame = meteostat
          .from_location()
          .location(location)
          .frequency(Frequency::Daily)
          .call()
          .await?
          .filter_daily_by_year(2023)?
          .collect()?;

  println!(
    "Daily Data for De Bilt (2023):\n{}",
    weather_data.head(Some(5))
  );

  plot_temperature(&weather_data);

  Ok(())
}

fn plot_temperature(dataset: &DataFrame) {
  LinePlot::builder()
    .data(&dataset)
    .x("date")
    .y("tavg") // Average temperature
    .additional_lines(vec!["tmin", "tmax"]) // Min and Max temps
    .colors(vec![
      Rgb(120, 120, 120),
      Rgb(69, 143, 196),
      Rgb(199, 115, 42),
    ])
    .lines(vec![Line::Solid, Line::Dot, Line::Dot])
    .width(3.0)
    .plot_title(
      Text::from("Temperature at De Bilt (2023)")
        .font("Arial")
        .size(18),
    )
    .build()
    .plot();
}
```

To run this specific example, enable the `examples` feature:
`cargo run --example graph_data --features examples`

*(This will generate a plot similar to the one shown at the top of this README)*

## Contributing

Contributions, bug reports, and feature requests are welcome! Please feel free to open an issue or submit a pull request
on the [GitHub repository](https://github.com/RuurdBijlsma/meteostat_rs).

## License

This crate is licensed under the Apache License 2.0. See
the [LICENSE](https://github.com/RuurdBijlsma/meteostat_rs/blob/main/LICENSE.md) file for details.
