//! examples/graph_data.rs
//!
//! This example demonstrates fetching hourly weather data for a specific location
//! using the `meteostat` crate and plotting the temperature using the `plotters` crate.
//!
//! To run this example:
//! cargo run --example graph_data --features plotting

use std::error::Error;

use meteostat::{Frequency, LatLon, Meteostat, MeteostatFrameFilterExt};
use plotlars::{Axis, Line, LinePlot, Plot, Rgb, Text};
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Fetching weather data from Meteostat...");

    // 1. Create a Meteostat client
    let meteostat = Meteostat::new().await?;

    // 2. Define location and time range
    let location = LatLon(52.118641, 5.185589); // de Bilt

    // 3. Build and execute the query using meteostat
    let weather_data: DataFrame = meteostat
        .from_location() // Query by geographic coordinates
        .location(location) // Set the location
        .frequency(Frequency::Daily) // Request daily data
        .call() // Execute the API call (returns LazyFrame)
        .await?
        .filter_daily_by_year(2023)? // Filter the LazyFrame by date range
        .collect()?; // Collect the results into a DataFrame

    // 4. Plot the data
    println!("Generating temperature plot...");
    plot_temperature(&weather_data);
    println!("Plot shown in browser.");

    Ok(())
}

fn plot_temperature(dataset: &DataFrame) {
    LinePlot::builder()
        .data(dataset)
        .x("date")
        .y("tavg")
        .additional_lines(vec!["tmin", "tmax"])
        .colors(vec![
            Rgb(120, 120, 120), // tavg
            Rgb(69, 143, 196),  // tmin
            Rgb(199, 115, 42),  // tmax
        ])
        .lines(vec![Line::Solid, Line::Dot, Line::Dot])
        .width(3.0)
        .plot_title(
            Text::from("Temperature at De Bilt (2023)")
                .font("Arial")
                .size(18),
        )
        .x_axis(
            &Axis::new()
                .tick_values(
                    (0..12)
                        .map(|i| 19358.0 + i as f64 * (19722.0 - 19358.0) / 11.0)
                        .collect(),
                )
                .tick_labels(vec![
                    "Jan", "Feb", "Mar", "April", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                    "Dec",
                ]),
        )
        .build()
        .plot();
}
