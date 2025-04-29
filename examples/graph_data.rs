//! examples/graph_data.rs
//!
//! This example demonstrates fetching hourly weather data for a specific location
//! using the `meteostat` crate and plotting the temperature using the `plotters` crate.
//!
//! To run this example:
//! cargo run --example graph_data --features plotting

use std::error::Error;

use meteostat::{LatLon, Meteostat, Year};
use plotlars::{Axis, Line, LinePlot, Plot, Rgb, Text};
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Fetching weather data from Meteostat...");

    // 1. Create a Meteostat client
    let client = Meteostat::new().await?;

    // 2. Define location and time range
    let location = LatLon(52.118641, 5.185589); // de Bilt

    // 3. Build and execute the query using meteostat
    let weather_data = client
        .daily()
        .location(location)
        .call()
        .await?
        .get_for_period(Year(2023))?
        .frame
        .collect()?;

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
