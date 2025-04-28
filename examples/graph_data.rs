//! examples/graph_data.rs
//!
//! This example demonstrates fetching hourly weather data for a specific location
//! using the `meteostat` crate and plotting the temperature using the `plotters` crate.
//!
//! To run this example:
//! cargo run --example graph_data --features plotting

use std::error::Error;

use meteostat::{Frequency, LatLon, Meteostat, MeteostatFrameFilterExt};
use plotlars::{Axis, Legend, Line, Plot, Rgb, Shape, Text, TimeSeriesPlot};
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

// --- Plotting Helper Function ---

/// Plots the temperature from the DataFrame's 'datetime' and 'temp' columns.
fn plot_temperature(data: &DataFrame) {
    TimeSeriesPlot::builder()
        .data(&data)
        .x("date")
        .y("tmax")
        .additional_series(vec!["tmin"])
        .size(8)
        .colors(vec![Rgb(235, 117, 0), Rgb(69, 157, 230)])
        .lines(vec![Line::Dash, Line::Solid])
        .with_shape(true)
        .shapes(vec![Shape::Circle, Shape::Square])
        .plot_title(Text::from("Meteostat Data").font("Arial").size(18))
        .legend(&Legend::new().x(0.05).y(0.9))
        .x_title("x")
        .y_title(Text::from("y").color(Rgb(0, 0, 0)))
        .y_title2(Text::from("y2").color(Rgb(0, 0, 0)))
        .y_axis(
            &Axis::new()
                .value_color(Rgb(0,0, 0))
                .show_grid(false)
                .zero_line_color(Rgb(0, 0, 0)),
        )
        .y_axis2(
            &Axis::new()
                .axis_side(plotlars::AxisSide::Right)
                .value_color(Rgb(0,0, 0))
                .show_grid(false),
        )
        .build()
        .plot();
}
