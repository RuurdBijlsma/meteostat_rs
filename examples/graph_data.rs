//! examples/graph_data.rs
//!
//! This example demonstrates fetching hourly weather data for a specific location
//! using the `meteostat` crate and plotting the temperature using the `plotters` crate.
//!
//! To run this example:
//! cargo run --example graph_data --features plotting

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::error::Error;

use meteostat::{Frequency, LatLon, Meteostat, MeteostatFrameFilterExt};
use plotters::coord::types::RangedDateTime;
use plotters::prelude::*;
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Fetching weather data from Meteostat...");

    // 1. Create a Meteostat client
    let meteostat = Meteostat::new().await?;

    // 2. Define location and time range
    let location = LatLon(52.118641, 5.185589); // de Bilt
    let start_utc = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end_utc = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

    // 3. Build and execute the query using meteostat
    let weather_data: DataFrame = meteostat
        .from_location() // Query by geographic coordinates
        .location(location) // Set the location
        .frequency(Frequency::Hourly) // Request hourly data
        .call() // Execute the API call (returns LazyFrame)
        .await?
        .filter_hourly(start_utc, end_utc) // Filter the LazyFrame by date range
        .collect()?; // Collect the results into a DataFrame

    // 4. Plot the data
    println!("Generating temperature plot...");
    plot_temperature(&weather_data, "temperature_plot.png")?;
    println!("Plot saved to temperature_plot.png");

    Ok(())
}

// --- Plotting Helper Function ---

/// Plots the temperature from the DataFrame's 'datetime' and 'temp' columns.
fn plot_temperature(data: &DataFrame, output_file: &str) -> Result<(), Box<dyn Error>> {
    // --- Data Preparation ---

    // Extract columns - Use expect as we assume collect() succeeded and columns exist
    // If this example fails here, it might indicate an issue upstream in meteostat data processing.
    let dt_col = data
        .column("datetime")
        .expect("DataFrame should contain 'datetime' column")
        .datetime()?;
    let temp_col = data
        .column("temp")
        .expect("DataFrame should contain 'temp' column")
        .f64()?;

    // Convert to Vec<(NaiveDateTime, f64)> for plotting, filtering out nulls
    let plot_data: Vec<(NaiveDateTime, f64)> = dt_col
        .downcast_iter()
        .flat_map(|chunk| chunk.into_iter()) // Get Option<i64> timestamps
        .zip(temp_col.into_iter()) // Zip with Option<f64> temperatures
        .filter_map(|(dt_opt, temp_opt)| {
            // Keep only pairs where both datetime (ts) and temp are non-null
            // and convert timestamp (assumed ms UTC) to NaiveDateTime
            match (dt_opt, temp_opt) {
                (Some(ts), Some(temp)) => {
                    timestamp_ms_to_naive_utc(*ts).map(|naive_dt| (naive_dt, temp))
                }
                _ => None,
            }
        })
        .collect();

    // --- Plot Setup ---
    let root = BitMapBackend::new(output_file, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    // Find data ranges for axes
    // This assumes plot_data is not empty, checked above.
    let (min_dt, max_dt) = plot_data
        .iter()
        .fold((plot_data[0].0, plot_data[0].0), |(min, max), (dt, _)| {
            (min.min(*dt), max.max(*dt))
        });
    let (min_temp, max_temp) = plot_data.iter().fold(
        (f64::INFINITY, f64::NEG_INFINITY),
        |(min, max), (_, temp)| (min.min(*temp), max.max(*temp)),
    );
    // Add a little padding to the y-axis if min/max are different
    let y_padding = if (max_temp - min_temp).abs() > 1e-6 {
        (max_temp - min_temp) * 0.1
    } else {
        1.0
    };
    let y_axis_start = min_temp - y_padding;
    let y_axis_end = max_temp + y_padding;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Hourly Temperature",
            ("sans-serif", 5.percent_height()), // Relative font size
        )
        .margin(1.percent()) // Relative margin
        .x_label_area_size(8.percent_height()) // Relative label area
        .y_label_area_size(8.percent_width()) // Relative label area
        .build_cartesian_2d(
            RangedDateTime::from(min_dt..max_dt), // Use plotters DateTime range
            y_axis_start..y_axis_end,             // Use f64 range
        )?;

    // Configure mesh (grid lines and labels)
    chart
        .configure_mesh()
        .x_desc("Date/Time")
        .y_desc("Temperature (°C)")
        .x_label_formatter(&|dt: &NaiveDateTime| dt.format("%Y-%m-%d %Hh").to_string()) // Simpler format
        .light_line_style(BLACK.mix(0.15)) // Lighter grid lines
        .draw()?;

    // --- Draw Data ---
    chart.draw_series(LineSeries::new(plot_data.into_iter(), BLUE))?;

    // Finalize plot
    root.present()?; // Ensure the plot is saved

    Ok(())
}

// --- Timestamp Conversion Helper (remains the same) ---
fn timestamp_ms_to_naive_utc(timestamp: i64) -> Option<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp_millis(timestamp).map(|dt| dt.naive_utc())
}
