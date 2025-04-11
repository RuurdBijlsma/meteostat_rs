// examples/fetch_london.rs
use meteostat_rs::{get_nearby_hourly_data, MeteostatError};
use geo::point;

fn main() -> Result<(), MeteostatError> {
    // Initialize logger to see informational messages
    // Set RUST_LOG=info (or debug, trace) environment variable to control level
    env_logger::init();

    // Define a location (Longitude, Latitude - using geo conventions)
    // Example: Near London, UK
    let location = point!(x: -0.1278, y: 51.5074);

    println!("Fetching hourly data for nearest station to Lon: {}, Lat: {}", location.x(), location.y());

    match get_nearby_hourly_data(location) {
        Ok(df) => {
            println!("Successfully fetched data!");
            println!("Shape: {:?}", df.shape());
            println!("Sample Data (last 5 rows):");
            // Print the last 5 rows for a quick look
            println!("{}", df.tail(Some(5)));

            // Example: Calculate average temperature
            if let Ok(temp_col) = df.column("temp") {
                if let Ok(avg_temp) = temp_col.mean() {
                    println!("Average temperature: {:.2}°C", avg_temp);
                }
            }

        }
        Err(e) => {
            eprintln!("Error: {}", e);
            // You might want to return the error or handle it differently
            return Err(e);
        }
    }

    Ok(())
}