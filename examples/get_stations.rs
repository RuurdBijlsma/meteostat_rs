use meteostat::{LatLon, Meteostat, MeteostatError};

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let lat = 50.0;
    let lon = 5.0;
    let n_results = 5;
    let max_km = 40.0;

    let meteostat = Meteostat::new().await?;
    let nearest = meteostat
        .find_stations()
        .location(LatLon(lat, lon))
        .max_distance_km(max_km)
        .station_limit(n_results)
        .call()
        .await?;

    println!("\nNearest stations to {} {}", lat, lon);
    for station in nearest {
        println!(
            "ID: {}, Name: {:?}",
            station.id,
            station.name.get("en")
        );
    }
    Ok(())
}
