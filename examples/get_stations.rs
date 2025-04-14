use meteostat::stations::locate_station::StationLocator;
use meteostat::stations::error::LocateStationError;
use meteostat::utils::get_cache_dir;

#[tokio::main]
async fn main() -> Result<(), LocateStationError> {
    let lat = 50.0;
    let lon = 5.0;
    let n_results = 5;
    let max_km = 29.0;
    let cache_dir = get_cache_dir()?;

    let db = StationLocator::new(&cache_dir).await?;
    let nearest = db.query(lat, lon, n_results, max_km);
    println!("\nNearest stations to {} {}", lat, lon);
    for (station, distance) in nearest {
        println!(
            "Distance: {} ID: {}, Name: {:?}",
            distance,
            station.id,
            station.name.get("en")
        );
    }
    Ok(())
}
