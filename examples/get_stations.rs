use meteostat::stations::locate_station::StationLocator;
use meteostat::stations::station_error::LocateStationError;

#[tokio::main]
async fn main() -> Result<(), LocateStationError> {
    let lat = 50.;
    let lon = 5.;
    let n_results = 5;

    let db = StationLocator::init().await?;
    let nearest = db.query(lat, lon, n_results);
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
