use anyhow::Result;
use meteostat::get_stations::StationCache;

#[tokio::main]
async fn main() -> Result<()> {
    let lat = 50.;
    let lon = 5.;
    let n_results =5;

    let db = StationCache::init().await?;
    let nearest = db.query(lat, lon, n_results);
    println!("\nNearest stations to {} {}", lat, lon);
    for station in nearest {
        println!("  ID: {}, Name: {:?}", station.id, station.name.get("en"));
    }
    Ok(())
}
