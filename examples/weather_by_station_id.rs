use meteostat::{Meteostat, MeteostatError};

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    let client = Meteostat::new().await?;
    let berlin_station_id = "10382";

    let data = client
        .hourly()
        .station(berlin_station_id)
        .call()
        .await?
        .frame
        .collect()?;

    println!("{:#?}", data);

    Ok(())
}
