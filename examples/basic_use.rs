use meteostat::{LatLon, Meteostat, MeteostatError, Year};
use std::env;

#[tokio::main]
async fn main() -> Result<(), MeteostatError> {
    configure_polars_display();
    let client = Meteostat::new().await?;

    let data = client
        .hourly()
        .location(LatLon(52.520008, 13.404954))
        .max_distance_km(50.0)
        .call()
        .await?
        .get_for_period(Year(2023))?
        .frame
        .collect()?;

    println!("{:#?}", data);

    Ok(())
}

fn configure_polars_display() {
    // show every column
    env::set_var("POLARS_FMT_MAX_COLS", "-1");
    // show 20 rows
    env::set_var("POLARS_FMT_MAX_ROWS", "20");
}
