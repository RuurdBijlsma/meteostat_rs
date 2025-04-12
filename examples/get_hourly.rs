use anyhow::Result;
use meteostat_rs::get_hourly::get_hourly;

#[tokio::main]
async fn main() -> Result<()> {
    let df = get_hourly("10637").await?;
    println!("DataFrame shape: {:?}", df.shape());
    Ok(())
}
