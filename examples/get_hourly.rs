use anyhow::Result;
use meteostat::get_hourly_lazy::get_hourly;

#[tokio::main]
async fn main() -> Result<()> {
    let _df = get_hourly("10637").await?;
    Ok(())
}
