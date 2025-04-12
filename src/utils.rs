use std::io;
use std::path::{Path, PathBuf};
use anyhow::Context;

const CACHE_DIR_NAME: &str = "meteostat_rs_cache";

pub fn get_cache_dir() -> anyhow::Result<PathBuf> {
    dirs::cache_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not determine system cache directory"))
        .map(|p| p.join(CACHE_DIR_NAME))
}


pub async fn ensure_cache_dir_exists(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(anyhow::anyhow!(
                        "Cache path exists but is not a directory: {}",
                        path.display()
                    ));
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            println!("Creating cache directory: {}", path.display());
            tokio::fs::create_dir_all(path).await.with_context(|| {
                format!("Failed to create cache directory: {}", path.display())
            })?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}