use std::io;
use std::path::{Path, PathBuf};

const CACHE_DIR_NAME: &str = "meteostat_rs_cache";

pub fn get_cache_dir() -> Result<PathBuf, io::Error> {
    dirs::cache_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Error getting cache dir."))
        .map(|p| p.join(CACHE_DIR_NAME))
}

pub async fn ensure_cache_dir_exists(path: &Path) -> Result<(), io::Error> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "Not a directory"));
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(path).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}
