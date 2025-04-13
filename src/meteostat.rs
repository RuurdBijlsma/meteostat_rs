use crate::stations::locate_station::StationLocator;

pub struct Meteostat{
    station_cache: StationLocator,
}

impl Meteostat{
    pub async fn new(){
        // let station_cache = StationCache::init().await?;
        //
        // return Meteostat{
        //     station_cache,
        // }
    }
}