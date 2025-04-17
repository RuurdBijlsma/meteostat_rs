use bitflags::bitflags;

bitflags! {
    /// Flags to specify which fields are required when fetching daily weather data.
    /// Used with `Meteostat::get_daily`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RequiredDailyField: u32 {
        const TEMP_AVG            = 1 << 0;
        const TEMP_MIN            = 1 << 1;
        const TEMP_MAX            = 1 << 2;
        const PRECIPITATION       = 1 << 3;
        const SNOW_DEPTH          = 1 << 4;
        const WIND_DIRECTION_AVG  = 1 << 5;
        const WIND_SPEED_AVG      = 1 << 6;
        const PEAK_WIND_GUST      = 1 << 7;
        const PRESSURE_AVG        = 1 << 8;
        const SUNSHINE_TOTAL      = 1 << 9;

        /// Requires all available fields.
        const ALL = Self::TEMP_AVG.bits()
                  | Self::TEMP_MIN.bits()
                  | Self::TEMP_MAX.bits()
                  | Self::PRECIPITATION.bits()
                  | Self::SNOW_DEPTH.bits()
                  | Self::WIND_DIRECTION_AVG.bits()
                  | Self::WIND_SPEED_AVG.bits()
                  | Self::PEAK_WIND_GUST.bits()
                  | Self::PRESSURE_AVG.bits()
                  | Self::SUNSHINE_TOTAL.bits();

        /// Requires none of the optional fields.
        const NONE = 0;
    }
}

impl Default for RequiredDailyField {
    fn default() -> Self {
        RequiredDailyField::ALL
    }
}
