use bitflags::bitflags;

bitflags! {
    /// Flags to specify which fields are required when fetching hourly weather data.
    /// Used with `Meteostat::get_hourly`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)] // Add necessary derives
    pub struct RequiredHourlyField: u32 {
        const TEMPERATURE         = 1 << 0;
        const DEW_POINT           = 1 << 1;
        const RELATIVE_HUMIDITY   = 1 << 2;
        const PRECIPITATION       = 1 << 3;
        const SNOW                = 1 << 4;
        const WIND_DIRECTION      = 1 << 5;
        const WIND_SPEED          = 1 << 6;
        const PEAK_WIND_GUST      = 1 << 7;
        const PRESSURE            = 1 << 8;
        const SUNSHINE            = 1 << 9;
        const CONDITION           = 1 << 10;

        /// Requires all available fields.
        const ALL = Self::TEMPERATURE.bits()
                  | Self::DEW_POINT.bits()
                  | Self::RELATIVE_HUMIDITY.bits()
                  | Self::PRECIPITATION.bits()
                  | Self::SNOW.bits()
                  | Self::WIND_DIRECTION.bits()
                  | Self::WIND_SPEED.bits()
                  | Self::PEAK_WIND_GUST.bits()
                  | Self::PRESSURE.bits()
                  | Self::SUNSHINE.bits()
                  | Self::CONDITION.bits();

        /// Requires none of the optional fields (useful only if you just need existence).
        const NONE = 0;
    }
}

// Optional: Provide a default if desired, e.g., requiring all fields by default.
impl Default for RequiredHourlyField {
    fn default() -> Self {
        RequiredHourlyField::ALL
    }
}