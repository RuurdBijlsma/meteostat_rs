use bitflags::bitflags;

bitflags! {
    /// Flags to specify which fields are required when fetching monthly weather data.
    /// Used with `Meteostat::get_monthly`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RequiredMonthlyField: u32 {
        const TEMP_AVG            = 1 << 0;
        const TEMP_MIN_AVG        = 1 << 1;
        const TEMP_MAX_AVG        = 1 << 2;
        const PRECIPITATION_TOTAL = 1 << 3;
        const WIND_SPEED_AVG      = 1 << 4;
        const PRESSURE_AVG        = 1 << 5;
        const SUNSHINE_TOTAL      = 1 << 6;

        /// Requires all available fields.
        const ALL = Self::TEMP_AVG.bits()
                  | Self::TEMP_MIN_AVG.bits()
                  | Self::TEMP_MAX_AVG.bits()
                  | Self::PRECIPITATION_TOTAL.bits()
                  | Self::WIND_SPEED_AVG.bits()
                  | Self::PRESSURE_AVG.bits()
                  | Self::SUNSHINE_TOTAL.bits();

        /// Requires none of the optional fields.
        const NONE = 0;
    }
}

impl Default for RequiredMonthlyField {
    fn default() -> Self {
        RequiredMonthlyField::ALL
    }
}