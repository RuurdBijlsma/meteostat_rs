use bitflags::bitflags;

bitflags! {
    /// Flags to specify which fields are required when fetching climate normal data.
    /// Used with `Meteostat::get_climate_normals`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RequiredClimateField: u32 {
        const TEMP_MIN_AVG      = 1 << 0;
        const TEMP_MAX_AVG      = 1 << 1;
        const PRECIPITATION_AVG = 1 << 2;
        const WIND_SPEED_AVG    = 1 << 3;
        const PRESSURE_AVG      = 1 << 4;
        const SUNSHINE_AVG      = 1 << 5;

        /// Requires all available fields.
        const ALL = Self::TEMP_MIN_AVG.bits()
                  | Self::TEMP_MAX_AVG.bits()
                  | Self::PRECIPITATION_AVG.bits()
                  | Self::WIND_SPEED_AVG.bits()
                  | Self::PRESSURE_AVG.bits()
                  | Self::SUNSHINE_AVG.bits();

        /// Requires none of the optional fields.
        const NONE = 0;
    }
}

impl Default for RequiredClimateField {
    fn default() -> Self {
        RequiredClimateField::ALL
    }
}