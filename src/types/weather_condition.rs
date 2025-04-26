//! Defines the `WeatherCondition` enum, mapping Meteostat's numeric weather condition codes
//! to descriptive variants.

/// Represents the weather condition code reported by Meteostat.
///
/// This enum maps the integer codes found in the `coco` column of hourly data
/// to meaningful weather condition descriptions. See the
/// [Meteostat documentation](https://dev.meteostat.net/formats.html#weather-condition-codes)
/// for the official code definitions.
///
/// You can convert an integer code (e.g., from a Polars DataFrame) into this enum
/// using the [`WeatherCondition::from_i64`] method.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum WeatherCondition {
    /// Code 1: Sky is clear.
    Clear = 1,
    /// Code 2: Sky is mostly clear (1-3 Oktas).
    Fair = 2,
    /// Code 3: Sky is partly cloudy (4-5 Oktas).
    Cloudy = 3,
    /// Code 4: Sky is mostly cloudy or overcast (6-8 Oktas).
    Overcast = 4,
    /// Code 5: Fog is reducing visibility.
    Fog = 5,
    /// Code 6: Freezing fog (rime).
    FreezingFog = 6,
    /// Code 7: Light rain.
    LightRain = 7,
    /// Code 8: Moderate rain.
    Rain = 8,
    /// Code 9: Heavy rain.
    HeavyRain = 9,
    /// Code 10: Light freezing rain (glaze).
    FreezingRain = 10,
    /// Code 11: Heavy freezing rain (glaze).
    HeavyFreezingRain = 11,
    /// Code 12: Light sleet (mix of rain and snow).
    Sleet = 12,
    /// Code 13: Heavy sleet.
    HeavySleet = 13,
    /// Code 14: Light snowfall.
    LightSnowfall = 14,
    /// Code 15: Moderate snowfall.
    Snowfall = 15,
    /// Code 16: Heavy snowfall.
    HeavySnowfall = 16,
    /// Code 17: Light rain shower(s).
    RainShower = 17,
    /// Code 18: Heavy rain shower(s).
    HeavyRainShower = 18,
    /// Code 19: Light sleet shower(s).
    SleetShower = 19,
    /// Code 20: Heavy sleet shower(s).
    HeavySleetShower = 20,
    /// Code 21: Light snow shower(s).
    SnowShower = 21,
    /// Code 22: Heavy snow shower(s).
    HeavySnowShower = 22,
    /// Code 23: Lightning observed.
    Lightning = 23,
    /// Code 24: Hail observed.
    Hail = 24,
    /// Code 25: Thunderstorm observed.
    Thunderstorm = 25,
    /// Code 26: Heavy thunderstorm observed.
    HeavyThunderstorm = 26,
    /// Code 27: Storm conditions (typically high winds).
    Storm = 27,
    // Note: Codes 0 (Unknown) and 28+ are not explicitly defined by Meteostat as standard conditions.
}

impl WeatherCondition {
    /// Attempts to convert a Meteostat weather condition code (integer) into a `WeatherCondition` variant.
    ///
    /// Meteostat uses integer codes (typically 1-27) in the `coco` column of hourly data.
    /// This function maps those codes to the corresponding enum variant.
    ///
    /// # Arguments
    ///
    /// * `value`: The integer weather condition code (usually from the `coco` column).
    ///
    /// # Returns
    ///
    /// * `Some(WeatherCondition)` if the `value` corresponds to a known condition code (1-27).
    /// * `None` if the `value` is outside the range of known codes (e.g., 0 or > 27).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use meteostat::WeatherCondition;
    ///
    /// // Convert a known code
    /// let condition_code_rain = 8;
    /// let weather = WeatherCondition::from_i64(condition_code_rain);
    /// assert_eq!(weather, Some(WeatherCondition::Rain));
    ///
    /// // Handle the result using `if let`
    /// if let Some(condition) = weather {
    ///     println!("The weather condition for code {} is: {:?}", condition_code_rain, condition);
    ///     // Output: The weather condition for code 8 is: Rain
    /// }
    ///
    /// // Convert an unknown code
    /// let condition_code_unknown = 99;
    /// let weather_unknown = WeatherCondition::from_i64(condition_code_unknown);
    /// assert_eq!(weather_unknown, None);
    ///
    /// // Handle using match
    /// match WeatherCondition::from_i64(5) {
    ///     Some(WeatherCondition::Fog) => println!("It's foggy!"),
    ///     Some(cond) => println!("Weather is: {:?}", cond),
    ///     None => println!("Unknown weather code."),
    /// }
    /// // Output: It's foggy!
    /// ```
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(WeatherCondition::Clear),
            2 => Some(WeatherCondition::Fair),
            3 => Some(WeatherCondition::Cloudy),
            4 => Some(WeatherCondition::Overcast),
            5 => Some(WeatherCondition::Fog),
            6 => Some(WeatherCondition::FreezingFog),
            7 => Some(WeatherCondition::LightRain),
            8 => Some(WeatherCondition::Rain),
            9 => Some(WeatherCondition::HeavyRain),
            10 => Some(WeatherCondition::FreezingRain),
            11 => Some(WeatherCondition::HeavyFreezingRain),
            12 => Some(WeatherCondition::Sleet),
            13 => Some(WeatherCondition::HeavySleet),
            14 => Some(WeatherCondition::LightSnowfall),
            15 => Some(WeatherCondition::Snowfall),
            16 => Some(WeatherCondition::HeavySnowfall),
            17 => Some(WeatherCondition::RainShower),
            18 => Some(WeatherCondition::HeavyRainShower),
            19 => Some(WeatherCondition::SleetShower),
            20 => Some(WeatherCondition::HeavySleetShower),
            21 => Some(WeatherCondition::SnowShower),
            22 => Some(WeatherCondition::HeavySnowShower),
            23 => Some(WeatherCondition::Lightning),
            24 => Some(WeatherCondition::Hail),
            25 => Some(WeatherCondition::Thunderstorm),
            26 => Some(WeatherCondition::HeavyThunderstorm),
            27 => Some(WeatherCondition::Storm),
            _ => None, // Return None for invalid values (including 0)
        }
    }
}