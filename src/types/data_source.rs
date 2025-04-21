use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Frequency {
    Hourly,
    Daily,
    Monthly,
    Climate,
}

impl Frequency {
    /// Returns the path segment for the URL and cache file name.
    pub fn path_segment(&self) -> &'static str {
        match self {
            Frequency::Hourly => "hourly",
            Frequency::Daily => "daily",
            Frequency::Monthly => "monthly",
            Frequency::Climate => "normals",
        }
    }

    /// Generates the base cache filename prefix.
    pub fn cache_file_prefix(&self) -> String {
        format!("{}-", self.path_segment())
    }

    /// Returns the expected column names for the raw CSV data.
    /// Must match the Meteostat documentation ORDER for each type.
    pub fn get_schema_column_names(&self) -> Vec<&'static str> {
        match self {
            Frequency::Hourly => vec![
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco",
            ],
            Frequency::Daily => vec![
                "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres",
                "tsun",
            ],
            Frequency::Monthly => vec![
                "year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                "pres", "tsun",
            ],
            Frequency::Climate => vec![
                "start_year",
                "end_year",
                "month",
                "tmin",
                "tmax",
                "prcp",
                "wspd",
                "pres",
                "tsun",
            ],
        }
    }
}

impl fmt::Display for Frequency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path_segment())
    }
}
