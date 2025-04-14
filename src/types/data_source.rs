use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataSource {
    Hourly,
    Daily,
    Monthly,
    Normals,
}

impl DataSource {
    /// Returns the path segment for the URL and cache file name.
    pub fn path_segment(&self) -> &'static str {
        match self {
            DataSource::Hourly => "hourly",
            DataSource::Daily => "daily",
            DataSource::Monthly => "monthly",
            DataSource::Normals => "normals",
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
            DataSource::Hourly => vec![
                "date", "hour", "temp", "dwpt", "rhum", "prcp", "snow", "wdir", "wspd", "wpgt",
                "pres", "tsun", "coco",
            ],
            DataSource::Daily => vec![
                "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres",
                "tsun",
            ],
            DataSource::Monthly => vec![
                "year", "month", "tavg", "tmin", "tmax", "prcp", "wspd",
                "pres", "tsun",
            ],
            DataSource::Normals => vec![
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

impl fmt::Display for DataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path_segment())
    }
}
