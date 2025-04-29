mod clients;
mod error;
mod meteostat;
mod stations;
mod types;
mod utils;
mod weather_data;

pub use error::MeteostatError;
pub use meteostat::*;

pub use clients::climate_client::*;
pub use clients::daily_client::*;
pub use clients::hourly_client::*;
pub use clients::monthly_client::*;

pub use types::frequency::*;
pub use types::station::*;
pub use types::weather_condition::WeatherCondition;

pub use types::traits::any::any_date::AnyDate;
pub use types::traits::any::any_datetime::AnyDateTime;
pub use types::traits::any::any_month::AnyMonth;
pub use types::traits::period::date_period::DatePeriod;
pub use types::traits::period::datetime_period::DateTimePeriod;
pub use types::traits::period::month_period::MonthPeriod;
pub use types::traits::types::Month;
pub use types::traits::types::Year;

pub use types::frequency_frames::climate_frame::*;
pub use types::frequency_frames::daily_frame::*;
pub use types::frequency_frames::hourly_frame::*;
pub use types::frequency_frames::monthly_frame::*;

pub use stations::error::LocateStationError;
pub use weather_data::error::WeatherDataError;
