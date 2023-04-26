use core::fmt;
use core::ops::Add;

use chrono::{NaiveDateTime, TimeZone, Utc};

/// Internal UTC DateTime encoding with second resolution
#[derive(PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct DateTime(u64);

impl DateTime {
    #[cfg(feature = "std")]
    pub fn now() -> Self {
        std::time::SystemTime::now().into()
    }

    pub fn from_secs(seconds: u64) -> Self {
        Self(seconds)
    }

    pub fn as_secs(&self) -> u64 {
        self.0
    }
}

impl Add<core::time::Duration> for DateTime {
    type Output = DateTime;

    /// Add a [`core::time::Duration`] to the provided [`DateTime`]
    fn add(self, rhs: core::time::Duration) -> Self::Output {
        let secs = rhs.as_secs() as u64;
        Self(self.0 + secs)
    }
}

impl fmt::Debug for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let u = Utc.timestamp(self.0 as i64, 0);
        write!(f, "{:?}", u)
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let u = Utc.timestamp(self.0 as i64, 0);
        write!(f, "{:?}", u)
    }
}

#[cfg(feature = "std")]
impl Into<std::time::SystemTime> for DateTime {
    fn into(self) -> std::time::SystemTime {
        let u = Utc.timestamp(self.0 as i64, 0);
        u.into()
    }
}

#[cfg(feature = "std")]
impl From<std::time::SystemTime> for DateTime {
    fn from(s: std::time::SystemTime) -> Self {
        let when = s.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        Self(when.as_secs())
    }
}

#[cfg(feature = "std")]
impl From<NaiveDateTime> for DateTime {
    fn from(s: NaiveDateTime) -> Self {
        Self(s.timestamp_millis() as u64 / 1000)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn date_time_system_conversions() {
        let d = DateTime::now();

        let s: std::time::SystemTime = d.clone().into();

        let d2: DateTime = s.clone().into();

        assert_eq!(d, d2);
    }

    #[test]
    fn date_time_second_conversions() {
        let d = DateTime(1553238684);

        assert_eq!(1553238684, d.as_secs());

        let d1 = DateTime::from_secs(1553238684);

        assert_eq!(d, d1);
    }
}
