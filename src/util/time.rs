//! Module containing helper functions to manipulate time
use chrono::{SecondsFormat, TimeZone, Utc};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;

/// Returns milliseconds since UNIX Epoch
pub fn now_msec() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("Fix your clock :o")
		.as_millis() as u64
}

/// Increment logical clock
pub fn increment_logical_clock(prev: u64) -> u64 {
	std::cmp::max(prev + 1, now_msec())
}

/// Increment two logical clocks
pub fn increment_logical_clock_2(prev: u64, prev2: u64) -> u64 {
	std::cmp::max(prev2 + 1, std::cmp::max(prev + 1, now_msec()))
}

/// Convert a timestamp represented as milliseconds since UNIX Epoch to
/// its RFC3339 representation, such as "2021-01-01T12:30:00Z"
pub fn msec_to_rfc3339(msecs: u64) -> String {
	let secs = msecs as i64 / 1000;
	let nanos = (msecs as i64 % 1000) as u32 * 1_000_000;
	let timestamp = Utc.timestamp_opt(secs, nanos).unwrap();
	timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Parse an RFC3339 timestamp string to milliseconds since UNIX Epoch
pub fn rfc3339_to_msec(s: &str) -> Result<u64, Error> {
	let dt = chrono::DateTime::parse_from_rfc3339(s)
		.map_err(|e| Error::Message(format!("invalid RFC3339 timestamp: {}", e)))?;
	Ok(dt.timestamp_millis() as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_rfc3339_to_msec() {
		let msec = rfc3339_to_msec("2025-06-15T12:30:00.000Z").unwrap();
		assert_eq!(msec, 1749990600000);
	}

	#[test]
	fn test_rfc3339_to_msec_with_tz() {
		let msec = rfc3339_to_msec("2025-01-01T00:00:00+05:00").unwrap();
		let msec_utc = rfc3339_to_msec("2024-12-31T19:00:00Z").unwrap();
		assert_eq!(msec, msec_utc);
	}

	#[test]
	fn test_rfc3339_to_msec_invalid() {
		assert!(rfc3339_to_msec("not-a-date").is_err());
	}

	#[test]
	fn test_rfc3339_to_msec_empty() {
		assert!(rfc3339_to_msec("").is_err());
	}

	#[test]
	fn test_rfc3339_to_msec_millis_precision() {
		let msec = rfc3339_to_msec("2025-06-15T12:30:00.123Z").unwrap();
		assert_eq!(msec, 1749990600123);
	}

	#[test]
	fn test_rfc3339_to_msec_leap_year() {
		let msec = rfc3339_to_msec("2024-02-29T00:00:00Z").unwrap();
		assert_eq!(msec, 1709164800000);
	}
}
