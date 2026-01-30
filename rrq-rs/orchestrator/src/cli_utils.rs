use chrono::{DateTime, Local, TimeZone, Utc};
use serde_json::Value;

pub fn parse_timestamp(raw: &str) -> Option<f64> {
    if raw.is_empty() {
        return None;
    }
    if let Ok(value) = raw.parse::<f64>() {
        return Some(value);
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
}

pub fn format_timestamp(raw: Option<&str>) -> String {
    let Some(value) = raw.and_then(parse_timestamp) else {
        return "N/A".to_string();
    };
    let dt = Local.timestamp_opt(value as i64, 0).single();
    let Some(dt) = dt else {
        return value.to_string();
    };
    let now = Local::now();
    let diff = now.signed_duration_since(dt);
    let seconds = diff.num_seconds();
    if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3600)
    } else {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

pub fn format_duration(seconds: Option<f64>) -> String {
    let Some(seconds) = seconds else {
        return "N/A".to_string();
    };
    if seconds < 0.001 {
        format!("{:.0}μs", seconds * 1_000_000.0)
    } else if seconds < 1.0 {
        format!("{:.1}ms", seconds * 1000.0)
    } else if seconds < 60.0 {
        format!("{seconds:.1}s")
    } else if seconds < 3600.0 {
        let minutes = (seconds / 60.0).floor() as i64;
        let secs = (seconds % 60.0).floor() as i64;
        format!("{minutes}m {secs}s")
    } else {
        let hours = (seconds / 3600.0).floor() as i64;
        let minutes = ((seconds % 3600.0) / 60.0).floor() as i64;
        format!("{hours}h {minutes}m")
    }
}

pub fn truncate(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }
    let keep = max_len.saturating_sub(3);
    format!("{}...", &value[..keep])
}

pub fn parse_json(raw: Option<&str>) -> Option<Value> {
    let raw = raw?;
    if raw.eq_ignore_ascii_case("null") {
        return None;
    }
    serde_json::from_str(raw).ok()
}

pub fn format_status(raw: Option<&str>) -> String {
    raw.unwrap_or("UNKNOWN").to_uppercase()
}

pub fn to_utc_rfc3339(epoch_seconds: f64) -> String {
    let seconds = epoch_seconds.floor() as i64;
    let nanos = ((epoch_seconds - epoch_seconds.floor()) * 1_000_000_000.0) as u32;
    let dt = Utc.timestamp_opt(seconds, nanos).single();
    dt.map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| epoch_seconds.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_timestamp_handles_numeric_and_rfc3339() {
        assert_eq!(parse_timestamp(""), None);
        assert_eq!(parse_timestamp("0"), Some(0.0));
        let parsed = parse_timestamp("2024-01-01T00:00:00Z");
        assert!(parsed.is_some());
    }

    #[test]
    fn format_timestamp_handles_missing() {
        assert_eq!(format_timestamp(None), "N/A");
        let formatted = format_timestamp(Some("0"));
        assert!(!formatted.is_empty());
        assert_ne!(formatted, "N/A");
    }

    #[test]
    fn format_duration_formats_ranges() {
        assert_eq!(format_duration(None), "N/A");
        assert_eq!(format_duration(Some(0.0000004)), "0μs");
        assert_eq!(format_duration(Some(0.0004)), "400μs");
        assert_eq!(format_duration(Some(0.5)), "500.0ms");
        assert_eq!(format_duration(Some(2.5)), "2.5s");
        assert_eq!(format_duration(Some(90.0)), "1m 30s");
    }

    #[test]
    fn truncate_and_status() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("longer_than", 6), "lon...");
        assert_eq!(format_status(Some("pending")), "PENDING");
        assert_eq!(format_status(None), "UNKNOWN");
    }

    #[test]
    fn parse_json_and_rfc3339() {
        assert_eq!(parse_json(None), None);
        assert_eq!(parse_json(Some("null")), None);
        assert!(parse_json(Some("{\"a\":1}")).is_some());
        assert_eq!(to_utc_rfc3339(0.0), "1970-01-01T00:00:00+00:00");
    }
}
