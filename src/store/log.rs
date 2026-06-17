//! Log-category value shape: append-only, server-stamped, bounded by retention.
//!
//! Pure (no I/O, no wall clock): callers inject `now` so the logic is
//! deterministic and unit-testable. The store layer wires this into the normal
//! versioned write path; the server layer enforces the kind boundary.

use serde_json::{json, Value};

/// Reserved key marking a category as a bounded append-only log.
pub const KIND_KEY: &str = "_kind";
pub const LOG_KIND: &str = "log";
const RETENTION_KEY: &str = "_retention";
const ENTRIES_KEY: &str = "entries";

/// How many entries / how much history a log keeps. At least one bound should
/// be effective; the default caps by count so a log can never grow unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub max_entries: Option<u64>,
    pub max_age_days: Option<u64>,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_entries: Some(200),
            max_age_days: None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogError {
    /// Existing category is not a log (`_kind` != "log").
    NotALog,
    /// `_kind` is "log" but the value is structurally malformed.
    MalformedLog,
}

/// True if `v` is a log-kind category value. Requires BOTH the `_kind` marker
/// and an `entries` array, so a plain memory category that merely happens to
/// carry a user key `"_kind": "log"` is not misclassified (and bricked).
pub fn is_log(v: &Value) -> bool {
    v.get(KIND_KEY).and_then(|k| k.as_str()) == Some(LOG_KIND)
        && v.get(ENTRIES_KEY).map(Value::is_array).unwrap_or(false)
}

/// Append `data` as a new entry stamped at `now` (RFC3339 UTC) into the log
/// built from `existing` (None = create a fresh log), then apply `retention`.
///
/// Errors if `existing` is present but not a log, so a memory category is never
/// silently converted.
pub fn append_entry(
    existing: Option<&Value>,
    data: Value,
    retention: Retention,
    now: &str,
) -> Result<Value, LogError> {
    let mut value = match existing {
        None => json!({
            KIND_KEY: LOG_KIND,
            RETENTION_KEY: retention_to_json(retention),
            ENTRIES_KEY: [],
        }),
        Some(v) if is_log(v) => v.clone(),
        Some(_) => return Err(LogError::NotALog),
    };

    // Record the effective retention so the file stays self-describing.
    value[RETENTION_KEY] = retention_to_json(retention);

    let entries = value
        .get_mut(ENTRIES_KEY)
        .and_then(Value::as_array_mut)
        .ok_or(LogError::MalformedLog)?;
    let id = next_id(entries);
    entries.push(json!({ "id": id, "ts": now, "data": data }));
    prune_entries(entries, retention, now);

    Ok(value)
}

/// Monotonically increasing per-log entry id: one past the highest existing id
/// (0 for a fresh log). Ids are never reused after pruning, so they remain a
/// stable handle for future cross-device union/tombstone merge.
fn next_id(entries: &[Value]) -> u64 {
    entries
        .iter()
        .filter_map(|e| e.get("id").and_then(Value::as_u64))
        .max()
        .map(|m| m + 1)
        .unwrap_or(0)
}

fn retention_to_json(r: Retention) -> Value {
    json!({ "max_entries": r.max_entries, "max_age_days": r.max_age_days })
}

/// The retention stored in a log value, if present and with at least one
/// effective cap. A stored policy with both caps absent is treated as "no
/// usable policy" (None) so the caller falls back to the bounded default
/// rather than growing unbounded.
pub fn stored_retention(v: &Value) -> Option<Retention> {
    let r = v.get(RETENTION_KEY)?;
    let ret = Retention {
        max_entries: r.get("max_entries").and_then(Value::as_u64),
        max_age_days: r.get("max_age_days").and_then(Value::as_u64),
    };
    if ret.max_entries.is_none() && ret.max_age_days.is_none() {
        return None;
    }
    Some(ret)
}

/// Drop entries older than `max_age_days`, then cap to the newest `max_entries`.
/// Entries are kept in append order (oldest first), so count-pruning drains the
/// front. An entry whose `ts` cannot be parsed is kept (never lose data on a
/// parse error).
fn prune_entries(entries: &mut Vec<Value>, retention: Retention, now: &str) {
    if let Some(days) = retention.max_age_days {
        if let Ok(now_dt) = chrono::DateTime::parse_from_rfc3339(now) {
            let cutoff = now_dt - chrono::Duration::days(days as i64);
            entries.retain(|e| {
                match e
                    .get("ts")
                    .and_then(Value::as_str)
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                {
                    Some(ts) => ts >= cutoff,
                    None => true,
                }
            });
        }
    }
    if let Some(max) = retention.max_entries {
        // Never drop the entry that was just appended: a log always retains at
        // least its newest entry, so max_entries=0 cannot cause silent loss.
        let max = (max as usize).max(1);
        if entries.len() > max {
            entries.drain(0..entries.len() - max);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_fresh_log_with_one_stamped_entry() {
        let out = append_entry(
            None,
            json!({"msg": "hi"}),
            Retention::default(),
            "2026-06-17T09:00:00Z",
        )
        .expect("append should succeed");

        assert!(is_log(&out), "result must be a log");
        let entries = out["entries"].as_array().expect("entries array");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["ts"], "2026-06-17T09:00:00Z");
        assert_eq!(entries[0]["data"], json!({"msg": "hi"}));
    }

    #[test]
    fn rejects_append_to_non_log_category() {
        let memory_cat = json!({ "current_state": "ok", "foo": 1 });
        let err = append_entry(
            Some(&memory_cat),
            json!({}),
            Retention::default(),
            "2026-06-17T09:00:00Z",
        )
        .expect_err("must refuse to convert a memory category");
        assert_eq!(err, LogError::NotALog);
    }

    #[test]
    fn prunes_oldest_beyond_max_entries() {
        let ret = Retention {
            max_entries: Some(2),
            max_age_days: None,
        };
        let mut log = append_entry(None, json!({"n": 1}), ret, "2026-06-17T09:00:00Z").unwrap();
        log = append_entry(Some(&log), json!({"n": 2}), ret, "2026-06-17T09:01:00Z").unwrap();
        log = append_entry(Some(&log), json!({"n": 3}), ret, "2026-06-17T09:02:00Z").unwrap();

        let entries = log["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2, "cap at max_entries");
        assert_eq!(entries[0]["data"], json!({"n": 2}), "oldest (n:1) dropped");
        assert_eq!(entries[1]["data"], json!({"n": 3}), "newest kept");
    }

    #[test]
    fn prunes_entries_older_than_max_age() {
        let ret = Retention {
            max_entries: None,
            max_age_days: Some(7),
        };
        // an old entry (20 days before "now") and a recent one
        let mut log =
            append_entry(None, json!({"old": true}), ret, "2026-05-28T09:00:00Z").unwrap();
        log = append_entry(
            Some(&log),
            json!({"recent": true}),
            ret,
            "2026-06-17T09:00:00Z",
        )
        .unwrap();

        let entries = log["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "stale entry pruned");
        assert_eq!(entries[0]["data"], json!({"recent": true}));
    }

    #[test]
    fn reads_back_stored_retention() {
        let ret = Retention {
            max_entries: Some(50),
            max_age_days: Some(14),
        };
        let log = append_entry(None, json!({}), ret, "2026-06-17T09:00:00Z").unwrap();
        assert_eq!(stored_retention(&log), Some(ret));
        assert_eq!(
            stored_retention(&json!({"_kind": "log"})),
            None,
            "no _retention -> None"
        );
        assert_eq!(stored_retention(&json!({"a": 1})), None, "non-log -> None");
    }

    #[test]
    fn zero_max_entries_still_keeps_the_just_appended_entry() {
        let ret = Retention {
            max_entries: Some(0),
            max_age_days: None,
        };
        let log = append_entry(None, json!({"n": 1}), ret, "2026-06-17T09:00:00Z").unwrap();
        let entries = log["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "an append must never drop its own entry");
        assert_eq!(entries[0]["data"], json!({"n": 1}));
    }

    #[test]
    fn is_log_requires_entries_array_not_just_kind() {
        // A user memory category that merely happens to carry a "_kind":"log"
        // string must NOT be misclassified as a log.
        assert!(!is_log(&json!({ "_kind": "log", "note": "user data" })));
        assert!(!is_log(&json!({ "_kind": "log" })));
        let real = append_entry(
            None,
            json!({}),
            Retention::default(),
            "2026-06-17T09:00:00Z",
        )
        .unwrap();
        assert!(is_log(&real));
    }

    #[test]
    fn stored_retention_is_none_when_both_caps_absent() {
        // Hand-edited / corrupt log with no effective bound must fall back to
        // the default (i.e. resolve to None here), never stay unbounded.
        let both_null = json!({ "_kind": "log", "_retention": { "max_entries": null, "max_age_days": null }, "entries": [] });
        assert_eq!(stored_retention(&both_null), None);
    }

    #[test]
    fn entries_carry_monotonic_ids() {
        let ret = Retention {
            max_entries: Some(2),
            max_age_days: None,
        };
        let mut log = append_entry(None, json!({"n": 0}), ret, "2026-06-17T09:00:00Z").unwrap();
        log = append_entry(Some(&log), json!({"n": 1}), ret, "2026-06-17T09:01:00Z").unwrap();
        log = append_entry(Some(&log), json!({"n": 2}), ret, "2026-06-17T09:02:00Z").unwrap();
        log = append_entry(Some(&log), json!({"n": 3}), ret, "2026-06-17T09:03:00Z").unwrap();

        let entries = log["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        // ids keep climbing even though older entries were pruned (no reuse).
        assert_eq!(entries[0]["id"], json!(2));
        assert_eq!(entries[1]["id"], json!(3));
    }
}
