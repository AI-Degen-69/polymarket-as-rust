//! Standardized log folder layout, UTC timestamp naming, and capture finalization.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};

use crate::feeds_report::canonical_target_path;

/// Default directory for finalized live feed captures.
pub(crate) const CAPTURES_DIR: &str = "logs/captures";

/// Format a `SystemTime` as UTC `DD-MM_HH-MM-SSZ` (e.g., `22-11_11-00-00Z`).
pub(crate) fn format_utc_stamp(now: SystemTime) -> String {
    let dt: DateTime<Utc> = now.into();
    dt.format("%d-%m_%H-%M-%SZ").to_string()
}

/// Build the default feature JSONL path `logs/<feature>/<DD-MM_HH-MM-SSZ>.jsonl`.
pub(crate) fn default_feature_log_path(feature: &str) -> PathBuf {
    default_feature_log_path_at(feature, SystemTime::now())
}

pub(crate) fn default_feature_log_path_at(feature: &str, now: SystemTime) -> PathBuf {
    let stamp = format_utc_stamp(now);
    PathBuf::from(format!("logs/{feature}/{stamp}.jsonl"))
}

/// Build the default sandbox JSONL path `logs/sandbox/<DD-MM_HH-MM-SSZ>_<action>.jsonl`.
pub(crate) fn default_sandbox_log_path(action: &str) -> PathBuf {
    default_sandbox_log_path_at(action, SystemTime::now())
}

pub(crate) fn default_sandbox_log_path_at(action: &str, now: SystemTime) -> PathBuf {
    let stamp = format_utc_stamp(now);
    PathBuf::from(format!("logs/sandbox/{stamp}_{action}.jsonl"))
}

/// Format a duration cleanly for capture filenames (e.g., `20s`, `5m`, `1h`, `1m30s`, `500ms`).
pub(crate) fn format_duration_compact(d: Duration) -> String {
    if d.as_secs() == 0 && d.subsec_millis() > 0 {
        return format!("{}ms", d.subsec_millis());
    }
    let total_secs = (d.as_millis() + 500) / 1_000;
    let hours = total_secs / 3_600;
    let mins = (total_secs % 3_600) / 60;
    let secs = total_secs % 60;

    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if mins > 0 {
        out.push_str(&format!("{mins}m"));
    }
    if secs > 0 || (hours == 0 && mins == 0) {
        out.push_str(&format!("{secs}s"));
    }
    out
}

const MAX_COLLISION_SUFFIX: usize = 10_000;

/// Construct the in-progress capture path `<captures_dir>/<stamp>_<target-dur>.inprogress.jsonl`.
#[cfg(test)]
pub(crate) fn capture_inprogress_path(
    captures_dir: &Path,
    start_stamp: &str,
    target_dur: Duration,
) -> PathBuf {
    capture_inprogress_path_with_suffix(captures_dir, start_stamp, target_dur, 1)
}

fn capture_inprogress_path_with_suffix(
    captures_dir: &Path,
    start_stamp: &str,
    target_dur: Duration,
    suffix: usize,
) -> PathBuf {
    let dur = format_duration_compact(target_dur);
    if suffix <= 1 {
        captures_dir.join(format!("{start_stamp}_{dur}.inprogress.jsonl"))
    } else {
        captures_dir.join(format!("{start_stamp}_{dur}-{suffix}.inprogress.jsonl"))
    }
}

/// Atomically reserve a unique `.inprogress.jsonl` capture file, appending `-2`, `-3`, ... on
/// same-second collisions so concurrent captures never truncate each other.
pub(crate) fn reserve_capture_inprogress_path(
    captures_dir: &Path,
    start_stamp: &str,
    target_dur: Duration,
) -> Result<PathBuf, std::io::Error> {
    std::fs::create_dir_all(captures_dir)?;
    for suffix in 1..=MAX_COLLISION_SUFFIX {
        let candidate =
            capture_inprogress_path_with_suffix(captures_dir, start_stamp, target_dur, suffix);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        ErrorKind::AlreadyExists,
        format!(
            "exhausted {MAX_COLLISION_SUFFIX} capture file collision suffixes in `{}`",
            captures_dir.display()
        ),
    ))
}

fn capture_final_path_with_suffix(
    inprogress_path: &Path,
    start_stamp: &str,
    actual_dur: Duration,
    windows_touched: usize,
    suffix: usize,
) -> PathBuf {
    let dur = format_duration_compact(actual_dur);
    let windows = windows_touched.max(1);
    if suffix <= 1 {
        inprogress_path.with_file_name(format!("{start_stamp}_{dur}_{windows}w.jsonl"))
    } else {
        inprogress_path.with_file_name(format!("{start_stamp}_{dur}_{windows}w-{suffix}.jsonl"))
    }
}

/// Atomically finalize an `.inprogress.jsonl` capture file to `<stamp>_<actual-dur>_<N>w.jsonl`
/// without overwriting an existing capture (appending `-2`, `-3`, ... on collision).
pub(crate) fn finalize_capture_file(
    inprogress_path: &Path,
    start_stamp: &str,
    actual_dur: Duration,
    windows_touched: usize,
) -> Result<PathBuf, std::io::Error> {
    for suffix in 1..=MAX_COLLISION_SUFFIX {
        let final_path = capture_final_path_with_suffix(
            inprogress_path,
            start_stamp,
            actual_dur,
            windows_touched,
            suffix,
        );
        match std::fs::hard_link(inprogress_path, &final_path) {
            Ok(()) => {
                std::fs::remove_file(inprogress_path)?;
                return Ok(final_path);
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) if err.kind() == ErrorKind::Unsupported => {
                if final_path.exists() {
                    continue;
                }
                std::fs::rename(inprogress_path, &final_path)?;
                return Ok(final_path);
            }
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        ErrorKind::AlreadyExists,
        format!(
            "exhausted {MAX_COLLISION_SUFFIX} finalized capture collision suffixes for `{}`",
            inprogress_path.display()
        ),
    ))
}

/// Pick the newest finalized `.jsonl` capture in `captures_dir` by `LastWriteTime` (modified time).
/// Ignores `*.inprogress.jsonl` files.
pub(crate) fn find_latest_capture(captures_dir: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let entries = std::fs::read_dir(captures_dir).map_err(|err| {
        format!(
            "no captures found in `{}` ({err}); run `poly-as feeds --discover` or pass `--input <path>`",
            captures_dir.display()
        )
    })?;

    let mut newest: Option<(SystemTime, PathBuf)> = None;
    for entry_res in entries {
        let entry = entry_res?;
        let path = entry.path();
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".jsonl") || name.ends_with(".inprogress.jsonl") {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        let replace = match &newest {
            None => true,
            Some((best_time, best_path)) => {
                modified > *best_time || (modified == *best_time && path > *best_path)
            }
        };
        if replace {
            newest = Some((modified, path));
        }
    }

    newest.map(|(_, p)| p).ok_or_else(|| {
        format!(
            "no finalized .jsonl captures found in `{}` (run `poly-as feeds --discover` or pass `--input <path>`)",
            captures_dir.display()
        )
        .into()
    })
}

/// Resolve `--input` for `--source replay`, auto-picking the newest capture in `logs/captures/`
/// when `--input` is omitted, and rejecting `--input == --jsonl`.
pub(crate) fn resolve_replay_input(
    input: Option<&Path>,
    jsonl: &Path,
) -> Result<PathBuf, Box<dyn Error>> {
    resolve_replay_input_in(input, jsonl, Path::new(CAPTURES_DIR))
}

pub(crate) fn resolve_replay_input_in(
    input: Option<&Path>,
    jsonl: &Path,
    captures_dir: &Path,
) -> Result<PathBuf, Box<dyn Error>> {
    let input_path = match input {
        Some(path) => path.to_path_buf(),
        None => find_latest_capture(captures_dir)?,
    };
    if !input_path.exists() {
        return Err(format!("replay input `{}` does not exist", input_path.display()).into());
    }
    if canonical_target_path(&input_path)? == canonical_target_path(jsonl)? {
        return Err("refusing to overwrite `--input` replay log with `--jsonl` output".into());
    }
    Ok(input_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("poly_as_log_paths_{tag}_{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn utc_stamp_formats_dd_mm_hh_mm_ss_z() {
        // 2026-11-22T11:00:00Z = 1_795_345_200 seconds since Unix epoch
        let ts = DateTime::parse_from_rfc3339("2026-11-22T11:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sys: SystemTime = ts.into();
        assert_eq!(format_utc_stamp(sys), "22-11_11-00-00Z");

        let path = default_feature_log_path_at("lifecycle", sys);
        assert_eq!(path, PathBuf::from("logs/lifecycle/22-11_11-00-00Z.jsonl"));

        let sandbox_health = default_sandbox_log_path_at("health", sys);
        assert_eq!(
            sandbox_health,
            PathBuf::from("logs/sandbox/22-11_11-00-00Z_health.jsonl")
        );
        let sandbox_feeds = default_sandbox_log_path_at("feeds", sys);
        assert_eq!(
            sandbox_feeds,
            PathBuf::from("logs/sandbox/22-11_11-00-00Z_feeds.jsonl")
        );
    }

    #[test]
    fn duration_compact_formats_cleanly() {
        assert_eq!(format_duration_compact(Duration::from_secs(0)), "0s");
        assert_eq!(format_duration_compact(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration_compact(Duration::from_secs(20)), "20s");
        assert_eq!(
            format_duration_compact(Duration::from_millis(19_800)),
            "20s"
        );
        assert_eq!(format_duration_compact(Duration::from_secs(60)), "1m");
        assert_eq!(format_duration_compact(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration_compact(Duration::from_secs(300)), "5m");
        assert_eq!(format_duration_compact(Duration::from_secs(3600)), "1h");
        assert_eq!(format_duration_compact(Duration::from_secs(3660)), "1h1m");
    }

    #[test]
    fn capture_filename_construction_and_atomic_finalize_rename() {
        let dir = unique_temp_dir("finalize");
        let stamp = "22-11_11-00-00Z";
        let inprogress = capture_inprogress_path(&dir, stamp, Duration::from_secs(20));
        assert_eq!(inprogress, dir.join("22-11_11-00-00Z_20s.inprogress.jsonl"));

        std::fs::write(&inprogress, "{\"event\":\"summary\"}\n").unwrap();
        let finalized =
            finalize_capture_file(&inprogress, stamp, Duration::from_secs(300), 2).unwrap();
        assert_eq!(finalized, dir.join("22-11_11-00-00Z_5m_2w.jsonl"));
        assert!(!inprogress.exists());
        assert!(finalized.exists());
        assert_eq!(
            std::fs::read_to_string(&finalized).unwrap(),
            "{\"event\":\"summary\"}\n"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_latest_capture_picks_newest_finalized_and_errors_when_empty() {
        let dir = unique_temp_dir("latest");
        assert!(find_latest_capture(&dir).is_err());

        // In-progress and non-jsonl files must be ignored.
        std::fs::write(dir.join("22-11_12-00-00Z_20s.inprogress.jsonl"), "{}\n").unwrap();
        std::fs::write(dir.join("notes.txt"), "hi").unwrap();
        assert!(find_latest_capture(&dir).is_err());

        let older = dir.join("22-11_10-00-00Z_20s_1w.jsonl");
        std::fs::write(&older, "{}\n").unwrap();
        std::thread::sleep(Duration::from_millis(25));
        let newer = dir.join("01-12_01-00-00Z_20s_1w.jsonl");
        std::fs::write(&newer, "{}\n").unwrap();

        let picked = find_latest_capture(&dir).unwrap();
        assert_eq!(picked, newer);

        let out = dir.join("out.jsonl");
        assert_eq!(resolve_replay_input_in(None, &out, &dir).unwrap(), newer);
        assert!(resolve_replay_input_in(None, &newer, &dir).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_second_capture_reservation_and_finalization_handle_collisions() {
        let dir = unique_temp_dir("collisions");
        let stamp = "22-11_11-00-00Z";

        // Same-second in-progress reservations get clean base name first, then -2, -3.
        let p1 = reserve_capture_inprogress_path(&dir, stamp, Duration::from_secs(20)).unwrap();
        let p2 = reserve_capture_inprogress_path(&dir, stamp, Duration::from_secs(20)).unwrap();
        let p3 = reserve_capture_inprogress_path(&dir, stamp, Duration::from_secs(20)).unwrap();
        assert_eq!(p1, dir.join("22-11_11-00-00Z_20s.inprogress.jsonl"));
        assert_eq!(p2, dir.join("22-11_11-00-00Z_20s-2.inprogress.jsonl"));
        assert_eq!(p3, dir.join("22-11_11-00-00Z_20s-3.inprogress.jsonl"));

        std::fs::write(&p1, "first\n").unwrap();
        std::fs::write(&p2, "second\n").unwrap();
        std::fs::write(&p3, "third\n").unwrap();

        // Finalizing with the same actual duration and window count never overwrites prior files.
        let f1 = finalize_capture_file(&p1, stamp, Duration::from_secs(5), 1).unwrap();
        let f2 = finalize_capture_file(&p2, stamp, Duration::from_secs(5), 1).unwrap();
        let f3 = finalize_capture_file(&p3, stamp, Duration::from_secs(5), 1).unwrap();

        assert_eq!(f1, dir.join("22-11_11-00-00Z_5s_1w.jsonl"));
        assert_eq!(f2, dir.join("22-11_11-00-00Z_5s_1w-2.jsonl"));
        assert_eq!(f3, dir.join("22-11_11-00-00Z_5s_1w-3.jsonl"));
        assert_eq!(std::fs::read_to_string(&f1).unwrap(), "first\n");
        assert_eq!(std::fs::read_to_string(&f2).unwrap(), "second\n");
        assert_eq!(std::fs::read_to_string(&f3).unwrap(), "third\n");
        assert!(!p1.exists() && !p2.exists() && !p3.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
