use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::error::ClockError;

/// Monotonic QPC-backed clock for wire-to-wire latency profiling and elapsed checks.
#[derive(Debug, Clone, Copy)]
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    /// Captures process-local monotonic epoch origin (`QueryPerformanceCounter` on Windows).
    #[must_use]
    pub fn start() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    /// Elapsed monotonic nanoseconds since `MonotonicClock::start()`.
    pub fn now_ns(&self) -> Result<u64, ClockError> {
        let nanos = self.origin.elapsed().as_nanos();
        u64::try_from(nanos).map_err(|_| ClockError::TimestampOverflow)
    }

    /// Wall-clock Unix timestamp in milliseconds.
    pub fn unix_now_ms() -> Result<u64, ClockError> {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| ClockError::SystemTimeBeforeEpoch {
                behind_ms: err.duration().as_millis(),
            })?;
        u64::try_from(dur.as_millis()).map_err(|_| ClockError::TimestampOverflow)
    }

    /// Returns Windows QPC frequency in Hz and nominal tick resolution in nanoseconds.
    #[cfg(windows)]
    pub fn qpc_resolution() -> Result<(u64, f64), ClockError> {
        let mut freq: i64 = 0;
        // SAFETY: `freq` points to a valid writable stack `i64` for the duration of the call.
        let ok = unsafe {
            windows_sys::Win32::System::Performance::QueryPerformanceFrequency(&mut freq)
        };
        if ok == 0 {
            let os_error = std::io::Error::last_os_error().to_string();
            return Err(ClockError::QpcUnavailable { os_error });
        }
        let Ok(hz) = u64::try_from(freq) else {
            return Err(ClockError::QpcUnavailable {
                os_error: format!("negative QueryPerformanceFrequency value: {freq}"),
            });
        };
        if hz == 0 {
            return Err(ClockError::QpcUnavailable {
                os_error: "QueryPerformanceFrequency returned 0 Hz".to_string(),
            });
        }
        let ns_per_tick = 1_000_000_000.0 / (hz as f64);
        Ok((hz, ns_per_tick))
    }

    /// Non-Windows fallback returning nanosecond resolution for portable builds.
    #[cfg(not(windows))]
    pub fn qpc_resolution() -> Result<(u64, f64), ClockError> {
        Ok((1_000_000_000, 1.0))
    }
}

/// Tracks signed millisecond offset between exchange server time and local system clock.
///
/// Fails closed (`is_drift_exceeded() == true`, `offset_ms()` returns `UnsyncedClock`) until at
/// least one valid sample is recorded.
#[derive(Debug)]
pub struct ClockOffsetEstimator {
    has_sample: AtomicBool,
    offset_ms: AtomicI64,
    drift_threshold_ms: i64,
}

impl ClockOffsetEstimator {
    /// Creates an unsynced estimator with `drift_threshold_ms` (e.g. `1_000ms` circuit breaker).
    #[must_use]
    pub const fn new(drift_threshold_ms: i64) -> Self {
        Self {
            has_sample: AtomicBool::new(false),
            offset_ms: AtomicI64::new(0),
            drift_threshold_ms,
        }
    }

    /// Returns `true` once at least one time offset sample has been recorded.
    #[must_use]
    pub fn has_sample(&self) -> bool {
        self.has_sample.load(Ordering::Acquire)
    }

    /// Updates estimated offset (`exchange_ms - local_ms`) atomically with overflow checking.
    pub fn record_sample(
        &self,
        local_unix_ms: u64,
        exchange_unix_ms: u64,
    ) -> Result<i64, ClockError> {
        let diff = i128::from(exchange_unix_ms) - i128::from(local_unix_ms);
        let diff_i64 = i64::try_from(diff).map_err(|_| ClockError::OffsetOverflow)?;
        self.offset_ms.store(diff_i64, Ordering::Release);
        self.has_sample.store(true, Ordering::Release);
        Ok(diff_i64)
    }

    /// Current signed clock offset in milliseconds (`exchange - local`).
    pub fn offset_ms(&self) -> Result<i64, ClockError> {
        if !self.has_sample.load(Ordering::Acquire) {
            return Err(ClockError::UnsyncedClock);
        }
        Ok(self.offset_ms.load(Ordering::Acquire))
    }

    /// Adjusts a local Unix millisecond timestamp by the current estimated exchange offset.
    pub fn adjusted_unix_ms(&self, local_unix_ms: u64) -> Result<u64, ClockError> {
        let offset = self.offset_ms()?;
        if offset >= 0 {
            local_unix_ms
                .checked_add(offset.unsigned_abs())
                .ok_or(ClockError::TimestampOverflow)
        } else {
            Ok(local_unix_ms.saturating_sub(offset.unsigned_abs()))
        }
    }

    /// Returns `true` when unsynced, misconfigured (`<= 0`), or `|offset_ms| > drift_threshold_ms`.
    #[must_use]
    pub fn is_drift_exceeded(&self) -> bool {
        if self.drift_threshold_ms <= 0 || !self.has_sample.load(Ordering::Acquire) {
            return true;
        }
        let threshold = self.drift_threshold_ms.unsigned_abs();
        self.offset_ms.load(Ordering::Acquire).unsigned_abs() > threshold
    }
}

/// RAII guard requesting 1ms Windows multimedia timer resolution (`timeBeginPeriod(1)`).
#[derive(Debug)]
pub struct HighResTimerGuard {
    period_ms: u32,
    active: bool,
}

impl HighResTimerGuard {
    /// Requests `1ms` Windows OS timer tick resolution (`15.625ms` default otherwise).
    #[cfg(windows)]
    pub fn acquire_1ms() -> Result<Self, ClockError> {
        const PERIOD_MS: u32 = 1;
        // SAFETY: `timeBeginPeriod(1)` is a process-wide Win32 multimedia timer request paired with `timeEndPeriod(1)` in `Drop`.
        let rc = unsafe { windows_sys::Win32::Media::timeBeginPeriod(PERIOD_MS) };
        if rc != windows_sys::Win32::Media::TIMERR_NOERROR {
            return Err(ClockError::WindowsTimerFailed { code: rc });
        }
        Ok(Self {
            period_ms: PERIOD_MS,
            active: true,
        })
    }

    /// Non-Windows stub reporting `active = false` because Win32 multimedia timers do not apply.
    #[cfg(not(windows))]
    pub fn acquire_1ms() -> Result<Self, ClockError> {
        Ok(Self {
            period_ms: 1,
            active: false,
        })
    }

    /// Whether the Win32 high-resolution timer request is active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Requested timer period in milliseconds.
    #[must_use]
    pub const fn period_ms(&self) -> u32 {
        self.period_ms
    }
}

impl Drop for HighResTimerGuard {
    fn drop(&mut self) {
        if self.active {
            #[cfg(windows)]
            {
                // SAFETY: Matches a preceding successful `timeBeginPeriod(self.period_ms)` call on this guard.
                let rc = unsafe { windows_sys::Win32::Media::timeEndPeriod(self.period_ms) };
                if rc != windows_sys::Win32::Media::TIMERR_NOERROR {
                    let _ = writeln!(
                        std::io::stderr(),
                        "WARN: Win32 timeEndPeriod({}) failed on HighResTimerGuard drop (code={rc})",
                        self.period_ms
                    );
                }
            }
            self.active = false;
        }
    }
}
