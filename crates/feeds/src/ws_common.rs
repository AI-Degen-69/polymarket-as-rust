use std::time::{Duration, Instant};

use core_types::{LatencyTracker, MonotonicClock};
use futures_util::{Sink, SinkExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::error::FeedError;
use crate::events::{FeedHealthEvent, FeedId, FeedStats};

const HEALTH_TRANSITION_TIMEOUT: Duration = Duration::from_millis(50);
const DIAGNOSTIC_THROTTLE_INTERVAL: Duration = Duration::from_millis(250);

/// Initial empty telemetry stats for a feed actor.
#[must_use]
pub(crate) const fn empty_stats(feed: FeedId) -> FeedStats {
    FeedStats {
        feed,
        frames_received: 0,
        events_pushed: 0,
        ring_overflows: 0,
        parse_warnings: 0,
        reconnects: 0,
        stale_timeouts: 0,
        pongs_received: 0,
        pings_replied: 0,
        health_events_dropped: 0,
        latency: None,
    }
}

/// Captures synchronized `(unix_ms, mono_ns)` timestamps cleanly.
pub(crate) fn now_pair(clock: &MonotonicClock) -> Result<(u64, u64), FeedError> {
    Ok((MonotonicClock::unix_now_ms()?, clock.now_ns()?))
}

/// Strips query string and fragment before logging a WebSocket URL to health telemetry.
#[must_use]
pub(crate) fn sanitize_ws_url(raw: &str) -> &str {
    let base = raw.split('?').next().unwrap_or(raw);
    base.split('#').next().unwrap_or(base)
}

/// Rate-limits bursty diagnostic telemetry (`RingOverflow`, `ParseWarning`) so state transitions never starve.
#[derive(Debug)]
pub(crate) struct HealthThrottle {
    min_interval: Duration,
    last_overflow: Option<Instant>,
    last_warning: Option<Instant>,
}

impl Default for HealthThrottle {
    fn default() -> Self {
        Self {
            min_interval: DIAGNOSTIC_THROTTLE_INTERVAL,
            last_overflow: None,
            last_warning: None,
        }
    }
}

impl HealthThrottle {
    fn should_emit(slot: &mut Option<Instant>, min_interval: Duration) -> bool {
        let now = Instant::now();
        match slot {
            Some(prev) if now.duration_since(*prev) < min_interval => false,
            _ => {
                *slot = Some(now);
                true
            }
        }
    }
}

/// Delivers state-transition health events cleanly without silent drops under normal consumer load.
pub(crate) async fn emit_health_transition(
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    stats: &mut FeedStats,
    ev: FeedHealthEvent,
) {
    match health_tx.try_send(ev) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Closed(_)) => {
            stats.health_events_dropped = stats.health_events_dropped.saturating_add(1);
        }
        Err(mpsc::error::TrySendError::Full(ev)) => {
            if !matches!(
                tokio::time::timeout(HEALTH_TRANSITION_TIMEOUT, health_tx.send(ev)).await,
                Ok(Ok(()))
            ) {
                stats.health_events_dropped = stats.health_events_dropped.saturating_add(1);
            }
        }
    }
}

/// Emits a non-blocking diagnostic health event, counting drops rather than silently discarding.
pub(crate) fn emit_health_diag(
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    stats: &mut FeedStats,
    ev: FeedHealthEvent,
) {
    if health_tx.try_send(ev).is_err() {
        stats.health_events_dropped = stats.health_events_dropped.saturating_add(1);
    }
}

/// Emits a `Disconnected` transition with current timestamps.
pub(crate) async fn emit_disconnect(
    feed: FeedId,
    clock: &MonotonicClock,
    stats: &mut FeedStats,
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    reason: String,
) -> Result<(), FeedError> {
    let (ts_unix_ms, mono_ns) = now_pair(clock)?;
    emit_health_transition(
        health_tx,
        stats,
        FeedHealthEvent::Disconnected {
            feed,
            reason,
            ts_unix_ms,
            mono_ns,
        },
    )
    .await;
    Ok(())
}

/// Checks feed inactivity against `stale_timeout` and closes the socket if tripped.
pub(crate) async fn check_stale_watchdog<S>(
    feed: FeedId,
    stale_timeout: Duration,
    last_msg_at: &Instant,
    clock: &MonotonicClock,
    stats: &mut FeedStats,
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    writer: &mut S,
) -> Result<bool, FeedError>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let silent_ms = u64::try_from(last_msg_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stale_limit = u64::try_from(stale_timeout.as_millis()).unwrap_or(u64::MAX);
    if silent_ms < stale_limit {
        return Ok(false);
    }
    stats.stale_timeouts = stats.stale_timeouts.saturating_add(1);
    let (ts_unix_ms, mono_ns) = now_pair(clock)?;
    emit_health_transition(
        health_tx,
        stats,
        FeedHealthEvent::StaleTimeout {
            feed,
            silent_ms,
            ts_unix_ms,
            mono_ns,
        },
    )
    .await;
    close_ws_bounded(writer).await;
    emit_disconnect(
        feed,
        clock,
        stats,
        health_tx,
        format!("stale feed silent for {silent_ms}ms"),
    )
    .await?;
    Ok(true)
}

/// Outcome of a bounded WebSocket handshake selectable with actor commands.
pub(crate) enum ConnectStep<C> {
    /// TLS + WS upgrade succeeded.
    Connected(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
    /// Connect failed or timed out cleanly.
    Failed(String),
    /// Actor command received before handshake completed.
    Command(Option<C>),
}

/// Runs a bounded `tokio_tungstenite` connect with `TCP_NODELAY=true`, waking immediately on `cmd_rx`.
pub(crate) async fn connect_ws_bounded<C>(
    url: &str,
    connect_timeout: Duration,
    cmd_rx: &mut mpsc::Receiver<C>,
) -> ConnectStep<C> {
    let connect_fut =
        tokio::time::timeout(connect_timeout, connect_async_with_config(url, None, true));
    tokio::select! {
        biased;
        cmd = cmd_rx.recv() => ConnectStep::Command(cmd),
        res = connect_fut => match res {
            Ok(Ok((ws, _))) => ConnectStep::Connected(Box::new(ws)),
            Ok(Err(err)) => ConnectStep::Failed(format!("connect to {} failed: {err}", sanitize_ws_url(url))),
            Err(_) => ConnectStep::Failed(format!(
                "connect timeout ({}ms) to {}",
                connect_timeout.as_millis(),
                sanitize_ws_url(url)
            )),
        },
    }
}

/// Pushes a `Copy` hot-path event into `rtrb` without ever blocking network or heartbeat tasks.
pub(crate) fn push_ring_event<T>(
    producer: &mut rtrb::Producer<T>,
    event: T,
    feed: FeedId,
    stats: &mut FeedStats,
    throttle: &mut HealthThrottle,
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    clock: &MonotonicClock,
) -> Result<(), FeedError> {
    match producer.push(event) {
        Ok(()) => stats.events_pushed = stats.events_pushed.saturating_add(1),
        Err(rtrb::PushError::Full(_)) => {
            stats.ring_overflows = stats.ring_overflows.saturating_add(1);
            if HealthThrottle::should_emit(&mut throttle.last_overflow, throttle.min_interval) {
                let (ts_unix_ms, mono_ns) = now_pair(clock)?;
                emit_health_diag(
                    health_tx,
                    stats,
                    FeedHealthEvent::RingOverflow {
                        feed,
                        dropped_count: stats.ring_overflows,
                        ts_unix_ms,
                        mono_ns,
                    },
                );
            }
        }
    }
    Ok(())
}

/// Emits an explicit throttled parse warning telemetry event without halting stream processing.
pub(crate) fn record_parse_warning(
    feed: FeedId,
    reason: String,
    stats: &mut FeedStats,
    throttle: &mut HealthThrottle,
    health_tx: &mpsc::Sender<FeedHealthEvent>,
    clock: &MonotonicClock,
) -> Result<(), FeedError> {
    stats.parse_warnings = stats.parse_warnings.saturating_add(1);
    if HealthThrottle::should_emit(&mut throttle.last_warning, throttle.min_interval) {
        let (ts_unix_ms, mono_ns) = now_pair(clock)?;
        emit_health_diag(
            health_tx,
            stats,
            FeedHealthEvent::ParseWarning {
                feed,
                reason,
                ts_unix_ms,
                mono_ns,
            },
        );
    }
    Ok(())
}

/// Sleeps for `delay` unless cancelled early by `cancel` signal.
pub(crate) async fn sleep_until_cancel(
    delay: Duration,
    cancel: impl std::future::Future<Output = ()>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = cancel => true,
    }
}

/// Attaches HDR parse latency percentiles before returning actor summary.
pub(crate) fn finalize_stats(mut stats: FeedStats, tracker: &LatencyTracker) -> FeedStats {
    let summary = tracker.summary();
    stats.latency = (summary.count > 0).then_some(summary);
    stats
}

/// Sends a best-effort WebSocket Close frame bounded by a 250ms timeout so stalled TCP sockets cannot block exit.
pub(crate) async fn close_ws_bounded<S>(writer: &mut S)
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let _ = tokio::time::timeout(
        Duration::from_millis(250),
        writer.send(Message::Close(None)),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    struct StalledSink;

    impl Sink<Message> for StalledSink {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn close_ws_bounded_returns_within_1s_on_stalled_sink() {
        let mut sink = StalledSink;
        let start = Instant::now();
        close_ws_bounded(&mut sink).await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");
        assert!(elapsed >= Duration::from_millis(200), "elapsed={elapsed:?}");
    }
}
