#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use core_types::{
    BookLevel, FeeSchedule, FixedBook, LatencyTracker, MicroUsdc, MonotonicClock, ONE_SHARE_MICRO,
    Outcome, SharesMicro, SignedSharesMicro,
};
use math_engine::{MathPipeline, MathPipelineConfig, PipelineInput, QuoteOutput};

struct CountingAlloc;

static COUNT_ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

// SAFETY: Delegates directly to `std::alloc::System`; atomic counters have no re-entrant allocs.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ACTIVE.load(Ordering::Relaxed) {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: Preconditions for `System::alloc` match `GlobalAlloc::alloc`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Preconditions for `System::dealloc` match `GlobalAlloc::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNT_ACTIVE.load(Ordering::Relaxed) {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: Preconditions for `System::realloc` match `GlobalAlloc::realloc`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn lvl(cents: u64, shares: u64) -> BookLevel {
    BookLevel {
        price: MicroUsdc(cents * 10_000),
        size: SharesMicro(shares * ONE_SHARE_MICRO),
    }
}

fn fixture_books() -> (FixedBook<10>, FixedBook<10>) {
    let up = FixedBook::<10>::try_from_slices(
        Outcome::Up,
        &[
            lvl(49, 120),
            lvl(48, 220),
            lvl(47, 350),
            lvl(46, 500),
            lvl(45, 700),
        ],
        &[
            lvl(51, 110),
            lvl(52, 230),
            lvl(53, 360),
            lvl(54, 480),
            lvl(55, 650),
        ],
        10,
        1_782_000_000_000,
    )
    .unwrap();
    let dn = FixedBook::<10>::try_from_slices(
        Outcome::Down,
        &[
            lvl(49, 120),
            lvl(48, 220),
            lvl(47, 350),
            lvl(46, 500),
            lvl(45, 700),
        ],
        &[
            lvl(51, 110),
            lvl(52, 230),
            lvl(53, 360),
            lvl(54, 480),
            lvl(55, 650),
        ],
        10,
        1_782_000_000_000,
    )
    .unwrap();
    (up, dn)
}

fn make_inp<'a>(
    now_ms: u64,
    expiry_ms: u64,
    q_whole: i64,
    spot: f64,
    up: &'a FixedBook<10>,
    dn: &'a FixedBook<10>,
    active: Option<(u64, u64)>,
) -> PipelineInput<'a> {
    PipelineInput {
        now_ms,
        expiry_ms,
        tick_size_micro: MicroUsdc(10_000),
        fee_schedule: FeeSchedule::btc_5m_default(),
        inventory_q: SignedSharesMicro(q_whole * ONE_SHARE_MICRO as i64),
        book_up: up,
        book_dn: dn,
        spot_usd: Some(spot),
        strike_usd: Some(90_000.0),
        active_bid_up: active.map(|(u, _)| MicroUsdc(u)),
        active_bid_dn: active.map(|(_, d)| MicroUsdc(d)),
    }
}

fn assert_zero_alloc_10k(up: &FixedBook<10>, dn: &FixedBook<10>) {
    let clock = MonotonicClock::start();
    let mut pipe = MathPipeline::new(MathPipelineConfig::try_default().unwrap(), clock).unwrap();
    let base_ts = 1_782_000_000_000_u64;
    let expiry_ms = base_ts + 200_000;

    for i in 0..8_u64 {
        let inp = make_inp(
            base_ts + i * 100,
            expiry_ms,
            0,
            90_000.0 + (i as f64),
            up,
            dn,
            None,
        );
        let _ = pipe.evaluate(&inp).unwrap();
    }

    ALLOC_CALLS.store(0, Ordering::SeqCst);
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    COUNT_ACTIVE.store(true, Ordering::SeqCst);

    let mut last_quote = QuoteOutput::Active {
        bid_up: None,
        bid_dn: None,
        pair_cost_micro: None,
        gross_margin_bps: None,
    };
    for i in 0..10_000_u64 {
        let spot = 90_000.0 + ((i % 25) as f64) * 2.5;
        let ts = base_ts + 1_000 + i * 5;
        let _ = pipe
            .on_binance_spot(ts, spot, Some(90_000.0), expiry_ms)
            .unwrap();
        pipe.on_poly_trade(Outcome::Up, SharesMicro(25 * ONE_SHARE_MICRO));
        let tr = pipe
            .evaluate(&make_inp(
                ts,
                expiry_ms,
                10,
                spot,
                up,
                dn,
                Some((450_000, 490_000)),
            ))
            .unwrap();
        last_quote = tr.quote;
    }

    COUNT_ACTIVE.store(false, Ordering::SeqCst);
    let calls = ALLOC_CALLS.load(Ordering::SeqCst);
    let bytes = ALLOC_BYTES.load(Ordering::SeqCst);
    assert!(matches!(last_quote, QuoteOutput::Active { .. }));
    assert_eq!(calls, 0, "expected 0 heap alloc calls, observed {calls}");
    assert_eq!(bytes, 0, "expected 0 heap bytes, observed {bytes}");
    println!("MATH_10K_ZERO_ALLOC: alloc_calls={calls} alloc_bytes={bytes}");
}

fn assert_100k_under_25us_p99(up: &FixedBook<10>, dn: &FixedBook<10>) {
    let clock = MonotonicClock::start();
    let mut pipe = MathPipeline::new(MathPipelineConfig::try_default().unwrap(), clock).unwrap();
    let mut tracker = LatencyTracker::new().unwrap();
    let base_ts = 1_782_000_000_000_u64;
    let expiry_ms = base_ts + 250_000;

    for i in 0..8_u64 {
        let inp = make_inp(base_ts + i * 100, expiry_ms, 0, 90_000.0, up, dn, None);
        let _ = pipe.evaluate(&inp).unwrap();
    }
    for i in 0..100_000_u64 {
        let spot = 90_000.0 + ((i % 40) as f64) * 1.5;
        let q = ((i % 3) as i64 - 1) * 15;
        let tr = pipe
            .evaluate(&make_inp(
                base_ts + 1_000 + i * 2,
                expiry_ms,
                q,
                spot,
                up,
                dn,
                None,
            ))
            .unwrap();
        tracker.record_ns(tr.total_compute_ns).unwrap();
    }

    let summary = tracker.summary();
    println!(
        "MATH_100K_BENCH: count={} min={}ns P50={:.3}us P95={:.3}us P99={:.3}us max={:.3}us",
        summary.count,
        summary.min_ns,
        summary.p50_ns as f64 / 1000.0,
        summary.p95_ns as f64 / 1000.0,
        summary.p99_ns as f64 / 1000.0,
        summary.max_ns as f64 / 1000.0
    );
    #[cfg(not(debug_assertions))]
    assert!(
        summary.p99_ns < 25_000,
        "expected release math pipeline P99 < 25,000 ns (25 us), got {} ns",
        summary.p99_ns
    );
    #[cfg(debug_assertions)]
    assert!(
        summary.count == 100_000 && summary.p99_ns < 500_000,
        "expected debug math pipeline count=100k & P99 < 500,000 ns, got count={} p99={}",
        summary.count,
        summary.p99_ns
    );
}

#[test]
fn adversarial_zero_alloc_and_100k_under_25us_p99() {
    let (up, dn) = fixture_books();
    assert_zero_alloc_10k(&up, &dn);
    assert_100k_under_25us_p99(&up, &dn);
}
