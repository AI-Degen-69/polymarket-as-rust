//! Shared terminal theme, color gating, badges, layout primitives, and compact formatters.
//!
//! Zero external dependencies: uses `std::io::IsTerminal` + `NO_COLOR` + global `--color`.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicU8, Ordering};

use clap::ValueEnum;
use core_types::MicroUsdc;
use risk_lifecycle::{EpochPhase, RiskTier};

const STATE_UNINIT: u8 = 0;
const STATE_PLAIN: u8 = 1;
const STATE_STYLED: u8 = 2;

const RULE_WIDTH: usize = 82;

static THEME_STATE: AtomicU8 = AtomicU8::new(STATE_UNINIT);

/// Global `--color` CLI flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ColorChoice {
    Auto,
    Always,
    Never,
}

/// Compact semantic badge kinds used across all CLI subcommands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Badge {
    Fill,
    Merge,
    Flatten,
    Breaker,
    Wait,
    Ready,
    Pass,
    Warn,
    Fail,
    Order,
    Trade,
    Suppr,
    Resume,
    Latency,
    Epoch,
    Signal,
    Phase,
    Tier,
    Info,
}

/// Resolved terminal styling capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Theme {
    color: bool,
    unicode: bool,
}

impl Theme {
    /// Resolves styling from an explicit `--color` flag, TTY state, and `NO_COLOR`.
    pub(crate) const fn from_choice(choice: ColorChoice, is_tty: bool, no_color: bool) -> Self {
        match choice {
            ColorChoice::Always => Self {
                color: true,
                unicode: true,
            },
            ColorChoice::Never => Self {
                color: false,
                unicode: false,
            },
            ColorChoice::Auto => {
                let enabled = is_tty && !no_color;
                Self {
                    color: enabled,
                    unicode: enabled,
                }
            }
        }
    }

    /// Deterministic styled theme (for `--color always` and unit tests).
    pub(crate) const fn styled() -> Self {
        Self {
            color: true,
            unicode: true,
        }
    }

    /// Deterministic plain ASCII theme (for `--color never`, `NO_COLOR`, piped output, and unit tests).
    pub(crate) const fn plain() -> Self {
        Self {
            color: false,
            unicode: false,
        }
    }

    pub(crate) const fn is_color(self) -> bool {
        self.color
    }

    fn wrap(self, code: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    pub(crate) fn muted(self, text: &str) -> String {
        self.wrap("2", text)
    }

    pub(crate) fn bold(self, text: &str) -> String {
        self.wrap("1", text)
    }

    pub(crate) fn cyan(self, text: &str) -> String {
        self.wrap("36", text)
    }

    pub(crate) fn bold_cyan(self, text: &str) -> String {
        self.wrap("1;36", text)
    }

    pub(crate) fn green(self, text: &str) -> String {
        self.wrap("32", text)
    }

    pub(crate) fn bold_green(self, text: &str) -> String {
        self.wrap("1;32", text)
    }

    pub(crate) fn yellow(self, text: &str) -> String {
        self.wrap("33", text)
    }

    pub(crate) fn bold_yellow(self, text: &str) -> String {
        self.wrap("1;33", text)
    }

    pub(crate) fn red(self, text: &str) -> String {
        self.wrap("31", text)
    }

    pub(crate) fn bold_red(self, text: &str) -> String {
        self.wrap("1;31", text)
    }

    pub(crate) fn bold_magenta(self, text: &str) -> String {
        self.wrap("1;35", text)
    }

    /// Renders a boxed command header (`╭─ TITLE ─╮` / `╰─╯`) with ASCII fallback.
    pub(crate) fn banner(self, title: &str, subtitle: Option<&str>) -> String {
        let title_len = title.chars().count();
        let sub_len = subtitle.map_or(0, |s| s.chars().count().saturating_add(2));
        let inner_width = RULE_WIDTH
            .saturating_sub(2)
            .max(title_len.saturating_add(4))
            .max(sub_len);

        let (tl, tr, bl, br, hz, vt) = if self.unicode {
            ("╭", "╮", "╰", "╯", "─", "│")
        } else {
            ("+", "+", "+", "+", "-", "|")
        };

        let fill_top = inner_width.saturating_sub(title_len.saturating_add(3));
        let top = format!(
            "{} {} {}{}",
            self.cyan(&format!("{tl}{hz}")),
            self.bold_cyan(title),
            self.cyan(&hz.repeat(fill_top)),
            self.cyan(tr),
        );

        let mut out = String::new();
        out.push_str(&top);
        if let Some(sub) = subtitle {
            let pad = inner_width.saturating_sub(sub.chars().count().saturating_add(1));
            out.push('\n');
            out.push_str(&format!(
                "{} {}{}{}",
                self.cyan(vt),
                self.muted(sub),
                " ".repeat(pad),
                self.cyan(vt),
            ));
        }
        out.push('\n');
        out.push_str(&self.cyan(&format!("{bl}{}{br}", hz.repeat(inner_width))));
        out
    }

    /// Renders a section divider (`━━ TITLE ━━`) with ASCII fallback.
    pub(crate) fn section(self, title: &str) -> String {
        let title_len = title.chars().count();
        let (lead, ch) = if self.unicode {
            ("━━", "━")
        } else {
            ("==", "=")
        };
        let fill = RULE_WIDTH.saturating_sub(title_len.saturating_add(4));
        format!(
            "{} {} {}",
            self.cyan(lead),
            self.bold_cyan(title),
            self.cyan(&ch.repeat(fill))
        )
    }

    /// Renders a horizontal divider line (`────`) with ASCII fallback.
    pub(crate) fn divider(self) -> String {
        let ch = if self.unicode { "─" } else { "-" };
        self.muted(&ch.repeat(RULE_WIDTH))
    }

    /// Renders an aligned key-value line (`  Label                  value`).
    pub(crate) fn kv(self, label: &str, value: &str) -> String {
        let padded = format!("{label:<22}");
        format!("  {} {}", self.muted(&padded), value)
    }

    /// Renders an epoch phase badge padded to 10 visible characters for column alignment.
    pub(crate) fn phase_badge(self, phase: EpochPhase) -> String {
        let (raw, colored) = match phase {
            EpochPhase::ActiveQuoting => ("[QUOTE]", self.bold_green("[QUOTE]")),
            EpochPhase::PreExpiryLockout => ("[LOCKOUT]", self.bold_yellow("[LOCKOUT]")),
            EpochPhase::MandatoryFlatten => ("[FLATTEN]", self.bold_red("[FLATTEN]")),
            EpochPhase::TerminalLockout => ("[TERMINAL]", self.bold_magenta("[TERMINAL]")),
        };
        let pad = 10usize.saturating_sub(raw.chars().count());
        format!("{colored}{}", " ".repeat(pad))
    }

    /// Renders a risk tier badge (`[T1]`..`[T4]`).
    pub(crate) fn tier_badge(self, tier: RiskTier) -> String {
        match tier {
            RiskTier::Tier1Normal => self.green("[T1]"),
            RiskTier::Tier2ModerateSkew => self.yellow("[T2]"),
            RiskTier::Tier3OneSided => self.bold_yellow("[T3]"),
            RiskTier::Tier4HardStop => self.bold_red("[T4]"),
        }
    }

    /// Renders a semantic status/event badge.
    pub(crate) fn badge(self, kind: Badge) -> String {
        match kind {
            Badge::Fill => self.bold_green("[FILL]"),
            Badge::Merge => self.bold_cyan("[MERGE]"),
            Badge::Flatten => self.bold_red("[FLATTEN]"),
            Badge::Breaker => self.bold_red("[BREAKER]"),
            Badge::Wait => self.bold_yellow("[WAIT]"),
            Badge::Ready => self.bold_green("[READY]"),
            Badge::Pass => self.bold_green("[PASS]"),
            Badge::Warn => self.bold_yellow("[WARN]"),
            Badge::Fail => self.bold_red("[FAIL]"),
            Badge::Order => self.cyan("[ORDER]"),
            Badge::Trade => self.muted("[TRADE]"),
            Badge::Suppr => self.yellow("[SUPPR]"),
            Badge::Resume => self.green("[RESUME]"),
            Badge::Latency => self.bold_yellow("[LATENCY]"),
            Badge::Epoch => self.bold_cyan("[EPOCH]"),
            Badge::Signal => self.bold_yellow("[SIGNAL]"),
            Badge::Phase => self.bold_cyan("[PHASE]"),
            Badge::Tier => self.bold_yellow("[TIER]"),
            Badge::Info => self.cyan("[INFO]"),
        }
    }

    /// Formats a signed PnL amount in micro-USDC (`+$0.20` green, `-$0.15` red, `+$0.00` muted).
    /// Preserves sub-cent precision when non-zero beyond 2 decimal places.
    pub(crate) fn fmt_pnl(self, micro: i64) -> String {
        let sign = if micro < 0 { "-" } else { "+" };
        let abs = micro.unsigned_abs();
        let dollars = abs / 1_000_000;
        let rem = abs % 1_000_000;
        let raw = if rem.is_multiple_of(10_000) {
            format!("{sign}${dollars}.{:02}", rem / 10_000)
        } else if rem.is_multiple_of(100) {
            format!("{sign}${dollars}.{:04}", rem / 100)
        } else {
            format!("{sign}${dollars}.{rem:06}")
        };

        if micro > 0 {
            self.green(&raw)
        } else if micro < 0 {
            self.red(&raw)
        } else {
            self.muted(&raw)
        }
    }

    /// Formats a price compactly: `.51` for sub-dollar cents, `.515` for sub-cents, `$1.00` for `>= $1`, `--` for `None`.
    pub(crate) fn fmt_price_compact(px: Option<MicroUsdc>) -> String {
        let Some(MicroUsdc(m)) = px else {
            return "--".to_string();
        };
        if m < 1_000_000 {
            if m.is_multiple_of(10_000) {
                format!(".{:02}", m / 10_000)
            } else if m.is_multiple_of(1_000) {
                format!(".{:03}", m / 1_000)
            } else {
                format!(".{:06}", m)
            }
        } else if m.is_multiple_of(10_000) {
            format!("${}.{:02}", m / 1_000_000, (m % 1_000_000) / 10_000)
        } else {
            format!("${}.{:06}", m / 1_000_000, m % 1_000_000)
        }
    }

    /// Formats best bid / ask pair cleanly (`.51/.52` or `--/--`).
    pub(crate) fn fmt_bbo(bid: Option<MicroUsdc>, ask: Option<MicroUsdc>) -> String {
        format!(
            "{}/{}",
            Self::fmt_price_compact(bid),
            Self::fmt_price_compact(ask)
        )
    }

    /// Formats resting UP/DN quotes and combined pair cost (`.50/.47 (.97)`).
    pub(crate) fn fmt_pair_quotes(up: Option<MicroUsdc>, dn: Option<MicroUsdc>) -> String {
        match (up, dn) {
            (Some(u), Some(d)) => {
                let sum = MicroUsdc(u.0.saturating_add(d.0));
                format!(
                    "{}/{} ({})",
                    Self::fmt_price_compact(Some(u)),
                    Self::fmt_price_compact(Some(d)),
                    Self::fmt_price_compact(Some(sum)),
                )
            }
            (Some(u), None) => format!("{}/--", Self::fmt_price_compact(Some(u))),
            (None, Some(d)) => format!("--/{}", Self::fmt_price_compact(Some(d))),
            (None, None) => "--/--".to_string(),
        }
    }

    /// Formats BTC spot vs strike delta (`+$14 (+2bp)` green / `-$14 (-2bp)` red / `n/a` muted).
    pub(crate) fn fmt_spot_vs_strike(self, spot: Option<f64>, strike: Option<f64>) -> String {
        let (Some(spot_px), Some(strike_px)) = (spot, strike) else {
            return self.muted("n/a");
        };
        if !spot_px.is_finite() || !strike_px.is_finite() || strike_px <= 0.0 {
            return self.muted("n/a");
        }
        let diff = spot_px - strike_px;
        let bp = ((diff / strike_px) * 10_000.0).round() as i64;
        let sign = if diff < 0.0 { "-" } else { "+" };
        let bp_sign = if bp < 0 { "-" } else { "+" };
        let abs_diff = diff.abs();
        let abs_bp = bp.unsigned_abs();
        let raw = if abs_diff >= 10.0 || (abs_diff - abs_diff.round()).abs() < 0.05 {
            format!("{sign}${abs_diff:.0} ({bp_sign}{abs_bp}bp)")
        } else {
            format!("{sign}${abs_diff:.2} ({bp_sign}{abs_bp}bp)")
        };

        if diff > 0.0 {
            self.green(&raw)
        } else if diff < 0.0 {
            self.red(&raw)
        } else {
            self.muted(&raw)
        }
    }

    /// Formats a nanosecond latency measurement in microseconds and highlights budget breaches.
    pub(crate) fn fmt_latency_us(self, ns: u64, budget_us: u64) -> String {
        let us = ns / 1_000;
        let raw = format!("{us}us");
        if us > budget_us {
            self.bold_red(&raw)
        } else {
            self.green(&raw)
        }
    }

    /// Formats a pass/fail boolean status badge.
    pub(crate) fn pass_fail(self, ok: bool) -> String {
        if ok {
            self.badge(Badge::Pass)
        } else {
            self.badge(Badge::Fail)
        }
    }
}

/// Returns `true` if `NO_COLOR` is set to a non-empty value.
pub(crate) fn no_color_env() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|val| !val.is_empty())
}

/// Initializes the process-wide terminal theme from the global `--color` flag.
pub(crate) fn init(choice: ColorChoice) {
    let theme = Theme::from_choice(choice, std::io::stdout().is_terminal(), no_color_env());
    let state = if theme.is_color() {
        STATE_STYLED
    } else {
        STATE_PLAIN
    };
    THEME_STATE.store(state, Ordering::Relaxed);
}

/// Returns the active terminal theme (resolves `Auto` if `init` has not been called yet).
pub(crate) fn current() -> Theme {
    match THEME_STATE.load(Ordering::Relaxed) {
        STATE_STYLED => Theme::styled(),
        STATE_PLAIN => Theme::plain(),
        _ => Theme::from_choice(
            ColorChoice::Auto,
            std::io::stdout().is_terminal(),
            no_color_env(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_choice_matrix_respects_tty_and_no_color() {
        assert_eq!(
            Theme::from_choice(ColorChoice::Auto, true, false),
            Theme::styled()
        );
        assert_eq!(
            Theme::from_choice(ColorChoice::Auto, false, false),
            Theme::plain()
        );
        assert_eq!(
            Theme::from_choice(ColorChoice::Auto, true, true),
            Theme::plain()
        );
        assert_eq!(
            Theme::from_choice(ColorChoice::Always, false, true),
            Theme::styled()
        );
        assert_eq!(
            Theme::from_choice(ColorChoice::Never, true, false),
            Theme::plain()
        );
    }

    #[test]
    fn plain_theme_emits_zero_ansi_escapes_and_uses_ascii_borders() {
        let t = Theme::plain();
        let banner = t.banner("POLY-AS LIFECYCLE", Some("Mode: SimEpoch"));
        assert!(!banner.contains('\x1b'));
        assert!(banner.starts_with("+-"));
        assert!(banner.contains("POLY-AS LIFECYCLE"));
        assert!(banner.contains("| Mode: SimEpoch"));
        let widths: Vec<usize> = banner.lines().map(|l| l.chars().count()).collect();
        assert_eq!(widths.len(), 3);
        assert_eq!(widths[0], widths[1]);
        assert_eq!(widths[1], widths[2]);

        let no_sub = t.banner("SHORT", None);
        let no_sub_widths: Vec<usize> = no_sub.lines().map(|l| l.chars().count()).collect();
        assert_eq!(no_sub_widths.len(), 2);
        assert_eq!(no_sub_widths[0], no_sub_widths[1]);

        let sec = t.section("EPOCH 1");
        assert!(!sec.contains('\x1b'));
        assert!(sec.starts_with("== EPOCH 1 =="));

        let div = t.divider();
        assert!(!div.contains('\x1b'));
        assert!(div.chars().all(|c| c == '-'));

        let kv = t.kv("Mode:", "Live");
        assert!(!kv.contains('\x1b'));
        assert!(kv.contains("Mode:"));
        assert!(kv.contains("Live"));

        assert!(!t.phase_badge(EpochPhase::ActiveQuoting).contains('\x1b'));
        assert!(!t.tier_badge(RiskTier::Tier4HardStop).contains('\x1b'));
        assert!(!t.badge(Badge::Fill).contains('\x1b'));
        assert!(!t.fmt_pnl(200_000).contains('\x1b'));
        assert!(!t.fmt_pnl(-150_000).contains('\x1b'));
        assert!(
            !t.fmt_spot_vs_strike(Some(90_014.0), Some(90_000.0))
                .contains('\x1b')
        );
    }

    #[test]
    fn styled_theme_emits_ansi_escapes_and_unicode_borders() {
        let t = Theme::styled();
        let banner = t.banner("POLY-AS LIFECYCLE", Some("Mode: Live"));
        assert!(banner.contains('\x1b'));
        assert!(banner.contains('╭'));
        assert!(banner.contains('╰'));

        let sec = t.section("EPOCH 1");
        assert!(sec.contains('\x1b'));
        assert!(sec.contains('━'));
    }

    #[test]
    fn fmt_pnl_renders_signed_compact_and_preserves_subcent_precision() {
        let plain = Theme::plain();
        assert_eq!(plain.fmt_pnl(200_000), "+$0.20");
        assert_eq!(plain.fmt_pnl(-150_000), "-$0.15");
        assert_eq!(plain.fmt_pnl(0), "+$0.00");
        assert_eq!(plain.fmt_pnl(4_000), "+$0.0040");
        assert_eq!(plain.fmt_pnl(-123), "-$0.000123");

        let styled = Theme::styled();
        assert_eq!(styled.fmt_pnl(200_000), "\x1b[32m+$0.20\x1b[0m");
        assert_eq!(styled.fmt_pnl(-150_000), "\x1b[31m-$0.15\x1b[0m");
        assert_eq!(styled.fmt_pnl(0), "\x1b[2m+$0.00\x1b[0m");
    }

    #[test]
    fn compact_price_bbo_pair_and_spot_formatters() {
        assert_eq!(Theme::fmt_price_compact(None), "--");
        assert_eq!(Theme::fmt_price_compact(Some(MicroUsdc(510_000))), ".51");
        assert_eq!(Theme::fmt_price_compact(Some(MicroUsdc(50_000))), ".05");
        assert_eq!(Theme::fmt_price_compact(Some(MicroUsdc(515_000))), ".515");
        assert_eq!(
            Theme::fmt_price_compact(Some(MicroUsdc(1_000_000))),
            "$1.00"
        );

        assert_eq!(
            Theme::fmt_bbo(Some(MicroUsdc(510_000)), Some(MicroUsdc(520_000))),
            ".51/.52"
        );
        assert_eq!(Theme::fmt_bbo(None, Some(MicroUsdc(520_000))), "--/.52");
        assert_eq!(
            Theme::fmt_pair_quotes(Some(MicroUsdc(500_000)), Some(MicroUsdc(470_000))),
            ".50/.47 (.97)"
        );
        assert_eq!(
            Theme::fmt_pair_quotes(Some(MicroUsdc(500_000)), None),
            ".50/--"
        );
        assert_eq!(Theme::fmt_pair_quotes(None, None), "--/--");

        let plain = Theme::plain();
        assert_eq!(
            plain.fmt_spot_vs_strike(Some(90_014.0), Some(90_000.0)),
            "+$14 (+2bp)"
        );
        assert_eq!(
            plain.fmt_spot_vs_strike(Some(89_986.0), Some(90_000.0)),
            "-$14 (-2bp)"
        );
        assert_eq!(plain.fmt_spot_vs_strike(None, Some(90_000.0)), "n/a");
    }

    #[test]
    fn latency_formatter_highlights_budget_breach() {
        let styled = Theme::styled();
        assert_eq!(styled.fmt_latency_us(12_000, 25), "\x1b[32m12us\x1b[0m");
        assert_eq!(styled.fmt_latency_us(31_000, 25), "\x1b[1;31m31us\x1b[0m");
    }
}
