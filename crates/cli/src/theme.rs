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
/// Width of the frame drawn around phase transitions (fits a standard terminal).
const PHASE_RULE_WIDTH: usize = 124;
/// Epoch blocks use the same width so both frames line up in scrollback.
const EPOCH_RULE_WIDTH: usize = PHASE_RULE_WIDTH;

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
    Tier,
    Info,
    Heartbeat,
    Maker,
    Taker,
}

impl Badge {
    /// Visible badge text, used to pad columns without counting ANSI escape codes.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Fill => "[FILL]",
            Self::Merge => "[MERGE]",
            Self::Flatten => "[FLATTEN]",
            Self::Breaker => "[BREAKER]",
            Self::Wait => "[WAIT]",
            Self::Ready => "[READY]",
            Self::Pass => "[PASS]",
            Self::Warn => "[WARN]",
            Self::Fail => "[FAIL]",
            Self::Order => "[ORDER]",
            Self::Trade => "[TRADE]",
            Self::Suppr => "[SUPPR]",
            Self::Resume => "[RESUME]",
            Self::Latency => "[LATENCY]",
            Self::Epoch => "[EPOCH]",
            Self::Signal => "[SIGNAL]",
            Self::Tier => "[TIER]",
            Self::Info => "[INFO]",
            Self::Heartbeat => "[♥ BEAT]",
            Self::Maker => "[MAKER]",
            Self::Taker => "[TAKER]",
        }
    }
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

    /// Resolves styling from an explicit `--color` flag, TTY state, `NO_COLOR`, and VT support.
    pub(crate) const fn from_choice_with_vt(
        choice: ColorChoice,
        is_tty: bool,
        no_color: bool,
        vt_supported: bool,
    ) -> Self {
        let base = Self::from_choice(choice, is_tty, no_color);
        if base.color && is_tty && !vt_supported {
            Self::plain()
        } else {
            base
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

    pub(crate) fn strikethrough(self, text: &str) -> String {
        if self.color {
            self.wrap("2;9", text)
        } else {
            format!("~{text}~")
        }
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

    /// Vertical column separator (`│`, ASCII `|` fallback).
    pub(crate) const fn bar(self) -> &'static str {
        if self.unicode { "│" } else { "|" }
    }

    /// Rightward transition arrow (`➔`, ASCII `->` fallback).
    pub(crate) const fn arrow(self) -> &'static str {
        if self.unicode { "➔" } else { "->" }
    }

    /// Clamp marker for a row that exceeded the width budget.
    pub(crate) const fn ellipsis(self) -> &'static str {
        if self.unicode { "…" } else { "..." }
    }

    /// Inline field separator (`·`, ASCII `-` fallback).
    pub(crate) const fn dot(self) -> &'static str {
        if self.unicode { "·" } else { "-" }
    }

    /// Low-emphasis gray (`\x1b[90m`) for trailing reason notes.
    pub(crate) fn gray(self, text: &str) -> String {
        self.wrap("90", text)
    }

    /// Full-width horizontal rule framing a phase transition.
    pub(crate) fn phase_rule(self) -> String {
        let ch = if self.unicode { "─" } else { "-" };
        self.muted(&ch.repeat(PHASE_RULE_WIDTH))
    }

    /// True when the terminal can render Unicode box-drawing glyphs.
    pub(crate) const fn box_drawing(self) -> bool {
        self.unicode
    }

    /// Full-width double rule anchoring an epoch boundary (`═`), ASCII `=` fallback.
    pub(crate) fn epoch_rule(self) -> String {
        let ch = if self.unicode { "═" } else { "=" };
        self.muted(&ch.repeat(EPOCH_RULE_WIDTH))
    }

    /// Single-character icon used by the epoch block: `◆` header, `★` rollover.
    pub(crate) const fn epoch_icon(self, rollover: bool) -> &'static str {
        match (self.unicode, rollover) {
            (true, true) => "★",
            (true, false) => "◆",
            (false, true) => "*",
            (false, false) => "+",
        }
    }

    /// Circuit-breaker icon: `⚡` when tripped, `✔` when armed.
    pub(crate) fn breaker_icon(self, tripped: bool) -> String {
        match (self.unicode, tripped) {
            (true, true) => self.bold_red("⚡"),
            (true, false) => self.bold_green("✔"),
            (false, true) => self.bold_red("!"),
            (false, false) => self.bold_green("+"),
        }
    }

    /// Short upper-case phase label (`QUOTE`, `LOCKOUT`, `FLATTEN`, `TERMINAL`).
    pub(crate) const fn phase_tag(phase: EpochPhase) -> &'static str {
        match phase {
            EpochPhase::ActiveQuoting => "QUOTE",
            EpochPhase::PreExpiryLockout => "LOCKOUT",
            EpochPhase::MandatoryFlatten => "FLATTEN",
            EpochPhase::TerminalLockout => "TERMINAL",
        }
    }

    /// Phase icon with its phase color: `▶` green, `⚠` amber, `▼` dark orange, `■` red.
    pub(crate) fn phase_icon(self, phase: EpochPhase) -> String {
        if !self.unicode {
            let ascii = match phase {
                EpochPhase::ActiveQuoting => ">",
                EpochPhase::PreExpiryLockout => "!",
                EpochPhase::MandatoryFlatten => "v",
                EpochPhase::TerminalLockout => "#",
            };
            return ascii.to_string();
        }
        match phase {
            EpochPhase::ActiveQuoting => self.bold_green("▶"),
            EpochPhase::PreExpiryLockout => self.bold_yellow("⚠"),
            EpochPhase::MandatoryFlatten => self.wrap("38;5;208", "▼"),
            EpochPhase::TerminalLockout => self.bold_red("■"),
        }
    }

    /// Short phase label colored to match its icon.
    pub(crate) fn phase_tag_styled(self, phase: EpochPhase) -> String {
        let tag = Self::phase_tag(phase);
        match phase {
            EpochPhase::ActiveQuoting => self.bold_green(tag),
            EpochPhase::PreExpiryLockout => self.bold_yellow(tag),
            EpochPhase::MandatoryFlatten => self.wrap("38;5;208", tag),
            EpochPhase::TerminalLockout => self.bold_red(tag),
        }
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
            Badge::Tier => self.bold_yellow("[TIER]"),
            Badge::Info => self.cyan("[INFO]"),
            Badge::Heartbeat => self.bold_red("[♥ BEAT]"),
            Badge::Maker => self.bold_green("[MAKER]"),
            Badge::Taker => self.bold_yellow("[TAKER]"),
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

    /// Formats a price in cents (`21¢`, `55.5¢`, `55.25¢`, rounded to `0.01¢`) or `--` for `None`.
    pub(crate) fn fmt_cents(px: Option<MicroUsdc>) -> String {
        let Some(MicroUsdc(m)) = px else {
            return "--".to_string();
        };
        let hundredths = m.saturating_add(50) / 100;
        let cents = hundredths / 100;
        let rem = hundredths % 100;
        if rem == 0 {
            format!("{cents}¢")
        } else if rem.is_multiple_of(10) {
            format!("{cents}.{}¢", rem / 10)
        } else {
            format!("{cents}.{rem:02}¢")
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
        self.fmt_latency_us_width(ns, budget_us, 0)
    }

    /// Pads a latency measurement to a fixed-width cell, outside the ANSI style. Right-aligned,
    /// so each value sits one space before its label while the labels stay aligned.
    pub(crate) fn fmt_latency_us_width(self, ns: u64, budget_us: u64, width: usize) -> String {
        let raw = latency_us_raw(ns);
        let styled = if ns > budget_us.saturating_mul(1_000) {
            self.bold_red(&raw)
        } else {
            self.green(&raw)
        };
        let pad = " ".repeat(width.saturating_sub(raw.chars().count()));
        format!("{pad}{styled}")
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

/// Formats nanoseconds as microseconds, keeping fractional detail so a breach cannot hide.
fn latency_us_raw(ns: u64) -> String {
    let whole = ns / 1_000;
    let frac = ns % 1_000;
    if frac == 0 {
        return format!("{whole}us");
    }
    let mut digits = format!("{frac:03}");
    while digits.ends_with('0') {
        digits.pop();
    }
    format!("{whole}.{digits}us")
}

/// Strips ANSI CSI/SGR escape sequences (`\x1b[...<final>`) while preserving Unicode scalars.
pub(crate) fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for seq_ch in chars.by_ref() {
                if ('@'..='~').contains(&seq_ch) {
                    break;
                }
            }
            continue;
        }
        out.push(ch);
    }
    out
}

/// Counts visible Unicode scalar positions after stripping ANSI escape sequences.
pub(crate) fn visible_width(s: &str) -> usize {
    strip_ansi(s).chars().count()
}

/// Pads `s` on the right with spaces so its visible width reaches at least `width`.
pub(crate) fn pad_visible_right(s: &str, width: usize) -> String {
    let pad = width.saturating_sub(visible_width(s));
    if pad == 0 {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(pad))
    }
}

/// Returns `true` if `NO_COLOR` is set to a non-empty value.
pub(crate) fn no_color_env() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|val| !val.is_empty())
}

fn resolve_stdout_theme(choice: ColorChoice) -> Theme {
    let is_tty = std::io::stdout().is_terminal();
    let no_color = no_color_env();
    let base = Theme::from_choice(choice, is_tty, no_color);
    let vt_supported = if base.is_color() && is_tty {
        core_types::enable_stdout_vt_processing()
    } else {
        true
    };
    Theme::from_choice_with_vt(choice, is_tty, no_color, vt_supported)
}

/// Initializes the process-wide terminal theme from the global `--color` flag.
pub(crate) fn init(choice: ColorChoice) {
    let theme = resolve_stdout_theme(choice);
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
        _ => resolve_stdout_theme(ColorChoice::Auto),
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
    fn compact_price_and_spot_formatters() {
        assert_eq!(Theme::fmt_cents(None), "--");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(210_000))), "21¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(10_000))), "1¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(500_000))), "50¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(950_000))), "95¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(555_000))), "55.5¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(552_500))), "55.25¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(473_318))), "47.33¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(466_682))), "46.67¢");
        assert_eq!(Theme::fmt_cents(Some(MicroUsdc(1_000_000))), "100¢");

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
    fn phase_icons_rules_and_tags_are_distinct_and_width_bounded() {
        let plain = Theme::plain();
        assert_eq!(plain.phase_icon(EpochPhase::ActiveQuoting), ">");
        assert_eq!(plain.phase_icon(EpochPhase::PreExpiryLockout), "!");
        assert_eq!(plain.phase_icon(EpochPhase::MandatoryFlatten), "v");
        assert_eq!(plain.phase_icon(EpochPhase::TerminalLockout), "#");
        assert_eq!(Theme::phase_tag(EpochPhase::ActiveQuoting), "QUOTE");
        assert_eq!(Theme::phase_tag(EpochPhase::PreExpiryLockout), "LOCKOUT");
        assert_eq!(Theme::phase_tag(EpochPhase::MandatoryFlatten), "FLATTEN");
        assert_eq!(Theme::phase_tag(EpochPhase::TerminalLockout), "TERMINAL");
        assert_eq!(plain.phase_rule().chars().count(), PHASE_RULE_WIDTH);
        assert_eq!(plain.bar(), "|");

        let styled = Theme::styled();
        assert_eq!(styled.bar(), "│");
        assert_eq!(styled.arrow(), "➔");
        assert_eq!(
            visible_width(&styled.phase_icon(EpochPhase::MandatoryFlatten)),
            1
        );
        assert!(
            styled
                .phase_icon(EpochPhase::MandatoryFlatten)
                .contains("\x1b[38;5;208m")
        );
        assert_eq!(
            strip_ansi(&styled.phase_tag_styled(EpochPhase::TerminalLockout)),
            "TERMINAL"
        );
        assert_eq!(strip_ansi(&styled.gray("note")), "note");
        assert!(styled.gray("note").starts_with("\x1b[90m"));
    }

    #[test]
    fn ansi_strip_visible_width_and_padding_handle_unicode_and_sgr() {
        let plain = "Book UP 21¢/22¢ DN 78¢/79¢";
        assert_eq!(strip_ansi(plain), plain);
        assert_eq!(visible_width(plain), 26);
        assert_eq!(pad_visible_right(plain, 28).chars().count(), 28);
        assert_eq!(pad_visible_right(plain, 20), plain);

        let styled = Theme::styled();
        let btc = format!("BTC {}", styled.red("-$29 (-5bp)"));
        assert_eq!(strip_ansi(&btc), "BTC -$29 (-5bp)");
        assert_eq!(visible_width(&btc), 15);
        let padded = pad_visible_right(&btc, 19);
        assert_eq!(visible_width(&padded), 19);
        assert_eq!(strip_ansi(&padded), "BTC -$29 (-5bp)    ");

        let inv = "Inv 0U/1.5D (Δ-1.5)";
        assert_eq!(visible_width(inv), 19);
        assert_eq!(visible_width(&pad_visible_right(inv, 25)), 25);

        let hb = styled.badge(Badge::Heartbeat);
        assert_eq!(strip_ansi(&hb), "[♥ BEAT]");
        assert_eq!(visible_width(&hb), 8);

        assert_eq!(Theme::plain().strikethrough("51.48¢"), "~51.48¢~");
        assert_eq!(styled.strikethrough("51.48¢"), "\x1b[2;9m51.48¢\x1b[0m");
    }

    #[test]
    fn latency_formatter_highlights_budget_breach() {
        let styled = Theme::styled();
        assert_eq!(styled.fmt_latency_us(12_000, 25), "\x1b[32m12us\x1b[0m");
        assert_eq!(styled.fmt_latency_us(31_000, 25), "\x1b[1;31m31us\x1b[0m");
    }

    #[test]
    fn latency_formatter_checks_sub_microsecond_boundaries_without_overflow() {
        let styled = Theme::styled();
        // Exact budgets stay green; +1ns breaches turn bold red even when truncated us matches.
        assert_eq!(styled.fmt_latency_us(10_000, 10), "\x1b[32m10us\x1b[0m");
        assert_eq!(
            styled.fmt_latency_us(10_001, 10),
            "\x1b[1;31m10.001us\x1b[0m"
        );
        assert_eq!(styled.fmt_latency_us(25_000, 25), "\x1b[32m25us\x1b[0m");
        assert_eq!(
            styled.fmt_latency_us(25_001, 25),
            "\x1b[1;31m25.001us\x1b[0m"
        );
        assert_eq!(styled.fmt_latency_us(100_000, 100), "\x1b[32m100us\x1b[0m");
        assert_eq!(
            styled.fmt_latency_us(100_001, 100),
            "\x1b[1;31m100.001us\x1b[0m"
        );
        // Large budget does not overflow when converting to nanoseconds.
        assert_eq!(
            styled.fmt_latency_us(u64::MAX, u64::MAX),
            format!("\x1b[32m{}.615us\x1b[0m", u64::MAX / 1_000)
        );
    }

    #[test]
    fn vt_processing_fallback_disables_color_on_unsupported_tty() {
        assert_eq!(
            Theme::from_choice_with_vt(ColorChoice::Auto, true, false, false),
            Theme::plain()
        );
        assert_eq!(
            Theme::from_choice_with_vt(ColorChoice::Always, true, false, false),
            Theme::plain()
        );
        // Non-TTY `--color always` does not require a Windows console handle.
        assert_eq!(
            Theme::from_choice_with_vt(ColorChoice::Always, false, false, false),
            Theme::styled()
        );
    }
}
