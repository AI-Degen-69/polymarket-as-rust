//! Fixed-width console contract (AGENTS.md §9): column widths, the terminal
//! budget, and ANSI-aware padding live here and nowhere else.
//!
//! Reports pick columns from this module instead of declaring local widths, so
//! a layout change lands in one place and every row kind moves together.

/// Canonical terminal width. Every rendered row must fit inside it, and all
/// truncation budgets derive from it rather than from ad-hoc local limits.
pub(crate) const TERMINAL_WIDTH: usize = 140;

/// Width of the phase-transition frame rule.
pub(crate) const PHASE_RULE_WIDTH: usize = 124;
/// Epoch blocks reuse the phase width so both frames line up in scrollback.
pub(crate) const EPOCH_RULE_WIDTH: usize = PHASE_RULE_WIDTH;
/// Width of plain dividers drawn inside framed summaries.
pub(crate) const RULE_WIDTH: usize = 82;

/// Event badge column (`[ORDER]`, `[FILL]`, `[♥ BEAT]`, ...).
pub(crate) const EVENT_BADGE_WIDTH: usize = 9;
/// Epoch-phase badge column (`[QUOTE]`, `[FLATTEN]`, ...).
pub(crate) const PHASE_BADGE_WIDTH: usize = 10;
/// Risk-tier badge column (`[T1]`..`[T4]`).
pub(crate) const TIER_BADGE_WIDTH: usize = 4;
/// Leg/action column: `UP`/`DN` actions and the status labels beside them.
pub(crate) const LEG_WIDTH: usize = 21;
/// Status column for suppression/resume notices; matches [`LEG_WIDTH`].
pub(crate) const STATUS_WIDTH: usize = LEG_WIDTH;
/// Value column that keeps `Pricing` / `Risk` / `Total` latency cells aligned.
pub(crate) const LATENCY_VALUE_WIDTH: usize = 10;
/// Label column for framed epoch blocks (`★ EPOCH ROLLOVER`, `◆ EPOCH #2`).
pub(crate) const EPOCH_LABEL_WIDTH: usize = 19;
/// Key column inside the lifecycle summary frame.
pub(crate) const SUMMARY_LABEL_WIDTH: usize = 16;
/// Value column inside the lifecycle summary frame.
pub(crate) const SUMMARY_VALUE_WIDTH: usize = 12;

/// Strips ANSI CSI/SGR escape sequences (`\x1b[...<final>]`) while preserving
/// Unicode scalars.
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

/// Counts visible Unicode scalar positions after stripping ANSI escape
/// sequences.
pub(crate) fn visible_width(s: &str) -> usize {
    strip_ansi(s).chars().count()
}

/// Pads `s` on the left with spaces so its visible width reaches at least
/// `width`. The right-aligned counterpart of [`pad_visible_right`], ignoring
/// ANSI escapes.
pub(crate) fn pad_visible_left(s: &str, width: usize) -> String {
    let pad = width.saturating_sub(visible_width(s));
    format!("{}{s}", " ".repeat(pad))
}

/// Pads `s` on the right with spaces so its visible width reaches at least
/// `width`, ignoring ANSI escapes.
pub(crate) fn pad_visible_right(s: &str, width: usize) -> String {
    let pad = width.saturating_sub(visible_width(s));
    if pad == 0 {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(pad))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padding_and_width_ignore_ansi() {
        let styled = "\x1b[1;31mBTC\x1b[0m -$29";
        assert_eq!(strip_ansi(styled), "BTC -$29");
        assert_eq!(visible_width(styled), 8);
        assert_eq!(visible_width(&pad_visible_right(styled, 12)), 12);
        assert_eq!(visible_width(&pad_visible_left(styled, 12)), 12);
    }
}
