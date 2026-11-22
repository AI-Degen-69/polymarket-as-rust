use core_types::{
    FeeSchedule, MICRO_SCALE, MicroUsdc, ONE_DOLLAR_MICRO, ONE_SHARE_MICRO, OrderSide, Outcome,
    SharesMicro, SignedSharesMicro,
};
use serde::{Deserialize, Serialize};

use crate::error::PaperSimError;
use crate::fifo_book::SimFill;

/// Default maker rebate share (`20%` scaled by `10^6`) applied to counterfactual maker fee pool.
pub const DEFAULT_MAKER_REBATE_SHARE_E6: u64 = 200_000;

/// Event emitted when matching UP and DOWN shares are atomically merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeEvent {
    /// Number of paired outcome shares merged into USDC collateral.
    pub merged_pairs: SharesMicro,
    /// Volume-weighted average entry price of merged UP shares.
    pub avg_cost_up: MicroUsdc,
    /// Volume-weighted average entry price of merged DOWN shares.
    pub avg_cost_dn: MicroUsdc,
    /// Total combined pair acquisition cost (`avg_cost_up + avg_cost_dn`).
    pub pair_cost: MicroUsdc,
    /// Realized PnL from this merge event in micro-USDC (`$1.00 - pair_cost`).
    pub realized_gain_micro: i64,
}

/// Cash, inventory, fee, and VWAP pair-merge accounting ledger for paper trading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperLedger {
    /// Active unmerged filled UP outcome shares.
    pub filled_up: SharesMicro,
    /// Active unmerged filled DOWN outcome shares.
    pub filled_dn: SharesMicro,
    /// Total acquisition cost basis of unmerged UP shares in micro-USDC.
    pub cost_basis_up_micro: u64,
    /// Total acquisition cost basis of unmerged DOWN shares in micro-USDC.
    pub cost_basis_dn_micro: u64,
    /// Cumulative realized PnL in micro-USDC (from pair merges and position closes).
    pub realized_pnl_micro: i64,
    /// Cumulative taker fees paid in micro-USDC (maker passive fills pay 0).
    pub fees_paid_micro: u64,
    /// Cumulative maker weighted volume `sum shares * p * (1 - p)` in micro-units.
    pub maker_weighted_volume_micro: u128,
    /// Estimated cumulative maker rebate accrued in micro-USDC.
    pub estimated_rebate_micro: u64,
    /// Cumulative pairs merged via CTF redemption.
    pub total_pairs_merged: SharesMicro,
    /// Total fill events processed.
    pub total_fills_count: u64,
    /// Active market fee schedule.
    pub fee_schedule: FeeSchedule,
    /// Maker rebates locked in before prior fee-schedule transitions.
    #[serde(default)]
    carried_rebate_micro: u64,
    /// Cumulative maker weighted volume already accounted under prior fee schedules.
    #[serde(default)]
    rate_volume_base_micro: u128,
    /// Fee rate (`fee_rate_e6`) currently paired with `rate_volume_base_micro`.
    #[serde(default)]
    active_fee_rate_e6: u64,
}

impl PaperLedger {
    /// Constructs an empty paper ledger with the given fee schedule.
    #[must_use]
    pub const fn new(fee_schedule: FeeSchedule) -> Self {
        Self {
            filled_up: SharesMicro::ZERO,
            filled_dn: SharesMicro::ZERO,
            cost_basis_up_micro: 0,
            cost_basis_dn_micro: 0,
            realized_pnl_micro: 0,
            fees_paid_micro: 0,
            maker_weighted_volume_micro: 0,
            estimated_rebate_micro: 0,
            total_pairs_merged: SharesMicro::ZERO,
            total_fills_count: 0,
            active_fee_rate_e6: fee_schedule.fee_rate_e6,
            fee_schedule,
            carried_rebate_micro: 0,
            rate_volume_base_micro: 0,
        }
    }

    /// Records a fill event, updates inventory and cost basis, and performs
    /// an atomic VWAP pair merge if both UP and DOWN shares are held.
    pub fn record_fill(&mut self, fill: &SimFill) -> Result<Option<MergeEvent>, PaperSimError> {
        self.total_fills_count = self
            .total_fills_count
            .checked_add(1)
            .ok_or(PaperSimError::ArithmeticOverflow)?;

        if fill.is_maker {
            self.sync_fee_schedule_change();
            // Maker fee is zero on Polymarket CLOB.
            // Accumulate maker weighted volume: shares * p * (1 - p)
            let p_raw = fill.price.0;
            let one_minus_p = ONE_DOLLAR_MICRO.saturating_sub(p_raw);
            let wv_prod = (fill.filled_shares.0 as u128)
                .checked_mul(p_raw as u128)
                .and_then(|v| v.checked_mul(one_minus_p as u128))
                .ok_or(PaperSimError::ArithmeticOverflow)?;
            // wv_prod is scaled by 10^18. Divide by 10^12 to scale to 10^6 micro-units.
            let wv_micro = wv_prod / (MICRO_SCALE as u128 * MICRO_SCALE as u128);
            self.maker_weighted_volume_micro = self
                .maker_weighted_volume_micro
                .checked_add(wv_micro)
                .ok_or(PaperSimError::ArithmeticOverflow)?;
            self.refresh_estimated_rebate();
        }

        // Fill cost = filled_shares * price / 10^6
        let cost_micro =
            ((fill.filled_shares.0 as u128 * fill.price.0 as u128) / (MICRO_SCALE as u128)) as u64;

        match fill.outcome {
            Outcome::Up => {
                self.filled_up.0 = self
                    .filled_up
                    .0
                    .checked_add(fill.filled_shares.0)
                    .ok_or(PaperSimError::ArithmeticOverflow)?;
                self.cost_basis_up_micro = self
                    .cost_basis_up_micro
                    .checked_add(cost_micro)
                    .ok_or(PaperSimError::ArithmeticOverflow)?;
            }
            Outcome::Down => {
                self.filled_dn.0 = self
                    .filled_dn
                    .0
                    .checked_add(fill.filled_shares.0)
                    .ok_or(PaperSimError::ArithmeticOverflow)?;
                self.cost_basis_dn_micro = self
                    .cost_basis_dn_micro
                    .checked_add(cost_micro)
                    .ok_or(PaperSimError::ArithmeticOverflow)?;
            }
        }

        // Check for atomic VWAP pair merge
        let pairs_to_merge = self.filled_up.0.min(self.filled_dn.0);
        if pairs_to_merge > 0 {
            let merge_event = self.execute_pair_merge(pairs_to_merge)?;
            Ok(Some(merge_event))
        } else {
            Ok(None)
        }
    }

    /// Executes VWAP atomic pair merge of `pairs` shares of UP + DOWN.
    ///
    /// Credits `pairs * ($1.00 - avg_up - avg_dn)` to `realized_pnl_micro`
    /// and proportionally deducts cost basis from both legs.
    fn execute_pair_merge(&mut self, pairs: u64) -> Result<MergeEvent, PaperSimError> {
        let total_up = self.filled_up.0;
        let total_dn = self.filled_dn.0;

        if total_up == 0 || total_dn == 0 {
            return Err(PaperSimError::ArithmeticOverflow);
        }

        let cost_up_deducted =
            ((pairs as u128 * self.cost_basis_up_micro as u128) / total_up as u128) as u64;
        let cost_dn_deducted =
            ((pairs as u128 * self.cost_basis_dn_micro as u128) / total_dn as u128) as u64;

        let avg_cost_up =
            (cost_up_deducted as u128 * ONE_SHARE_MICRO as u128 / pairs as u128) as u64;
        let avg_cost_dn =
            (cost_dn_deducted as u128 * ONE_SHARE_MICRO as u128 / pairs as u128) as u64;
        let pair_cost = avg_cost_up.saturating_add(avg_cost_dn);

        // Redemption value: pairs * $1.00
        let redemption_micro =
            ((pairs as u128 * ONE_DOLLAR_MICRO as u128) / ONE_SHARE_MICRO as u128) as u64;
        let total_cost_deducted = cost_up_deducted.saturating_add(cost_dn_deducted);
        let realized_gain_micro = redemption_micro as i64 - total_cost_deducted as i64;

        self.realized_pnl_micro = self
            .realized_pnl_micro
            .checked_add(realized_gain_micro)
            .ok_or(PaperSimError::ArithmeticOverflow)?;

        self.total_pairs_merged.0 = self
            .total_pairs_merged
            .0
            .checked_add(pairs)
            .ok_or(PaperSimError::ArithmeticOverflow)?;

        self.filled_up.0 = self.filled_up.0.saturating_sub(pairs);
        self.filled_dn.0 = self.filled_dn.0.saturating_sub(pairs);

        self.cost_basis_up_micro = if self.filled_up.0 == 0 {
            0
        } else {
            self.cost_basis_up_micro.saturating_sub(cost_up_deducted)
        };
        self.cost_basis_dn_micro = if self.filled_dn.0 == 0 {
            0
        } else {
            self.cost_basis_dn_micro.saturating_sub(cost_dn_deducted)
        };

        Ok(MergeEvent {
            merged_pairs: SharesMicro(pairs),
            avg_cost_up: MicroUsdc(avg_cost_up),
            avg_cost_dn: MicroUsdc(avg_cost_dn),
            pair_cost: MicroUsdc(pair_cost),
            realized_gain_micro,
        })
    }

    /// Records an aggressive simulated taker flatten (FAK) order.
    ///
    /// Computes and deducts exact taker fee from realized PnL,
    /// and adjusts inventory and cash balances.
    pub fn record_taker_flatten(
        &mut self,
        outcome: Outcome,
        side: OrderSide,
        price: MicroUsdc,
        shares: SharesMicro,
    ) -> Result<MicroUsdc, PaperSimError> {
        let (fee, _) = self.record_taker_flatten_with_merge(outcome, side, price, shares)?;
        Ok(fee)
    }

    /// Records an aggressive simulated taker flatten (FAK) order and returns any
    /// resulting atomic pair merge event when a buy-side flatten pairs opposite shares.
    pub fn record_taker_flatten_with_merge(
        &mut self,
        outcome: Outcome,
        side: OrderSide,
        price: MicroUsdc,
        shares: SharesMicro,
    ) -> Result<(MicroUsdc, Option<MergeEvent>), PaperSimError> {
        let fee = self.fee_schedule.compute_taker_fee_micro(shares, price)?;
        self.fees_paid_micro = self
            .fees_paid_micro
            .checked_add(fee.0)
            .ok_or(PaperSimError::ArithmeticOverflow)?;
        self.realized_pnl_micro = self
            .realized_pnl_micro
            .checked_sub(fee.0 as i64)
            .ok_or(PaperSimError::ArithmeticOverflow)?;

        // Adjust position
        let notional = ((shares.0 as u128 * price.0 as u128) / (MICRO_SCALE as u128)) as u64;
        let merge_event = match side {
            OrderSide::Sell => {
                // Selling long position
                match outcome {
                    Outcome::Up => {
                        let shares_to_close = shares.0.min(self.filled_up.0);
                        if shares_to_close > 0 {
                            let cost_deducted = ((shares_to_close as u128
                                * self.cost_basis_up_micro as u128)
                                / self.filled_up.0 as u128)
                                as u64;
                            let proceeds = ((shares_to_close as u128 * price.0 as u128)
                                / (MICRO_SCALE as u128))
                                as u64;
                            let trade_pnl = proceeds as i64 - cost_deducted as i64;
                            self.realized_pnl_micro = self
                                .realized_pnl_micro
                                .checked_add(trade_pnl)
                                .ok_or(PaperSimError::ArithmeticOverflow)?;
                            self.filled_up.0 = self.filled_up.0.saturating_sub(shares_to_close);
                            self.cost_basis_up_micro = if self.filled_up.0 == 0 {
                                0
                            } else {
                                self.cost_basis_up_micro.saturating_sub(cost_deducted)
                            };
                        }
                    }
                    Outcome::Down => {
                        let shares_to_close = shares.0.min(self.filled_dn.0);
                        if shares_to_close > 0 {
                            let cost_deducted = ((shares_to_close as u128
                                * self.cost_basis_dn_micro as u128)
                                / self.filled_dn.0 as u128)
                                as u64;
                            let proceeds = ((shares_to_close as u128 * price.0 as u128)
                                / (MICRO_SCALE as u128))
                                as u64;
                            let trade_pnl = proceeds as i64 - cost_deducted as i64;
                            self.realized_pnl_micro = self
                                .realized_pnl_micro
                                .checked_add(trade_pnl)
                                .ok_or(PaperSimError::ArithmeticOverflow)?;
                            self.filled_dn.0 = self.filled_dn.0.saturating_sub(shares_to_close);
                            self.cost_basis_dn_micro = if self.filled_dn.0 == 0 {
                                0
                            } else {
                                self.cost_basis_dn_micro.saturating_sub(cost_deducted)
                            };
                        }
                    }
                }
                None
            }
            OrderSide::Buy => {
                // Buying shares aggressively
                match outcome {
                    Outcome::Up => {
                        self.filled_up.0 = self
                            .filled_up
                            .0
                            .checked_add(shares.0)
                            .ok_or(PaperSimError::ArithmeticOverflow)?;
                        self.cost_basis_up_micro = self
                            .cost_basis_up_micro
                            .checked_add(notional)
                            .ok_or(PaperSimError::ArithmeticOverflow)?;
                    }
                    Outcome::Down => {
                        self.filled_dn.0 = self
                            .filled_dn
                            .0
                            .checked_add(shares.0)
                            .ok_or(PaperSimError::ArithmeticOverflow)?;
                        self.cost_basis_dn_micro = self
                            .cost_basis_dn_micro
                            .checked_add(notional)
                            .ok_or(PaperSimError::ArithmeticOverflow)?;
                    }
                }
                let pairs_to_merge = self.filled_up.0.min(self.filled_dn.0);
                if pairs_to_merge > 0 {
                    Some(self.execute_pair_merge(pairs_to_merge)?)
                } else {
                    None
                }
            }
        };

        Ok((fee, merge_event))
    }

    /// Returns the volume-weighted average price (VWAP) of active UP shares.
    #[must_use]
    pub fn vwap_up(&self) -> Option<MicroUsdc> {
        if self.filled_up.0 == 0 {
            None
        } else {
            let vwap = (self.cost_basis_up_micro as u128 * ONE_SHARE_MICRO as u128
                / self.filled_up.0 as u128) as u64;
            Some(MicroUsdc(vwap))
        }
    }

    /// Returns the volume-weighted average price (VWAP) of active DOWN shares.
    #[must_use]
    pub fn vwap_dn(&self) -> Option<MicroUsdc> {
        if self.filled_dn.0 == 0 {
            None
        } else {
            let vwap = (self.cost_basis_dn_micro as u128 * ONE_SHARE_MICRO as u128
                / self.filled_dn.0 as u128) as u64;
            Some(MicroUsdc(vwap))
        }
    }

    /// Returns the active VWAP acquisition cost of unmerged UP shares (`None` when flat UP).
    #[must_use]
    pub fn avg_cost_up(&self) -> Option<MicroUsdc> {
        self.vwap_up()
    }

    /// Returns the active VWAP acquisition cost of unmerged DOWN shares (`None` when flat DOWN).
    #[must_use]
    pub fn avg_cost_dn(&self) -> Option<MicroUsdc> {
        self.vwap_dn()
    }

    /// Returns signed net inventory `q = q_UP - q_DOWN`.
    #[must_use]
    pub fn net_inventory(&self) -> SignedSharesMicro {
        let diff = (self.filled_up.0 as i64) - (self.filled_dn.0 as i64);
        SignedSharesMicro(diff)
    }

    /// Computes mark-to-market unrealized PnL in micro-USDC given current fair/mid prices.
    #[must_use]
    pub fn unrealized_pnl_micro(&self, mid_up: MicroUsdc, mid_dn: MicroUsdc) -> i64 {
        let mark_val_up =
            ((self.filled_up.0 as u128 * mid_up.0 as u128) / (MICRO_SCALE as u128)) as i64;
        let mark_val_dn =
            ((self.filled_dn.0 as u128 * mid_dn.0 as u128) / (MICRO_SCALE as u128)) as i64;

        let pnl_up = mark_val_up - (self.cost_basis_up_micro as i64);
        let pnl_dn = mark_val_dn - (self.cost_basis_dn_micro as i64);
        pnl_up + pnl_dn
    }

    /// Computes total net PnL (realized + unrealized) in micro-USDC.
    #[must_use]
    pub fn net_pnl_micro(&self, mid_up: MicroUsdc, mid_dn: MicroUsdc) -> i64 {
        self.realized_pnl_micro + self.unrealized_pnl_micro(mid_up, mid_dn)
    }

    fn compute_rebate_for_volume(weighted_volume_micro: u128, fee_rate_e6: u64) -> u64 {
        let fee_rate = fee_rate_e6 as u128;
        let rebate_share = DEFAULT_MAKER_REBATE_SHARE_E6 as u128;
        let scale_sq = (MICRO_SCALE as u128) * (MICRO_SCALE as u128);
        let rebate_u128 = weighted_volume_micro
            .saturating_mul(fee_rate)
            .saturating_mul(rebate_share)
            / scale_sq;
        u64::try_from(rebate_u128).unwrap_or(u64::MAX)
    }

    fn sync_fee_schedule_change(&mut self) {
        if self.fee_schedule.fee_rate_e6 != self.active_fee_rate_e6 {
            let prev_wv = self
                .maker_weighted_volume_micro
                .saturating_sub(self.rate_volume_base_micro);
            let prev_rebate = Self::compute_rebate_for_volume(prev_wv, self.active_fee_rate_e6);
            self.carried_rebate_micro = self.carried_rebate_micro.saturating_add(prev_rebate);
            self.rate_volume_base_micro = self.maker_weighted_volume_micro;
            self.active_fee_rate_e6 = self.fee_schedule.fee_rate_e6;
        }
    }

    /// Updates the active market fee schedule while locking in previously accrued maker rebates.
    pub fn set_fee_schedule(&mut self, next_schedule: FeeSchedule) {
        self.fee_schedule = next_schedule;
        self.sync_fee_schedule_change();
        self.refresh_estimated_rebate();
    }

    /// Updates `estimated_rebate_micro` deterministically from accumulated maker weighted volume.
    pub fn refresh_estimated_rebate(&mut self) {
        self.sync_fee_schedule_change();
        let current_wv = self
            .maker_weighted_volume_micro
            .saturating_sub(self.rate_volume_base_micro);
        let current_rebate =
            Self::compute_rebate_for_volume(current_wv, self.fee_schedule.fee_rate_e6);
        self.estimated_rebate_micro = self.carried_rebate_micro.saturating_add(current_rebate);
    }

    /// Settles residual binary inventory at epoch expiry (`winning_outcome` pays `$1.00`,
    /// losing outcome pays `$0.00`), locks the resulting PnL into `realized_pnl_micro`,
    /// and clears active leg balances and cost basis for the next epoch.
    ///
    /// Returns the PnL delta realized by this terminal settlement.
    pub fn settle_epoch(&mut self, winning_outcome: Outcome) -> i64 {
        let settled_total = self.settlement_pnl_micro(winning_outcome);
        let epoch_settlement_delta = settled_total.saturating_sub(self.realized_pnl_micro);
        self.realized_pnl_micro = settled_total;
        self.filled_up = SharesMicro::ZERO;
        self.filled_dn = SharesMicro::ZERO;
        self.cost_basis_up_micro = 0;
        self.cost_basis_dn_micro = 0;
        epoch_settlement_delta
    }

    /// Computes terminal binary settlement PnL when the epoch resolves
    /// with `winning_outcome` paying $1.00 and the losing outcome paying $0.00.
    #[must_use]
    pub fn settlement_pnl_micro(&self, winning_outcome: Outcome) -> i64 {
        let (val_up, val_dn) = match winning_outcome {
            Outcome::Up => (self.filled_up.0 as i64, 0_i64),
            Outcome::Down => (0_i64, self.filled_dn.0 as i64),
        };
        let settlement_unrealized =
            (val_up - self.cost_basis_up_micro as i64) + (val_dn - self.cost_basis_dn_micro as i64);
        self.realized_pnl_micro + settlement_unrealized
    }
}
