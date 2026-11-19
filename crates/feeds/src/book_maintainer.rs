use std::collections::BTreeMap;

use core_types::{
    BookLevel, FixedBook, MicroUsdc, ONE_DOLLAR_MICRO, OrderSide, Outcome, SharesMicro,
};

use crate::error::FeedError;

/// Deterministic L2 orderbook maintainer sorting bids descending and asks ascending on the hot path.
///
/// Normalizes Polymarket's raw wire ordering (which often emits bids ascending and asks descending),
/// deletes zero-size levels, and explicitly detects crossed books (`best_bid >= best_ask`).
#[derive(Debug, Clone)]
pub struct BookMaintainer {
    outcome: Outcome,
    bids: BTreeMap<MicroUsdc, SharesMicro>,
    asks: BTreeMap<MicroUsdc, SharesMicro>,
    last_update_ns: u64,
    exchange_ts_ms: u64,
}

impl BookMaintainer {
    /// Creates an empty L2 orderbook maintainer for `outcome`.
    #[must_use]
    pub const fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_ns: 0,
            exchange_ts_ms: 0,
        }
    }

    /// Replaces both sides atomically from a REST or WS `book` snapshot and returns sorted top-10 depth.
    ///
    /// On error (e.g., invalid price or crossed snapshot), the existing orderbook state is left unchanged.
    pub fn apply_snapshot(
        &mut self,
        bids_in: &[BookLevel],
        asks_in: &[BookLevel],
        rx_mono_ns: u64,
        exch_ts_ms: u64,
    ) -> Result<FixedBook<10>, FeedError> {
        let (mut next_bids, mut next_asks) = (BTreeMap::new(), BTreeMap::new());
        for level in bids_in {
            if level.size.0 > 0 {
                validate_level_price(level.price)?;
                next_bids.insert(level.price, level.size);
            }
        }
        for level in asks_in {
            if level.size.0 > 0 {
                validate_level_price(level.price)?;
                next_asks.insert(level.price, level.size);
            }
        }
        check_maps_uncrossed(self.outcome, &next_bids, &next_asks)?;
        self.bids = next_bids;
        self.asks = next_asks;
        self.last_update_ns = rx_mono_ns;
        self.exchange_ts_ms = exch_ts_ms;
        self.top_n::<10>()
    }

    /// Atomically applies one incremental L2 delta (`size == 0` deletes level) without crossing the book.
    ///
    /// On error (including `FeedError::BookCrossed`), internal book levels and timestamps remain unchanged.
    pub fn apply_price_change_delta(
        &mut self,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        rx_mono_ns: u64,
        exch_ts_ms: u64,
    ) -> Result<(), FeedError> {
        validate_level_price(price)?;
        if size.0 > 0 {
            check_delta_would_cross(self.outcome, side, price, &self.bids, &self.asks)?;
        }
        self.apply_delta_unchecked(side, price, size, rx_mono_ns, exch_ts_ms);
        Ok(())
    }

    /// Applies one incremental L2 delta allowing transient mid-batch crossings; caller must invoke `check_uncrossed()`.
    pub fn apply_delta_allow_transient_cross(
        &mut self,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        rx_mono_ns: u64,
        exch_ts_ms: u64,
    ) -> Result<(), FeedError> {
        validate_level_price(price)?;
        self.apply_delta_unchecked(side, price, size, rx_mono_ns, exch_ts_ms);
        Ok(())
    }

    fn apply_delta_unchecked(
        &mut self,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        rx_mono_ns: u64,
        exch_ts_ms: u64,
    ) {
        let map = match side {
            OrderSide::Buy => &mut self.bids,
            OrderSide::Sell => &mut self.asks,
        };
        if size.0 == 0 {
            map.remove(&price);
        } else {
            map.insert(price, size);
        }
        self.last_update_ns = rx_mono_ns;
        self.exchange_ts_ms = exch_ts_ms;
    }

    /// Applies one incremental L2 delta (`size == 0` deletes level) and returns sorted top-10 depth.
    pub fn apply_price_change(
        &mut self,
        side: OrderSide,
        price: MicroUsdc,
        size: SharesMicro,
        rx_mono_ns: u64,
        exch_ts_ms: u64,
    ) -> Result<FixedBook<10>, FeedError> {
        self.apply_price_change_delta(side, price, size, rx_mono_ns, exch_ts_ms)?;
        self.top_n::<10>()
    }

    /// Extracts sorted top `N` levels (`bids` descending, `asks` ascending) with zero heap allocations.
    pub fn top_n<const N: usize>(&self) -> Result<FixedBook<N>, FeedError> {
        const { assert!(N <= u8::MAX as usize) };
        let mut bids_top = [BookLevel::EMPTY; N];
        let mut b_len = 0_usize;
        for (dst, (&price, &size)) in bids_top.iter_mut().zip(self.bids.iter().rev()) {
            *dst = BookLevel { price, size };
            b_len += 1;
        }
        let mut asks_top = [BookLevel::EMPTY; N];
        let mut a_len = 0_usize;
        for (dst, (&price, &size)) in asks_top.iter_mut().zip(self.asks.iter()) {
            *dst = BookLevel { price, size };
            a_len += 1;
        }

        Ok(FixedBook::<N>::try_from_slices(
            self.outcome,
            bids_top.get(..b_len).unwrap_or(&[]),
            asks_top.get(..a_len).unwrap_or(&[]),
            self.last_update_ns,
            self.exchange_ts_ms,
        )?)
    }

    /// Best (highest) bid price level if present.
    #[must_use]
    pub fn best_bid(&self) -> Option<BookLevel> {
        self.bids
            .iter()
            .next_back()
            .map(|(&price, &size)| BookLevel { price, size })
    }

    /// Best (lowest) ask price level if present.
    #[must_use]
    pub fn best_ask(&self) -> Option<BookLevel> {
        self.asks
            .iter()
            .next()
            .map(|(&price, &size)| BookLevel { price, size })
    }

    /// Returns `true` when `best_bid >= best_ask`.
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price >= ask.price,
            _ => false,
        }
    }

    /// Associated market outcome (`Up` or `Down`).
    #[must_use]
    pub const fn outcome(&self) -> Outcome {
        self.outcome
    }

    /// Validates that `best_bid < best_ask`, returning `FeedError::BookCrossed` otherwise.
    pub fn check_uncrossed(&self) -> Result<(), FeedError> {
        check_maps_uncrossed(self.outcome, &self.bids, &self.asks)
    }
}

fn check_maps_uncrossed(
    outcome: Outcome,
    bids: &BTreeMap<MicroUsdc, SharesMicro>,
    asks: &BTreeMap<MicroUsdc, SharesMicro>,
) -> Result<(), FeedError> {
    if let (Some((&bid_p, _)), Some((&ask_p, _))) = (bids.iter().next_back(), asks.iter().next())
        && bid_p >= ask_p
    {
        return Err(FeedError::BookCrossed {
            outcome,
            best_bid_micro: bid_p.0,
            best_ask_micro: ask_p.0,
        });
    }
    Ok(())
}

fn check_delta_would_cross(
    outcome: Outcome,
    side: OrderSide,
    price: MicroUsdc,
    bids: &BTreeMap<MicroUsdc, SharesMicro>,
    asks: &BTreeMap<MicroUsdc, SharesMicro>,
) -> Result<(), FeedError> {
    match side {
        OrderSide::Buy => {
            if let Some((&ask_p, _)) = asks.iter().next()
                && price >= ask_p
            {
                return Err(FeedError::BookCrossed {
                    outcome,
                    best_bid_micro: price.0,
                    best_ask_micro: ask_p.0,
                });
            }
        }
        OrderSide::Sell => {
            if let Some((&bid_p, _)) = bids.iter().next_back()
                && bid_p >= price
            {
                return Err(FeedError::BookCrossed {
                    outcome,
                    best_bid_micro: bid_p.0,
                    best_ask_micro: price.0,
                });
            }
        }
    }
    Ok(())
}

fn validate_level_price(price: MicroUsdc) -> Result<(), FeedError> {
    if price.0 == 0 || price.0 >= ONE_DOLLAR_MICRO {
        return Err(FeedError::InvalidMarket {
            detail: format!("book level price {} outside (0, 1_000_000)", price.0),
        });
    }
    Ok(())
}
