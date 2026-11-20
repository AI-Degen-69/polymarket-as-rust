use alloy::primitives::{Address, B256, FixedBytes, U256, address};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::{Eip712Domain, SolStruct, eip712_domain};
use core_types::market::{MarketDescriptor, ProtocolVersion, WalletMode};
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use std::str::FromStr;

use crate::error::ExecutionError;

// Canonical Polymarket Exchange Contract Addresses on Polygon (Chain ID 137)
pub const POLY_V2_EXCHANGE: Address = address!("e3333700cA9d93003F00f0F71f8515005F6c00Aa");
pub const CTF_EXCHANGE: Address = address!("E111180000d2663C0091e4f400237545B87B996B");
pub const CTF_NEG_RISK_EXCHANGE: Address = address!("e2222d279d744050d28e00520010520000310F59");

pub const POLYGON_CHAIN_ID: u64 = 137;
pub const EXCHANGE_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
pub const DEPOSIT_WALLET_DOMAIN_NAME: &str = "DepositWallet";
pub const DEPOSIT_WALLET_DOMAIN_VERSION: &str = "1";

pub const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1; // 9_007_199_254_740_991

pub const ORDER_TYPE_STRING: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

pub const TYPED_DATA_SIGN_TYPE_STRING: &str = "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

/// Precomputed hashes for zero-allocation DepositWallet EIP-712 signing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepositWalletConstants {
    pub typed_data_sign_type_hash: B256,
    pub name_hash: B256,
    pub version_hash: B256,
    pub chain_id_bytes: [u8; 32],
}

impl Default for DepositWalletConstants {
    fn default() -> Self {
        Self::new()
    }
}

impl DepositWalletConstants {
    #[must_use]
    pub fn new() -> Self {
        Self::for_chain(POLYGON_CHAIN_ID)
    }

    /// Builds constants for an arbitrary EVM chain id (NEX sandbox uses Amoy `80002`).
    #[must_use]
    pub fn for_chain(chain_id: u64) -> Self {
        Self {
            typed_data_sign_type_hash: alloy::primitives::keccak256(
                TYPED_DATA_SIGN_TYPE_STRING.as_bytes(),
            ),
            name_hash: alloy::primitives::keccak256(DEPOSIT_WALLET_DOMAIN_NAME.as_bytes()),
            version_hash: alloy::primitives::keccak256(DEPOSIT_WALLET_DOMAIN_VERSION.as_bytes()),
            chain_id_bytes: U256::from(chain_id).to_be_bytes::<32>(),
        }
    }

    /// Computes the EIP-712 hashStruct of `TypedDataSign` with zero heap allocations.
    #[must_use]
    pub fn hash_typed_data_sign(&self, contents_hash: B256, verifying_contract: Address) -> B256 {
        let mut buf = [0u8; 224]; // 7 * 32
        buf[0..32].copy_from_slice(self.typed_data_sign_type_hash.as_slice());
        buf[32..64].copy_from_slice(contents_hash.as_slice());
        buf[64..96].copy_from_slice(self.name_hash.as_slice());
        buf[96..128].copy_from_slice(self.version_hash.as_slice());
        buf[128..160].copy_from_slice(&self.chain_id_bytes);
        buf[172..192].copy_from_slice(verifying_contract.as_slice()); // 12 zero bytes + 20 address bytes
        // buf[192..224] is already 32 zero bytes for salt

        alloy::primitives::keccak256(buf)
    }

    /// Computes the full EIP-712 signing digest: `keccak256("\x19\x01" || domain_separator || hash_struct)`.
    #[must_use]
    pub fn signing_digest(domain_separator: B256, struct_hash: B256) -> B256 {
        let mut buf = [0u8; 66];
        buf[0] = 0x19;
        buf[1] = 0x01;
        buf[2..34].copy_from_slice(domain_separator.as_slice());
        buf[34..66].copy_from_slice(struct_hash.as_slice());
        alloy::primitives::keccak256(buf)
    }
}

/// Generates a CSPRNG salt strictly <= Number.MAX_SAFE_INTEGER (2^53 - 1).
#[must_use]
pub fn generate_salt() -> u64 {
    let mut rng = rand::rng();
    rng.next_u64() % (MAX_SAFE_INTEGER + 1)
}

/// Precomputed EIP-712 domain info for a specific market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDomain {
    pub exchange_address: Address,
    pub domain_version: &'static str,
    pub domain: Eip712Domain,
    pub domain_separator: B256,
}

impl MarketDomain {
    /// Builds precomputed domain separator for given market descriptor on Polygon `137`.
    #[must_use]
    pub fn for_market(market: &MarketDescriptor) -> Self {
        let exchange_address = exchange_for_market(market);
        Self::for_market_on_chain(market, POLYGON_CHAIN_ID, exchange_address)
    }

    /// Builds the domain for a market on an explicit chain and exchange contract.
    ///
    /// NEX sandbox uses this with Amoy (`80002`) and the onboarding-provided exchange.
    #[must_use]
    pub fn for_market_on_chain(
        market: &MarketDescriptor,
        chain_id: u64,
        exchange_address: Address,
    ) -> Self {
        let domain_version = match market.version {
            ProtocolVersion::PolyV2 => "3",
            ProtocolVersion::CtfV1 => "2",
        };

        let domain = eip712_domain! {
            name: EXCHANGE_DOMAIN_NAME,
            version: domain_version,
            chain_id: chain_id,
            verifying_contract: exchange_address,
        };
        let domain_separator = domain.hash_struct();

        Self {
            exchange_address,
            domain_version,
            domain,
            domain_separator,
        }
    }

    /// Chain id embedded in the produced EIP-712 domain.
    #[must_use]
    pub fn chain_id(&self) -> u64 {
        self.domain
            .chain_id
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(POLYGON_CHAIN_ID)
    }
}

fn exchange_for_market(market: &MarketDescriptor) -> Address {
    match market.version {
        ProtocolVersion::PolyV2 => POLY_V2_EXCHANGE,
        ProtocolVersion::CtfV1 => {
            if market.neg_risk {
                CTF_NEG_RISK_EXCHANGE
            } else {
                CTF_EXCHANGE
            }
        }
    }
}

sol! {
    /// Exact 11-field Polymarket CLOB Order structure.
    #[derive(Debug, PartialEq, Eq)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

/// Order signing engine implementing EIP-712 signing across V2/V3 and WalletModes (0..=3).
#[derive(Clone)]
pub struct OrderSigner {
    signer: PrivateKeySigner,
    wallet_mode: WalletMode,
    maker_address: Address,
    signer_address: Address,
    deposit_constants: DepositWalletConstants,
}

impl OrderSigner {
    /// Creates a signer backed by a fresh ephemeral key (dry-run / tests only).
    ///
    /// The key is generated via [`PrivateKeySigner::random`] and never leaves
    /// memory; there is no hardcoded test vector to leak. Address derivation
    /// follows the same `maker`/`signer` rules as [`Self::new`].
    pub fn new_ephemeral(
        wallet_mode: WalletMode,
        proxy_or_deposit_wallet: Option<Address>,
    ) -> Result<Self, ExecutionError> {
        let signer = PrivateKeySigner::random();
        Self::from_signer(signer, wallet_mode, proxy_or_deposit_wallet)
    }

    /// Shared constructor once a [`PrivateKeySigner`] exists.
    fn from_signer(
        signer: PrivateKeySigner,
        wallet_mode: WalletMode,
        proxy_or_deposit_wallet: Option<Address>,
    ) -> Result<Self, ExecutionError> {
        let eoa_address = signer.address();

        let (maker_address, signer_address) = match wallet_mode {
            WalletMode::Eoa => (eoa_address, eoa_address),
            WalletMode::PolyProxy | WalletMode::GnosisSafe => {
                let proxy = proxy_or_deposit_wallet.ok_or_else(|| {
                    ExecutionError::Config(format!(
                        "Proxy address required for wallet mode {wallet_mode:?}"
                    ))
                })?;
                (proxy, eoa_address)
            }
            WalletMode::DepositWallet1271 => {
                let deposit_wallet = proxy_or_deposit_wallet.ok_or_else(|| {
                    ExecutionError::Config(
                        "Deposit wallet address required for DepositWallet1271 mode".to_string(),
                    )
                })?;
                // Enforce maker == signer == deposit_wallet for the Order fields
                (deposit_wallet, deposit_wallet)
            }
        };

        Ok(Self {
            signer,
            wallet_mode,
            maker_address,
            signer_address,
            deposit_constants: DepositWalletConstants::new(),
        })
    }

    /// Creates a new `OrderSigner`.
    ///
    /// For `WalletMode::DepositWallet1271`, enforces `maker == signer == deposit_wallet`.
    /// For `WalletMode::Eoa`, `maker == signer == eoa`.
    /// For `WalletMode::PolyProxy` / `WalletMode::GnosisSafe`, `maker == proxy`, `signer == eoa`.
    pub fn new(
        private_key: &SecretString,
        wallet_mode: WalletMode,
        proxy_or_deposit_wallet: Option<Address>,
    ) -> Result<Self, ExecutionError> {
        let raw_hex = private_key.expose_secret().trim();
        let key_str = raw_hex.strip_prefix("0x").unwrap_or(raw_hex);
        let signer = PrivateKeySigner::from_str(key_str)
            .map_err(|err| ExecutionError::Signing(format!("Invalid private key: {err}")))?;

        Self::from_signer(signer, wallet_mode, proxy_or_deposit_wallet)
    }

    /// Returns the address of the underlying EOA key.
    #[must_use]
    pub fn eoa_address(&self) -> Address {
        self.signer.address()
    }

    /// Returns the maker address configured for orders.
    #[must_use]
    pub fn maker_address(&self) -> Address {
        self.maker_address
    }

    /// Returns the signer address configured for orders.
    #[must_use]
    pub fn signer_address(&self) -> Address {
        self.signer_address
    }

    /// Returns configured wallet mode.
    #[must_use]
    pub fn wallet_mode(&self) -> WalletMode {
        self.wallet_mode
    }

    /// Re-targets the signer at another EVM chain (NEX sandbox: Amoy `80002`).
    ///
    /// Only affects `DepositWallet1271` domain constants; EOA order signatures
    /// derive their chain from the supplied `MarketDomain`.
    #[must_use]
    pub fn for_chain(mut self, chain_id: u64) -> Self {
        self.deposit_constants = DepositWalletConstants::for_chain(chain_id);
        self
    }

    /// Prepares an 11-field `Order` struct.
    pub fn build_order(
        &self,
        salt: u64,
        token_id_decimal_str: &str,
        maker_amount: u64,
        taker_amount: u64,
        side_buy: bool,
        timestamp_secs: u64,
    ) -> Result<Order, ExecutionError> {
        let token_id = U256::from_str(token_id_decimal_str).map_err(|err| {
            ExecutionError::Signing(format!("Invalid token_id '{token_id_decimal_str}': {err}"))
        })?;

        let side = if side_buy { 0u8 } else { 1u8 };

        Ok(Order {
            salt: U256::from(salt),
            maker: self.maker_address,
            signer: self.signer_address,
            tokenId: token_id,
            makerAmount: U256::from(maker_amount),
            takerAmount: U256::from(taker_amount),
            side,
            signatureType: self.wallet_mode.as_u8(),
            timestamp: U256::from(timestamp_secs),
            metadata: FixedBytes::ZERO,
            builder: FixedBytes::ZERO,
        })
    }

    /// Signs an `Order` using the precomputed `MarketDomain`.
    ///
    /// For `Eoa`, `PolyProxy`, `GnosisSafe`: signs `Order` directly and returns 65-byte hex signature (`0x...`).
    /// For `DepositWallet1271`: signs outer `TypedDataSign` and appends ERC-7739 trailer:
    /// `innerSig (65) || appDomainSeparator (32) || contentsHash (32) || ORDER_TYPE || uint16(len)`
    /// returning 317-byte hex signature (`0x...`).
    pub fn sign_order(
        &self,
        order: &Order,
        market_domain: &MarketDomain,
    ) -> Result<String, ExecutionError> {
        match self.wallet_mode {
            WalletMode::Eoa | WalletMode::PolyProxy | WalletMode::GnosisSafe => {
                let digest = order.eip712_signing_hash(&market_domain.domain);
                let sig = self
                    .signer
                    .sign_hash_sync(&digest)
                    .map_err(|err| ExecutionError::Signing(err.to_string()))?;

                let sig_bytes = sig.as_bytes();
                Ok(format!("0x{}", hex::encode(sig_bytes)))
            }
            WalletMode::DepositWallet1271 => {
                // In DepositWallet1271, maker must equal signer
                if order.maker != order.signer || order.maker != self.maker_address {
                    return Err(ExecutionError::Signing(
                        "DepositWallet1271 requires maker == signer == deposit_wallet".to_string(),
                    ));
                }

                let contents_hash = order.eip712_hash_struct();
                let typed_data_sign_hash = self
                    .deposit_constants
                    .hash_typed_data_sign(contents_hash, self.maker_address);

                let digest = DepositWalletConstants::signing_digest(
                    market_domain.domain_separator,
                    typed_data_sign_hash,
                );

                let inner_sig = self
                    .signer
                    .sign_hash_sync(&digest)
                    .map_err(|err| ExecutionError::Signing(err.to_string()))?;

                let inner_sig_bytes = inner_sig.as_bytes(); // 65 bytes
                let app_domain_separator = market_domain.domain_separator; // 32 bytes
                let order_type_bytes = ORDER_TYPE_STRING.as_bytes();
                let type_len = order_type_bytes.len() as u16;
                let len_bytes = type_len.to_be_bytes(); // 2 bytes

                let mut wrapped = Vec::with_capacity(65 + 32 + 32 + order_type_bytes.len() + 2);
                wrapped.extend_from_slice(&inner_sig_bytes);
                wrapped.extend_from_slice(app_domain_separator.as_slice());
                wrapped.extend_from_slice(contents_hash.as_slice());
                wrapped.extend_from_slice(order_type_bytes);
                wrapped.extend_from_slice(&len_bytes);

                Ok(format!("0x{}", hex::encode(wrapped)))
            }
        }
    }

    /// Verifies and recovers the EOA signer address from a generated signature.
    pub fn verify_signature(
        &self,
        order: &Order,
        signature_hex: &str,
        market_domain: &MarketDomain,
    ) -> Result<Address, ExecutionError> {
        let raw_hex = signature_hex.strip_prefix("0x").unwrap_or(signature_hex);
        let raw_bytes = hex::decode(raw_hex)
            .map_err(|err| ExecutionError::Signing(format!("Hex decode error: {err}")))?;

        match self.wallet_mode {
            WalletMode::Eoa | WalletMode::PolyProxy | WalletMode::GnosisSafe => {
                let digest = order.eip712_signing_hash(&market_domain.domain);
                let sig = alloy::primitives::Signature::try_from(raw_bytes.as_slice())
                    .map_err(|err| ExecutionError::Signing(err.to_string()))?;
                sig.recover_address_from_prehash(&digest)
                    .map_err(|err| ExecutionError::Signing(err.to_string()))
            }
            WalletMode::DepositWallet1271 => {
                let inner_sig = raw_bytes.get(0..65).ok_or_else(|| {
                    ExecutionError::Signing("Invalid signature length".to_string())
                })?;
                let contents_hash = order.eip712_hash_struct();
                let typed_data_sign_hash = self
                    .deposit_constants
                    .hash_typed_data_sign(contents_hash, self.maker_address);

                let digest = DepositWalletConstants::signing_digest(
                    market_domain.domain_separator,
                    typed_data_sign_hash,
                );

                let sig = alloy::primitives::Signature::try_from(inner_sig)
                    .map_err(|err| ExecutionError::Signing(err.to_string()))?;
                sig.recover_address_from_prehash(&digest)
                    .map_err(|err| ExecutionError::Signing(err.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::Signature;
    use core_types::money::{FeeSchedule, MicroUsdc, SharesMicro};
    use core_types::sandbox::{NEX_AMOY_EXCHANGE, NEX_CHAIN_ID};

    fn random_address() -> Address {
        use rand::RngCore;
        loop {
            let mut bytes = [0u8; 20];
            rand::rng().fill_bytes(&mut bytes);
            let addr = Address::from_slice(&bytes);
            if addr != Address::ZERO {
                return addr;
            }
        }
    }

    fn dummy_market(version: ProtocolVersion, neg_risk: bool) -> MarketDescriptor {
        MarketDescriptor {
            condition_id: "0x1234".to_string(),
            slug: "btc-5m".to_string(),
            question: "Will BTC go up?".to_string(),
            version,
            neg_risk,
            asset_id_up: "111".to_string(),
            asset_id_dn: "222".to_string(),
            tick_size_micro: MicroUsdc::from_raw(10_000),
            min_order_size_micro: SharesMicro::from_raw(5_000_000),
            taker_delay_ms: 50,
            fee_schedule: FeeSchedule::btc_5m_default(),
            start_ts_ms: 1000,
            expiry_ts_ms: 2000,
            strike_price_usd: None,
        }
    }

    #[test]
    fn test_salt_bounds() {
        for _ in 0..1000 {
            let salt = generate_salt();
            assert!(salt <= MAX_SAFE_INTEGER);
        }
    }

    #[test]
    fn test_market_domain_dispatch() {
        let v3_market = dummy_market(ProtocolVersion::PolyV2, false);
        let d3 = MarketDomain::for_market(&v3_market);
        assert_eq!(d3.exchange_address, POLY_V2_EXCHANGE);
        assert_eq!(d3.domain_version, "3");

        let ctf_market = dummy_market(ProtocolVersion::CtfV1, false);
        let d2 = MarketDomain::for_market(&ctf_market);
        assert_eq!(d2.exchange_address, CTF_EXCHANGE);
        assert_eq!(d2.domain_version, "2");

        let ctf_nr_market = dummy_market(ProtocolVersion::CtfV1, true);
        let d2_nr = MarketDomain::for_market(&ctf_nr_market);
        assert_eq!(d2_nr.exchange_address, CTF_NEG_RISK_EXCHANGE);
        assert_eq!(d2_nr.domain_version, "2");
    }

    #[test]
    fn test_eip712_safe_signing_and_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let safe_proxy = random_address();
        let signer = OrderSigner::new_ephemeral(WalletMode::GnosisSafe, Some(safe_proxy))?;
        assert_eq!(signer.wallet_mode(), WalletMode::GnosisSafe);
        assert_eq!(signer.maker_address(), safe_proxy);
        assert_eq!(signer.signer_address(), signer.eoa_address());

        let market = dummy_market(ProtocolVersion::CtfV1, false);
        let domain = MarketDomain::for_market(&market);

        let order = signer.build_order(
            123_456,
            "71321045678901234567890",
            5_200_000,
            10_000_000,
            true,
            1782753357,
        )?;

        let sig_hex = signer.sign_order(&order, &domain)?;
        assert!(sig_hex.starts_with("0x"));
        // Standard 65-byte signature is 130 hex chars + 2 prefix = 132 chars
        assert_eq!(sig_hex.len(), 132);

        // Verify recovery directly from digest
        let digest = order.eip712_signing_hash(&domain.domain);
        let sig_bytes = hex::decode(&sig_hex[2..])?;
        let sig = Signature::try_from(sig_bytes.as_slice())?;
        let recovered = sig.recover_address_from_prehash(&digest)?;
        assert_eq!(recovered, signer.eoa_address());

        Ok(())
    }

    #[test]
    fn test_eip712_deposit_wallet_wrapped_signature_layout()
    -> Result<(), Box<dyn std::error::Error>> {
        let deposit_wallet = random_address();
        let signer =
            OrderSigner::new_ephemeral(WalletMode::DepositWallet1271, Some(deposit_wallet))?;
        assert_eq!(signer.wallet_mode(), WalletMode::DepositWallet1271);
        assert_eq!(signer.maker_address(), deposit_wallet);
        assert_eq!(signer.signer_address(), deposit_wallet);

        let market = dummy_market(ProtocolVersion::PolyV2, false);
        let domain = MarketDomain::for_market(&market);

        let order = signer.build_order(
            479_249_096_354,
            "101010101010101010101010",
            5_200_000,
            10_000_000,
            true,
            1782753357,
        )?;

        let sig_hex = signer.sign_order(&order, &domain)?;
        assert!(sig_hex.starts_with("0x"));

        let raw_bytes = hex::decode(&sig_hex[2..])?;
        let expected_len = 65 + 32 + 32 + ORDER_TYPE_STRING.len() + 2;
        assert_eq!(raw_bytes.len(), expected_len);
        assert_eq!(expected_len, 317);

        // Check trailer slices safely
        let _inner_sig = raw_bytes
            .get(0..65)
            .ok_or_else(|| ExecutionError::Signing("slice 0..65".to_string()))?;
        let app_domain_sep = raw_bytes
            .get(65..97)
            .ok_or_else(|| ExecutionError::Signing("slice 65..97".to_string()))?;
        let contents_hash = raw_bytes
            .get(97..129)
            .ok_or_else(|| ExecutionError::Signing("slice 97..129".to_string()))?;
        let type_string_bytes = raw_bytes
            .get(129..129 + ORDER_TYPE_STRING.len())
            .ok_or_else(|| ExecutionError::Signing("slice type_string".to_string()))?;
        let len_bytes = raw_bytes
            .get(129 + ORDER_TYPE_STRING.len()..expected_len)
            .ok_or_else(|| ExecutionError::Signing("slice len".to_string()))?;

        assert_eq!(app_domain_sep, domain.domain_separator.as_slice());
        assert_eq!(contents_hash, order.eip712_hash_struct().as_slice());
        assert_eq!(type_string_bytes, ORDER_TYPE_STRING.as_bytes());
        assert_eq!(len_bytes, &(ORDER_TYPE_STRING.len() as u16).to_be_bytes());

        // Verify signature recovery using verify_signature
        let recovered = signer.verify_signature(&order, &sig_hex, &domain)?;
        assert_eq!(recovered, signer.eoa_address());

        Ok(())
    }

    #[test]
    fn test_amoy_sandbox_domain_uses_chain_80002() -> Result<(), Box<dyn std::error::Error>> {
        let nex_exchange: Address = NEX_AMOY_EXCHANGE.parse()?;
        let market = dummy_market(ProtocolVersion::PolyV2, false);
        let domain = MarketDomain::for_market_on_chain(&market, NEX_CHAIN_ID, nex_exchange);
        assert_eq!(domain.chain_id(), NEX_CHAIN_ID);
        assert_eq!(domain.exchange_address, nex_exchange);

        let signer = OrderSigner::new_ephemeral(WalletMode::Eoa, None)?.for_chain(NEX_CHAIN_ID);
        let order = signer.build_order(7, "111", 50_000, 5_000_000, true, 1_785_000_000)?;
        let sig = signer.sign_order(&order, &domain)?;
        let recovered = signer.verify_signature(&order, &sig, &domain)?;
        assert_eq!(recovered, signer.eoa_address());
        Ok(())
    }

    #[test]
    fn test_signing_latency_under_80_micros() -> Result<(), Box<dyn std::error::Error>> {
        let deposit_wallet = random_address();
        let signer =
            OrderSigner::new_ephemeral(WalletMode::DepositWallet1271, Some(deposit_wallet))?;
        let market = dummy_market(ProtocolVersion::PolyV2, false);
        let domain = MarketDomain::for_market(&market);

        let order = signer.build_order(
            123_456,
            "101010101010101010101010",
            5_200_000,
            10_000_000,
            true,
            1782753357,
        )?;

        // Warm up
        for _ in 0..100 {
            let _ = signer.sign_order(&order, &domain)?;
        }

        // Measure 1,000 iterations
        let iterations = 1000;
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _ = signer.sign_order(&order, &domain)?;
        }
        let elapsed = start.elapsed();
        let avg_micros = elapsed.as_micros() as f64 / iterations as f64;
        println!("Average EIP-712 sign latency (DepositWallet1271): {avg_micros:.2} us");

        // Strict phase invariant: tau_3 < 80 us in optimized release build.
        // In unoptimized debug mode, bigint arithmetic takes ~3ms without compiler inlining.
        if cfg!(debug_assertions) {
            println!("Debug build detected: measured {avg_micros:.2} us (unoptimized)");
            assert!(
                avg_micros < 10_000.0,
                "Debug signing latency too high: {avg_micros:.2} us"
            );
        } else {
            assert!(
                avg_micros < 80.0,
                "tau_3 signing latency exceeded 80 us in release build: {avg_micros:.2} us"
            );
        }

        Ok(())
    }
}
