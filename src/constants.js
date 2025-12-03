import { PublicKey } from '@solana/web3.js';
import BN from 'bn.js';

/**
 * Application Constants
 * Centralized magic numbers and configuration values
 */

// Solana Constants
export const LAMPORTS_PER_SOL = 1_000_000_000;
export const LAMPORTS_PER_SOL_BN = new BN(LAMPORTS_PER_SOL.toString());
export const MIN_TRANSFER_SOL = 0.0001;
export const MIN_TRANSFER_LAMPORTS = BigInt(Math.floor(MIN_TRANSFER_SOL * LAMPORTS_PER_SOL));
export const MIN_SOL_BUFFER_LAMPORTS_BN = new BN('5000');

// Priority Fees
export const PRIORITY_FEE_MICRO_LAMPORTS = 10_000;
export const BASE_TX_FEE_LAMPORTS = 5_000;

// Time Constants
export const MAX_COOLDOWN_AGE_HOURS = 24;
export const MAX_COOLDOWN_AGE_MS = MAX_COOLDOWN_AGE_HOURS * 60 * 60 * 1000;
export const WALLET_COOLDOWN_DELAY_MS = 30_000;

// Trading Constants
export const MIN_MEME_TOKENS = new BN('1000000');
export const DEFAULT_SLIPPAGE_RETAIL = 1.0;
export const DEFAULT_SLIPPAGE_WHALE = 0.3;
export const SENTIMENT_THRESHOLDS = {
  bullish: 70,
  bearish: 30,
};
export const HODLER_SELL_CHANCE = 0.3;

// Market Data Constants
export const MARKET_DATA_CACHE_DURATION_MS = 60_000;
export const SENTIMENT_FETCH_COOLDOWN_MS = 3_600_000;
export const API_TIMEOUT = 8_000;
export const API_RATE_LIMIT_MS = 1_000;
export const DEFAULT_SOL_USD_FALLBACK = 150;
export const MAX_RETRY_ATTEMPTS = 3;
export const INITIAL_RETRY_DELAY_MS = 300;

// File Constants
export const DEFAULT_WALLET_FILE = 'wallets.json';

// Addresses
export const BURN_ADDRESS = new PublicKey('1nc1nerator11111111111111111111111111111111');
export const USDC_MINT = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');

// Behavior Constants
export const PERSONALITIES = ['flipper', 'hodler', 'momentum'];
export const BEHAVIOR_PROFILES = ['retail', 'whale', 'mixed'];
export const TRADE_MODES = ['adaptive', 'buy_first', 'sell_first', 'buy_only', 'sell_only', 'random'];

// Market Types
export const SUPPORTED_MARKETS = [
  'PUMP_FUN', 'PUMP_SWAP', 'RAYDIUM_AMM', 'RAYDIUM_CLMM', 'RAYDIUM_CPMM',
  'RAYDIUM_LAUNCHPAD', 'ORCA_WHIRLPOOL', 'METEORA_DLMM', 'METEORA_DAMM_V1',
  'METEORA_DAMM_V2', 'METEORA_DBC', 'MOONIT', 'HEAVEN', 'SUGAR', 'BOOP_FUN',
];

// Bot Constants
export const SESSION_PAUSE_MULTIPLIER = 2;
export const ROTATION_STATS_INTERVAL = 10;
export const WITHDRAWAL_CHUNK_SIZE = 50;
export const SINK_BALANCE_CHECK_INTERVAL = 5;
export const WALLET_CLEANUP_CHANCE = 0.01;

// Burner Wallet Churning (BWC) Constants
export const BWC_MODES = ['disabled', 'hybrid', 'burner_only'];
export const BWC_WALLET_TYPES = {
  REGULAR: 'regular',
  BURNER: 'burner',
};
export const BWC_DEFAULT_CONFIG = {
  enabled: false,
  mode: 'hybrid', // 'disabled', 'hybrid', 'burner_only'
  burnerRatio: 0.3, // 30% burner wallets in hybrid mode
  maxBurnerWallets: 50,
  burnerLifetimeTxs: 5, // max transactions before disposal
  burnerFundAmount: 0.02, // SOL amount to fund each burner
  burnerCreationInterval: 30000, // create new burner every 30s if needed
  burnerDisposalDelay: 10000, // wait 10s before disposal
  minBurnerBalance: 0.001, // minimum SOL before disposal
  emergencyBurnerRatio: 0.7, // use more burners during high activity
};
export const BWC_STAT_INTERVAL = 20; // log BWC stats every 20 cycles