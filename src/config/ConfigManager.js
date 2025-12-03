import { config } from 'dotenv';
import { PublicKey } from '@solana/web3.js';
import { SUPPORTED_MARKETS, BEHAVIOR_PROFILES, TRADE_MODES, DEFAULT_WALLET_FILE, BWC_MODES, BWC_DEFAULT_CONFIG } from '../constants.js';
import BN from 'bn.js';

// Load environment variables from .env file
config();

/**
 * Configuration Validator and Manager
 * Ensures all environment variables are validated and sanitized
 */
export class ConfigManager {
  constructor() {
    this.validateAndLoadConfig();
  }

  /**
   * Validates and loads all configuration from environment variables
   * @throws {Error} If any required config is invalid
   */
  validateAndLoadConfig() {
    // Required configs
    this.rpcUrl = this.validate('RPC_URL', this.validateRpcUrl);
    this.memeCoinMint = this.validate('MEME_COIN_MINT', this.validatePublicKey);
    this.memeCoinSymbol = this.validate('MEME_COIN_SYMBOL', this.validateString, 'solana');
    this.market = this.validate('MARKET', this.validateMarket);

    // Wallet configs
    this.maxWallets = this.validate('MAX_WALLETS', this.validateMaxWallets, 'all');
    this.numWalletsToGenerate = this.validate('NUM_WALLETS_TO_GENERATE', this.validateNumber(1, 10000), 10);
    this.fundAmount = this.validate('FUND_AMOUNT', this.validateSolAmount(0.001, 10), 0.05);

    // Trading configs
    this.tradeMode = this.validate('TRADE_MODE', this.validateTradeMode, 'adaptive');
    this.buyProb = this.validate('BUY_PROB', this.validateProbability, 0.5);
    this.numActionsPerCycle = this.validate('NUM_ACTIONS_PER_CYCLE', this.validateNumber(1, 10), 2);
    this.swapAmount = this.validate('SWAP_AMOUNT', this.validateNumber(0.0001, 1), 0.01);

    // Timing configs
    this.baseDelayMs = this.validate('DELAY_MS', this.validateNumber(100, 60000), 5000);
    this.jitterPct = this.validate('JITTER_PCT', this.validatePercentage, 10);

    // Stealth configs
    this.sinkPrivateKey = this.validate('SINK_PRIVATE_KEY', this.validateOptionalPrivateKey);
    this.relayerPrivateKeys = this.validate('RELAYER_PRIVATE_KEYS', this.validateOptionalPrivateKeysArray);
    this.enableRebalancing = this.validate('ENABLE_REBALANCING', this.validateBoolean, true);

    // Market data configs
    this.birdeyeApiKey = this.validate('BIRDEYE_API_KEY', this.validateOptionalString);
    this.memeCoinPairAddress = this.validate('MEME_COIN_PAIR_ADDRESS', this.validateOptionalString);
    this.minLiquidity = this.validate('MIN_LIQUIDITY_USD', this.validateNumber(1000, 10000000), 5000);
    this.maxPriceImpact = this.validate('MAX_PRICE_IMPACT_PCT', this.validatePercentage, 5);

    // Circuit breaker configs
    this.enableCircuitBreaker = this.validate('ENABLE_CIRCUIT_BREAKER', this.validateBoolean, true);
    this.maxConsecutiveFailures = this.validate('MAX_CONSECUTIVE_FAILURES', this.validateNumber(1, 100), 10);
    this.maxFailureRate = this.validate('MAX_FAILURE_RATE_PCT', this.validatePercentage, 50);
    this.failureRateWindow = this.validate('FAILURE_RATE_WINDOW', this.validateNumber(5, 100), 10);
    this.emergencyStopLoss = this.validate('EMERGENCY_STOP_LOSS_PCT', this.validatePercentage, 30);

    // Other configs
    this.concurrency = this.validate('CONCURRENCY', this.validateNumber(1, 1000), 50);
    this.batchSize = this.validate('BATCH_SIZE', this.validateNumber(1, 1000), 100);
    this.retryAttempts = this.validate('RETRY_ATTEMPTS', this.validateNumber(1, 10), 3);

    // Wallet seasoning
    this.enableSeasoning = this.validate('ENABLE_SEASONING', this.validateBoolean, false);
    this.seasoningMinTxs = this.validate('SEASONING_MIN_TXS', this.validateNumber(1, 20), 3);
    this.seasoningMaxTxs = this.validate('SEASONING_MAX_TXS', this.validateNumber(1, 50), 10);
    this.seasoningDelayMs = this.validate('SEASONING_DELAY_MS', this.validateNumber(1000, 30000), 5000);

    // Burner wallet seasoning (optional for extra stealth)
    this.enableBurnerSeasoning = this.validate('ENABLE_BURNER_SEASONING', this.validateBoolean, false);
    this.burnerSeasoningMinTxs = this.validate('BURNER_SEASONING_MIN_TXS', this.validateNumber(1, 10), 1);
    this.burnerSeasoningMaxTxs = this.validate('BURNER_SEASONING_MAX_TXS', this.validateNumber(1, 20), 3);
    this.burnerSeasoningDelayMs = this.validate('BURNER_SEASONING_DELAY_MS', this.validateNumber(500, 15000), 2000);

    // Jito MEV
    this.enableJito = this.validate('ENABLE_JITO', this.validateBoolean, true);
    this.jitoFee = this.validate('JITO_PRIORITY_FEE_SOL', this.validateSolAmount(0.0001, 0.01), 0.002);
    this.jitoTipBuy = this.validate('JITO_TIP_SOL_BUY', this.validateSolAmount(0.0001, 0.01), 0.0012);
    this.jitoTipSell = this.validate('JITO_TIP_SOL_SELL', this.validateSolAmount(0.0001, 0.01), 0.0018);

    // Auto-scaling
    this.autoScale = this.validate('AUTO_SCALE_CONCURRENCY', this.validateBoolean, true);

    // Partial sell
    this.partialSellEnabled = this.validate('PARTIAL_SELL_ENABLED', this.validateBoolean, true);
    this.partialSellMin = this.validate('PARTIAL_SELL_MIN_PCT', this.validatePercentage, 22);
    this.partialSellMax = this.validate('PARTIAL_SELL_MAX_PCT', this.validatePercentage, 68);

    // Behavior
    this.behaviorProfile = this.validate('BEHAVIOR_PROFILE', this.validateBehaviorProfile, 'retail');

    // Wallet rotation
    this.minWalletCooldownMs = this.validate('MIN_WALLET_COOLDOWN_MS', this.validateNumber(60000, 3600000), 300000);
    this.maxWalletCooldownMs = this.validate('MAX_WALLET_COOLDOWN_MS', this.validateNumber(60000, 3600000), 1800000);
    this.shuffleWallets = this.validate('SHUFFLE_WALLETS', this.validateBoolean, true);

    // Rebalancing
    this.minWalletBalance = this.validate('MIN_WALLET_BALANCE_SOL', this.validateNumber(0.001, 1), 0.005);
    this.targetWalletBalance = this.validate('TARGET_WALLET_BALANCE_SOL', this.validateNumber(0.001, 1), 0.05);
    this.rebalanceInterval = this.validate('REBALANCE_INTERVAL_CYCLES', this.validateNumber(1, 1000), 50);
    this.dustThreshold = this.validate('DUST_THRESHOLD_SOL', this.validateNumber(0.0001, 0.01), 0.001);
    this.minSolBufferLamports = this.validate('MIN_SOL_BUFFER_SOL', this.validateNumber(0.0001, 0.01), 0.0005);

    // Session
    this.sessionPauseMin = this.validate('SESSION_PAUSE_MIN', this.validateNumber(1, 100), 10);
    this.minInterBatchDelayMs = this.validate('MIN_INTER_BATCH_DELAY_MS', this.validateNumber(1000, 30000), 5000);
    this.maxInterBatchDelayMs = this.validate('MAX_INTER_BATCH_DELAY_MS', this.validateNumber(5000, 60000), 15000);

    // TWAP
    this.twapParts = this.validate('TWAP_PARTS', this.validateNumber(1, 20), 5);
    this.twapMaxDelay = this.validate('TWAP_MAX_DELAY', this.validateNumber(1000, 60000), 10000);

    // Vol threshold
    this.volThreshold = this.validate('VOL_THRESHOLD', this.validatePercentage, 0.05);

    // Ramp cycles
    this.rampCycles = this.validate('RAMP_CYCLES', this.validateNumber(1, 1000), 30);

    // Keyboard triggers
    this.enableKeyboard = this.validate('ENABLE_KEYBOARD_TRIGGERS', this.validateBoolean, false);

    // Alerting
    this.enableAlerting = this.validate('ENABLE_ALERTING', this.validateBoolean, false);

    // Wallet file
    this.walletFile = this.validate('WALLET_FILE', this.validateString, DEFAULT_WALLET_FILE);

    // Burner Wallet Churning (BWC) Configuration
    this.bwcEnabled = this.validate('BWC_ENABLED', this.validateBoolean, BWC_DEFAULT_CONFIG.enabled);
    this.bwcMode = this.validate('BWC_MODE', this.validateBWCMode, BWC_DEFAULT_CONFIG.mode);
    this.bwcBurnerRatio = this.validate('BWC_BURNER_RATIO', this.validateProbability, BWC_DEFAULT_CONFIG.burnerRatio);
    this.bwcMaxBurnerWallets = this.validate('BWC_MAX_BURNER_WALLETS', this.validateNumber(1, 1000), BWC_DEFAULT_CONFIG.maxBurnerWallets);
    this.bwcBurnerLifetimeTxs = this.validate('BWC_BURNER_LIFETIME_TXS', this.validateNumber(1, 50), BWC_DEFAULT_CONFIG.burnerLifetimeTxs);
    this.bwcBurnerFundAmount = this.validate('BWC_BURNER_FUND_AMOUNT', this.validateNumber(0.001, 0.1), BWC_DEFAULT_CONFIG.burnerFundAmount);
    this.bwcBurnerCreationInterval = this.validate('BWC_BURNER_CREATION_INTERVAL', this.validateNumber(5000, 300000), BWC_DEFAULT_CONFIG.burnerCreationInterval);
    this.bwcBurnerDisposalDelay = this.validate('BWC_BURNER_DISPOSAL_DELAY', this.validateNumber(1000, 60000), BWC_DEFAULT_CONFIG.burnerDisposalDelay);
    this.bwcMinBurnerBalance = this.validate('BWC_MIN_BURNER_BALANCE', this.validateNumber(0.0001, 0.01), BWC_DEFAULT_CONFIG.minBurnerBalance);
    this.bwcEmergencyBurnerRatio = this.validate('BWC_EMERGENCY_BURNER_RATIO', this.validateProbability, BWC_DEFAULT_CONFIG.emergencyBurnerRatio);

    // Derived configs
    this.isDevnet = this.rpcUrl.includes('devnet');
    this.useBirdeye = !!this.birdeyeApiKey;
  }

  validate(key, validator, defaultValue) {
    const value = process.env[key];
    if (value === undefined || value === null || value === '') {
      if (defaultValue !== undefined) {
        return defaultValue;
      }
      // If no default value provided, this is likely an optional field
      // Allow validator to handle empty values
      if (arguments.length < 3) {
        return validator.call(this, key, value);
      }
      throw new Error(`Missing required environment variable: ${key}`);
    }
    return validator.call(this, key, value);
  }

  validateRpcUrl = (key, value) => {
    const url = this.validateString(key, value);
    try {
      new URL(url);
      if (!url.startsWith('https://') && !url.startsWith('http://')) {
        throw new Error('Invalid protocol');
      }
      return url;
    } catch {
      throw new Error(`Invalid ${key}: must be a valid HTTP/HTTPS URL`);
    }
  }

  validateString = (key, value) => {
    if (!value || typeof value !== 'string' || value.trim() === '') {
      throw new Error(`Invalid ${key}: must be a non-empty string`);
    }
    return value.trim();
  }

  validateOptionalString = (key, value) => {
    return value && typeof value === 'string' ? value.trim() : null;
  }

  validateNumber = (min, max) => {
    return (key, value) => {
      const num = parseFloat(value);
      if (isNaN(num) || num < min || num > max) {
        throw new Error(`Invalid ${key}: must be a number between ${min} and ${max}`);
      }
      return num;
    };
  }

  validateSolAmount = (min, max) => {
    return (key, value) => {
      const amountStr = String(value);
      const amountNum = parseFloat(amountStr);

      if (isNaN(amountNum) || amountNum < min || amountNum > max) {
        throw new Error(`Invalid ${key}: must be a number between ${min} and ${max}`);
      }

      // Use BN for precise lamport conversion to avoid floating point errors
      const [integer, fraction = ''] = amountStr.split('.');
      const paddedFraction = fraction.padEnd(9, '0').slice(0, 9);
      const lamportsStr = integer + paddedFraction;

      // Validate the result is within acceptable range
      const result = new BN(lamportsStr);
      const minLamports = new BN(Math.floor(min * 1000000000).toString());
      const maxLamports = new BN(Math.floor(max * 1000000000).toString());

      if (result.lt(minLamports) || result.gt(maxLamports)) {
        throw new Error(`Invalid ${key}: must be between ${min} and ${max} SOL`);
      }

      return result;
    };
  }

  validatePercentage = (key, value) => {
    const pct = this.validateNumber(0, 100)(key, value);
    return pct / 100;
  }

  validateProbability = (key, value) => {
    return this.validateNumber(0, 1)(key, value);
  }

  validateBoolean = (key, value) => {
    const str = value.toString().toLowerCase();
    if (['true', '1', 'yes'].includes(str)) return true;
    if (['false', '0', 'no'].includes(str)) return false;
    throw new Error(`Invalid ${key}: must be a boolean (true/false)`);
  }

  validatePublicKey = (key, value) => {
    try {
      return new PublicKey(value);
    } catch (error) {
      throw new Error(`Invalid ${key}: must be a valid Solana public key`);
    }
  }

  validateOptionalPrivateKey = (key, value) => {
    if (!value) return null;
    try {
      const parsed = JSON.parse(value);
      if (!Array.isArray(parsed) || parsed.length !== 64 || !parsed.every(n => typeof n === 'number' && n >= 0 && n <= 255)) {
        throw new Error('Invalid private key format: must be an array of 64 numbers (Uint8Array)');
      }
      return parsed;
    } catch (error) {
      throw new Error(`Invalid ${key}: must be a valid JSON array representing a private key`);
    }
  }

  validateOptionalPrivateKeysArray = (key, value) => {
    if (!value) return null;
    try {
      const parsed = JSON.parse(value);
      if (!Array.isArray(parsed)) {
        throw new Error('Must be an array');
      }
      for (const item of parsed) {
        if (!Array.isArray(item) || item.length !== 64 || !item.every(n => typeof n === 'number' && n >= 0 && n <= 255)) {
          throw new Error('Invalid private key format: each private key must be an array of 64 numbers (Uint8Array)');
        }
      }
      return parsed;
    } catch (error) {
      throw new Error(`Invalid ${key}: must be a valid JSON array of private keys`);
    }
  }

  validateMarket = (key, value) => {
    const market = this.validateString(key, value);
    if (!SUPPORTED_MARKETS.includes(market)) {
      throw new Error(`Invalid ${key}: must be one of ${SUPPORTED_MARKETS.join(', ')}`);
    }
    return market;
  }

  validateTradeMode = (key, value) => {
    const mode = this.validateString(key, value);
    if (!TRADE_MODES.includes(mode)) {
      throw new Error(`Invalid ${key}: must be one of ${TRADE_MODES.join(', ')}`);
    }
    return mode;
  }

  validateBehaviorProfile = (key, value) => {
    const profile = this.validateString(key, value);
    if (!BEHAVIOR_PROFILES.includes(profile)) {
      throw new Error(`Invalid ${key}: must be one of ${BEHAVIOR_PROFILES.join(', ')}`);
    }
    return profile;
  }

  validateMaxWallets = (key, value) => {
    if (value.toLowerCase() === 'all') {
      return Infinity;
    }
    return this.validateNumber(100, 100000)(key, value);
  }

  validateBWCMode = (key, value) => {
    const mode = this.validateString(key, value);
    if (!BWC_MODES.includes(mode)) {
      throw new Error(`Invalid ${key}: must be one of ${BWC_MODES.join(', ')}`);
    }
    return mode;
  }
}