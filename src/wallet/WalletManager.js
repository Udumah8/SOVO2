import { Keypair, Transaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
import { setTimeout as delay } from 'timers/promises';
import { ComputeBudgetProgram, SystemProgram } from '@solana/web3.js';
import BN from 'bn.js';
import {
  MIN_SOL_BUFFER_LAMPORTS_BN,
  PRIORITY_FEE_MICRO_LAMPORTS,
  MAX_COOLDOWN_AGE_MS,
  PERSONALITIES,
  WALLET_CLEANUP_CHANCE,
} from '../constants.js';
import { getRandomNumberBetween } from '../utils.js';
import { WalletDataLoader } from './WalletDataLoader.js';

/**
 * Wallet Management System
 * Handles wallet generation, loading, funding, and rotation
 */
export class WalletManager {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, connection, logger) {
    this.config = config;
    this.connection = connection;
    this.logger = logger;
    this.walletDataLoader = new WalletDataLoader(config, logger);
    this.walletData = [];
    this.allPubkeys = new Set();
    this.funded = new Set();
    this.activeWallets = [];
    this.walletCooldowns = new Map();
    this.walletTradeCount = new Map();
    this.walletPersonalities = new Map();
    this.sinkKeypair = null;
    this.relayerKeypairs = [];
  }

  /**
   * Loads or generates wallets based on configuration
   */
  async loadOrGenerateWallets() {
    try {
      this.walletData = await this.walletDataLoader.loadWallets();
      this.normalizeWalletData();
      this.allPubkeys = new Set(this.walletData.map(w => w.pubkey));
      this.logger.info(`Loaded ${this.walletData.length.toLocaleString()} existing wallets`);

      if (this.walletData.length < this.config.numWalletsToGenerate) {
        await this.generateWallets();
      }

      this.loadSpecialWallets();

      if (this.config.autoScale) {
        this.adjustConcurrency();
      }

      await this.fundWalletsInParallel();
    } catch (error) {
      this.logger.error('Failed to load or generate wallets', { error: error.message });
      throw error;
    }
  }

  /**
   * Normalizes wallet data structure
   */
  normalizeWalletData() {
    this.walletData = this.walletData.map(w => ({
      pubkey: w.pubkey || w.publicKey || Keypair.fromSecretKey(new Uint8Array(w.privateKey)).publicKey.toBase58(),
      privateKey: w.privateKey,
      name: w.name || `Wallet`,
      isSeasoned: w.isSeasoned || false,
    }));
  }

  /**
   * Generates additional wallets to meet the required count
   */
  async generateWallets() {
    const remaining = this.config.numWalletsToGenerate - this.walletData.length;
    this.logger.info(`Generating ${remaining.toLocaleString()} wallets...`);

    const batchSize = 1000;
    for (let i = 0; i < remaining; i += batchSize) {
      const size = Math.min(batchSize, remaining - i);
      const batch = Array.from({ length: size }, () => {
        const kp = Keypair.generate();
        return {
          pubkey: kp.publicKey.toBase58(),
          privateKey: Array.from(kp.secretKey),
          name: `Wallet${this.walletData.length + i + 1}`,
          isSeasoned: false,
        };
      });

      batch.forEach(wallet => this.allPubkeys.add(wallet.pubkey));
      this.walletData.push(...batch);
      this.logger.info(`${this.walletData.length.toLocaleString()}/${this.config.numWalletsToGenerate.toLocaleString()}`);
      await delay(10);
    }

    await this.walletDataLoader.writeWallets(this.walletData);
  }

  /**
   * Loads sink and relayer wallets
   */
  loadSpecialWallets() {
    if (this.config.sinkPrivateKey) {
      this.sinkKeypair = Keypair.fromSecretKey(new Uint8Array(this.config.sinkPrivateKey));
      this.logger.info('Sink wallet loaded');
    } else {
      this.logger.warn('No SINK_PRIVATE_KEY found in .env. Withdrawals will not be possible.');
    }

    if (this.config.relayerPrivateKeys?.length > 0) {
      this.relayerKeypairs = this.config.relayerPrivateKeys.map(pk => Keypair.fromSecretKey(new Uint8Array(pk)));
      this.logger.info(`Loaded ${this.relayerKeypairs.length} relayer wallets`);
    } else {
      this.logger.warn('No RELAYER_PRIVATE_KEYS found in .env. Relayer functionality will be limited.');
    }
  }

  /**
   * Adjusts concurrency based on the number of wallets
   */
  adjustConcurrency() {
    this.config.concurrency = Math.min(50, Math.max(3, Math.floor(this.walletData.length / 200) + 3));
    this.config.batchSize = Math.min(20, Math.max(2, Math.floor(this.walletData.length / 300) + 2));
  }

  /**
   * Funds wallets in parallel using relayer wallets
   */
  async fundWalletsInParallel() {
    if (this.relayerKeypairs.length === 0) {
      this.logger.info('No relayer wallets configured, skipping funding');
      return;
    }

    this.logger.info(`Checking funding for ${this.walletData.length} wallets...`);
    const toCheck = this.walletData.filter(w => !this.funded.has(w.pubkey));

    const fundBatchSize = 50;
    for (let i = 0; i < toCheck.length; i += fundBatchSize) {
      const batch = toCheck.slice(i, i + fundBatchSize);
      const promises = batch.map(wallet => this.fundSingleWallet(wallet));
      await Promise.allSettled(promises);
      this.logger.info(`Funding progress: ${Math.min(i + fundBatchSize, toCheck.length)}/${toCheck.length}`);
    }
  }

  /**
   * Funds a single wallet
   * @param {Object} wallet
   */
  async fundSingleWallet(wallet) {
    // Check if already funded and lock immediately to prevent race condition
    if (this.funded.has(wallet.pubkey)) return;
    this.funded.add(wallet.pubkey); // Lock immediately to prevent race condition

    const kp = Keypair.fromSecretKey(new Uint8Array(wallet.privateKey));
    try {
      const balance = BigInt(await this.connection.getBalance(kp.publicKey));
      const balanceBN = new BN(balance.toString());
      // Convert fundAmount from SOL to lamports
      const fundAmountLamports = new BN(Math.floor(this.config.fundAmount * LAMPORTS_PER_SOL).toString());
      const threshold = fundAmountLamports.mul(new BN('8')).div(new BN('10'));
      if (balanceBN.gte(threshold)) return; // Already funded, lock is already set

      let remaining = fundAmountLamports.sub(balanceBN);
      const parts = Math.floor(Math.random() * 4) + 1;

      for (let p = 0; p < parts && remaining.gt(MIN_SOL_BUFFER_LAMPORTS_BN); p++) {
        const cappedPart = this.calculateFundingPart(remaining, parts - p);
        const relayer = this.relayerKeypairs[Math.floor(Math.random() * this.relayerKeypairs.length)];

        const tx = new Transaction()
          .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 25000 }))
          .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }))
          .add(SystemProgram.transfer({
            fromPubkey: relayer.publicKey,
            toPubkey: kp.publicKey,
            lamports: BigInt(cappedPart.toString()),
          }));

        const sig = await this.connection.sendTransaction(tx, [relayer], { skipPreflight: true });
        await this.connection.confirmTransaction(sig, 'confirmed');

        this.logger.info(`Funded ${wallet.name}: ${(cappedPart.toNumber() / LAMPORTS_PER_SOL).toFixed(4)} SOL`);
        remaining = remaining.sub(cappedPart);
        await delay(1000 + Math.random() * 2000);
      }
    } catch (error) {
      this.logger.warn(`Failed to fund ${wallet.name}`, { error: error.message });
      // Don't remove from funded set on failure to prevent retry storms
    }
  }

  /**
   * Calculates the amount for a single funding transaction
   * @param {BN} remaining
   * @param {number} remainingParts
   * @returns {BN}
   */
  calculateFundingPart(remaining, remainingParts) {
    const factor = Math.floor((0.6 + Math.random() * 0.8) * 1000);
    const basePart = remaining.mul(new BN(factor)).div(new BN(1000)).div(new BN(remainingParts.toString()));
    const partNum = BN.max(basePart, MIN_SOL_BUFFER_LAMPORTS_BN);
    return BN.min(partNum, remaining);
  }

  /**
   * Loads the next batch of active wallets for trading
   */
  async loadActiveBatch() {
    const now = Date.now();
    this.cleanupOldCooldowns(now);

    // Check if seasoning is required
    const requireSeasoning = this.config.enableSeasoning;

    const ready = this.walletData.filter(w => {
      // Check cooldown status
      const lastTrade = this.walletCooldowns.get(w.pubkey) || 0;
      const cooldown = this.getWalletCooldown(w.pubkey);
      const cooldownPassed = now - lastTrade >= cooldown;

      // Check seasoning status if required
      const isSeasoned = !requireSeasoning || w.isSeasoned === true;

      return cooldownPassed && isSeasoned;
    });

    if (ready.length === 0) {
      if (requireSeasoning) {
        const unseasonedCount = this.walletData.filter(w => !w.isSeasoned).length;
        this.logger.info('No wallets ready for trading - all unseasoned wallets need seasoning', {
          totalWallets: this.walletData.length,
          unseasonedWallets: unseasonedCount,
          seasonedWallets: this.walletData.length - unseasonedCount
        });
      } else {
        this.logger.debug('No wallets ready for a new batch.');
      }
      return [];
    }

    const shuffled = this.config.shuffleWallets ? this.shuffleArray(ready) : ready;
    const selected = shuffled.slice(0, this.config.batchSize);

    this.activeWallets = selected.map(w => ({
      keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
      name: w.name || w.pubkey.slice(0, 6),
      pubkey: w.pubkey,
    }));

    this.logger.info(`Loaded batch: ${this.activeWallets.length} wallets (Pool: ${ready.length}/${this.walletData.length})`, {
      requireSeasoning,
      seasonedWallets: ready.filter(w => w.isSeasoned).length,
      unseasonedEligible: ready.filter(w => !w.isSeasoned).length
    });
    return this.activeWallets;
  }

  /**
   * Cleans up old cooldown entries to prevent memory leaks
   * @param {number} now
   */
  cleanupOldCooldowns(now) {
    // Clean up old cooldowns with better memory management
    const cleanupThreshold = MAX_COOLDOWN_AGE_MS;
    const entriesToDelete = [];

    for (const [key, timestamp] of this.walletCooldowns.entries()) {
      if (now - timestamp > cleanupThreshold) {
        entriesToDelete.push(key);
      }
    }

    // Delete in batch to avoid concurrent modification issues
    for (const key of entriesToDelete) {
      this.walletCooldowns.delete(key);
    }

    // Clean up trade counts periodically (only 1% of the time to avoid performance impact)
    if (Math.random() < WALLET_CLEANUP_CHANCE) {
      let cleanedCount = 0;
      for (const [key, count] of this.walletTradeCount.entries()) {
        if (count > 1000) {
          this.walletTradeCount.set(key, 1000);
          cleanedCount++;
        }
      }

      // Log cleanup activity if significant cleanup occurred
      if (cleanedCount > 10) {
        this.logger.debug('Cleaned up trade counts', {
          cleanedWallets: cleanedCount,
          totalWallets: this.walletTradeCount.size
        });
      }
    }

    // Additional memory management: periodically clean up wallets that are no longer active
    if (Math.random() < 0.001) { // Very rare cleanup (0.1%)
      const inactiveThreshold = 7 * 24 * 60 * 60 * 1000; // 7 days
      const inactiveWallets = [];

      for (const [key, timestamp] of this.walletCooldowns.entries()) {
        if (now - timestamp > inactiveThreshold) {
          inactiveWallets.push(key);
        }
      }

      // Only clean up if we have many inactive wallets to avoid removing active ones
      if (inactiveWallets.length > 100) {
        for (const key of inactiveWallets) {
          this.walletCooldowns.delete(key);
          this.walletTradeCount.delete(key);
        }

        this.logger.info('Deep cleanup of inactive wallets', {
          removedWallets: inactiveWallets.length,
          remainingActive: this.walletCooldowns.size
        });
      }
    }
  }

  /**
   * Shuffles an array using Fisher-Yates algorithm
   * @param {Array} array
   * @returns {Array}
   */
  shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
      // Use Math.random() directly for proper integer generation
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
  }

  /**
   * Gets the cooldown period for a wallet
   * @param {string} walletKey
   * @returns {number}
   */
  getWalletCooldown(walletKey) {
    const tradeCount = this.walletTradeCount.get(walletKey) || 0;
    const cooldownMultiplier = 1 + (tradeCount % 5) * 0.2;
    const baseCooldown = getRandomNumberBetween(this.config.minWalletCooldownMs, this.config.maxWalletCooldownMs);
    return Math.floor(baseCooldown * cooldownMultiplier);
  }

  /**
   * Marks a wallet as used after trading
   * @param {Object} wallet
   */
  markWalletUsed(wallet) {
    const walletKey = wallet.keypair.publicKey.toBase58();
    this.walletCooldowns.set(walletKey, Date.now());
    this.walletTradeCount.set(walletKey, (this.walletTradeCount.get(walletKey) || 0) + 1);
  }

  /**
   * Assigns personalities to wallets for varied behavior
   */
  assignPersonalities() {
    this.allPubkeys.forEach(pubkey => {
      this.walletPersonalities.set(pubkey, PERSONALITIES[Math.floor(Math.random() * PERSONALITIES.length)]);
    });
  }

  /**
   * Gets the personality of a wallet
   * @param {string|PublicKey} pubkey
   * @returns {string}
   */
  getPersonality(pubkey) {
    const key = typeof pubkey === 'string' ? pubkey : pubkey.toBase58();
    return this.walletPersonalities.get(key) || 'flipper';
  }
}