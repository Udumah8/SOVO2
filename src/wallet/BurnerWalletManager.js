import { Keypair, Transaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
import { setTimeout as delay } from 'timers/promises';
import { ComputeBudgetProgram, SystemProgram } from '@solana/web3.js';
import BN from 'bn.js';
import {
  PRIORITY_FEE_MICRO_LAMPORTS,
  BWC_WALLET_TYPES,
} from '../constants.js';
import { getRandomNumberBetween } from '../utils.js';

/**
 * Burner Wallet Manager - Handles Burner Wallet Churning (BWC)
 * Creates temporary wallets that are used for a limited number of transactions
 * and then disposed of to maintain privacy and avoid detection
 */
export class BurnerWalletManager {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {Logger} logger
   * @param {WalletManager} walletManager
   */
  constructor(config, connection, logger, walletManager) {
    this.config = config;
    this.connection = connection;
    this.logger = logger;
    this.walletManager = walletManager;

    // Burner wallet storage
    this.burnerWallets = new Map(); // pubkey -> wallet data
    this.burnerCooldowns = new Map(); // pubkey -> last used timestamp
    this.burnerTradeCount = new Map(); // pubkey -> transaction count
    this.burnerSeasoningCount = new Map(); // pubkey -> seasoning transaction count
    this.pendingDisposal = new Set(); // wallets ready for disposal
    this.lastCreationCheck = 0;

    // Failure tracking and circuit breaker
    this.consecutiveFundingFailures = 0;
    this.maxFundingFailures = 10; // Stop creating wallets after 10 consecutive failures
    this.lastSuccessfulFunding = 0;
    this.creationPausedUntil = 0;

    // Statistics
    this.stats = {
      created: 0,
      disposed: 0,
      totalTransactions: 0,
      averageLifetime: 0,
      activeBurners: 0,
      seasonedBurners: 0,
    };
  }

  /**
   * Initializes the burner wallet manager
   */
  async init() {
    if (!this.config.bwcEnabled) {
      this.logger.info('BWC is disabled');
      return;
    }

    this.logger.info('Initializing Burner Wallet Manager', {
      mode: this.config.bwcMode,
      maxBurners: this.config.bwcMaxBurnerWallets,
      lifetimeTxs: this.config.bwcBurnerLifetimeTxs,
      fundAmount: this.config.bwcBurnerFundAmount,
    });

    // Check prerequisites before creating wallets
    await this.checkPrerequisites();

    // Create initial batch of burner wallets if needed
    if (this.config.bwcMode !== 'disabled') {
      await this.ensureMinimumBurners();
    }
  }

  /**
   * Checks prerequisites for BWC functionality
   */
  async checkPrerequisites() {
    // Check if relayer wallets are available
    if (!this.walletManager.relayerKeypairs || this.walletManager.relayerKeypairs.length === 0) {
      this.logger.warn('No relayer wallets found - BWC funding will not work');
      this.logger.warn('Configure RELAYER_PRIVATE_KEYS in .env for BWC to function');
    }

    // Check if sink wallet is available
    if (!this.walletManager.sinkKeypair) {
      this.logger.warn('No sink wallet found - BWC withdrawals will not be possible');
      this.logger.warn('Configure SINK_PRIVATE_KEY in .env for fund recovery');
    }

    this.logger.info('BWC prerequisites check completed');
  }

  /**
   * Gets available burner wallets for trading
   * @param {number} count - Number of wallets needed
   * @param {boolean} emergencyMode - Use emergency ratio if true
   * @returns {Array} Array of burner wallet objects
   */
  getAvailableBurners(count, emergencyMode = false) {
    if (!this.config.bwcEnabled || this.config.bwcMode === 'disabled') {
      return [];
    }

    const now = Date.now();
    const available = [];

    // Clean up old entries and ready disposals
    this.cleanupBurners(now);

    // Get ready wallets
    for (const [pubkey, walletData] of this.burnerWallets.entries()) {
      if (this.pendingDisposal.has(pubkey)) continue;

      const lastUsed = this.burnerCooldowns.get(pubkey) || 0;
      const txCount = this.burnerTradeCount.get(pubkey) || 0;
      const cooldown = this.getBurnerCooldown(txCount);

      // Check cooldown and transaction limits
      const cooldownPassed = now - lastUsed >= cooldown;
      const withinLifetime = txCount < this.config.bwcBurnerLifetimeTxs;

      // Check seasoning status if burner seasoning is enabled
      const seasoningCount = this.burnerSeasoningCount.get(pubkey) || 0;
      const isSeasoned = !this.config.enableBurnerSeasoning || seasoningCount >= this.config.burnerSeasoningMinTxs;

      if (cooldownPassed && withinLifetime && isSeasoned) {
        available.push({
          ...walletData,
          pubkey,
          txCount,
          seasoningCount,
          isSeasoned,
          type: BWC_WALLET_TYPES.BURNER,
        });
      }
    }

    // Log availability stats
    if (this.config.enableBurnerSeasoning) {
      const seasonedCount = available.length;
      const totalEligible = Array.from(this.burnerWallets.entries()).filter(([pubkey, walletData]) => {
        if (this.pendingDisposal.has(pubkey)) return false;
        const lastUsed = this.burnerCooldowns.get(pubkey) || 0;
        const txCount = this.burnerTradeCount.get(pubkey) || 0;
        const cooldown = this.getBurnerCooldown(txCount);
        const seasoningCount = this.burnerSeasoningCount.get(pubkey) || 0;
        const isSeasoned = seasoningCount >= this.config.burnerSeasoningMinTxs;
        return (Date.now() - lastUsed >= cooldown) &&
          (txCount < this.config.bwcBurnerLifetimeTxs) &&
          isSeasoned;
      }).length;

      this.logger.debug('Burner wallet availability', {
        available: seasonedCount,
        totalBurners: this.burnerWallets.size,
        pendingDisposal: this.pendingDisposal.size,
        requireSeasoning: this.config.enableBurnerSeasoning
      });
    }

    // Shuffle for randomness
    this.shuffleArray(available);

    // Apply emergency ratio if needed
    const targetCount = emergencyMode ?
      Math.ceil(count * this.config.bwcEmergencyBurnerRatio) :
      Math.ceil(count * this.config.bwcBurnerRatio);

    return available.slice(0, Math.min(targetCount, count));
  }

  /**
   * Creates new burner wallets if needed
   */
  async ensureMinimumBurners() {
    const now = Date.now();

    // Enhanced circuit breaker - pause creation if too many failures
    if (this.consecutiveFundingFailures >= this.maxFundingFailures) {
      if (now < this.creationPausedUntil) {
        this.logger.warn('BWC creation paused due to funding failures', {
          failures: this.consecutiveFundingFailures,
          pausedUntil: new Date(this.creationPausedUntil).toLocaleTimeString(),
          cooldownRemaining: Math.ceil((this.creationPausedUntil - now) / 1000) + 's'
        });
        return;
      } else {
        // Reset failure count after cooldown period
        this.consecutiveFundingFailures = 0;
        this.logger.info('BWC creation circuit breaker reset after cooldown');
      }
    }

    // Check if enough time has passed since last creation attempt
    if (now - this.lastCreationCheck < this.config.bwcBurnerCreationInterval) {
      return;
    }

    const activeCount = this.getActiveBurnerCount();
    const targetCount = this.config.bwcMaxBurnerWallets;

    // Only create if we truly need more wallets
    if (activeCount >= targetCount) {
      this.logger.debug('Sufficient burner wallets available', {
        active: activeCount,
        target: targetCount
      });
      this.lastCreationCheck = now;
      return;
    }

    // Additional check: don't create if we have no relayer wallets (prerequisite failure)
    if (!this.walletManager.relayerKeypairs || this.walletManager.relayerKeypairs.length === 0) {
      this.logger.warn('Cannot create burner wallets: no relayer wallets available');
      this.lastCreationCheck = now;
      return;
    }

    const toCreate = Math.min(targetCount - activeCount, 2); // Further reduced to prevent spam
    this.logger.info(`Creating ${toCreate} new burner wallets`, {
      active: activeCount,
      target: targetCount,
      recentFailures: this.consecutiveFundingFailures,
      reason: 'Below target threshold'
    });

    let successCount = 0;
    for (let i = 0; i < toCreate; i++) {
      try {
        const success = await this.createBurnerWallet();

        if (success) {
          successCount++;
          this.consecutiveFundingFailures = 0; // Reset on success
          this.lastSuccessfulFunding = now;
        } else {
          this.consecutiveFundingFailures++;
          this.lastSuccessfulFunding = now;

          // If we hit max failures, activate circuit breaker
          if (this.consecutiveFundingFailures >= this.maxFundingFailures) {
            this.creationPausedUntil = now + 300000; // 5 minutes
            this.logger.error('BWC creation circuit breaker activated', {
              failures: this.consecutiveFundingFailures,
              pauseDuration: '5 minutes',
              successfulCreations: successCount
            });
            break;
          }

          // Progressive backoff: increase delay between attempts on failures
          if (this.consecutiveFundingFailures > 0) {
            const backoffDelay = Math.min(this.consecutiveFundingFailures * 5000, 30000); // Max 30s backoff
            this.logger.debug('Applying backoff delay', {
              failures: this.consecutiveFundingFailures,
              backoffDelay: backoffDelay + 'ms'
            });
            await delay(backoffDelay);
          }
        }
      } catch (error) {
        this.logger.error('Unexpected error during burner creation', {
          error: error.message,
          attempt: i + 1
        });
        this.consecutiveFundingFailures++;
      }

      // Small delay between creation attempts to prevent overwhelming the network
      if (i < toCreate - 1) {
        await delay(2000);
      }
    }

    this.lastCreationCheck = now;

    // Log final status
    this.logger.debug('BWC creation cycle completed', {
      attempts: toCreate,
      successes: successCount,
      failures: this.consecutiveFundingFailures,
      nextCheckIn: this.config.bwcBurnerCreationInterval + 'ms'
    });
  }

  /**
   * Creates a single burner wallet
   */
  async createBurnerWallet() {
    try {
      // Check if prerequisites are met before creating
      if (!this.walletManager.relayerKeypairs || this.walletManager.relayerKeypairs.length === 0) {
        this.logger.error('Cannot create burner wallet: no relayer wallets available');
        return false;
      }

      const kp = Keypair.generate();
      const pubkey = kp.publicKey.toBase58();

      const walletData = {
        keypair: kp,
        name: `Burner-${pubkey.slice(0, 8)}`,
        created: Date.now(),
        type: BWC_WALLET_TYPES.BURNER,
      };

      this.burnerWallets.set(pubkey, walletData);
      this.burnerTradeCount.set(pubkey, 0);
      this.burnerCooldowns.set(pubkey, 0);

      this.stats.created++;
      this.logger.debug('Created burner wallet', {
        pubkey: pubkey.slice(0, 8),
        totalCreated: this.stats.created
      });

      // Fund the burner wallet and return success status
      const fundingSuccess = await this.fundBurnerWallet(walletData);

      if (fundingSuccess) {
        return true;
      } else {
        // Remove the wallet if funding failed
        this.burnerWallets.delete(pubkey);
        this.burnerTradeCount.delete(pubkey);
        this.burnerCooldowns.delete(pubkey);
        this.stats.created--; // Don't count failed creation
        return false;
      }

    } catch (error) {
      this.logger.error('Failed to create burner wallet', { error: error.message });
      return false;
    }
  }

  /**
   * Funds a burner wallet
   * @param {Object} walletData
   */
  async fundBurnerWallet(walletData) {
    if (!this.walletManager.relayerKeypairs || this.walletManager.relayerKeypairs.length === 0) {
      this.logger.error('Cannot fund burner wallet: no relayer wallets configured', {
        required: 'RELAYER_PRIVATE_KEYS in .env',
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8)
      });
      return false;
    }

    try {
      // Check if relayer has sufficient balance
      const relayer = this.walletManager.relayerKeypairs[
        Math.floor(Math.random() * this.walletManager.relayerKeypairs.length)
      ];

      const relayerBalance = await this.connection.getBalance(relayer.publicKey);
      const requiredAmount = Math.floor(this.config.bwcBurnerFundAmount * LAMPORTS_PER_SOL);
      const totalRequired = requiredAmount + (PRIORITY_FEE_MICRO_LAMPORTS * 25000 / 1000000); // Add estimated fees

      if (relayerBalance < totalRequired) {
        this.logger.error('Relayer wallet has insufficient balance for funding', {
          relayer: relayer.publicKey.toBase58().slice(0, 8),
          balance: (relayerBalance / LAMPORTS_PER_SOL).toFixed(4),
          required: (totalRequired / LAMPORTS_PER_SOL).toFixed(4),
          pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8)
        });
        return false;
      }

      const fundAmountLamports = BigInt(requiredAmount);

      const tx = new Transaction()
        .add(ComputeBudgetProgram.setComputeUnitLimit({ units: 25000 }))
        .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }))
        .add(SystemProgram.transfer({
          fromPubkey: relayer.publicKey,
          toPubkey: walletData.keypair.publicKey,
          lamports: fundAmountLamports,
        }));

      this.logger.debug('Funding burner wallet', {
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8),
        relayer: relayer.publicKey.toBase58().slice(0, 8),
        amount: this.config.bwcBurnerFundAmount,
        relayerBalance: (relayerBalance / LAMPORTS_PER_SOL).toFixed(4)
      });

      const sig = await this.connection.sendTransaction(tx, [relayer], {
        skipPreflight: true,
        commitment: 'confirmed'
      });

      // Wait for confirmation with timeout
      await this.connection.confirmTransaction({
        signature: sig,
        blockhash: (await this.connection.getLatestBlockhash()).blockhash,
        lastValidBlockHeight: (await this.connection.getBlockHeight()) + 30
      }, 'confirmed');

      this.logger.debug('Successfully funded burner wallet', {
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8),
        amount: this.config.bwcBurnerFundAmount,
        tx: sig,
      });

      return true;
    } catch (error) {
      // Check if this is a timeout error (most common issue)
      const isTimeout = error.message.includes('not confirmed') || error.message.includes('timeout');

      this.logger.error('Failed to fund burner wallet', {
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8),
        error: error.message,
        timeout: isTimeout,
        likelyCauses: isTimeout ? [
          'Network congestion',
          'RPC endpoint issues',
          'Insufficient priority fees',
          'Relayer wallet balance issues'
        ] : 'Transaction failed'
      });
      return false;
    }
  }

  /**
   * Marks a burner wallet as used
   * @param {string} pubkey
   * @param {number} txCount - Number of transactions performed
   */
  markBurnerUsed(pubkey, txCount = 1) {
    if (!this.burnerWallets.has(pubkey)) return;

    const currentCount = this.burnerTradeCount.get(pubkey) || 0;
    const newCount = currentCount + txCount;

    this.burnerTradeCount.set(pubkey, newCount);
    this.burnerCooldowns.set(pubkey, Date.now());
    this.stats.totalTransactions += txCount;

    // Mark seasoning progress if burner seasoning is enabled
    if (this.config.enableBurnerSeasoning) {
      this.markBurnerSeasoningProgress(pubkey, txCount);
    }

    // Check if wallet should be disposed
    if (newCount >= this.config.bwcBurnerLifetimeTxs) {
      this.scheduleBurnerDisposal(pubkey);
    }
  }

  /**
   * Tracks seasoning progress for burner wallets
   * @param {string} pubkey
   * @param {number} txCount - Number of transactions performed
   */
  markBurnerSeasoningProgress(pubkey, txCount) {
    if (!this.config.enableBurnerSeasoning) return;

    const currentSeasoning = this.burnerSeasoningCount.get(pubkey) || 0;
    const newSeasoningCount = currentSeasoning + txCount;
    this.burnerSeasoningCount.set(pubkey, newSeasoningCount);

    // Update seasoned burner count for stats
    this.updateSeasonedBurnerCount();

    // Log seasoning progress
    const minTxs = this.config.burnerSeasoningMinTxs;
    if (newSeasoningCount >= minTxs) {
      this.logger.info('Burner wallet seasoned', {
        pubkey: pubkey.slice(0, 8),
        seasoningTxs: newSeasoningCount,
        required: minTxs
      });
    } else {
      this.logger.debug('Burner seasoning progress', {
        pubkey: pubkey.slice(0, 8),
        progress: `${newSeasoningCount}/${minTxs}`
      });
    }
  }

  /**
   * Updates the count of seasoned burner wallets for statistics
   */
  updateSeasonedBurnerCount() {
    let seasonedCount = 0;
    const minTxs = this.config.burnerSeasoningMinTxs;

    for (const [pubkey, walletData] of this.burnerWallets.entries()) {
      if (this.pendingDisposal.has(pubkey)) continue;

      const seasoningCount = this.burnerSeasoningCount.get(pubkey) || 0;
      if (seasoningCount >= minTxs) {
        seasonedCount++;
      }
    }

    this.stats.seasonedBurners = seasonedCount;
  }

  /**
   * Schedules a burner wallet for disposal
   * @param {string} pubkey
   */
  scheduleBurnerDisposal(pubkey) {
    if (!this.burnerWallets.has(pubkey) || this.pendingDisposal.has(pubkey)) {
      return;
    }

    this.pendingDisposal.add(pubkey);
    this.logger.info('Scheduled burner for disposal', {
      pubkey: pubkey.slice(0, 8),
      txCount: this.burnerTradeCount.get(pubkey)
    });

    // Auto-dispose after delay
    setTimeout(() => {
      this.disposeBurnerWallet(pubkey);
    }, this.config.bwcBurnerDisposalDelay);
  }

  /**
   * Disposes of a burner wallet
   * @param {string} pubkey
   */
  async disposeBurnerWallet(pubkey) {
    try {
      const walletData = this.burnerWallets.get(pubkey);
      if (!walletData) return;

      // Withdraw remaining funds if sink wallet is available
      if (this.walletManager.sinkKeypair) {
        await this.withdrawBurnerFunds(walletData);
      }

      const seasoningCount = this.burnerSeasoningCount.get(pubkey) || 0;
      const tradeCount = this.burnerTradeCount.get(pubkey) || 0;

      // Remove from storage
      this.burnerWallets.delete(pubkey);
      this.burnerCooldowns.delete(pubkey);
      this.burnerTradeCount.delete(pubkey);
      this.burnerSeasoningCount.delete(pubkey);
      this.pendingDisposal.delete(pubkey);

      this.stats.disposed++;
      this.logger.info('Disposed burner wallet', {
        pubkey: pubkey.slice(0, 8),
        lifetime: tradeCount,
        seasoningTxs: seasoningCount,
        totalDisposed: this.stats.disposed
      });

      // Update average lifetime and seasoned count
      this.updateAverageLifetime();
      this.updateSeasonedBurnerCount();

    } catch (error) {
      this.logger.error('Failed to dispose burner wallet', {
        pubkey: pubkey.slice(0, 8),
        error: error.message
      });
    }
  }

  /**
   * Withdraws remaining funds from a burner wallet to sink
   * @param {Object} walletData
   */
  async withdrawBurnerFunds(walletData) {
    if (!this.walletManager.sinkKeypair) return;

    try {
      const balance = await this.connection.getBalance(walletData.keypair.publicKey);
      const minBalanceLamports = BigInt(
        Math.floor(this.config.bwcMinBurnerBalance * LAMPORTS_PER_SOL)
      );

      if (balance <= Number(minBalanceLamports)) {
        return; // Not enough funds to withdraw
      }

      const withdrawAmount = BigInt(balance - Number(minBalanceLamports));

      const tx = new Transaction()
        .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }))
        .add(SystemProgram.transfer({
          fromPubkey: walletData.keypair.publicKey,
          toPubkey: this.walletManager.sinkKeypair.publicKey,
          lamports: withdrawAmount,
        }));

      const sig = await this.connection.sendTransaction(tx, [walletData.keypair]);
      await this.connection.confirmTransaction(sig, 'confirmed');

      this.logger.debug('Withdrew burner funds', {
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8),
        amount: (Number(withdrawAmount) / LAMPORTS_PER_SOL).toFixed(6),
        tx: sig,
      });

    } catch (error) {
      this.logger.error('Failed to withdraw burner funds', {
        pubkey: walletData.keypair.publicKey.toBase58().slice(0, 8),
        error: error.message
      });
    }
  }

  /**
   * Gets cooldown period for a burner wallet
   * @param {number} txCount
   * @returns {number}
   */
  getBurnerCooldown(txCount) {
    const baseCooldown = getRandomNumberBetween(30000, 60000); // 30-60 seconds
    const txMultiplier = 1 + (txCount * 0.1); // Increase cooldown with usage
    return Math.floor(baseCooldown * txMultiplier);
  }

  /**
   * Gets the number of active burner wallets
   * @returns {number}
   */
  getActiveBurnerCount() {
    let count = 0;
    for (const [pubkey, walletData] of this.burnerWallets.entries()) {
      if (!this.pendingDisposal.has(pubkey)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Cleans up old burner wallet data
   * @param {number} now
   */
  cleanupBurners(now) {
    // Clean up old cooldowns
    for (const [pubkey, timestamp] of this.burnerCooldowns.entries()) {
      if (now - timestamp > 3600000) { // 1 hour
        this.burnerCooldowns.delete(pubkey);
      }
    }

    // Clean up old trade counts (keep but cap)
    for (const [pubkey, count] of this.burnerTradeCount.entries()) {
      if (count > 100) {
        this.burnerTradeCount.set(pubkey, 100);
      }
    }

    // Clean up old seasoning counts
    for (const [pubkey, count] of this.burnerSeasoningCount.entries()) {
      if (count > 20) { // Cap seasoning counts
        this.burnerSeasoningCount.set(pubkey, 20);
      }
    }
  }

  /**
   * Shuffles an array using Fisher-Yates algorithm
   * @param {Array} array
   */
  shuffleArray(array) {
    const shuffled = [...array];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
  }

  /**
   * Updates the average lifetime statistics
   */
  updateAverageLifetime() {
    if (this.stats.disposed > 0) {
      const totalLifetime = Array.from(this.burnerTradeCount.values())
        .reduce((sum, count) => sum + count, 0);
      this.stats.averageLifetime = totalLifetime / this.stats.disposed;
    }
  }

  /**
   * Gets current BWC statistics
   * @returns {Object}
   */
  getStats() {
    return {
      ...this.stats,
      activeBurners: this.getActiveBurnerCount(),
      pendingDisposal: this.pendingDisposal.size,
      totalBurners: this.burnerWallets.size,
      requireBurnerSeasoning: this.config.enableBurnerSeasoning,
      burnerSeasoningMinTxs: this.config.burnerSeasoningMinTxs,
      burnerSeasoningMaxTxs: this.config.burnerSeasoningMaxTxs,
    };
  }

  /**
   * Logs BWC statistics
   */
  logStats() {
    const stats = this.getStats();
    this.logger.info('BWC Statistics', stats);
  }

  /**
   * Emergency disposal of all burner wallets
   */
  async emergencyDisposeAll() {
    this.logger.warn('Emergency disposal of all burner wallets initiated');

    const disposalPromises = Array.from(this.burnerWallets.keys())
      .map(pubkey => this.disposeBurnerWallet(pubkey));

    await Promise.allSettled(disposalPromises);

    // Clear all tracking data
    this.burnerWallets.clear();
    this.burnerCooldowns.clear();
    this.burnerTradeCount.clear();
    this.burnerSeasoningCount.clear();
    this.pendingDisposal.clear();

    this.logger.info('Emergency disposal completed');
  }
}