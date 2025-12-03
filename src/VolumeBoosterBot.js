import { Connection, Keypair, Transaction, LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import { ConfigManager } from './config/ConfigManager.js';
import { Logger } from './logging/Logger.js';
import { WalletManager } from './wallet/WalletManager.js';
import { MarketDataProvider } from './market/MarketDataProvider.js';
import { TradingEngine } from './trading/TradingEngine.js';
import { CircuitBreaker } from './safety/CircuitBreaker.js';
import { WalletRebalancer } from './rebalancing/WalletRebalancer.js';
import { WalletSeasoner } from './seasoning/WalletSeasoner.js';
import { BurnerWalletManager } from './wallet/BurnerWalletManager.js';
import { setTimeout as delay } from 'timers/promises';
import { ComputeBudgetProgram, SystemProgram } from '@solana/web3.js';
import { getRandomNumberBetween } from './utils.js';
import { createCloseAccountInstruction, getAssociatedTokenAddressSync, getAccount, AccountLayout, TOKEN_PROGRAM_ID } from '@solana/spl-token';
import {
  USDC_MINT,
  LAMPORTS_PER_SOL_BN,
  PRIORITY_FEE_MICRO_LAMPORTS,
  MIN_TRANSFER_LAMPORTS,
  BASE_TX_FEE_LAMPORTS,
  WALLET_COOLDOWN_DELAY_MS,
  SESSION_PAUSE_MULTIPLIER,
  ROTATION_STATS_INTERVAL,
  WITHDRAWAL_CHUNK_SIZE,
  BWC_STAT_INTERVAL,
} from './constants.js';
import { WalletDataLoader } from './wallet/WalletDataLoader.js';
import { AlertManager } from './logging/AlertManager.js';

/**
 * Main Volume Booster Bot - Orchestrator
 */
export class VolumeBoosterBot {
  /**
   * Constructor - Initializes all systems
   */
  constructor() {
    try {
      this.config = new ConfigManager();
      this.logger = new Logger();
      this.alertManager = new AlertManager(this.config, this.logger);
      this.connection = new Connection(this.config.rpcUrl, 'confirmed');
      this.walletManager = new WalletManager(this.config, this.connection, this.logger);
      this.marketData = new MarketDataProvider(this.config, this.connection, this.logger);
      this.burnerWalletManager = new BurnerWalletManager(this.config, this.connection, this.logger, this.walletManager);
      this.tradingEngine = new TradingEngine(this.config, this.connection, this.marketData, this.walletManager, this.logger, this.burnerWalletManager);
      this.circuitBreaker = new CircuitBreaker(this.config, this.logger);
      this.rebalancer = new WalletRebalancer(this.config, this.walletManager, this.connection, this.logger);
      this.seasoner = new WalletSeasoner(this.config, this.walletManager, this.connection, this.logger);
      this.walletDataLoader = new WalletDataLoader(this.config, this.logger);
    } catch (error) {
      const errorMessage = `FATAL: Configuration initialization failed: ${error.message}`;
      console.error(errorMessage);
      if (this.alertManager) {
        this.alertManager.sendCriticalAlert('Bot Initialization Failed', error.message);
      }
      throw error;
    }

    this.isRunning = false;
    this.cycleCount = 0;
    this.totalVolume = 0;
    this.isWithdrawing = false;
  }

  /**
   * Initializes the bot
   */
  async init() {
    try {
      this.logger.info('Initializing Volume Booster Bot');
      await this.walletManager.loadOrGenerateWallets();
      this.walletManager.assignPersonalities();
      await this.seasoner.seasonWallets();
      await this.burnerWalletManager.init();
      await this.marketData.getMarketData();
      this.isRunning = true;
      this.logInitialSettings();
      if (this.walletManager.sinkKeypair) {
        this.circuitBreaker.initialSinkBalance = BigInt(await this.connection.getBalance(this.walletManager.sinkKeypair.publicKey));
        this.logger.info('Initial sink balance locked', {
          balance: (Number(this.circuitBreaker.initialSinkBalance) / LAMPORTS_PER_SOL).toFixed(4),
        });
      }
      await this.runLoop();
    } catch (error) {
      this.logger.error('Bot initialization failed', { error: error.message });
      this.alertManager.sendCriticalAlert('Bot Initialization Failed', error.message);
      throw error;
    }
  }

  /**
   * Logs the initial bot settings
   */
  logInitialSettings() {
    this.logger.info('Bot starting', {
      wallets: this.walletManager.allPubkeys.size,
      activeBatch: this.walletManager.activeWallets.length,
      concurrency: this.config.concurrency,
      batchSize: this.config.batchSize,
      network: this.config.isDevnet ? 'Devnet' : 'Mainnet',
      market: this.config.market,
      tradeMode: this.config.tradeMode,
      buyProb: this.config.buyProb,
      actionsPerCycle: this.config.numActionsPerCycle,
      cooldown: `${(this.config.minWalletCooldownMs / 60000).toFixed(1)}-${(this.config.maxWalletCooldownMs / 60000).toFixed(1)} min`,
      shuffle: this.config.shuffleWallets,
      birdeye: this.config.useBirdeye,
      rebalancing: this.config.enableRebalancing,
      circuitBreaker: this.config.enableCircuitBreaker,
      bwcEnabled: this.config.bwcEnabled,
      bwcMode: this.config.bwcMode,
    });

    if (this.config.useBirdeye) {
      this.logger.info('Market settings', {
        minLiquidity: this.config.minLiquidity.toLocaleString(),
        maxPriceImpact: this.config.maxPriceImpact,
      });
    }

    if (this.config.enableRebalancing) {
      this.logger.info('Rebalancing settings', {
        targetBalance: (Number(this.config.targetWalletBalance) / LAMPORTS_PER_SOL).toFixed(3),
        interval: `Every ${this.config.rebalanceInterval} cycles`,
      });
    }

    if (this.config.enableCircuitBreaker) {
      this.logger.info('Circuit breaker settings', {
        maxFailures: this.config.maxConsecutiveFailures,
        maxFailureRate: this.config.maxFailureRate,
        stopLoss: this.config.emergencyStopLoss,
      });
    }

    if (this.walletManager.sinkKeypair) {
      this.logger.info('Sink wallet detected, but balance will be locked during initialization.');
    }
  }

  /**
   * Main trading loop
   */
  async runLoop() {
    let batchNum = 0;
    while (this.isRunning) {
      try {
        if (await this.handleCircuitBreaker()) break;

        // Enhanced error handling for BWC operations
        if (this.config.bwcEnabled && this.config.bwcMode !== 'disabled') {
          try {
            const availableBurners = this.burnerWalletManager.getAvailableBurners(1);
            if (availableBurners.length === 0) {
              this.logger.info('No burner wallets available, checking if new ones can be created');
              await this.burnerWalletManager.ensureMinimumBurners();
            }
          } catch (bwcError) {
            this.logger.error('BWC operation failed, continuing with regular wallets only', {
              error: bwcError.message
            });
            // Continue with regular wallets even if BWC fails
          }
        }

        const walletBatch = await this.walletManager.loadActiveBatch();
        let combinedBatch = [...walletBatch];

        // Add burner wallets in hybrid mode
        if (this.config.bwcEnabled && this.config.bwcMode === 'hybrid') {
          try {
            const burnerWallets = this.burnerWalletManager.getAvailableBurners(walletBatch.length);
            combinedBatch = [...walletBatch, ...burnerWallets];
            
            // Log seasoning status for hybrid mode
            if (this.config.enableSeasoning || this.config.enableBurnerSeasoning) {
              const seasonedRegular = walletBatch.filter(w => w.isSeasoned !== false).length;
              const seasonedBurners = burnerWallets.filter(w => w.isSeasoned !== false).length;
              this.logger.info('Hybrid mode wallet seasoning status', {
                regularWallets: walletBatch.length,
                seasonedRegular,
                burnerWallets: burnerWallets.length,
                seasonedBurners,
                requireRegularSeasoning: this.config.enableSeasoning,
                requireBurnerSeasoning: this.config.enableBurnerSeasoning
              });
            }
          } catch (hybridError) {
            this.logger.error('Hybrid mode error, using regular wallets only', {
              error: hybridError.message
            });
            combinedBatch = walletBatch; // Fallback to regular wallets
          }
        } else if (this.config.bwcEnabled && this.config.bwcMode === 'burner_only') {
          try {
            const burnerWallets = this.burnerWalletManager.getAvailableBurners(this.config.batchSize);
            combinedBatch = burnerWallets;
            
            // Log seasoning status for burner-only mode
            if (this.config.enableBurnerSeasoning) {
              const seasonedBurners = burnerWallets.filter(w => w.isSeasoned !== false).length;
              this.logger.info('Burner-only mode wallet seasoning status', {
                burnerWallets: burnerWallets.length,
                seasonedBurners,
                requireBurnerSeasoning: this.config.enableBurnerSeasoning
              });
            }
          } catch (burnerError) {
            this.logger.error('Burner-only mode error, no wallets available', {
              error: burnerError.message
            });
            combinedBatch = []; // No wallets available
          }
        }

        if (combinedBatch.length === 0) {
          this.logger.info('No wallets available, waiting for cooldowns');
          await delay(WALLET_COOLDOWN_DELAY_MS);
          continue;
        }

        batchNum++;
        this.cycleCount++;
        await this.processBatch(batchNum, combinedBatch);

        if (batchNum % ROTATION_STATS_INTERVAL === 0) {
          this.printRotationStats();
        }

        // Log BWC stats periodically
        if (this.config.bwcEnabled && this.cycleCount % BWC_STAT_INTERVAL === 0) {
          try {
            this.burnerWalletManager.logStats();
          } catch (statsError) {
            this.logger.warn('Failed to log BWC stats', { error: statsError.message });
          }
        }

        if (this.config.enableRebalancing && this.cycleCount % this.config.rebalanceInterval === 0) {
          try {
            await this.rebalancer.rebalanceWallets();
          } catch (rebalanceError) {
            this.logger.error('Rebalancing failed', { error: rebalanceError.message });
            // Continue execution even if rebalancing fails
          }
        }

        await this.handleSessionPause(batchNum);
        await this.handleInterBatchDelay();
      } catch (loopError) {
        this.logger.error('Unexpected error in main loop', {
          error: loopError.message,
          stack: loopError.stack
        });
        
        // Add a delay before retrying to prevent tight error loops
        await delay(10000);
        
        // Continue the loop unless it's a critical error
        if (loopError.message.includes('fatal') || loopError.message.includes('critical')) {
          this.logger.error('Critical error detected, stopping bot');
          await this.stop();
          process.exit(1);
        }
      }
    }
  }

  /**
   * Processes a single batch of wallets
   * @param {number} batchNum
   * @param {Array} walletBatch
   */
  async processBatch(batchNum, walletBatch) {
    const marketData = await this.marketData.getMarketData();
    this.logger.info(`Current market price: $${marketData.price.toFixed(6)} (Source: ${marketData.source})`);
    this.logger.info(`Starting batch ${batchNum}`, {
      cycle: this.cycleCount,
      ramp: Math.min(1, (this.cycleCount / this.config.rampCycles) * 100).toFixed(0),
    });

    const promises = walletBatch.map(wallet => this.tradingEngine.processWalletCycle(wallet, this.circuitBreaker));
    const results = await Promise.allSettled(promises);
    this.logBatchResults(batchNum, results, walletBatch.length);
  }

  /**
   * Logs the results of a batch
   * @param {number} batchNum
   * @param {Array} results
   * @param {number} total
   */
  logBatchResults(batchNum, results, total) {
    const successes = results.filter(r => r.status === 'fulfilled' && r.value.success).length;
    const failures = results.filter(r => r.status === 'rejected' || (r.status === 'fulfilled' && !r.value.success));
    this.logger.info(`Batch ${batchNum} complete`, {
      successful: successes,
      failed: failures.length,
      total,
    });

    if (failures.length > 0) {
      this.logger.warn('Details of failed transactions in batch:');
      failures.forEach((failure, index) => {
        this.logger.warn(`  Failure ${index + 1}: ${failure.reason || failure.value?.error}`);
      });
    }
  }

  /**
   * Handles the session pause
   * @param {number} batchNum
   */
  async handleSessionPause(batchNum) {
    if (batchNum % this.config.sessionPauseMin === 0) {
      const minPauseMs = this.config.sessionPauseMin * 60 * 1000;
      const maxPauseMs = minPauseMs * SESSION_PAUSE_MULTIPLIER;
      const pauseMs = getRandomNumberBetween(minPauseMs, maxPauseMs);
      this.logger.info('Session pause', { duration: `${(pauseMs / 1000).toFixed(0)}s` });
      await delay(pauseMs);
    }
  }

  /**
   * Handles the inter-batch delay
   */
  async handleInterBatchDelay() {
    const interBatchDelay = getRandomNumberBetween(this.config.minInterBatchDelayMs, this.config.maxInterBatchDelayMs);
    this.logger.debug('Inter-batch delay', { duration: `${(interBatchDelay / 1000).toFixed(2)}s` });
    await delay(interBatchDelay);
  }

  /**
   * Checks the circuit breaker and stops the bot if tripped
   * @returns {Promise<boolean>}
   */
  async handleCircuitBreaker() {
    const circuitCheck = await this.circuitBreaker.checkCircuitBreakers(this.connection, this.walletManager.sinkKeypair);
    if (circuitCheck.tripped) {
      const errorMessage = `Circuit breaker tripped: ${circuitCheck.reason}. Bot stopped for safety.`;
      this.logger.error(errorMessage);
      this.alertManager.sendCriticalAlert('Circuit Breaker Tripped', errorMessage);
      await this.stop();
      process.exit(1);
    }
    return circuitCheck.tripped;
  }

  /**
   * Prints wallet rotation statistics
   */
  printRotationStats() {
    const stats = {
      activeBatch: this.walletManager.activeWallets.length,
      onCooldown: this.walletManager.walletCooldowns.size,
    };

    // Add BWC stats if enabled
    if (this.config.bwcEnabled) {
      const bwcStats = this.burnerWalletManager.getStats();
      stats.burnerWallets = bwcStats.activeBurners;
      stats.burnerCreated = bwcStats.created;
      stats.burnerDisposed = bwcStats.disposed;
      stats.burnerTxs = bwcStats.totalTransactions;
    }

    this.logger.info('Wallet Rotation Stats', stats);

    const sortedWallets = Array.from(this.walletManager.walletTradeCount.entries())
      .sort((a, b) => b - a)
      .slice(0, 5);

    if (sortedWallets.length > 0) {
      this.logger.info('Top Active Wallets');
      sortedWallets.forEach(([key, count], i) => {
        this.logger.info(`  ${i + 1}. ${key.slice(0, 8)}...: ${count} trades`);
      });
    }
  }

  /**
   * Stops the bot and performs cleanup
   */
  async stop() {
    this.isRunning = false;
    this.logger.info('Bot stopping');
    
    // Dispose of all burner wallets
    if (this.config.bwcEnabled) {
      await this.burnerWalletManager.emergencyDisposeAll();
    }
    
    await this.closeAllTokenAccounts();
    if (this.walletManager.sinkKeypair) {
      await this.withdrawAllFunds();
    }
    
    // Final BWC stats
    if (this.config.bwcEnabled) {
      this.burnerWalletManager.logStats();
    }
    
    this.logger.info('Bot stopped', { totalVolume: this.totalVolume.toFixed(2) });
  }

  /**
   * Withdraws all funds to sink wallet
   */
  async withdrawAllFunds() {
    if (!this.walletManager.sinkKeypair) {
      this.logger.info('No sink wallet configured');
      return;
    }

    this.logger.info('Starting parallel withdrawal');
    const allWalletsData = await this.walletDataLoader.loadWallets();
    let successCount = 0;

    for (let i = 0; i < allWalletsData.length; i += WITHDRAWAL_CHUNK_SIZE) {
      const chunk = allWalletsData.slice(i, i + WITHDRAWAL_CHUNK_SIZE).map(w => ({
        keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
        name: w.name || w.pubkey.slice(0, 6),
      }));

      const balancePromises = chunk.map(wallet => this.connection.getBalance(wallet.keypair.publicKey));
      const balances = await Promise.allSettled(balancePromises);

      const withdrawalPromises = chunk.map((wallet, index) => {
        const balance = balances[index].status === 'fulfilled' ? balances[index].value : undefined;
        return this.withdrawSingleWallet(wallet, balance);
      });

      const results = await Promise.allSettled(withdrawalPromises);
      successCount += results.filter(r => r.status === 'fulfilled' && r.value).length;
      this.logger.info('Withdrawal progress', {
        processed: Math.min(i + WITHDRAWAL_CHUNK_SIZE, allWalletsData.length),
        total: allWalletsData.length,
      });
    }

    const sinkBalance = BigInt(await this.connection.getBalance(this.walletManager.sinkKeypair.publicKey));
    this.logger.info('Withdrawal complete', {
      successful: successCount,
      total: allWalletsData.length,
      finalSinkBalance: (Number(sinkBalance) / LAMPORTS_PER_SOL).toFixed(4),
    });

    await this.cleanupSinkTokenAccounts();
  }

  /**
   * Cleans up empty token accounts for the sink wallet
   */
  async cleanupSinkTokenAccounts() {
    if (!this.walletManager.sinkKeypair) return;
    const tokensToCheck = [this.config.memeCoinMint, USDC_MINT];
    for (const mint of tokensToCheck) {
      const sinkTokenAcc = getAssociatedTokenAddressSync(mint, this.walletManager.sinkKeypair.publicKey);
      try {
        const accountInfo = await getAccount(this.connection, sinkTokenAcc);
        if (accountInfo.amount === 0n) {
          const closeTx = new Transaction().add(createCloseAccountInstruction(
            sinkTokenAcc,
            this.walletManager.sinkKeypair.publicKey,
            this.walletManager.sinkKeypair.publicKey
          ));
          await this.connection.sendTransaction(closeTx, [this.walletManager.sinkKeypair]);
          this.logger.info('Sink token account closed', { mint: mint.toBase58().slice(0, 8) });
        }
      } catch (error) {
        if (!error.message.includes('Account not found')) {
          this.logger.error('Failed to cleanup sink token account', {
            mint: mint.toBase58().slice(0, 8),
            error: error.message,
          });
        }
      }
    }
  }

  /**
   * Withdraws funds from a single wallet
   * @param {Object} wallet
   * @param {number} [cachedBalance] - Optional cached balance to avoid re-fetching
   */
  async withdrawSingleWallet(wallet, cachedBalance) {
    try {
      const balance = cachedBalance ?? await this.connection.getBalance(wallet.keypair.publicKey);
      
      // Create a dummy transaction to get accurate fee estimation
      const dummyTransferIx = SystemProgram.transfer({
        fromPubkey: wallet.keypair.publicKey,
        toPubkey: this.walletManager.sinkKeypair.publicKey,
        lamports: 1, // Placeholder amount
      });
      
      const dummyTx = new Transaction()
        .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }))
        .add(dummyTransferIx);
      dummyTx.feePayer = wallet.keypair.publicKey;
      dummyTx.recentBlockhash = (await this.connection.getLatestBlockhash()).blockhash;
      
      // Get the actual fee for this transaction type
      const feeResponse = await this.connection.getFeeForMessage(dummyTx.compileMessage());
      const estimatedTxFee = BigInt(feeResponse.value || BASE_TX_FEE_LAMPORTS);
      
      // Convert minSolBufferLamports from SOL to lamports
      const minBufferLamports = BigInt(Math.floor(this.config.minSolBufferLamports * LAMPORTS_PER_SOL));
      
      // Calculate transfer amount with accurate fee estimation
      let transferAmount = BigInt(balance) - minBufferLamports - estimatedTxFee;
      transferAmount -= BigInt(Math.floor(getRandomNumberBetween(0, 1000))); // Add jitter

      if (transferAmount < MIN_TRANSFER_LAMPORTS) return false;

      const tx = new Transaction().add(
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }),
        SystemProgram.transfer({
          fromPubkey: wallet.keypair.publicKey,
          toPubkey: this.walletManager.sinkKeypair.publicKey,
          lamports: transferAmount,
        })
      );

      const signature = await this.connection.sendTransaction(tx, [wallet.keypair]);
      this.logger.info('Swept SOL from wallet', {
        wallet: wallet.keypair.publicKey.toBase58(),
        amount: (Number(transferAmount) / Number(LAMPORTS_PER_SOL_BN)).toFixed(6),
        tx: signature,
      });
      return true;
    } catch (error) {
      const errorMessage = `Failed to sweep SOL from wallet ${wallet.keypair.publicKey.toBase58()}: ${error.message}`;
      this.logger.error(errorMessage);
      this.alertManager.sendErrorAlert('SOL Sweep Failed', errorMessage);
      return false;
    }
  }

  /**
   * Estimates the fee for a withdrawal transaction
   * @param {Object} wallet
   * @returns {Promise<bigint>}
   */
  async estimateWithdrawalFee(wallet) {
    const dummyTransferIx = SystemProgram.transfer({
      fromPubkey: wallet.keypair.publicKey,
      toPubkey: this.walletManager.sinkKeypair.publicKey,
      lamports: 1,
    });
    const dummyTx = new Transaction()
      .add(ComputeBudgetProgram.setComputeUnitPrice({ microLamports: PRIORITY_FEE_MICRO_LAMPORTS }))
      .add(dummyTransferIx);
    dummyTx.feePayer = wallet.keypair.publicKey;
    dummyTx.recentBlockhash = (await this.connection.getLatestBlockhash()).blockhash;
    const fee = (await this.connection.getFeeForMessage(dummyTx.compileMessage())).value;
    return BigInt(fee || BASE_TX_FEE_LAMPORTS);
  }

  /**
   * Closes all empty token accounts for rent recovery
   */
  async closeAllTokenAccounts() {
    this.logger.info('Closing empty token accounts');
    const allWalletsData = await this.walletDataLoader.loadWallets();
    for (let i = 0; i < allWalletsData.length; i += WITHDRAWAL_CHUNK_SIZE) {
      const chunk = allWalletsData.slice(i, i + WITHDRAWAL_CHUNK_SIZE).map(w => ({
        keypair: Keypair.fromSecretKey(new Uint8Array(w.privateKey)),
        name: w.name || w.pubkey.slice(0, 6),
      }));
      const promises = chunk.map(wallet => this.closeWalletTokenAccounts(wallet));
      await Promise.allSettled(promises);
      this.logger.info('Token account closure progress', {
        processed: Math.min(i + WITHDRAWAL_CHUNK_SIZE, allWalletsData.length),
        total: allWalletsData.length,
      });
    }
  }

  /**
   * Closes token accounts for a single wallet
   * @param {Object} wallet
   */
  async closeWalletTokenAccounts(wallet) {
    const mintsToClose = [this.config.memeCoinMint, USDC_MINT];
    const ataAddresses = mintsToClose.map(mint => getAssociatedTokenAddressSync(mint, wallet.keypair.publicKey));
    const accountInfos = await this.connection.getMultipleAccounts(ataAddresses, { commitment: 'confirmed' });

    for (let i = 0; i < mintsToClose.length; i++) {
      const mint = mintsToClose[i];
      const ataAddress = ataAddresses[i];
      const accountInfo = accountInfos[i];

      if (!accountInfo) continue;

      try {
        const { amount } = AccountLayout.decode(accountInfo.data);
        if (amount !== 0n) {
          this.logger.warn('Skipping ATA close: non-zero balance', {
            wallet: wallet.name,
            mint: mint.toBase58().slice(0, 8),
            balance: amount.toString(),
          });
          continue;
        }
      } catch (error) {
        this.logger.warn('Failed to decode token account data', {
          wallet: wallet.name,
          mint: mint.toBase58().slice(0, 8),
          error: error.message,
        });
        continue;
      }

      try {
        const closeIx = createCloseAccountInstruction(ataAddress, wallet.keypair.publicKey, wallet.keypair.publicKey, [], TOKEN_PROGRAM_ID);
        const tx = new Transaction().add(closeIx);
        const signature = await this.connection.sendTransaction(tx, [wallet.keypair]);
        this.logger.info('Closed ATA', {
          wallet: wallet.name,
          mint: mint.toBase58().slice(0, 8),
          tx: signature,
        });
      } catch (error) {
        const errorMessage = `Failed to close ATA for wallet ${wallet.name}, mint ${mint.toBase58().slice(0, 8)}: ${error.message}`;
        this.logger.error(errorMessage);
        this.alertManager.sendErrorAlert('ATA Closure Failed', errorMessage);
      }
    }
  }
}