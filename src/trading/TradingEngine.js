import { LAMPORTS_PER_SOL, PublicKey } from '@solana/web3.js';
import { SolanaTrade } from 'solana-trade';
import { getAssociatedTokenAddressSync, getAccount } from '@solana/spl-token';
import axios from 'axios';
import pLimit from 'p-limit';
import { setTimeout as delay } from 'timers/promises';
import BN from 'bn.js';
import {
  DEFAULT_SLIPPAGE_RETAIL,
  DEFAULT_SLIPPAGE_WHALE,
  MIN_MEME_TOKENS,
  SENTIMENT_FETCH_COOLDOWN_MS,
  SENTIMENT_THRESHOLDS,
  HODLER_SELL_CHANCE,
} from '../constants.js';
import { getJitteredValue } from '../utils.js';

/**
 * Trading Engine with Advanced Strategies
 */
export class TradingEngine {
  /**
   * @param {ConfigManager} config
   * @param {Connection} connection
   * @param {MarketDataProvider} marketData
   * @param {WalletManager} walletManager
   * @param {Logger} logger
   * @param {BurnerWalletManager} [burnerWalletManager]
   */
  constructor(config, connection, marketData, walletManager, logger, burnerWalletManager = null) {
    this.config = config;
    this.connection = connection;
    this.marketData = marketData;
    this.walletManager = walletManager;
    this.burnerWalletManager = burnerWalletManager;
    this.logger = logger;
    this.trader = new SolanaTrade(this.config.rpcUrl);
    this.sentimentScore = 50;
    this.lastSentimentFetch = 0;
    this.limiter = pLimit(this.config.concurrency);
    this.cycleCount = 0;
    this.totalVolume = 0;
    this.isRunning = true;

    // [Fixed: Ensure mint is a PublicKey]
    try {
      this.mintPubkey = new PublicKey(this.config.memeCoinMint);
    } catch (e) {
      this.logger.error("Invalid Mint Address in config", { error: e.message });
    }
  }

  /**
   * Gets random delay
   * @returns {number}
   */
  getRandomDelay() {
    return getJitteredValue(this.config.baseDelayMs, this.config.jitterPct);
  }

  /**
   * Fetches market sentiment
   * @returns {Promise<number>}
   */
  async fetchSentiment() {
    if (Date.now() - this.lastSentimentFetch < SENTIMENT_FETCH_COOLDOWN_MS) return this.sentimentScore;
    try {
      const response = await axios.get('https://api.alternative.me/fng/?limit=1', { timeout: 5000 });
      this.sentimentScore = parseInt(response.data.data.value);
      this.lastSentimentFetch = Date.now();
      this.logger.info('Sentiment updated', { score: this.sentimentScore });
    } catch (error) {
      this.logger.error('Sentiment fetch failed', { error: error.message });
    }
    return this.sentimentScore;
  }

  /**
   * Gets sentiment-based bias
   * @returns {number}
   */
  getSentimentBias() {
    const { sentimentScore } = this;
    if (sentimentScore > SENTIMENT_THRESHOLDS.bullish) return 0.8;
    if (sentimentScore < SENTIMENT_THRESHOLDS.bearish) return 0.3;
    return 0.5;
  }

  /**
   * Gets profile-adjusted amount
   * @param {Object} wallet
   * @param {boolean} isBuy
   * @returns {Promise<BN>}
   */
  async getProfileAdjustedAmount(wallet, isBuy) {
    // [Fixed: Handle decimal swap amounts safely]
    let amount;
    if (isBuy) {
      // Assuming config.swapAmount is in SOL (e.g. 0.1). 
      // If it is already lamports in config, remove the multiplication.
      const amountInLamports = Math.floor(Number(this.config.swapAmount) * LAMPORTS_PER_SOL);
      amount = new BN(amountInLamports.toString());
    } else {
      // For sells, we just use a placeholder to be calculated later or derived from balance
      amount = new BN(0); // Placeholder, updated below if not buying
    }

    let maxCapMultiplier = 2;

    if (this.config.behaviorProfile === 'whale') {
      amount = amount.mul(new BN(5));
      maxCapMultiplier = 5;
    } else if (this.config.behaviorProfile === 'mixed') {
      amount = amount.mul(new BN(Math.random() < 0.3 ? 3 : 1));
      maxCapMultiplier = 3;
    }

    const personality = this.walletManager.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && isBuy) {
      amount = amount.mul(new BN(2));
      maxCapMultiplier = Math.max(maxCapMultiplier, 4);
    } else if (personality === 'momentum') {
      const sentiment = await this.fetchSentiment();
      // Ensure we don't multiply by zero. Scale amount based on sentiment, min factor of 1.
      const scaleFactor = Math.max(1, sentiment / 50);
      const scaledAmount = amount.mul(new BN(Math.floor(scaleFactor * 100))).div(new BN(100));
      // Only apply scaling if it results in a meaningful amount (at least 1 lamport)
      amount = scaledAmount.gt(new BN(0)) ? scaledAmount : amount;
      maxCapMultiplier = Math.max(maxCapMultiplier, 4);
    }

    if (!isBuy) {
      amount = await this.getCurrentMemeBalance(wallet);
    }

    if (isBuy) {
      // Re-calculate maxAmount based on safe SOL math
      const baseLamports = Math.floor(Number(this.config.swapAmount) * LAMPORTS_PER_SOL);
      const maxAmount = new BN(baseLamports.toString()).mul(new BN(maxCapMultiplier));
      amount = BN.min(amount, maxAmount);
    }

    return amount;
  }

  /**
   * Gets current meme token balance
   * @param {Object} wallet
   * @returns {Promise<BN>}
   */
  async getCurrentMemeBalance(wallet) {
    // [Fixed: Use the initialized PublicKey object instead of config string]
    const tokenAccount = getAssociatedTokenAddressSync(this.mintPubkey, wallet.keypair.publicKey);
    try {
      const accountInfo = await getAccount(this.connection, tokenAccount);
      return new BN(accountInfo.amount.toString());
    } catch {
      return new BN('0');
    }
  }

  /**
   * Gets adaptive amount with market adjustments
   * @param {BN} baseAmount
   * @returns {Promise<BN>}
   */
  async getAdaptiveAmount(baseAmount) {
    let amount = new BN(baseAmount.toString());
    const vol = await this.marketData.fetchVolatility();

    if (vol > this.config.volThreshold) amount = amount.mul(new BN(5)).div(new BN(10));
    else if (vol < 0.01) amount = amount.mul(new BN(15)).div(new BN(10));

    if (this.config.useBirdeye) {
      const marketData = await this.marketData.getMarketData();
      const liquidityRatio = marketData.liquidity / 10000;
      if (liquidityRatio < 1) amount = amount.mul(new BN(5)).div(new BN(10));
      else if (liquidityRatio > 10) amount = amount.mul(new BN(12)).div(new BN(10));
    }

    return amount;
  }

  /**
   * Gets trade actions for a wallet cycle
   * @param {Object} wallet
   * @returns {Array<boolean>}
   */
  getTradeActions(wallet) {
    const { tradeMode, numActionsPerCycle, buyProb } = this.config;
    switch (tradeMode) {
      case 'buy_only':
        return Array(numActionsPerCycle).fill(true);
      case 'sell_only':
        return Array(numActionsPerCycle).fill(false);
      case 'buy_first':
        return [true, ...Array(numActionsPerCycle - 1).fill(false)];
      case 'sell_first':
        return [false, ...Array(numActionsPerCycle - 1).fill(true)];
      case 'random':
        return Array.from({ length: numActionsPerCycle }, () => Math.random() < buyProb);
      default: // adaptive
        const buyFirst = this.isBuyFirst(wallet);
        const baseActions = buyFirst ? [true, false] : [false, true];
        return Array.from({ length: numActionsPerCycle }, (_, i) => baseActions[i % baseActions.length]);
    }
  }

  /**
   * Determines if wallet should buy first
   * @param {Object} wallet
   * @returns {boolean}
   */
  isBuyFirst(wallet) {
    const sentimentBias = this.getSentimentBias();
    const personality = this.walletManager.getPersonality(wallet.keypair.publicKey);
    let prob = this.config.buyProb * sentimentBias;
    if (personality === 'hodler') prob = Math.min(0.9, prob * 1.5);
    else if (personality === 'flipper') prob = 0.5;
    else if (personality === 'momentum') prob = Math.min(1.0, prob * 1.2);
    return Math.random() < prob;
  }

  /**
   * Performs TWAP (Time-Weighted Average Price) swap
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @param {BN} amount
   * @returns {Promise<boolean>}
   */
  async twapSwap(isBuy, wallet, amount) {
    const partAmount = amount.div(new BN(this.config.twapParts));
    const remainder = amount.mod(new BN(this.config.twapParts));
    let parts = Array(this.config.twapParts).fill(partAmount);
    if (parts.length > 0 && remainder.gt(new BN(0))) {
      parts = parts.map((part, index) => {
        return index === 0 ? part.add(remainder) : part;
      });
    }

    let anySuccess = false;
    for (let i = 0; i < parts.length; i++) {
      if (!this.isRunning) break;
      const part = await this.getAdaptiveAmount(parts[i]);
      if (part.lte(new BN('0'))) continue;

      // [Fixed: Use floating point division for logging to avoid '0.000000']
      const amountInSolDisplay = Number(part.toString()) / LAMPORTS_PER_SOL;

      this.logger.info(`${wallet.name} TWAP part ${i + 1}/${parts.length}`, {
        action: isBuy ? 'Buy' : 'Sell',
        amount: amountInSolDisplay.toFixed(6),
      });

      const success = await this.performSingleSwap(isBuy, wallet, part);
      if (success) anySuccess = true;
      if (!success) break;

      if (i < parts.length - 1) {
        const partDelay = Math.random() * this.config.twapMaxDelay;
        await delay(partDelay);
      }
    }
    return anySuccess;
  }

  /**
   * Performs a single swap
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @param {BN} amount
   * @returns {Promise<boolean>}
   */
  async performSingleSwap(isBuy, wallet, amount) {
    const maxRetries = 3;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const slippage = this.config.behaviorProfile === 'retail' ? DEFAULT_SLIPPAGE_RETAIL : DEFAULT_SLIPPAGE_WHALE;
        const jitoConfig = this.config.enableJito ? {
          sender: 'JITO',
          antimev: true,
          priorityFeeSol: Number(this.config.jitoFee) / LAMPORTS_PER_SOL,
          tipAmountSol: isBuy ? Number(this.config.jitoTipBuy) / LAMPORTS_PER_SOL : Number(this.config.jitoTipSell) / LAMPORTS_PER_SOL,
        } : {};

        // [Fixed: Convert BN Lamports to Float String for SOL trades]
        // BN.div(LAMPORTS_PER_SOL) is Integer Division, which floors <1 SOL to 0.
        const tradeAmount = isBuy
          ? (Number(amount.toString()) / LAMPORTS_PER_SOL).toFixed(9)
          : amount.toString();

        const tradeParams = {
          market: this.config.market,
          wallet: wallet.keypair,
          mint: this.config.memeCoinMint,
          amount: tradeAmount,
          slippage,
          ...jitoConfig,
        };

        const sig = isBuy ? await this.trader.buy(tradeParams) : await this.trader.sell(tradeParams);

        this.logger.info(`${wallet.name} Swap TX`, {
          tx: `https://solscan.io/tx/${sig}`,
          action: isBuy ? 'Buy' : 'Sell',
        });

        return !!sig;
      } catch (error) {
        if (attempt === maxRetries - 1) throw error;
        await delay(Math.pow(2, attempt) * 1000);
      }
    }
  }

  /**
   * Performs a swap with partial sell logic
   * @param {boolean} isBuy
   * @param {Object} wallet
   * @returns {Promise<boolean>}
   */
  async performSwap(isBuy, wallet) {
    await this.fetchSentiment();

    if (!isBuy && this.config.partialSellEnabled) {
      const currentTokens = await this.getCurrentMemeBalance(wallet);
      if (currentTokens.lt(MIN_MEME_TOKENS)) return false;

      const pct = this.config.partialSellMin + Math.random() * (this.config.partialSellMax - this.config.partialSellMin);
      const sellAmount = currentTokens.mul(new BN(Math.floor(pct * 100))).div(new BN(100));
      return this.performSingleSwap(false, wallet, sellAmount);
    }

    const baseAmount = await this.getProfileAdjustedAmount(wallet, isBuy);
    const adjustedAmount = await this.getAdaptiveAmount(baseAmount);
    const rampFactor = Math.min(1, (this.cycleCount + 1) / this.config.rampCycles);
    const finalAmount = adjustedAmount.mul(new BN(Math.floor(rampFactor * 100))).div(new BN(100));

    if (finalAmount.lte(new BN('0'))) {
      this.logger.info(`${wallet.name} Skipping: No balance`);
      return false;
    }

    // [Fixed: Use Number/Floating point division to check market conditions]
    const amountInSol = isBuy
      ? Number(finalAmount.toString()) / LAMPORTS_PER_SOL
      : 0;
    const marketCheck = await this.marketData.checkMarketConditions(amountInSol);

    if (!marketCheck.safe) {
      this.logger.warn(`${wallet.name} Trade skipped`, { reason: marketCheck.reason, sentiment: this.sentimentScore, impact: marketCheck.priceImpact });
      return false;
    }

    const personality = this.walletManager.getPersonality(wallet.keypair.publicKey);
    if (personality === 'hodler' && !isBuy && Math.random() < HODLER_SELL_CHANCE) {
      this.logger.info(`${wallet.name} Hodling: Skipping sell cycle`);
      return true;
    }

    const useTwap = isBuy ? amountInSol > 0.005 : Number(finalAmount.toString()) > 1000000;
    return useTwap ? this.twapSwap(isBuy, wallet, finalAmount) : this.performSingleSwap(isBuy, wallet, finalAmount);
  }

  /**
   * Processes a wallet cycle
   * @param {Object} wallet
   * @param {CircuitBreaker} circuitBreaker
   * @returns {Promise<Object>}
   */
  async processWalletCycle(wallet, circuitBreaker) {
    try {
      const balance = BigInt(await this.connection.getBalance(wallet.keypair.publicKey));
      if (balance < BigInt(Math.floor(0.01 * LAMPORTS_PER_SOL))) {
        this.logger.warn(`${wallet.name} low balance, skipping`);
        return { success: false, volume: 0, error: 'Insufficient SOL balance' };
      }

      const walletKey = wallet.keypair.publicKey.toBase58();
      const tradeCount = this.walletManager.walletTradeCount.get(walletKey) || 0;
      let cycleVolume = 0;
      const actions = this.getTradeActions(wallet);

      this.logger.info(`${wallet.name} Cycle actions`, {
        actions: actions.map(a => (a ? 'Buy' : 'Sell')).join(', '),
        trades: tradeCount,
      });

      for (const isBuy of actions) {
        if (!isBuy) {
          const tokens = await this.getCurrentMemeBalance(wallet);
          if (tokens.lt(MIN_MEME_TOKENS)) {
            this.logger.info(`${wallet.name} No tokens to sell, skipping`);
            continue;
          }
        }

        const solBefore = new BN((await this.connection.getBalance(wallet.keypair.publicKey)).toString());
        const success = await this.performSwap(isBuy, wallet);
        if (success) {
          const solAfter = new BN((await this.connection.getBalance(wallet.keypair.publicKey)).toString());
          const solDelta = isBuy ? solBefore.sub(solAfter) : solAfter.sub(solBefore);
          // Use precise BN division for volume calculation to avoid floating point errors
          const volumeInSOL = solDelta.div(new BN(LAMPORTS_PER_SOL.toString())).toNumber() +
            (solDelta.mod(new BN(LAMPORTS_PER_SOL.toString())).toNumber() / LAMPORTS_PER_SOL);
          cycleVolume += volumeInSOL;
        }
        circuitBreaker.recordTradeResult(success);
        await delay(this.getRandomDelay());
      }

      // Mark wallet as used (regular or burner)
      if (wallet.type === 'burner' && this.burnerWalletManager) {
        this.burnerWalletManager.markBurnerUsed(wallet.keypair.publicKey.toBase58(), actions.length);
      } else {
        this.walletManager.markWalletUsed(wallet);
      }

      this.totalVolume += cycleVolume;
      this.logger.info(`${wallet.name} completed`, {
        volume: cycleVolume.toFixed(2),
        totalVolume: this.totalVolume.toFixed(2),
        type: wallet.type || 'regular'
      });

      return { success: true, volume: cycleVolume };
    } catch (error) {
      this.logger.error(`${wallet.name} Cycle failed`, { error: error.message });
      circuitBreaker.recordTradeResult(false);
      return { success: false, volume: 0, error: error.message };
    }
  }
}