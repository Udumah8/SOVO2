import axios from 'axios';
import {
  MARKET_DATA_CACHE_DURATION_MS,
  API_TIMEOUT,
  API_RATE_LIMIT_MS,
  DEFAULT_SOL_USD_FALLBACK,
  MAX_RETRY_ATTEMPTS,
  INITIAL_RETRY_DELAY_MS,
} from '../constants.js';

/**
 * Robust Market Data Provider with:
 * - multi-tier fallback (Birdeye, DexScreener, CoinGecko)
 * - per-source parsing & resilient error handling
 * - exponential backoff retry for transient errors (incl. 429)
 * - per-source caching + global cache
 * - chain autodiscovery for DexScreener
 * - volatility from available price history (DEX / CoinGecko) or internal price feed
 * - clear logging and deterministic fallbacks
 */
export class MarketDataProvider {
  constructor(config, connection, logger) {
    this.config = config;
    this.connection = connection;
    this.logger = logger;

    // Global cache returned by getMarketData()
    this.cache = {
      price: 0,
      priceChange24h: 0,
      volume24h: 0,
      liquidity: 0,
      lastUpdate: 0,
      source: 'none',
      priceHistory: [], // [{ts, price}] // optional
    };

    // Per-source short-term caches to reduce calls
    this.sourceCache = {
      Birdeye: { ts: 0, data: null },
      DexScreener: { ts: 0, data: null },
      CoinGecko: { ts: 0, data: null },
    };

    this.cacheDuration = MARKET_DATA_CACHE_DURATION_MS || 30_000; // default 30s
    this.lastApiCall = 0;
    this.rateLimitMs = API_RATE_LIMIT_MS || 250;
    this.solFallback = DEFAULT_SOL_USD_FALLBACK || 150;
  }

  // --- Public API -----------------------------------------------------

  async getMarketData({ forceRefresh = false } = {}) {
    const now = Date.now();
    if (!forceRefresh && now - this.cache.lastUpdate < this.cacheDuration) {
      return this.cache;
    }

    await this._globalRateLimit();

    // Try sources in priority order but skip ones disabled in config
    const sources = [
      { name: 'Birdeye', fn: this._fetchFromBirdeye.bind(this) },
      { name: 'DexScreener', fn: this._fetchFromDexScreener.bind(this) },
      { name: 'CoinGecko', fn: this._fetchFromCoinGecko.bind(this) },
    ];

    for (const src of sources) {
      if (!this._isSourceEnabled(src.name)) continue;
      try {
        const data = await src.fn();
        if (data && this._isValidMarketData(data)) {
          // Merge and stamp
          this.cache = {
            ...this.cache,
            ...data,
            lastUpdate: Date.now(),
            source: src.name,
          };
          // keep priceHistory if provided else preserve
          if (data.priceHistory && Array.isArray(data.priceHistory)) {
            this.cache.priceHistory = data.priceHistory;
          }

          this.logger.info(`Market data fetched from ${src.name}`, {
            price: this.cache.price,
            liquidity: this.cache.liquidity,
            source: src.name,
          });

          return this.cache;
        }
      } catch (err) {
        // log and continue to next source
        this.logger.warn(`${src.name} fetch failed`, { error: err?.message || err });
      }
    }

    this.logger.error('All market data sources failed; returning stale cache');
    // still update timestamp so callers know this is stale by checking age
    this.cache.lastUpdate = this.cache.lastUpdate || Date.now();
    return this.cache;
  }

  async estimatePriceImpact(amountInSol) {
    await this.getMarketData();
    const solPriceUsd = await this.getSolPriceUsd();
    const amountInUsd = amountInSol * solPriceUsd;
    const { liquidity } = this.cache;
    if (!liquidity || liquidity <= 0) return 100; // no liquidity means 100% impact

    // Prevent division by extremely small liquidity
    if (liquidity < 1_000) return 100;

    return Math.min((amountInUsd / liquidity) * 100, 100);
  }

  async checkMarketConditions(amountInSol) {
    await this.getMarketData();

    if (this.cache.liquidity < (this.config.minLiquidity || 0)) {
      return {
        safe: false,
        reason: `Low liquidity: $${Number(this.cache.liquidity).toFixed(0)} < $${this.config.minLiquidity} (Source: ${this.cache.source})`,
      };
    }

    const priceImpact = await this.estimatePriceImpact(amountInSol);
    if (priceImpact > (this.config.maxPriceImpact || 5)) {
      return {
        safe: false,
        reason: `High price impact: ${priceImpact.toFixed(2)}% > ${this.config.maxPriceImpact}%`,
        priceImpact,
      };
    }

    return { safe: true, reason: '', priceImpact };
  }

  /**
   * Fetch volatility across last N days. Prefer price history from DEX, then CoinGecko, then internal history.
   * Returns standard deviation of returns (daily) or 0 if insufficient data.
   */
  async fetchVolatility({ days = 7 } = {}) {
    // Try to fetch price history from best available source
    let prices = null;

    // 1) Use DEX price history if available in source cache
    const dexData = this.sourceCache.DexScreener.data;
    if (dexData?.priceHistory && dexData.priceHistory.length >= 2) {
      prices = dexData.priceHistory.map(p => p.price);
    }

    // 2) Fallback: CoinGecko market_chart endpoint
    if (!prices) {
      try {
        const cgPrices = await this._fetchCoinGeckoMarketChart(days);
        if (cgPrices && cgPrices.length >= 2) prices = cgPrices;
      } catch (err) {
        this.logger.debug('CoinGecko market chart unavailable for volatility fallback', { err: err?.message });
      }
    }

    // 3) Fallback: use internal cached history if present
    if (!prices && this.cache.priceHistory && this.cache.priceHistory.length >= 2) {
      prices = this.cache.priceHistory.map(p => p.price);
    }

    if (!prices || prices.length < 2) return 0;

    // compute daily simple returns from consecutive samples
    const returns = prices.slice(1).map((p, i) => (p - prices[i]) / prices[i]);
    if (returns.length < 2) return 0;

    const mean = returns.reduce((s, x) => s + x, 0) / returns.length;
    const variance = returns.reduce((s, x) => s + Math.pow(x - mean, 2), 0) / (returns.length - 1);
    return Math.sqrt(variance);
  }

  async getSolPriceUsd() {
    try {
      const resp = await this._fetchWithRetry(
        `https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd`,
        { timeout: API_TIMEOUT }
      );
      if (resp?.data?.solana?.usd) return resp.data.solana.usd;
    } catch (err) {
      this.logger.warn('Error fetching SOL price', { err: err?.message });
    }
    return this.solFallback;
  }

  // --- Internal helpers -----------------------------------------------

  _isSourceEnabled(name) {
    if (name === 'Birdeye') return !!this.config.useBirdeye && !!this.config.birdeyeApiKey;
    if (name === 'DexScreener') return !!this.config.memeCoinPairAddress;
    if (name === 'CoinGecko') return !!this.config.memeCoinSymbol;
    return false;
  }

  _isValidMarketData(d) {
    // price should be finite and non-negative
    return Number.isFinite(d.price) && d.price >= 0;
  }

  async _globalRateLimit() {
    const now = Date.now();
    const since = now - this.lastApiCall;
    if (since < this.rateLimitMs) {
      await new Promise(resolve => setTimeout(resolve, this.rateLimitMs - since));
    }
    this.lastApiCall = Date.now();
  }

  async _fetchWithRetry(url, opts = {}, attempts = MAX_RETRY_ATTEMPTS || 3) {
    let attempt = 0;
    let delay = INITIAL_RETRY_DELAY_MS || 300;
    while (attempt < attempts) {
      try {
        return await axios.get(url, { timeout: API_TIMEOUT, ...opts });
      } catch (err) {
        attempt += 1;
        const status = err?.response?.status;
        // If client error other than 429, don't retry
        if (status && status >= 400 && status < 500 && status !== 429) throw err;

        // 429 (rate limit) -> exponentially backoff
        this.logger.debug('Request failed; retrying', { url, attempt, status, message: err?.message });
        await new Promise(resolve => setTimeout(resolve, delay));
        delay *= 2;
      }
    }
    // last attempt
    return axios.get(url, { timeout: API_TIMEOUT, ...opts });
  }

  // --- Source implementations ----------------------------------------

  async _fetchFromBirdeye() {
    if (!this._isSourceEnabled('Birdeye')) return null;

    // per-source cache (short-lived)
    const cached = this.sourceCache.Birdeye;
    if (cached.ts && Date.now() - cached.ts < this.cacheDuration) return cached.data;

    const url = `https://public-api.birdeye.so/defi/price?address=${this.config.memeCoinMint.toBase58()}&include_liquidity=true&include_volume=true`;
    try {
      const resp = await this._fetchWithRetry(url, { headers: { 'X-API-KEY': this.config.birdeyeApiKey } });
      const body = resp.data;
      if (body?.success && body.data) {
        const { value, priceChange24h, volumeH24, liquidity } = body.data;
        const out = {
          price: Number(value) || 0,
          priceChange24h: Number(priceChange24h) || 0,
          volume24h: Number(volumeH24) || 0,
          liquidity: Number(liquidity) || 0,
          source: 'Birdeye',
          // Birdeye sometimes provides time-series; if present, normalize it
          priceHistory: (body.data.history || []).map(h => ({ ts: h[0], price: Number(h[1]) })).filter(x => Number.isFinite(x.price)),
        };

        this.sourceCache.Birdeye = { ts: Date.now(), data: out };
        return out;
      }
    } catch (err) {
      const status = err?.response?.status;
      if (status === 429) this.logger.warn('Birdeye rate limit hit');
      this.logger.warn('Birdeye fetch error', { err: err?.message });
    }
    return null;
  }

  async _fetchFromDexScreener() {
    if (!this._isSourceEnabled('DexScreener')) return null;

    const cached = this.sourceCache.DexScreener;
    if (cached.ts && Date.now() - cached.ts < this.cacheDuration) return cached.data;

    // Support multiple chains via config.chainId or autodetect default to solana
    const chain = (this.config.chainId || 'solana').toLowerCase();
    const pair = this.config.memeCoinPairAddress;
    const url = `https://api.dexscreener.com/latest/dex/pairs/${encodeURIComponent(chain)}/${encodeURIComponent(pair)}`;

    try {
      const resp = await this._fetchWithRetry(url, {});
      const body = resp.data;
      // DexScreener shape changes; normalize defensively
      const pairData = body?.pair || (Array.isArray(body?.pairs) ? body.pairs[0] : null);
      if (!pairData) return null;

      // Extract values carefully
      const priceUsd = Number(pairData.priceUsd || pairData.price || pairData.price_usd);
      const priceChange24h = Number((pairData.priceChange && pairData.priceChange.h24) || pairData.priceChange24h || pairData.price_change_24h) || 0;

      // volume & liquidity may be nested
      const volume24h = Number((pairData.volume && pairData.volume.h24) || pairData.volumeUsd24h || pairData.volume24h) || 0;
      const liquidityUsd = Number((pairData.liquidity && pairData.liquidity.usd) || pairData.liquidityUsd || pairData.liquidity_usd) || 0;

      // priceChangeChart or price chart may be available for history
      const priceHistory = (pairData.priceChangeChart || pairData.price_change_chart || pairData.priceHistory || []).map((v, idx) => {
        // DexScreener often gives array of numbers [price,...] or [ts, price]
        if (Array.isArray(v)) {
          if (v.length >= 2) return { ts: Date.now() - (priceHistory?.length - idx) * 60000, price: Number(v[1]) };
          return { ts: Date.now() - idx * 60000, price: Number(v[0]) };
        }
        return { ts: Date.now() - idx * 60000, price: Number(v) };
      }).filter(x => Number.isFinite(x.price));

      const out = {
        price: Number(priceUsd) || 0,
        priceChange24h: priceChange24h || 0,
        volume24h: volume24h || 0,
        liquidity: liquidityUsd || 0,
        source: 'DexScreener',
        priceHistory: priceHistory,
      };

      this.sourceCache.DexScreener = { ts: Date.now(), data: out };
      return out;
    } catch (err) {
      const status = err?.response?.status;
      if (status === 429) this.logger.warn('DexScreener rate limit hit');
      this.logger.warn('DexScreener fetch failed', { err: err?.message });
      return null;
    }
  }

  async _fetchFromCoinGecko() {
    if (!this._isSourceEnabled('CoinGecko')) return null;

    const cached = this.sourceCache.CoinGecko;
    if (cached.ts && Date.now() - cached.ts < this.cacheDuration) return cached.data;

    const id = encodeURIComponent(this.config.memeCoinSymbol);
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${id}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true`;

    try {
      const resp = await this._fetchWithRetry(url, {});
      const body = resp.data;
      if (!body || !body[this.config.memeCoinSymbol]) return null;
      const d = body[this.config.memeCoinSymbol];
      const out = {
        price: Number(d.usd) || 0,
        priceChange24h: Number(d.usd_24h_change) || 0,
        volume24h: Number(d.usd_24h_vol) || 0,
        liquidity: this.cache.liquidity || 0, // CoinGecko simple price doesn't provide liquidity
        source: 'CoinGecko',
      };

      this.sourceCache.CoinGecko = { ts: Date.now(), data: out };
      return out;
    } catch (err) {
      this.logger.warn('CoinGecko fetch failed', { err: err?.message });
      return null;
    }
  }

  async _fetchCoinGeckoMarketChart(days = 7) {
    if (!this.config.memeCoinSymbol) return null;
    const id = encodeURIComponent(this.config.memeCoinSymbol);
    const url = `https://api.coingecko.com/api/v3/coins/${id}/market_chart?vs_currency=usd&days=${days}`;
    try {
      const resp = await this._fetchWithRetry(url, {});
      const raw = resp.data?.prices;
      if (!raw || raw.length < 2) return null;
      // raw is [[ts, price], ...]
      return raw.map(p => Number(p[1])).filter(x => Number.isFinite(x));
    } catch (err) {
      throw err;
    }
  }
}