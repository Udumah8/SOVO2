import { Keypair } from '@solana/web3.js';
import jsonfile from 'jsonfile';

/**
 * Manages loading and parsing of wallet data from the wallet file.
 */
export class WalletDataLoader {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.cachedWalletData = null;
  }

  /**
   * Loads wallet data from the configured wallet file.
   * Caches the data after the first read.
   * @returns {Array<Object>} An array of wallet objects.
   */
  async loadWallets() {
    if (this.cachedWalletData) {
      return this.cachedWalletData;
    }

    try {
      const rawWallets = await jsonfile.readFile(this.config.walletFile);
      this.cachedWalletData = rawWallets;
      this.logger.info('Wallet data loaded and cached', { file: this.config.walletFile, count: this.cachedWalletData.length });
      return this.cachedWalletData;
    } catch (error) {
      this.logger.error('Failed to load wallet data', { file: this.config.walletFile, error: error.message });
      throw new Error(`Failed to load wallet data from ${this.config.walletFile}: ${error.message}`);
    }
  }

  /**
   * Writes wallet data to the configured wallet file.
   * @param {Array<Object>} wallets - The array of wallet objects to write.
   */

  async writeWallets(wallets) {
    try {
      // Map back to raw format for saving (privateKey as array)
      const rawWallets = wallets.map(w => {
        // Handle both wallet objects with keypair and raw wallet data objects
        if (w.keypair && w.keypair.secretKey) {
          return {
            privateKey: Array.from(w.keypair.secretKey),
            pubkey: w.keypair.publicKey.toBase58(),
            name: w.name
          };
        } else {
          // Handle raw wallet data objects (from loadWallets)
          return w;
        }
      });
      await jsonfile.writeFile(this.config.walletFile, rawWallets, { spaces: 2 });
      this.logger.info('Wallet data saved to file', { file: this.config.walletFile, count: rawWallets.length });
      this.cachedWalletData = rawWallets; // Update cache with the raw written data
    } catch (error) {
      this.logger.error('Failed to write wallet data', { file: this.config.walletFile, error: error.message });
      throw new Error(`Failed to write wallet data to ${this.config.walletFile}: ${error.message}`);
    }
  }
  /**
   * Clears the cached wallet data.
   */
  clearCache() {
    this.cachedWalletData = null;
    this.logger.debug('Wallet data cache cleared');
  }
}