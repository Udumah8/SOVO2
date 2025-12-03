import { SolanaTrade } from 'solana-trade';

/**
 * Base class for bot modules.
 */
export class BotModule {
  /**
   * @param {ConfigManager} config
   * @param {WalletManager} walletManager
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, walletManager, connection, logger) {
    this.config = config;
    this.walletManager = walletManager;
    this.connection = connection;
    this.logger = logger;
    this.trader = new SolanaTrade(this.config.rpcUrl);
  }
}