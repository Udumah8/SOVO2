import { SINK_BALANCE_CHECK_INTERVAL } from '../constants.js';

/**
 * Circuit Breaker System
 */
export class CircuitBreaker {
  /**
   * @param {ConfigManager} config
   * @param {Logger} logger
   */
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.consecutiveFailures = 0;
    this.recentTrades = [];
    this.initialSinkBalance = 0n;
    this.checkCounter = 0;
  }

  /**
   * Records a trade result
   * @param {boolean} success
   */
  recordTradeResult(success) {
    if (!this.config.enableCircuitBreaker) return;

    this.recentTrades.push(success);
    if (this.recentTrades.length > this.config.failureRateWindow) {
      this.recentTrades.shift();
    }

    if (success) {
      this.consecutiveFailures = 0;
    } else {
      this.consecutiveFailures++;
    }
  }

  /**
   * Checks if circuit breaker should trip
   * @param {Connection} connection
   * @param {Keypair|null} sinkKeypair
   * @returns {Promise<{tripped: boolean, reason: string}>}
   */
  async checkCircuitBreakers(connection, sinkKeypair) {
    if (!this.config.enableCircuitBreaker) return { tripped: false, reason: '' };

    const checks = [
      this.checkConsecutiveFailures.bind(this),
      this.checkFailureRate.bind(this),
      () => this.checkEmergencyStopLoss(connection, sinkKeypair),
    ];

    for (const check of checks) {
      const result = await check();
      if (result.tripped) {
        return result;
      }
    }

    return { tripped: false, reason: '' };
  }

  /**
   * Checks for consecutive failures
   * @returns {{tripped: boolean, reason: string}}
   */
  checkConsecutiveFailures() {
    if (this.consecutiveFailures >= this.config.maxConsecutiveFailures) {
      return {
        tripped: true,
        reason: `Circuit Breaker: ${this.consecutiveFailures} consecutive failures`,
      };
    }
    return { tripped: false, reason: '' };
  }

  /**
   * Checks the failure rate
   * @returns {{tripped: boolean, reason: string}}
   */
  checkFailureRate() {
    if (this.recentTrades.length >= this.config.failureRateWindow) {
      const failures = this.recentTrades.filter(r => !r).length;
      const failureRate = failures / this.recentTrades.length;

      if (failureRate > this.config.maxFailureRate) {
        return {
          tripped: true,
          reason: `Circuit Breaker: ${(failureRate * 100).toFixed(1)}% failure rate (last ${this.config.failureRateWindow} trades)`,
        };
      }
    }
    return { tripped: false, reason: '' };
  }

  /**
   * Checks for emergency stop loss
   * @param {Connection} connection
   * @param {Keypair|null} sinkKeypair
   * @returns {Promise<{tripped: boolean, reason: string}>}
   */
  async checkEmergencyStopLoss(connection, sinkKeypair) {
    this.checkCounter++;
    if (sinkKeypair && this.initialSinkBalance > 0n && this.checkCounter % SINK_BALANCE_CHECK_INTERVAL === 0) {
      const currentBalance = BigInt(await connection.getBalance(sinkKeypair.publicKey));
      // Calculate loss as a decimal (0-1 range) to match config.emergencyStopLoss
      const lossDecimal = Number(this.initialSinkBalance - currentBalance) / Number(this.initialSinkBalance);

      if (lossDecimal > this.config.emergencyStopLoss) {
        return {
          tripped: true,
          reason: `Circuit Breaker: ${(lossDecimal * 100).toFixed(1)}% loss from initial balance`,
        };
      }
    }
    return { tripped: false, reason: '' };
  }
}