import { ConfigManager } from '../config/ConfigManager.js';
import { Logger } from './Logger.js';

/**
 * Manages sending alerts for critical events.
 * This is a placeholder and would integrate with actual alerting services (e.g., PagerDuty, Slack, email).
 */
export class AlertManager {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    // In a real application, you would initialize connections to alerting services here
    if (this.config.enableAlerting) {
      this.logger.info('Alerting is enabled. Critical errors will trigger alerts.');
    } else {
      this.logger.info('Alerting is disabled.');
    }
  }

  /**
   * Sends a critical alert.
   * @param {string} title - The title of the alert.
   * @param {string} message - The detailed message of the alert.
   * @param {Object} [details] - Additional details to include in the alert.
   */
  sendCriticalAlert(title, message, details) {
    if (!this.config.enableAlerting) {
      this.logger.debug('Alerting is disabled, skipping critical alert.', { title, message });
      return;
    }
    this.logger.critical(`ALERT: ${title} - ${message}`, details);
    // Placeholder for actual alerting mechanism (e.g., send to PagerDuty, Slack, email)
    console.error(`--- CRITICAL ALERT ---`);
    console.error(`Title: ${title}`);
    console.error(`Message: ${message}`);
    if (details) {
      console.error('Details:', JSON.stringify(details, null, 2));
    }
    console.error(`----------------------`);
  }

  /**
   * Sends an error alert (less severe than critical, but still important).
   * @param {string} title - The title of the alert.
   * @param {string} message - The detailed message of the alert.
   * @param {Object} [details] - Additional details to include in the alert.
   */
  sendErrorAlert(title, message, details) {
    if (!this.config.enableAlerting) {
      this.logger.debug('Alerting is disabled, skipping error alert.', { title, message });
      return;
    }
    this.logger.error(`ERROR ALERT: ${title} - ${message}`, details);
    // Placeholder for actual alerting mechanism
    console.error(`--- ERROR ALERT ---`);
    console.error(`Title: ${title}`);
    console.error(`Message: ${message}`);
    if (details) {
      console.error('Details:', JSON.stringify(details, null, 2));
    }
    console.error(`-------------------`);
  }
}