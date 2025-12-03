import winston from 'winston';

/**
 * Application Logger
 */
export class Logger {
  constructor(config = null) {
    // Use provided config or create a basic one for standalone use
    this.config = config;
    
    // Configure Winston logger
    this.logger = winston.createLogger({
      level: 'debug', // Set to 'debug' to capture all levels, actual output controlled by transports
      format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.json()
      ),
      transports: [
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
          )
        }),
        new winston.transports.File({ filename: 'bot.log' })
      ]
    });

    // Add a custom transport for critical error alerting if enabled
    if (this.config?.enableAlerting) {
      this.logger.add(new winston.transports.Console({
        level: 'critical', // Custom level for critical errors
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.printf(info => `[CRITICAL ALERT] ${info.timestamp} ${info.level}: ${info.message} ${info.meta ? JSON.stringify(info.meta) : ''}`)
        )
      }));
    }
  }

  /**
   * Logs an info message
   * @param {string} message 
   * @param {Object} [meta] 
   */
  info(message, meta) {
    this.logger.info(message, meta);
  }

  /**
   * Logs a warning message
   * @param {string} message 
   * @param {Object} [meta] 
   */
  warn(message, meta) {
    this.logger.warn(message, meta);
  }

  /**
   * Logs an error message
   * @param {string} message 
   * @param {Object} [meta]
   */
  error(message, meta) {
    this.logger.error(message, meta);
  }

  /**
   * Logs a critical message (for alerting)
   * @param {string} message
   * @param {Object} [meta]
   */
  critical(message, meta) {
    this.logger.log('critical', message, meta);
  }

  /**
   * Logs a debug message
   * @param {string} message
   * @param {Object} [meta]
   */
  debug(message, meta) {
    this.logger.debug(message, meta);
  }
}