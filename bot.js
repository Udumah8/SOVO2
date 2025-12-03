import { VolumeBoosterBot } from './src/VolumeBoosterBot.js';

// Initialize bot first
let bot = null;

// Graceful shutdown function
async function gracefulShutdown(exitCode = 0) {
  console.log('Shutting down...');
  // Safe check for bot existence and properties
  if (bot && bot.config && bot.config.enableKeyboard && process.stdin.isTTY) {
    process.stdin.setRawMode(false);
  }
  if (bot && typeof bot.stop === 'function') {
    try {
      // Ensure bot.stop() is awaited and its potential errors are caught
      // If bot.stop() itself has unhandled rejections, they should be handled internally by VolumeBoosterBot.
      // Here, we ensure the shutdown process doesn't hang if stop() fails.
      await Promise.race([
        bot.stop(),
        new Promise(resolve => setTimeout(resolve, 5000)) // Timeout after 5 seconds
      ]);
    } catch (stopErr) {
      console.error('Error during bot stop:', stopErr);
      exitCode = 1;
    }
  }
  process.exit(exitCode);
}

// Set up global error handlers after bot initialization
function setupErrorHandlers() {
  process.on('uncaughtException', async (err) => {
    console.error('UNCAUGHT EXCEPTION:', err);
    // Ensure graceful shutdown completes before process exits
    await gracefulShutdown(1);
  });

  process.on('unhandledRejection', async (reason, promise) => {
    console.error('UNHANDLED REJECTION:', reason);
    // Ensure graceful shutdown completes before process exits
    await gracefulShutdown(1);
  });

  // Keyboard triggers
  process.on('SIGINT', () => gracefulShutdown(0));
  process.on('SIGTERM', () => gracefulShutdown(0));
}

(async () => {
  try {
    // Initialize the bot first
    bot = new VolumeBoosterBot();
    
    // Set up error handlers after bot initialization
    setupErrorHandlers();
    
    await bot.init(); // Await bot initialization
    // Ensure bot.config is available before accessing it
    if (bot.config && bot.config.enableKeyboard) {
      if (process.stdin.isTTY) {
        try {
          process.stdin.setRawMode(true);
          process.stdin.resume();
          process.stdin.on('data', async key => {
            const keyStr = key.toString().toLowerCase();
            if (keyStr.includes('q') || keyStr.charCodeAt(0) === 3) { // q or Ctrl+C
              console.log('\nGraceful shutdown triggered...');
              await gracefulShutdown(0); // Await for proper cleanup
            } else if (keyStr.includes('w')) {
              console.log('\nInitiating secure asset transfer...'); // More stealthy log
              if (!bot.isWithdrawing) { // Prevent multiple concurrent withdrawals
                bot.isWithdrawing = true;
                try {
                  await bot.withdrawAllFunds(); // Call the dedicated withdrawal function
                } catch (withdrawErr) {
                  console.error('Error during manual withdrawal:', withdrawErr);
                  // Optionally, decide if you want to exit or just log and continue
                } finally {
                  bot.isWithdrawing = false;
                }
              } else {
                console.log('Withdrawal already in progress. Please wait.');
              }
            }
          });
        } catch (rawModeErr) {
          console.error('Error setting raw mode for stdin:', rawModeErr);
          console.log('Keyboard triggers disabled due to raw mode error.');
        }
      } else {
        console.log('Not running in TTY mode, keyboard triggers disabled.');
      }
    }
  } catch (err) {
    console.error('Bot initialization failed:', err);
    await gracefulShutdown(1); // Use graceful shutdown for consistency
  }
})();
