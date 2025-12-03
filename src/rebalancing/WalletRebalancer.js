import { setTimeout as delay } from 'timers/promises';
import { SystemProgram, Transaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
import BN from 'bn.js';
import { BotModule } from '../bot/BotModule.js';

/**
 * Wallet Rebalancing System
 */
export class WalletRebalancer extends BotModule {
  /**
   * @param {ConfigManager} config
   * @param {WalletManager} walletManager
   * @param {Connection} connection
   * @param {Logger} logger
   */
  constructor(config, walletManager, connection, logger) {
    super(config, walletManager, connection, logger);
    // Convert SOL config values to lamports BN for comparisons
    this.minWalletBalanceLamports = new BN(Math.floor(this.config.minWalletBalance * LAMPORTS_PER_SOL).toString());
    this.targetWalletBalanceLamports = new BN(Math.floor(this.config.targetWalletBalance * LAMPORTS_PER_SOL).toString());
    this.dustThresholdLamports = new BN(Math.floor(this.config.dustThreshold * LAMPORTS_PER_SOL).toString());
  }

  /**
   * Rebalances wallets for stealth
   * @returns {Promise<void>}
   */
  async rebalanceWallets() {
    if (!this.config.enableRebalancing || this.walletManager.relayerKeypairs.length === 0) {
      this.logger.info('Rebalancing skipped: disabled or no relayer wallets');
      return;
    }

    this.logger.info('Starting P2P Stealth Rebalancing');

    const walletsToCheck = this.walletManager.activeWallets;
    if (walletsToCheck.length < 2) {
      this.logger.info('Not enough active wallets for rebalancing');
      return;
    }

    const walletBalances = [];
    let totalBalance = new BN('0');

    for (const wallet of walletsToCheck) {
      const balance = new BN(BigInt(await this.connection.getBalance(wallet.keypair.publicKey)).toString());
      walletBalances.push({ wallet, balance });
      totalBalance = totalBalance.add(balance);
    }

    const avgBalance = totalBalance.div(new BN(walletsToCheck.length.toString()));
    this.logger.info('Average active wallet balance', { balance: (avgBalance.toNumber() / LAMPORTS_PER_SOL).toFixed(4) });

    let needsFunding = walletBalances.filter(wb => wb.balance.lt(this.minWalletBalanceLamports)).sort((a, b) => a.balance.cmp(b.balance));
    let hasSurplus = walletBalances.filter(wb => wb.balance.gt(this.targetWalletBalanceLamports.mul(new BN('3')).div(new BN('2')))).sort((a, b) => b.balance.cmp(a.balance));

    this.logger.info('Rebalancing stats', {
      needsFunding: needsFunding.length,
      hasSurplus: hasSurplus.length
    });

    // P2P Internal Funding
    if (needsFunding.length > 0 && hasSurplus.length > 0) {
      this.logger.info('Attempting P2P internal funding');
      let surplusIndex = 0;

      for (const needy of needsFunding) {
        if (surplusIndex >= hasSurplus.length) break;

        const neededAmount = this.targetWalletBalanceLamports.sub(needy.balance);
        const surplusProvider = hasSurplus[surplusIndex];

        if (surplusProvider.wallet.pubkey === needy.wallet.pubkey) continue;

        const availableSurplus = surplusProvider.balance.sub(this.targetWalletBalanceLamports);

        if (availableSurplus.gt(neededAmount)) {
          try {
            const tx = new Transaction().add(SystemProgram.transfer({
              fromPubkey: surplusProvider.wallet.keypair.publicKey,
              toPubkey: needy.wallet.keypair.publicKey,
              lamports: BigInt(neededAmount.toString()),
            }));
            const sig = await this.connection.sendTransaction(tx, [surplusProvider.wallet.keypair], { skipPreflight: true });
            await this.connection.confirmTransaction(sig, 'confirmed');

            this.logger.info('P2P Transfer', {
              from: surplusProvider.wallet.name,
              to: needy.wallet.name,
              amount: (neededAmount.toNumber() / LAMPORTS_PER_SOL).toFixed(4)
            });

            surplusProvider.balance = surplusProvider.balance.sub(neededAmount);
            needy.balance = needy.balance.add(neededAmount);
            await delay(1000 + Math.random() * 2000);

          } catch (error) {
            this.logger.error('P2P transfer failed', { error: error.message });
          }
        }

        if (surplusProvider.balance.sub(this.targetWalletBalanceLamports).lt(this.dustThresholdLamports)) {
          surplusIndex++;
        }
      }
    }

    // Consolidate surplus to sink
    const remainingSurplus = hasSurplus.filter(wb => wb.balance.gt(this.targetWalletBalanceLamports.mul(new BN('11')).div(new BN('10'))));
    if (remainingSurplus.length > 0 && this.walletManager.sinkKeypair) {
      await this.consolidateSurplusToSink(remainingSurplus);
    }

    this.logger.info('Rebalancing complete');
  }

  /**
   * Consolidates surplus to sink wallet
   * @param {Array} surplusWallets
   */
  async consolidateSurplusToSink(surplusWallets) {
    this.logger.info('Consolidating surplus to sink wallet');

    for (const { wallet, balance } of surplusWallets) {
      const surplus = balance.sub(this.targetWalletBalanceLamports);

      if (surplus.gt(this.dustThresholdLamports)) {
        try {
          const randomPct = 0.90 + Math.random() * 0.08;
          const transferAmount = surplus.mul(new BN(Math.floor(randomPct * 100))).div(new BN(100));

          if (transferAmount.lt(this.dustThresholdLamports)) continue;

          const tx = new Transaction().add(SystemProgram.transfer({
            fromPubkey: wallet.keypair.publicKey,
            toPubkey: this.walletManager.sinkKeypair.publicKey,
            lamports: BigInt(transferAmount.toString()),
          }));

          const sig = await this.connection.sendTransaction(tx, [wallet.keypair], {
            skipPreflight: true,
            maxRetries: 2
          });
          await this.connection.confirmTransaction(sig, 'confirmed');

          this.logger.info('Consolidated to sink', {
            wallet: wallet.name,
            amount: (transferAmount.toNumber() / LAMPORTS_PER_SOL).toFixed(4)
          });

          await delay(500 + Math.random() * 1500);

        } catch (error) {
          this.logger.error('Consolidation to sink failed', {
            wallet: wallet.name,
            error: error.message
          });
        }
      }
    }
  }
}