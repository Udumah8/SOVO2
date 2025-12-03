import jsonfile from 'jsonfile';
import { Keypair, Transaction, LAMPORTS_PER_SOL, SystemProgram } from '@solana/web3.js';
import { setTimeout as delay } from 'timers/promises';
import { getAssociatedTokenAddressSync, getAccount } from '@solana/spl-token';
import BN from 'bn.js';
import { USDC_MINT, BURN_ADDRESS } from '../constants.js';
import { BotModule } from '../bot/BotModule.js';

/**
 * Wallet Seasoning System
 */
export class WalletSeasoner extends BotModule {
    /**
     * @param {ConfigManager} config
     * @param {WalletManager} walletManager
     * @param {Connection} connection
     * @param {Logger} logger
     */
    constructor(config, walletManager, connection, logger) {
        super(config, walletManager, connection, logger);
    }

    /**
     * Seasons wallets for stealth
     * @returns {Promise<void>}
     */
    async seasonWallets() {
        if (!this.config.enableSeasoning) {
            this.logger.info('Wallet seasoning disabled');
            return;
        }

        this.logger.info('Starting Wallet Seasoning');

        const walletData = await jsonfile.readFile(this.config.walletFile);
        const unseasonedWallets = walletData.filter(w => !w.isSeasoned);

        if (unseasonedWallets.length === 0) {
            this.logger.info('All wallets already seasoned');
            this.logger.info('Seasoning complete');
            return;
        }

        this.logger.info(`Processing ${unseasonedWallets.length} unseasoned wallets`);

        for (let i = 0; i < unseasonedWallets.length; i++) {
            const walletObj = unseasonedWallets[i];
            const wallet = {
                keypair: Keypair.fromSecretKey(new Uint8Array(walletObj.privateKey)),
                name: walletObj.name,
                pubkey: walletObj.pubkey
            };

            try {
                const balance = BigInt(await this.connection.getBalance(wallet.keypair.publicKey));
                if (balance < BigInt(Math.floor(0.005 * Number(LAMPORTS_PER_SOL)))) {
                    this.logger.info(`Skipping ${wallet.name}: insufficient SOL`);
                    continue;
                }

                const numTxs = this.config.seasoningMinTxs + Math.floor(Math.random() * (this.config.seasoningMaxTxs - this.config.seasoningMinTxs + 1));
                this.logger.info(`Seasoning ${wallet.name} with ${numTxs} transactions`);

                for (let j = 0; j < numTxs; j++) {
                    const actionType = Math.random() < 0.7 ? 'swap' : 'burn';

                    if (actionType === 'swap') {
                        const isSolToUsdc = Math.random() < 0.5;
                        const amount = (0.0001 + Math.random() * 0.0004) * Number(LAMPORTS_PER_SOL);

                        try {
                            if (isSolToUsdc) {
                                await this.trader.buy({
                                    market: 'RAYDIUM_AMM',
                                    wallet: wallet.keypair,
                                    mint: USDC_MINT,
                                    amount: (amount / Number(LAMPORTS_PER_SOL)).toString(),
                                    slippage: 5
                                });
                                this.logger.info(`Seasoning swap: SOL to USDC`, { amount: (amount / Number(LAMPORTS_PER_SOL)).toFixed(5) });
                            } else {
                                // USDC_MINT is already a PublicKey from constants
                                const usdcBalance = await this.getTokenBalance(wallet.keypair.publicKey, USDC_MINT);
                                if (usdcBalance.gt(new BN(0))) {
                                    await this.trader.sell({
                                        market: 'RAYDIUM_AMM',
                                        wallet: wallet.keypair,
                                        mint: USDC_MINT,
                                        amount: usdcBalance.toString(),
                                        slippage: 5
                                    });
                                    this.logger.info(`Seasoning swap: USDC to SOL`);
                                } else {
                                    this.logger.info(`Skipping USDC->SOL: no balance`);
                                }
                            }
                        } catch (error) {
                            this.logger.warn(`Seasoning swap failed`, { wallet: wallet.name, error: error.message.slice(0, 50) });
                        }
                    } else { // burn
                        try {
                            const burnAmount = Math.floor((0.00001 + Math.random() * 0.00004) * Number(LAMPORTS_PER_SOL));
                            const tx = new Transaction().add(SystemProgram.transfer({
                                fromPubkey: wallet.keypair.publicKey,
                                toPubkey: BURN_ADDRESS,
                                lamports: BigInt(burnAmount),
                            }));
                            await this.connection.sendTransaction(tx, [wallet.keypair], { skipPreflight: true });
                            this.logger.info(`Seasoning burn`, { amount: (burnAmount / Number(LAMPORTS_PER_SOL)).toFixed(6) });
                        } catch (error) {
                            this.logger.warn(`Seasoning burn failed`, { wallet: wallet.name, error: error.message.slice(0, 50) });
                        }
                    }

                    const delayMs = this.config.seasoningDelayMs + (Math.random() - 0.5) * (this.config.seasoningDelayMs * 0.5);
                    await delay(delayMs);
                }

                // Mark as seasoned
                const originalIndex = walletData.findIndex(w => w.pubkey === wallet.pubkey);
                if (originalIndex !== -1) {
                    walletData[originalIndex].isSeasoned = true;
                    await jsonfile.writeFile(this.config.walletFile, walletData, { spaces: 2 });
                    this.walletManager.walletData = walletData;
                }

            } catch (error) {
                this.logger.error(`Error seasoning ${wallet.name}`, { error: error.message });
            }
        }

        this.logger.info('Seasoning complete');
    }

    /**
     * Gets token balance
     * @param {PublicKey} owner
     * @param {PublicKey} mint
     * @returns {Promise<BN>}
     */
    async getTokenBalance(owner, mint) {
        try {
            const tokenAccount = getAssociatedTokenAddressSync(mint, owner);
            const accountInfo = await getAccount(this.connection, tokenAccount);
            return new BN(accountInfo.amount.toString());
        } catch {
            return new BN('0');
        }
    }
}