import fs from 'fs';
import { Keypair } from '@solana/web3.js';

async function generateWallets(amount) {
    const wallets = [];
    for (let i = 0; i < amount; i++) {
        const keypair = Keypair.generate();
        wallets.push({
            pubkey: keypair.publicKey.toBase58(),
            privateKey: Array.from(keypair.secretKey),
            name: `Wallet${i + 1}`,
            isSeasoned: false
        });
    }
    return wallets;
}

async function main() {
    const amount = process.argv[2];
    if (!amount || isNaN(amount)) {
        console.error('Please provide a valid number of wallets to generate.');
        process.exit(1);
    }

    const wallets = await generateWallets(parseInt(amount, 10));
    fs.writeFileSync('SINK.json', JSON.stringify(wallets, null, 2));
    console.log(`Successfully generated ${amount} wallets and saved them to wallets.json`);
}

main();