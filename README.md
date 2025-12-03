# Volume Booster Bot

A sophisticated Solana volume boosting bot with advanced trading strategies, wallet management, and safety features.

## ⚠️ Security Warning

**This bot handles cryptocurrency wallets and private keys. Use at your own risk.**

- Never commit private keys to version control
- Store wallet files securely and encrypt if possible
- Use dedicated wallets for bot operations
- Monitor transactions closely
- Test on devnet first

## Features

- Multi-wallet volume boosting
- Adaptive trading strategies
- Real-time market data integration
- Circuit breaker safety systems
- Wallet seasoning for stealth
- Automatic rebalancing
- Partial sell strategies
- Jito MEV protection

## Installation

1. Clone the repository
2. Install dependencies: `npm install`
3. Copy `.env.example` to `.env` and configure
4. Generate wallets: `node scripts/generate-wallets.js 10`
5. Run: `npm start`

## Configuration

See `.env.example` for all configuration options.

Required:
- `RPC_URL`: Solana RPC endpoint
- `MEME_COIN_MINT`: Token mint address
- `MEME_COIN_SYMBOL`: Token symbol
- `MARKET`: DEX market (e.g., RAYDIUM_AMM)

## Architecture

- **VolumeBoosterBot**: Main orchestrator
- **TradingEngine**: Handles buy/sell logic
- **WalletManager**: Manages wallet rotation and funding
- **MarketDataProvider**: Fetches price data
- **CircuitBreaker**: Safety mechanisms
- **ConfigManager**: Configuration validation

## Testing

Run tests: `npm test`

## License

MIT