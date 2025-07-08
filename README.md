# frostbyte

A high-performance Avalanche L1 indexer kit with TypeScript plugin support.

## Installation

```bash
npm install -g frostbyte-sdk
```

## Quick Start

```bash
# Create a new plugin
frostbyte init --name my-indexer

# Run the indexer
frostbyte run --plugins-dir ./plugins --data-dir ./data
```

## Writing Plugins

Plugins are TypeScript files that implement the `IndexerModule` interface:

```typescript
import type { IndexerModule } from "frostbyte";

const module: IndexerModule = {
    name: "my-indexer",
    version: 1,
    usesTraces: false,

    // Called when version changes
    wipe: (db) => {
        db.exec(`DROP TABLE IF EXISTS my_data`);
    },

    // Called once on startup
    initialize: (db) => {
        db.exec(`CREATE TABLE IF NOT EXISTS my_data (...)`);
    },

    // Called for each batch of transactions
    handleTxBatch: (db, blocksDb, batch) => {
        for (const tx of batch.txs) {
            // Index transaction data
        }
    },

    // Optional: Add API endpoints
    registerRoutes: (app, dbCtx) => {
        app.get("/:evmChainId/my-endpoint", async (request, reply) => {
            // Handle API request
        });
    },
};

export default module;
```

## Configuration

Create `chains.json` in your data directory:

```json
[{
    "chainName": "Avalanche C-Chain",
    "blockchainId": "2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5",
    "evmChainId": 43114,
    "rpcConfig": {
        "rpcUrl": "https://api.avax.network/ext/bc/C/rpc",
        "requestBatchSize": 20,
        "maxConcurrentRequests": 10,
        "rps": 50,
        "rpcSupportsDebug": false,
        "blocksPerBatch": 100
    }
}]
```

## CLI Commands

### `frostbyte run`

- `--plugins-dir` (required): Directory containing plugin files
- `--data-dir`: Data storage directory (default: `./data`)

### `frostbyte init`

- `--name` (required): Plugin name
- `--plugins-dir`: Where to create the plugin (default: `./plugins`)

## License

MIT
