# frostbyte

A high-performance Avalanche L1 indexer kit with TypeScript plugin support.

## Installation

```bash
npm install -g frostbyte-sdk
```

## Quick Start

```bash
# Create a new indexing plugin
frostbyte init --name my-indexer --type indexing

# Create a new API plugin
frostbyte init --name my-api --type api

# Run the indexer and API server
frostbyte run --plugins-dir ./plugins --data-dir ./data
```

## Docker

You can run frostbyte using the pre-built Docker image:

```bash
# Pull the latest image
docker pull ghcr.io/containerman17/frostbyte:latest

# Run the container
docker run -it --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/plugins:/plugins \
  ghcr.io/containerman17/frostbyte:latest
```

### Path Mappings

When running in Docker, the following paths are used:

- **Host `./data` → Container `/data`**: This directory contains:
  - `chains.json` - Your blockchain configuration
  - SQLite database files for each chain
  - Any other persistent data

- **Host `./plugins` → Container `/plugins`**: This directory contains:
  - Your TypeScript plugin files (`.ts` files)
  - These are compiled and loaded at runtime

The container expects these paths to exist:

- Create `./data/chains.json` with your chain configuration
- Place your plugin files in `./plugins/`

Example directory structure:

```
.
├── data/
│   ├── chains.json
│   └── *.db (created automatically)
└── plugins/
    ├── my-indexer.ts
    └── another-plugin.ts
```

## Writing Plugins

frostbyte supports two types of plugins:

### Indexing Plugins

Indexing plugins process blockchain data and store it in their own SQLite
database:

```typescript
import type { IndexingPlugin } from "frostbyte-sdk";

const module: IndexingPlugin = {
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
};

export default module;
```

### API Plugins

API plugins serve REST endpoints using data from indexer databases:

```typescript
import type { ApiPlugin } from "frostbyte-sdk";

const module: ApiPlugin = {
    name: "my-api",
    requiredIndexers: ["my-indexer"], // Declare which indexers this API needs

    registerRoutes: (app, dbCtx) => {
        app.get("/:evmChainId/my-endpoint", async (request, reply) => {
            const { evmChainId } = request.params;
            // Access the indexer's database
            const db = dbCtx.indexerDbFactory(evmChainId, "my-indexer");
            // Query and return data
        });
    },
};

export default module;
```

### Built-in API Plugins

frostbyte includes two standard API plugins that are always available:

- **chains**: Provides `/chains` endpoint showing status of all configured
  chains
- **rpc**: Provides `/:evmChainId/rpc` JSON-RPC endpoint for querying blockchain
  data

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
- `--type`: Plugin type - `indexing` or `api` (default: `indexing`)
- `--plugins-dir`: Where to create the plugin (default: `./plugins`)

Examples:

```bash
# Create an indexing plugin
frostbyte init --name my-indexer --type indexing

# Create an API plugin
frostbyte init --name my-api --type api
```

## License

MIT
