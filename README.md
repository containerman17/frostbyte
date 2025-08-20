# frostbyte

A high-performance Avalanche L1 indexer kit with TypeScript plugin support.

## Requirements

- **Node.js 22.0.0 or higher** (required for native TypeScript support)
- npm or compatible package manager

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
  - sqlite databases for each chain
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

type ExtractedData = {
  // Define your extracted data structure
  transactions: Array<{ from: string; value: bigint }>;
};

const module: IndexingPlugin<ExtractedData> = {
  name: "my-indexer",
  version: 1,
  usesTraces: false,

  // Called once on startup
  initialize: (db) => {
    db.exec(`CREATE TABLE IF NOT EXISTS my_data (...)`);
  },

  // Extract data from transaction batch
  extractData: (batch) => {
    const transactions = batch.txs.map(tx => ({
      from: tx.receipt.from,
      value: BigInt(tx.receipt.value),
    }));
    return { transactions };
  },

  // Save extracted data to database
  saveExtractedData: (db, blocksDb, data) => {
    const stmt = db.prepare(`INSERT INTO my_data (from_address, value) VALUES (?, ?)`);
    for (const tx of data.transactions) {
      stmt.run(tx.from, tx.value.toString());
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
    app.get("/:evmChainId/my-endpoint", (request, reply) => {
      const { evmChainId } = request.params;
      // Access the indexer's database
      const db = dbCtx.getIndexerDbConnection(evmChainId, "my-indexer");
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
