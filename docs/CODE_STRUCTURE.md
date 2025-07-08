# Code Structure

This document gives a high level overview of the repository layout. Only files tracked by Git are listed (items ignored via `.gitignore` such as `node_modules`, `data/`, `dist/`, and `.env` are omitted).

## Root level

- `.cursorrules` – project guidelines emphasizing fail–fast error handling.
- `.dockerignore` – paths excluded when building Docker images.
- `.github/workflows/docker-publish.yml` – GitHub Actions workflow to build and publish Docker images.
- `.gitignore` – ignores local dependencies and build output.
- `.npmignore` – npm packaging ignore rules.
- `Dockerfile` – container build instructions.
- `README.md` – introduction and quick start guide.
- `cli.ts` – command line interface with `run` and `init` commands.
- `config.ts` – environment variable helpers and chain configuration parser.
- `index.ts` – exports types and standard indexer modules for plugin authors.
- `indexer.ts` – logic to run one or more indexing plugins against stored blocks.
- `server.ts` – Fastify based API server exposing JSON‑RPC and other endpoints.
- `start.ts` – cluster entry point that spawns fetcher, indexer and API workers.
- `package.json` / `package-lock.json` – Node package metadata and lock file.
- `tsconfig.json` – TypeScript compiler configuration.
- `types/tsx.d.ts` – shim declaration for `tsx` when used via CommonJS.

## Block fetching subsystem (`blockFetcher/`)

- `BatchRpc.ts` – performs batched JSON‑RPC calls with optional dynamic batch sizing.
- `BlockDB.ts` – SQLite wrapper storing blocks, transactions and traces.
- `DynamicBatchSizeManager.ts` – adjusts RPC batch sizes based on success rate.
- `evmTypes.ts` – TypeScript interfaces for Ethereum style RPC data.
- `startFetchingLoop.ts` – continuously pulls blocks from a chain and stores them.

## Library utilities (`lib/`)

- `dateUtils.ts` – helpers for time‑bucketed statistics.
- `dbHelper.ts` – SQLite pragma setup and maintenance functions for indexers.
- `dbPaths.ts` – resolves database paths and cleans up old versions.
- `pluginTemplate.ts` – generates a boilerplate plugin file via the CLI.
- `plugins.ts` – dynamic loader for plugin modules from a directory.
- `prep.ts` – caching wrapper around `better-sqlite3` prepared statements.
- `types.ts` – TypeScript interfaces defining the plugin API.

## Standard indexers (`std/`)

- `chains.ts` – exposes a `/chains` API showing status of configured chains.
- `rpc.ts` – lightweight JSON‑RPC proxy served through Fastify routes.

## Example plugins (`pluginExamples/__removeme_glacier-compatible/`)

Contains sample indexers demonstrating common patterns:

- `activeAddresses.ts`
- `activeSenders.ts`
- `cumulativeContracts.ts`
- `cumulativeTxCount.ts`
- `teleporterMetrics.ts`
- `txCount.ts`
- `lib/evmUtils.ts` – shared helpers used by the examples.

These files serve as reference implementations for custom plugins.


## Documentation (`docs/`)

- `DATABASE_MAINTENANCE_IMPLEMENTATION.md` – explains the SQLite maintenance strategy implemented in the project.
- `CODE_STRUCTURE.md` – **this file**.

