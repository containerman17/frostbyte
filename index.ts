// Main exports for frostbyte plugin developers

// The main interfaces plugins must implement
export type { IndexingPlugin, ApiPlugin, RegisterRoutesContext } from './lib/types';

// Types that are passed to plugin methods
export * as evmTypes from './blockFetcher/evmTypes';

export type { ChainConfig, RpcConfigWithBlockSize } from './config';

// Re-export third-party types that plugins receive
export type { Database } from 'better-sqlite3';
export type { FastifyInstance } from 'fastify';

export { prepQueryCached } from './lib/prep';
// BlockDB is passed to handleTxBatch
export type { BlockDB } from './blockFetcher/BlockDB';

export * as dateUtils from "./lib/dateUtils";
export * as encodingUtils from "./lib/encodingUtils";

export { default as RPCIndexerAPIPlugin } from './standardPlugins/rpcApi';
export { default as ChainsIndexerAPIPlugin } from './standardPlugins/chainsApi';
