// Main exports for frostbyte plugin developers

// The main interface plugins must implement
export type { IndexerModule, RegisterRoutesContext } from './lib/types';

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

import { IndexerModule } from './lib/types';
import rpcModule from './std/rpc';
import chainsModule from './std/chains';

export namespace StandardIndexers {
    export const RPCIndexer: IndexerModule = rpcModule;
    export const ChainsIndexer: IndexerModule = chainsModule;
}
