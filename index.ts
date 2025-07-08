// Main exports for index-kit plugin developers

// The main interface plugins must implement
export type { IndexerModule, RegisterRoutesContext } from './lib/types';

// Types that are passed to plugin methods
export type { StoredTx, RpcTraceResult } from './blockFetcher/evmTypes';

// Re-export third-party types that plugins receive
export type { Database } from 'better-sqlite3';
export type { FastifyInstance } from 'fastify';

// BlockDB is passed to handleTxBatch
export type { BlockDB } from './blockFetcher/BlockDB'; 
