// Main exports for frostbyte plugin developers

// The main interfaces plugins must implement
export type { IndexingPlugin, ApiPlugin, RegisterRoutesContext, TxBatch } from './lib/types';

// Types that are passed to plugin methods
export * as evmTypes from './blockFetcher/evmTypes';

export type { ChainConfig, RpcConfigWithBlockSize } from './config';

// Re-export third-party types that plugins receive
export type { FastifyInstance } from 'fastify';

// BlocksDBHelper is passed to handleTxBatch
export type { BlocksDBHelper } from './blockFetcher/BlocksDBHelper';

export * as abiUtils from './lib/abiUtils';

export * as dateUtils from "./lib/dateUtils";
export * as encodingUtils from "./lib/encodingUtils";
export * as logsBloom from "./lib/logsBloom";

export { default as RPCIndexerAPIPlugin } from './standardPlugins/rpcApi';
export { default as ChainsIndexerAPIPlugin } from './standardPlugins/chainsApi';

export * as viem from "viem";
