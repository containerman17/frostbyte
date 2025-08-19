// Main exports for frostbyte plugin developers

// The main interfaces plugins must implement
export type { IndexingPlugin, ApiPlugin, RegisterRoutesContext, TxBatch } from './lib/types.ts';

// Types that are passed to plugin methods
export * as evmTypes from './blockFetcher/evmTypes.ts';

export type { ChainConfig, RpcConfigWithBlockSize } from './config';

// Re-export third-party types that plugins receive
export type { FastifyInstance } from 'fastify';

// BlocksDBHelper is passed to saveExtractedData
export type { BlocksDBHelper } from './blockFetcher/BlocksDBHelper';

export * as abiUtils from './lib/abiUtils.ts';

export * as dateUtils from "./lib/dateUtils.ts";
export * as encodingUtils from "./lib/encodingUtils.ts";
export * as logsBloom from "./lib/logsBloom.ts";

export { default as RPCIndexerAPIPlugin } from './standardPlugins/rpcApi.ts';
export { default as ChainsIndexerAPIPlugin } from './standardPlugins/chainsApi.ts';

export * as viem from "viem";
