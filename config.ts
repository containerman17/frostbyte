import dotenv from 'dotenv';
import path from 'node:path';
import fs from 'node:fs';
import { z } from 'zod';

dotenv.config();

export const IS_DEVELOPMENT = process.env['NODE_ENV'] !== 'production';
export const DATA_DIR = requiredEnvString('DATA_DIR');

function requiredEnvString(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Required environment variable ${name} is not set`);
    }
    return value;
}

// Define Zod schemas
const RpcConfigSchema = z.object({
    rpcUrl: z.string().url(),
    requestBatchSize: z.number().positive(),
    maxConcurrentRequests: z.number().positive(),
    rps: z.number().positive(),
    rpcSupportsDebug: z.boolean(),
    enableBatchSizeGrowth: z.boolean().optional()
});

const RpcConfigWithBlockSize = RpcConfigSchema.extend({
    blocksPerBatch: z.number().positive()
});

const ChainConfigSchema = z.object({
    chainName: z.string().min(1),
    blockchainId: z.string().min(1),
    evmChainId: z.number().int().positive(),
    rpcConfig: RpcConfigWithBlockSize
});

const ChainsConfigSchema = z.array(ChainConfigSchema);

const configLocation = path.join(DATA_DIR, 'chains.json');
const rawChains = JSON.parse(fs.readFileSync(configLocation, 'utf8'));

export const CHAIN_CONFIGS = ChainsConfigSchema.parse(rawChains);

export type RpcConfig = z.infer<typeof RpcConfigSchema>;
export type ChainConfig = z.infer<typeof ChainConfigSchema>;
export type RpcConfigWithBlockSize = z.infer<typeof RpcConfigWithBlockSize>;

export function getCurrentChainConfig(): ChainConfig {
    const CHAIN_ID = requiredEnvString('CHAIN_ID');
    const chain = CHAIN_CONFIGS.find(chain => chain.blockchainId === CHAIN_ID);
    if (!chain) {
        throw new Error(`Chain with ID ${CHAIN_ID} not found`);
    }
    return chain;
}

export function getPluginDirs(): string[] {
    const PLUGIN_DIRS = process.env['PLUGIN_DIRS'];
    if (!PLUGIN_DIRS) {
        // Default to pluginExamples if no plugin dirs specified
        return [path.join(__dirname, 'pluginExamples')];
    }
    // Split by comma or semicolon to support multiple directories
    return PLUGIN_DIRS.split(/[,;]/).map(dir => dir.trim()).filter(dir => dir.length > 0);
}
