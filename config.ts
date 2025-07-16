import dotenv from 'dotenv';
import path from 'node:path';
import fs from 'node:fs';
import { z } from 'zod';
import mysql from 'mysql2/promise';

dotenv.config();

export const IS_DEVELOPMENT = process.env['NODE_ENV'] !== 'production';
export const DATA_DIR = requiredEnvString('DATA_DIR');
export const ASSETS_DIR = process.env['ASSETS_DIR'];

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
        throw new Error('PLUGIN_DIRS is not set');
    }
    // Split by comma or semicolon to support multiple directories
    return PLUGIN_DIRS.split(/[,;]/).map(dir => dir.trim()).filter(dir => dir.length > 0);
}

// MySQL Pool Singleton
let mysqlPoolPromise: Promise<mysql.Pool> | null = null;

export async function getMysqlPool(debugEnabled: boolean): Promise<mysql.Pool> {
    if (!mysqlPoolPromise) {
        mysqlPoolPromise = createMysqlPool(debugEnabled);
    }
    return mysqlPoolPromise;
}

async function createMysqlPool(debugEnabled: boolean): Promise<mysql.Pool> {
    const chainConfig = getCurrentChainConfig();
    const dbName = debugEnabled
        ? `${chainConfig.blockchainId}`
        : `${chainConfig.blockchainId}_no_dbg`;

    // MySQL connection details (could be moved to env vars if needed)
    const host = process.env['MYSQL_HOST'] || 'localhost';
    const port = parseInt(process.env['MYSQL_PORT'] || '3306');
    const user = process.env['MYSQL_USER'] || 'root';
    const password = process.env['MYSQL_PASSWORD'] || 'root';

    // First create the database if it doesn't exist
    const connection = await mysql.createConnection({
        host,
        port,
        user,
        password
    });

    try {
        await connection.execute(
            `CREATE DATABASE IF NOT EXISTS \`${dbName}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
        );
        console.log(`Database ${dbName} ready`);
    } finally {
        await connection.end();
    }

    // Create and return the pool
    const pool = mysql.createPool({
        host,
        port,
        user,
        password,
        database: dbName,
        waitForConnections: true,
        connectionLimit: 20, // Adjust based on needs
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 0
    });

    return pool;
}
