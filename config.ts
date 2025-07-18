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

export function getChainConfig(chainId: string): ChainConfig {
    const chain = CHAIN_CONFIGS.find(chain => chain.blockchainId === chainId);
    if (!chain) {
        throw new Error(`Chain with ID ${chainId} not found`);
    }
    return chain;
}

export function getCurrentChainConfig(): ChainConfig {
    const CHAIN_ID = requiredEnvString('CHAIN_ID');
    return getChainConfig(CHAIN_ID);
}

export function getPluginDirs(): string[] {
    const PLUGIN_DIRS = process.env['PLUGIN_DIRS'];
    if (!PLUGIN_DIRS) {
        throw new Error('PLUGIN_DIRS is not set');
    }
    // Split by comma or semicolon to support multiple directories
    return PLUGIN_DIRS.split(/[,;]/).map(dir => dir.trim()).filter(dir => dir.length > 0);
}

// MySQL Pool cache - separate pools for blocks and each plugin
const poolCache = new Map<string, Promise<mysql.Pool>>();

type CreatePoolConfig = {
    debugEnabled: boolean;
    type: "plugin" | "blocks";
    indexerName?: string;
    pluginVersion?: number;
    chainId?: string;
}

export async function getMysqlPool(config: CreatePoolConfig): Promise<mysql.Pool> {
    const chainId = config.chainId || requiredEnvString('CHAIN_ID');

    // Create a unique key for each pool
    let poolKey = config.type === "blocks"
        ? `blocks_${chainId}_${config.debugEnabled}`
        : `plugin_${chainId}_${config.indexerName}_${config.debugEnabled}`;

    poolKey = chainId + "_" + poolKey;

    if (!poolCache.has(poolKey)) {
        poolCache.set(poolKey, createMysqlPool({ ...config, chainId }));
    }

    return poolCache.get(poolKey)!;
}


async function createMysqlPool(config: CreatePoolConfig & { chainId: string }): Promise<mysql.Pool> {

    if (config.type === "plugin" && typeof config.pluginVersion !== "number") {
        throw new Error("Plugin version is required for plugin");
    }

    if (config.type === "plugin" && !config.indexerName) {
        throw new Error("Plugin name is required for plugin");
    } else if (config.type === "blocks" && config.indexerName) {
        throw new Error("Plugin name is not allowed for blocks");
    }

    let prefix = ""
    if (config.type === "plugin") {
        prefix = "p"
    } else if (config.type === "blocks") {
        prefix = "b"
    } else {
        throw new Error("Invalid type");
    }

    let postfix = config.debugEnabled ? "" : "_ndbg"

    const chainConfig = getChainConfig(config.chainId);
    // Remove version from database name for plugins
    const dbName = `${prefix}${chainConfig.blockchainId.slice(0, 20)}${config.type === "blocks" ? "" : "_" + config.indexerName}${postfix}`;

    // MySQL connection details (could be moved to env vars if needed)
    const host = process.env['MYSQL_HOST'] || 'localhost';
    const port = parseInt(process.env['MYSQL_PORT'] || '3306');
    const user = process.env['MYSQL_USER'] || 'root';
    const password = process.env['MYSQL_PASSWORD'] || 'root';

    // For plugin databases, check version and drop if needed
    if (config.type === "plugin") {
        await handlePluginDatabaseVersioning({
            host,
            port,
            user,
            password,
            dbName,
            currentVersion: config.pluginVersion!
        });
    } else {
        // For blocks database, just create if doesn't exist
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

interface PluginDatabaseVersioningOptions {
    host: string;
    port: number;
    user: string;
    password: string;
    dbName: string;
    currentVersion: number;
}

async function handlePluginDatabaseVersioning(options: PluginDatabaseVersioningOptions): Promise<void> {
    const { host, port, user, password, dbName, currentVersion } = options;

    console.log(`[DB Version Check] Checking database ${dbName} for version ${currentVersion}...`);

    // First connect without database to check if it exists
    const connection = await mysql.createConnection({
        host,
        port,
        user,
        password
    });

    try {
        // Check if database exists
        const [databases] = await connection.execute<mysql.RowDataPacket[]>(
            `SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?`,
            [dbName]
        );

        const dbExists = databases.length > 0;

        if (dbExists) {
            console.log(`[DB Version Check] Database ${dbName} exists, checking version...`);
            // Database exists, check version
            let shouldDropDb = false;
            let previousVersion: number | null = null;

            try {
                // Check if kv_int table exists
                const [tables] = await connection.execute<mysql.RowDataPacket[]>(
                    `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'kv_int'`,
                    [dbName]
                );

                if (tables.length === 0) {
                    // No kv_int table, drop database just in case
                    console.log(`[DB Version Check] Database ${dbName} has no kv_int table, will drop and recreate`);
                    shouldDropDb = true;
                } else {
                    // Check stored version - create a new connection to the specific database
                    const dbConnection = await mysql.createConnection({
                        host,
                        port,
                        user,
                        password,
                        database: dbName
                    });

                    try {
                        const [rows] = await dbConnection.execute<mysql.RowDataPacket[]>(
                            `SELECT value FROM kv_int WHERE \`key\` = 'indexer_version'`
                        );

                        if (rows.length === 0) {
                            // No version stored, drop database just in case
                            console.log(`[DB Version Check] Database ${dbName} has no stored version, will drop and recreate`);
                            shouldDropDb = true;
                        } else {
                            previousVersion = rows[0]!['value'];
                            if (previousVersion !== currentVersion) {
                                console.log(`[DB Version Check] Database ${dbName} version changed: v${previousVersion} → v${currentVersion}, will drop and recreate`);
                                shouldDropDb = true;
                            } else {
                                console.log(`[DB Version Check] Database ${dbName} version unchanged (v${currentVersion}), keeping existing database`);
                            }
                        }
                    } finally {
                        await dbConnection.end();
                    }
                }
            } catch (error) {
                // Error accessing database or table, drop it
                console.log(`[DB Version Check] Error checking database ${dbName} version, will drop and recreate`);
                console.error(error);
                shouldDropDb = true;
            }

            if (shouldDropDb) {
                await connection.execute(`DROP DATABASE IF EXISTS \`${dbName}\``);
                console.log(`[DB Version Check] Database ${dbName} dropped successfully`);
            }
        } else {
            console.log(`[DB Version Check] Database ${dbName} does not exist, will create new`);
        }

        // Create database if it doesn't exist (or was just dropped)
        await connection.execute(
            `CREATE DATABASE IF NOT EXISTS \`${dbName}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
        );

        // Create kv_int table and store version - create a new connection to the database
        const dbConnection = await mysql.createConnection({
            host,
            port,
            user,
            password,
            database: dbName
        });

        try {
            await dbConnection.execute(`
                CREATE TABLE IF NOT EXISTS kv_int (
                    \`key\`   VARCHAR(255) PRIMARY KEY,
                    \`value\` BIGINT NOT NULL
                )
            `);

            // Store the current version
            await dbConnection.execute(
                `INSERT INTO kv_int (\`key\`, \`value\`) VALUES ('indexer_version', ?) ON DUPLICATE KEY UPDATE \`value\` = VALUES(\`value\`)`,
                [currentVersion]
            );

            console.log(`[DB Version Check] Database ${dbName} is ready with version ${currentVersion}`);
        } finally {
            await dbConnection.end();
        }
    } finally {
        await connection.end();
    }
}
