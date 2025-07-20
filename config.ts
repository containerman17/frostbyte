import dotenv from 'dotenv';
import path from 'node:path';
import fs from 'node:fs';
import { z } from 'zod';
import Database from 'better-sqlite3';

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

// SQLite database cache - separate databases for blocks and each plugin
const dbCache = new Map<string, Database.Database>();

type CreateDbConfig = {
    debugEnabled: boolean;
    type: "plugin" | "blocks";
    indexerName?: string;
    pluginVersion?: number;
    chainId?: string;
}

export function getSqliteDb(config: CreateDbConfig): Database.Database {
    const chainId = config.chainId || requiredEnvString('CHAIN_ID');

    // Create a unique key for each database
    let dbKey = config.type === "blocks"
        ? `blocks_${chainId}_${config.debugEnabled}`
        : `plugin_${chainId}_${config.indexerName}_${config.debugEnabled}`;

    dbKey = chainId + "_" + dbKey;

    if (!dbCache.has(dbKey)) {
        dbCache.set(dbKey, createSqliteDb({ ...config, chainId }));
    }

    return dbCache.get(dbKey)!;
}

function createSqliteDb(config: CreateDbConfig & { chainId: string }): Database.Database {
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
    // Database file name
    const dbName = `${prefix}${chainConfig.blockchainId.slice(0, 20)}${config.type === "blocks" ? "" : "_" + config.indexerName}${postfix}.db`;

    // Create directory structure
    const dbDir = path.join(DATA_DIR, config.chainId);
    if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true });
    }

    const dbPath = path.join(dbDir, dbName);

    // For plugin databases, check version and drop if needed
    if (config.type === "plugin") {
        handlePluginDatabaseVersioning({
            dbPath,
            dbName,
            currentVersion: config.pluginVersion!
        });
    }

    // Create and open the database
    const db = new Database(dbPath);
    console.log(`Database ${dbName} ready at ${dbPath}`);

    // Enable WAL mode for better concurrency
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');

    return db;
}

interface PluginDatabaseVersioningOptions {
    dbPath: string;
    dbName: string;
    currentVersion: number;
}

function handlePluginDatabaseVersioning(options: PluginDatabaseVersioningOptions): void {
    const { dbPath, dbName, currentVersion } = options;

    console.log(`[DB Version Check] Checking database ${dbName} for version ${currentVersion}...`);

    if (fs.existsSync(dbPath)) {
        console.log(`[DB Version Check] Database ${dbName} exists, checking version...`);

        let shouldDeleteDb = false;
        let previousVersion: number | null = null;

        try {
            // Open database to check version
            const db = new Database(dbPath);

            try {
                // Check if kv_int table exists
                const tableInfo = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='kv_int'").get();

                if (!tableInfo) {
                    // No kv_int table, delete database
                    console.log(`[DB Version Check] Database ${dbName} has no kv_int table, will delete and recreate`);
                    shouldDeleteDb = true;
                } else {
                    // Check stored version
                    const versionRow = db.prepare("SELECT value FROM kv_int WHERE key = ?").get('indexer_version') as { value: number } | undefined;

                    if (!versionRow) {
                        // No version stored, delete database
                        console.log(`[DB Version Check] Database ${dbName} has no stored version, will delete and recreate`);
                        shouldDeleteDb = true;
                    } else {
                        previousVersion = versionRow.value;
                        if (previousVersion !== currentVersion) {
                            console.log(`[DB Version Check] Database ${dbName} version changed: v${previousVersion} → v${currentVersion}, will delete and recreate`);
                            shouldDeleteDb = true;
                        } else {
                            console.log(`[DB Version Check] Database ${dbName} version unchanged (v${currentVersion}), keeping existing database`);
                        }
                    }
                }
            } finally {
                db.close();
            }
        } catch (error) {
            // Error accessing database, delete it
            console.log(`[DB Version Check] Error checking database ${dbName} version, will delete and recreate`);
            console.error(error);
            shouldDeleteDb = true;
        }

        if (shouldDeleteDb) {
            fs.unlinkSync(dbPath);
            console.log(`[DB Version Check] Database ${dbName} deleted successfully`);
        }
    } else {
        console.log(`[DB Version Check] Database ${dbName} does not exist, will create new`);
    }

    // If database was deleted or didn't exist, create kv_int table and store version
    if (!fs.existsSync(dbPath)) {
        const db = new Database(dbPath);

        try {
            db.exec(`
                CREATE TABLE IF NOT EXISTS kv_int (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            `);

            // Store the current version
            db.prepare("INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)").run('indexer_version', currentVersion);

            console.log(`[DB Version Check] Database ${dbName} is ready with version ${currentVersion}`);
        } finally {
            db.close();
        }
    }
}
