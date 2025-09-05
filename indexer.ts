import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs, ChainConfig } from './config.js';
import sqlite3 from 'better-sqlite3';

const TXS_PER_LOOP = 10000;
const SLEEP_TIME = 300;

// Read-through FIFO cache for getTxBatch results with TTL
class TxBatchCache {
    private cache = new Map<string, { data: { txs: any[], traces: any[] | undefined }, timestamp: number }>();
    private keys: string[] = [];
    private maxSize = 10;
    private ttlMs = 2000; // 2 second TTL

    constructor(private blocksDb: BlocksDBHelper) { }

    private makeKey(from: number, to: number, includeTraces: boolean, filterEvents?: string[]): string {
        return `${from}-${to}-${includeTraces}-${filterEvents?.join(',') || ''}`;
    }

    getTxBatch(from: number, to: number, includeTraces: boolean, filterEvents?: string[]): { txs: any[], traces: any[] | undefined } {
        const key = this.makeKey(from, to, includeTraces, filterEvents);
        const now = Date.now();

        // Check cache
        const cached = this.cache.get(key);
        if (cached && (now - cached.timestamp) < this.ttlMs) {
            console.log(`[Cache HIT] ${from}-${to} (${cached.data.txs.length} txs)`);
            return cached.data;
        }

        // Cache miss or expired - fetch from DB
        const data = this.blocksDb.getTxBatch(from, to, includeTraces, filterEvents);

        // Remove expired entry if it exists
        if (cached) {
            const keyIndex = this.keys.indexOf(key);
            if (keyIndex > -1) {
                this.keys.splice(keyIndex, 1);
            }
            this.cache.delete(key);
        }

        // FIFO eviction if needed
        if (this.keys.length >= this.maxSize) {
            const oldestKey = this.keys.shift()!;
            this.cache.delete(oldestKey);
        }

        // Store in cache with timestamp
        this.cache.set(key, { data, timestamp: now });
        this.keys.push(key);

        return data;
    }
}

// Single chain indexer - one process per chain
export async function startIndexingLoop() {
    const chainConfig = getCurrentChainConfig();
    const indexers = await loadIndexingPlugins(getPluginDirs());

    // Get the blocks database once for all indexers
    const blocksDb = new BlocksDBHelper(
        getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "blocks",
            chainId: chainConfig.blockchainId,
            readonly: true,
        }),
        true,
        chainConfig.rpcConfig.rpcSupportsDebug
    );

    // Initialize read-through cache wrapping the blocks DB
    const txBatchCache = new TxBatchCache(blocksDb);

    // Initialize all indexers first
    const indexerConfigs: Array<{ indexer: IndexingPlugin<any>, db: sqlite3.Database }> = [];

    for (const indexer of indexers) {
        console.log(`[${indexer.name} - ${chainConfig.chainName}] Starting indexer v${indexer.version}`);
        const db = getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "plugin",
            indexerName: indexer.name,
            pluginVersion: indexer.version,
            chainId: chainConfig.blockchainId,
            readonly: false,
        });

        // Initialize indexer
        console.log(`[${indexer.name} - ${chainConfig.chainName}] Initializing database`);

        // Run initialization in a transaction
        const initializeTransaction = db.transaction(() => {
            indexer.initialize(db);
        });

        initializeTransaction();

        indexerConfigs.push({ indexer, db });
    }

    // Main loop - process all indexers sequentially
    const startTime = performance.now();
    while (true) {
        let didWork = false;

        for (const { indexer, db } of indexerConfigs) {
            // Get last indexed transaction from db
            const lastIndexedTx = getIntValue(db, `lastIndexedTx_${indexer.name}`, -1);
            const totalTxCount = blocksDb.getTxCount();

            if (lastIndexedTx >= totalTxCount) {
                continue; // Nothing to do for this indexer
            }

            didWork = true;
            const getStart = performance.now();

            // Process next batch
            const toTx = Math.min(totalTxCount, lastIndexedTx + TXS_PER_LOOP);

            // Get transactions (cache handles everything)
            const transactions = txBatchCache.getTxBatch(lastIndexedTx, toTx, indexer.usesTraces, indexer.filterEvents);

            // Extract data inline
            const extractStart = performance.now();
            const extractedData = indexer.extractData(transactions);
            const extractFinish = performance.now();

            // Save extracted data in SQLite transaction
            const saveStart = performance.now();
            const saveDataTransaction = db.transaction(() => {
                indexer.saveExtractedData(db, blocksDb, extractedData);
                setIntValue(db, `lastIndexedTx_${indexer.name}`, toTx);
            });

            saveDataTransaction();
            const saveFinish = performance.now();

            // Get progress information
            const lastStoredBlock = blocksDb.getLastStoredBlockNumber();
            const indexingPercentage = ((lastIndexedTx / totalTxCount) * 100).toFixed(2);

            if (transactions.txs.length > 0) {
                console.log(
                    `[${indexer.name} - ${chainConfig.chainName}] Retrieved ${transactions.txs.length} txs in ${Math.round(extractStart - getStart)}ms`,
                    `Extracted in ${Math.round(extractFinish - extractStart)}ms`,
                    `Saved in ${Math.round(saveFinish - saveStart)}ms`,
                    `(${indexingPercentage}% - tx ${lastIndexedTx}/${totalTxCount})`,
                    `Total time: ${Math.round((performance.now() - startTime) / 1000)}s`
                );
            }
        }

        // If no indexer had work, sleep
        if (!didWork) {
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
        }
    }
}
