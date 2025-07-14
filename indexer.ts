import { BlockDB } from './blockFetcher/BlockDB.js';
import Database from 'better-sqlite3';
import { loadIndexingPlugins } from './lib/plugins.js';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getIntValue, initializeIndexingDB, setIntValue, performIndexingPostCatchUpMaintenance, performIndexingPeriodicMaintenance } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import fs from 'node:fs';
import { getIndexerDbPath } from './lib/dbPaths.js';
import { getPluginDirs } from './config.js';

export interface IndexerOptions {
    blocksDbPath: string;
    chainId: string;
    exitWhenDone?: boolean;
    debugEnabled: boolean;
}

export interface SingleIndexerOptions extends IndexerOptions {
    indexerName: string;
}

async function startIndexer(
    indexer: IndexingPlugin,
    blocksDb: BlockDB,
    chainId: string,
    exitWhenDone: boolean,
    debugEnabled: boolean
): Promise<void> {
    console.log(`[${indexer.name}] Starting indexer v${indexer.version}`);
    const name = indexer.name;
    const version = indexer.version;

    if (version < 0) {
        throw new Error(`Indexer ${name} has invalid version ${version}`);
    }

    // Get the path for this specific indexer and version (old versions will be deleted)
    const indexerDbPath = getIndexerDbPath(chainId, name, version, debugEnabled);

    // Check if database exists
    const dbExists = fs.existsSync(indexerDbPath);

    // Open the database
    const indexingDb = new Database(indexerDbPath, { readonly: false });
    initializeIndexingDB({ db: indexingDb, isReadonly: false });

    if (!dbExists) {
        // Initialize a new database for this indexer
        console.log(`[${name}] Initializing new database at ${indexerDbPath}`);
        await indexer.initialize(indexingDb);
    }

    let hadSomethingToIndex = false;
    let consecutiveEmptyBatches = 0;
    const requiredEmptyBatches = 3;
    let needsPostCatchUpMaintenance = false;
    let needsPeriodicMaintenance = false;

    const runIndexing = indexingDb.transaction(() => {
        const lastIndexedTx = getIntValue(indexingDb, `lastIndexedTx_${name}`, -1);

        const getStart = performance.now();
        const transactions = blocksDb.getTxBatch(lastIndexedTx, 100000, indexer.usesTraces);
        const indexingStart = performance.now();
        hadSomethingToIndex = transactions.txs.length > 0;


        if (!hadSomethingToIndex) {
            consecutiveEmptyBatches++;

            // Check if blocks database has caught up and trigger maintenance if needed
            const blocksDbCaughtUp = blocksDb.getIsCaughtUp();
            const indexingDbCaughtUp = getIntValue(indexingDb, 'is_caught_up', -1);

            if (blocksDbCaughtUp === 1 && indexingDbCaughtUp !== 1) {
                // Blocks DB caught up but indexing DB hasn't - flag for post-catch-up maintenance
                console.log(`[${name}] Blocks DB caught up! Will trigger indexing DB post-catch-up maintenance...`);
                needsPostCatchUpMaintenance = true;
            } else if (blocksDbCaughtUp === 1 && indexingDbCaughtUp === 1) {
                // Both caught up - flag for periodic maintenance
                needsPeriodicMaintenance = true;
            }

            return;
        }

        consecutiveEmptyBatches = 0;

        indexer.handleTxBatch(indexingDb, blocksDb, transactions);

        const indexingFinish = performance.now();
        // Assuming txs have an 'id' or similar sequential identifier
        const lastTx = transactions.txs[transactions.txs.length - 1]!;
        setIntValue(indexingDb, `lastIndexedTx_${name}`, lastTx.txNum);

        console.log(
            `[${name}] Retrieved ${transactions.txs.length} txs in ${Math.round(indexingStart - getStart)}ms`,
            `Indexed ${transactions.txs.length} txs in ${Math.round(indexingFinish - indexingStart)}ms`
        );
    });

    try {
        if (exitWhenDone) {
            // Run until we have consecutive empty batches
            while (consecutiveEmptyBatches < requiredEmptyBatches) {
                runIndexing();

                // Perform maintenance outside of transaction
                if (needsPostCatchUpMaintenance) {
                    performIndexingPostCatchUpMaintenance(indexingDb);
                    needsPostCatchUpMaintenance = false;
                } else if (needsPeriodicMaintenance) {
                    performIndexingPeriodicMaintenance(indexingDb);
                    needsPeriodicMaintenance = false;
                }

                if (!hadSomethingToIndex) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }
            console.log(`[${name}] Indexing complete - no more blocks to process`);
        } else {
            // Run continuously
            while (true) {
                runIndexing();

                // Perform maintenance outside of transaction
                if (needsPostCatchUpMaintenance) {
                    performIndexingPostCatchUpMaintenance(indexingDb);
                    needsPostCatchUpMaintenance = false;
                } else if (needsPeriodicMaintenance) {
                    performIndexingPeriodicMaintenance(indexingDb);
                    needsPeriodicMaintenance = false;
                }

                if (!hadSomethingToIndex) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                }
            }
        }
    } finally {
        // Always close the database when done
        indexingDb.close();
    }
}

export async function startAllIndexers(options: IndexerOptions): Promise<void> {
    const { blocksDbPath, chainId, exitWhenDone = false, debugEnabled } = options;

    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: debugEnabled });

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadIndexingPlugins(getPluginDirs());

    // Start all indexers in parallel
    const indexerPromises = indexers.map(indexer =>
        startIndexer(indexer, blocksDb, chainId, exitWhenDone, debugEnabled)
    );

    await Promise.all(indexerPromises);

    // Clean up (only reached in exitWhenDone mode)
    blocksDb.close();
}

export async function startSingleIndexer(options: SingleIndexerOptions): Promise<void> {
    const { blocksDbPath, chainId, exitWhenDone = false, indexerName, debugEnabled } = options;

    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: debugEnabled });

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadIndexingPlugins(getPluginDirs());

    // Find the specific indexer
    const indexer = indexers.find(i => i.name === indexerName);
    if (!indexer) {
        throw new Error(`Indexer ${indexerName} not found`);
    }

    // Start the single indexer
    await startIndexer(indexer, blocksDb, chainId, exitWhenDone, debugEnabled);

    // Clean up
    blocksDb.close();
}

export async function getAvailableIndexers(): Promise<string[]> {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadIndexingPlugins(getPluginDirs());
    return indexers.map(i => i.name);
}
