import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs, ChainConfig } from './config.js';
import sqlite3 from 'better-sqlite3';
import Piscina from 'piscina';
import { lookaheadManager, type LookaheadManager } from './lib/lookaheadManager.js';
import os from 'node:os';
import executeIndexingTask from './indexer_worker.js';

const piscina = new Piscina({
    filename: new URL('./indexer_worker.ts', import.meta.url).toString(),
    maxThreads: os.cpus().length,
    execArgv: process.execArgv
});

const TXS_PER_LOOP = 50000;
const SLEEP_TIME = 3000;

export async function startIndexingLoop(chainConfig: ChainConfig) {
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

    const startPromises = new Array<Promise<void>>();

    //Initialize indexers
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

        // Initialize indexer if needed
        const isIndexerInitialized = getIntValue(db, `isIndexerInitialized_${indexer.name}`, 0) === 1;

        if (!isIndexerInitialized) {
            // Initialize a new database for this indexer
            console.log(`[${indexer.name} - ${chainConfig.chainName}] Initializing new database`);

            // Combine indexer.initialize and setIntValue in one SQLite transaction
            const initializeTransaction = db.transaction(() => {
                indexer.initialize(db);
                setIntValue(db, `isIndexerInitialized_${indexer.name}`, 1);
            });

            initializeTransaction();
        }

        startPromises.push(startSingleIndexer(indexer, db, blocksDb));
    }

    await Promise.all(startPromises);
}
const startTime = performance.now();
async function startSingleIndexer(indexer: IndexingPlugin<any>, db: sqlite3.Database, blocksDb: BlocksDBHelper) {
    const chainConfig = getCurrentChainConfig();

    const batchPromises = new Map<number, Promise<{ extractedData: any, indexedTxs: number }>>();

    // Main indexing loop
    while (true) {
        // Get last indexed transaction from db (outside of transaction)
        const lastIndexedTx = getIntValue(db, `lastIndexedTx_${indexer.name}`, -1);
        const totalTxCount = blocksDb.getTxCount();

        if (lastIndexedTx >= totalTxCount) {
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        const getStart = performance.now();

        // Only lookahead for WHOLE batches
        for (let i = 0; i < lookaheadManager.getCurrentLookahead(); i++) {
            const fromTx = lastIndexedTx + i * TXS_PER_LOOP;
            const toTx = lastIndexedTx + (i + 1) * TXS_PER_LOOP;

            // Skip if this would be a partial batch
            if (toTx > totalTxCount) {
                break;
            }

            if (batchPromises.has(fromTx)) {
                continue;
            }

            batchPromises.set(fromTx, piscina.run({
                chainConfig,
                pluginName: indexer.name,
                pluginVersion: indexer.version,
                fromTx,
                toTx
            }));
        }

        // Check if we have a pre-fetched batch or need to process final partial batch
        let batch: Awaited<ReturnType<typeof executeIndexingTask>>;
        let processedToTx: number;

        if (batchPromises.has(lastIndexedTx)) {
            // Use pre-fetched whole batch
            batch = await batchPromises.get(lastIndexedTx)!;
            batchPromises.delete(lastIndexedTx);
            processedToTx = lastIndexedTx + TXS_PER_LOOP;
        } else if (lastIndexedTx < totalTxCount) {
            // Process final partial batch (not pre-fetched)
            const toTx = Math.min(totalTxCount, lastIndexedTx + TXS_PER_LOOP);
            batch = await piscina.run({
                chainConfig,
                pluginName: indexer.name,
                pluginVersion: indexer.version,
                fromTx: lastIndexedTx,
                toTx
            });
            processedToTx = toTx;
        } else {
            // No work to do
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        const indexingStart = performance.now();

        // Save extracted data in SQLite transaction
        const saveDataTransaction = db.transaction(() => {
            indexer.saveExtractedData(db, blocksDb, batch.extractedData);
            setIntValue(db, `lastIndexedTx_${indexer.name}`, processedToTx);
        });

        saveDataTransaction();

        const indexingFinish = performance.now();

        // Get progress information
        const lastStoredBlock = blocksDb.getLastStoredBlockNumber();
        const indexingPercentage = ((lastIndexedTx / lastStoredBlock) * 100).toFixed(2);

        if (batch.indexedTxs > 0) {
            console.log(
                `[${indexer.name} - ${chainConfig.chainName}] Retrieved ${batch.indexedTxs} txs in ${Math.round(indexingStart - getStart)}ms`,
                `Indexed ${batch.indexedTxs} txs in ${Math.round(indexingFinish - indexingStart)}ms`,
                `(${indexingPercentage}% - tx ${lastIndexedTx}/${totalTxCount}, queue: ${batchPromises.size}, lookahead: ${lookaheadManager.getCurrentLookahead()})`,
                `Total time: ${Math.round((performance.now() - startTime) / 1000)}s`
            );
        } else {
            // Debug when no work is being processed
            console.log(`[${indexer.name}] DEBUG: No work processed. Queue: ${batchPromises.size}, Lookahead: ${lookaheadManager.getCurrentLookahead()}, LastTx: ${lastIndexedTx}/${totalTxCount}`);
        }
    }
}

