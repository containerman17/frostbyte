import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs, ChainConfig } from './config.js';
import sqlite3 from 'better-sqlite3';
import Piscina from 'piscina';
import { lookaheadManager } from './lib/lookaheadManager.js';
import os from 'node:os';
import executeIndexingTask from './indexer_worker.js';

let piscina: Piscina | null = null;

function getPiscina(): Piscina {
    if (!piscina) {
        piscina = new Piscina({
            filename: new URL('./indexer_worker.ts', import.meta.url).toString(),
            maxThreads: os.cpus().length,//use * 2 if fixed problems with memory
            execArgv: process.execArgv,
            env: {
                ...process.env,
                NODE_PATH: process.env['NODE_PATH']
            }
        });
    }
    return piscina;
}

const TXS_PER_LOOP = 50000;
const INLINE_THRESHOLD = TXS_PER_LOOP / 10; // Process inline if less than 5000 txs
const SLEEP_TIME = 3000;

export async function startIndexingLoopAllChains(chainConfigs: ChainConfig[]) {
    await Promise.all(chainConfigs.map(chainConfig => startIndexingLoop(chainConfig)));
}

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

        // Initialize indexer
        console.log(`[${indexer.name} - ${chainConfig.chainName}] Initializing database`);

        // Run initialization in a transaction
        const initializeTransaction = db.transaction(() => {
            indexer.initialize(db);
        });

        initializeTransaction();

        startPromises.push(startSingleIndexer(chainConfig, indexer, db, blocksDb));
    }

    await Promise.all(startPromises);
}
const startTime = performance.now();
async function startSingleIndexer(chainConfig: ChainConfig, indexer: IndexingPlugin<any>, db: sqlite3.Database, blocksDb: BlocksDBHelper) {
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

            // Full batches always use workers (they're always TXS_PER_LOOP = 50k)
            batchPromises.set(fromTx, getPiscina().run({
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
            const batchSize = toTx - lastIndexedTx;

            if (batchSize < INLINE_THRESHOLD) {
                // Process small batches inline to avoid worker overhead
                // Wrap sync call in Promise.resolve for type consistency
                batch = await Promise.resolve(executeIndexingTask({
                    chainConfig,
                    pluginName: indexer.name,
                    pluginVersion: indexer.version,
                    fromTx: lastIndexedTx,
                    toTx
                }));
            } else {
                // Use worker threads for larger batches
                batch = await getPiscina().run({
                    chainConfig,
                    pluginName: indexer.name,
                    pluginVersion: indexer.version,
                    fromTx: lastIndexedTx,
                    toTx
                });
            }
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
            const processingMode = batch.indexedTxs < INLINE_THRESHOLD ? 'inline' : 'worker';
            console.log(
                `[${indexer.name} - ${chainConfig.chainName}] Retrieved ${batch.indexedTxs} txs in ${Math.round(indexingStart - getStart)}ms (${processingMode})`,
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

