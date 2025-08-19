import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs, ChainConfig } from './config.js';
import sqlite3 from 'better-sqlite3';
import Piscina from 'piscina';

const piscina = new Piscina({
    filename: new URL('./indexer_worker.ts', import.meta.url).toString(),
    maxThreads: 10,
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

    // Main indexing loop
    while (true) {
        // Get last indexed transaction from db (outside of transaction)
        const lastIndexedTx = getIntValue(db, `lastIndexedTx_${indexer.name}`, -1);

        // Fetch data from BlocksDBHelper (async operation)
        const getStart = performance.now();

        const maxTx = Math.min(blocksDb.getTxCount(), lastIndexedTx + TXS_PER_LOOP);
        if (maxTx <= lastIndexedTx) {
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        const { extractedData, indexedTxs } = await piscina.run({
            chainConfig,
            pluginName: indexer.name,
            pluginVersion: indexer.version,
            fromTx: lastIndexedTx,
            toTx: maxTx
        });

        const indexingStart = performance.now();

        // Save extracted data in SQLite transaction
        const saveDataTransaction = db.transaction(() => {
            indexer.saveExtractedData(db, blocksDb, extractedData);
            setIntValue(db, `lastIndexedTx_${indexer.name}`, maxTx);
        });

        saveDataTransaction();

        const indexingFinish = performance.now();

        // Get progress information (async operations outside transaction)
        const lastStoredBlock = blocksDb.getLastStoredBlockNumber();
        const indexingPercentage = ((lastIndexedTx ?? 0) / lastStoredBlock * 100).toFixed(2);

        if (indexedTxs > 0) {
            console.log(
                `[${indexer.name} - ${chainConfig.chainName}] Retrieved ${indexedTxs} txs in ${Math.round(indexingStart - getStart)}ms`,
                `Indexed ${indexedTxs} txs in ${Math.round(indexingFinish - indexingStart)}ms`,
                `(${indexingPercentage}% - block ${lastIndexedTx}/${lastStoredBlock})`,
                `Total time: ${Math.round((performance.now() - startTime) / 1000)}s`
            );
        }
    }
}
