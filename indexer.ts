import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin, TxBatch } from './lib/types.js';
import fs from 'node:fs';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs } from './config.js';
import sqlite3 from 'better-sqlite3';

const TXS_PER_LOOP = 50000;
const SLEEP_TIME = 3000;

export interface IndexerOptions {
    chainId: string;
    exitWhenDone?: boolean;
    debugEnabled: boolean;
}

export interface SingleIndexerOptions extends IndexerOptions {
    indexerName: string;
}

async function startIndexer(
    indexer: IndexingPlugin,
    blocksDb: BlocksDBHelper,
    chainId: string,
    exitWhenDone: boolean,
    debugEnabled: boolean
): Promise<void> {
    const chainConfig = getCurrentChainConfig();
    console.log(`[${indexer.name} - ${chainConfig.chainName}] Starting indexer v${indexer.version}`);
    const name = indexer.name;
    const version = indexer.version;
    const filterEvents: string[] | undefined = indexer.filterEvents;

    if (version < 0) {
        throw new Error(`Indexer ${name} has invalid version ${version}`);
    }

    // Get the db for this specific indexer with version checking
    const db = getSqliteDb({
        debugEnabled,
        type: "plugin",
        indexerName: name,
        pluginVersion: version,
        chainId: chainId,
        readonly: false,
    });

    // kv_int table is already created by getSqliteDb, no need to call initializeIndexingDB

    // Initialize indexer if needed
    const isIndexerInitialized = getIntValue(db, `isIndexerInitialized_${name}`, 0) === 1;

    if (!isIndexerInitialized) {
        // Initialize a new database for this indexer
        console.log(`[${name} - ${chainConfig.chainName}] Initializing new database`);

        // Combine indexer.initialize and setIntValue in one SQLite transaction
        const initializeTransaction = db.transaction(() => {
            indexer.initialize(db);
            setIntValue(db, `isIndexerInitialized_${name}`, 1);
        });

        initializeTransaction();
    }

    let hadSomethingToIndex = false;
    let consecutiveEmptyBatches = 0;
    const requiredEmptyBatches = 3;

    // Main indexing loop
    while (true) {
        // Get last indexed transaction from db (outside of transaction)
        const lastIndexedTx = getIntValue(db, `lastIndexedTx_${name}`, -1);

        // Fetch data from BlocksDBHelper (async operation)
        const getStart = performance.now();
        const transactions = blocksDb.getTxBatch(lastIndexedTx, TXS_PER_LOOP, indexer.usesTraces, filterEvents);
        const indexingStart = performance.now();
        hadSomethingToIndex = transactions.txs.length > 0;

        if (!hadSomethingToIndex) {
            consecutiveEmptyBatches++;

            // // Check if there are more transactions in the database that we haven't processed yet
            const totalTxCount = blocksDb.getTxCount();
            if (totalTxCount > lastIndexedTx) {
                setIntValue(db, `lastIndexedTx_${name}`, totalTxCount);
            }

            // Check if we should exit
            if (exitWhenDone && consecutiveEmptyBatches >= requiredEmptyBatches) {
                console.log(`[${name} - ${chainConfig.chainName}] No more transactions to index after ${requiredEmptyBatches} consecutive empty batches, exiting...`);
                break;
            }

            // Sleep briefly before next iteration
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        consecutiveEmptyBatches = 0;

        // Handle transaction batch in SQLite transaction
        const handleBatchTransaction = db.transaction(() => {
            indexer.handleTxBatch(db, blocksDb, transactions);
            setIntValue(db, `lastIndexedTx_${name}`, transactions.txs[transactions.txs.length - 1]!.txNum);
        });

        handleBatchTransaction();

        const indexingFinish = performance.now();

        // Get progress information (async operations outside transaction)
        const lastStoredBlock = blocksDb.getLastStoredBlockNumber();
        const lastIndexedBlock = transactions.txs[transactions.txs.length - 1]!.receipt.blockNumber;
        const lastIndexedBlockNum = parseInt(lastIndexedBlock);
        const indexingPercentage = ((lastIndexedBlockNum / lastStoredBlock) * 100).toFixed(2);

        console.log(
            `[${name} - ${chainConfig.chainName}] Retrieved ${transactions.txs.length} txs in ${Math.round(indexingStart - getStart)}ms`,
            `Indexed ${transactions.txs.length} txs in ${Math.round(indexingFinish - indexingStart)}ms`,
            `(${indexingPercentage}% - block ${lastIndexedBlockNum}/${lastStoredBlock})`
        );
    }

    console.log(`[${name} - ${chainConfig.chainName}] Indexer finished`);
    db.close();
}

export async function startAllIndexers(options: IndexerOptions): Promise<void> {
    const blocksDb = new BlocksDBHelper(
        getSqliteDb({
            debugEnabled: options.debugEnabled,
            type: "blocks",
            chainId: options.chainId,
            readonly: true,
        }),
        true,
        options.debugEnabled
    );

    //FIXME: figure out how to actually land evmChainID here and compare
    const evmChainId = blocksDb.getEvmChainId();
    // if (evmChainId !== parseInt(options.chainId)) {
    //     throw new Error(`BlocksDB chain ID mismatch: expected ${options.chainId}, got ${evmChainId}`);
    // }

    const pluginDirs = getPluginDirs();
    const indexers = await loadIndexingPlugins(pluginDirs);

    if (options.exitWhenDone) {
        // Run indexers sequentially when exitWhenDone is true
        for (const indexer of indexers) {
            await startIndexer(indexer, blocksDb, options.chainId, options.exitWhenDone, options.debugEnabled);
        }
    } else {
        // Run indexers in parallel
        const promises = indexers.map(indexer =>
            startIndexer(indexer, blocksDb, options.chainId, options.exitWhenDone || false, options.debugEnabled)
        );
        await Promise.all(promises);
    }
}

export async function startSingleIndexer(options: SingleIndexerOptions): Promise<void> {
    const blocksDb = new BlocksDBHelper(
        getSqliteDb({
            debugEnabled: options.debugEnabled,
            type: "blocks",
            chainId: options.chainId,
            readonly: true,
        }),
        true,
        options.debugEnabled
    );

    //FIXME: figure out how to actually land evmChainID here and compare
    const evmChainId = blocksDb.getEvmChainId();
    // if (evmChainId !== parseInt(options.chainId)) {
    //     throw new Error(`BlocksDB chain ID mismatch: expected ${options.chainId}, got ${evmChainId}`);
    // }

    const pluginDirs = getPluginDirs();
    const indexers = await loadIndexingPlugins(pluginDirs);
    const indexer = indexers.find(i => i.name === options.indexerName);

    if (!indexer) {
        throw new Error(`Indexer ${options.indexerName} not found`);
    }

    await startIndexer(indexer, blocksDb, options.chainId, options.exitWhenDone || false, options.debugEnabled);
    blocksDb.close();
}

export async function getAvailableIndexers(): Promise<string[]> {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadIndexingPlugins(getPluginDirs());
    return indexers.map(i => i.name);
}
