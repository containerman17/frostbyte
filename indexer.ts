import { BlockDB } from './blockFetcher/BlockDB';
import Database from 'better-sqlite3';
import { DEBUG_RPC_AVAILABLE } from './config';
import { loadPlugins } from './lib/plugins';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getIntValue, initializeIndexingDB, setIntValue } from './lib/dbHelper';
import { IndexerModule } from './lib/types';

export interface IndexerOptions {
    blocksDbPath: string;
    indexingDbPath: string;
    exitWhenDone?: boolean;
}

export interface SingleIndexerOptions extends IndexerOptions {
    indexerName: string;
}

async function startIndexer(
    indexer: IndexerModule,
    blocksDb: BlockDB,
    indexingDb: Database.Database,
    exitWhenDone: boolean
): Promise<void> {
    console.log(`[${indexer.name}] Starting indexer`);
    const name = indexer.name;
    const version = indexer.version;

    if (version < 0) {
        throw new Error(`Indexer ${name} has invalid version ${version}`);
    }

    const lastVersion = getIntValue(indexingDb, `indexer_version_${name}`, -1);

    if (lastVersion === -1) {
        // Initialize a new database for this indexer
        await indexer.initialize(indexingDb);
    } else if (lastVersion !== version) {
        // Wipe the database and initialize a new one
        await indexer.wipe(indexingDb);
        // Reset the last indexed transaction when wiping
        setIntValue(indexingDb, `lastIndexedTx_${name}`, -1);
        await indexer.initialize(indexingDb);
    }

    // Update the stored version
    setIntValue(indexingDb, `indexer_version_${name}`, version);

    let hadSomethingToIndex = false;
    let consecutiveEmptyBatches = 0;
    const requiredEmptyBatches = 3;

    const runIndexing = indexingDb.transaction(() => {
        const lastIndexedTx = getIntValue(indexingDb, `lastIndexedTx_${name}`, -1);

        const getStart = performance.now();
        const transactions = blocksDb.getTxBatch(lastIndexedTx, 10000, indexer.usesTraces);
        const indexingStart = performance.now();
        hadSomethingToIndex = transactions.txs.length > 0;


        if (!hadSomethingToIndex) {
            consecutiveEmptyBatches++;
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

    if (exitWhenDone) {
        // Run until we have consecutive empty batches
        while (consecutiveEmptyBatches < requiredEmptyBatches) {
            runIndexing();
            if (!hadSomethingToIndex) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        console.log(`[${name}] Indexing complete - no more blocks to process`);
    } else {
        // Run continuously
        while (true) {
            runIndexing();
            if (!hadSomethingToIndex) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
    }
}

export async function startAllIndexers(options: IndexerOptions): Promise<void> {
    const { blocksDbPath, indexingDbPath, exitWhenDone = false } = options;

    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: DEBUG_RPC_AVAILABLE });
    const indexingDb = new Database(indexingDbPath, { readonly: false });

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadPlugins([path.join(__dirname, 'plugins')]);

    initializeIndexingDB({ db: indexingDb, isReadonly: false });

    // Start all indexers in parallel
    const indexerPromises = indexers.map(indexer =>
        startIndexer(indexer, blocksDb, indexingDb, exitWhenDone)
    );

    await Promise.all(indexerPromises);

    // Clean up (only reached in exitWhenDone mode)
    blocksDb.close();
    indexingDb.close();
}

export async function startSingleIndexer(options: SingleIndexerOptions): Promise<void> {
    const { blocksDbPath, indexingDbPath, exitWhenDone = false, indexerName } = options;

    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: DEBUG_RPC_AVAILABLE });
    const indexingDb = new Database(indexingDbPath, { readonly: false });

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadPlugins([path.join(__dirname, 'plugins')]);

    // Find the specific indexer
    const indexer = indexers.find(i => i.name === indexerName);
    if (!indexer) {
        throw new Error(`Indexer ${indexerName} not found`);
    }

    initializeIndexingDB({ db: indexingDb, isReadonly: false });

    // Start the single indexer
    await startIndexer(indexer, blocksDb, indexingDb, exitWhenDone);

    // Clean up
    blocksDb.close();
    indexingDb.close();
}

export async function getAvailableIndexers(): Promise<string[]> {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadPlugins([path.join(__dirname, 'plugins')]);
    return indexers.map(i => i.name);
} 
