import { BlockDB } from './blockFetcher/BlockDB';
import Database from 'better-sqlite3';
import { executePragmas, IndexingDbHelper } from './indexers/dbHelper';
import { Indexer } from './indexers/types';
import { createRPCIndexer } from './indexers/rpc';
import { createMetricsIndexer } from './indexers/metrics/index';
import { createTeleporterMetricsIndexer } from './indexers/teleporterMetrics';
import { createInfoIndexer } from './indexers/info';
import { createSanityChecker } from './indexers/sanityChecker';
import { IS_DEVELOPMENT, DEBUG_RPC_AVAILABLE } from './config';

export interface IndexerOptions {
    blocksDbPath: string;
    indexingDbPath: string;
    exitWhenDone?: boolean;
}

export async function startIndexer(options: IndexerOptions): Promise<void> {
    const { blocksDbPath, indexingDbPath, exitWhenDone = false } = options;

    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: DEBUG_RPC_AVAILABLE });
    const indexingDb = new Database(indexingDbPath, { readonly: false });
    const indexingDbHelper = new IndexingDbHelper(indexingDb);

    const indexerFactories = [
        createRPCIndexer,
        createMetricsIndexer,
        createTeleporterMetricsIndexer,
        createInfoIndexer,
    ];
    if (IS_DEVELOPMENT) {
        indexerFactories.push(createSanityChecker);
    }

    const indexers: Indexer[] = indexerFactories.map(factory => {
        const indexer = factory(blocksDb, indexingDb);
        indexer.initialize();
        return indexer;
    });

    await executePragmas({ db: indexingDb, isReadonly: false });

    let hadSomethingToIndex = false;
    let consecutiveEmptyBatches = 0;
    const requiredEmptyBatches = 3; // For exit mode, wait for 3 empty batches

    const runIndexing = indexingDb.transaction((lastIndexedBlock) => {
        const getStart = performance.now();
        const blocks = blocksDb.getBlocks(lastIndexedBlock + 1, 10 * 1000); // batches of 10k txs
        const indexingStart = performance.now();
        hadSomethingToIndex = blocks.length > 0;

        if (!hadSomethingToIndex) {
            consecutiveEmptyBatches++;
            return;
        }

        consecutiveEmptyBatches = 0;
        let debugTxCount = 0;

        const timeSpentPerIndexer = new Map<string, number>();


        for (const indexer of indexers) {
            const indexerStart = performance.now();
            indexer.indexBlocks(blocks);
            const indexerFinish = performance.now();
            timeSpentPerIndexer.set(indexer.constructor.name, (timeSpentPerIndexer.get(indexer.constructor.name) || 0) + (indexerFinish - indexerStart));
        }
        for (const block of blocks) {
            debugTxCount += block.txs.length
        }

        console.log('Time spent per indexer:');
        for (const [indexerName, timeSpent] of timeSpentPerIndexer) {
            console.log(`   ${indexerName}: ${timeSpent.toFixed(2)}ms`);
        }

        const indexingFinish = performance.now();
        indexingDbHelper.setInteger('lastIndexedBlock', blocks[blocks.length - 1]!.block.number);
        console.log('Indexed', debugTxCount, 'txs in', Math.round(indexingStart - getStart), 'ms', 'indexing', Math.round(indexingFinish - indexingStart), 'ms');
    });

    if (exitWhenDone) {
        // Run until we have consecutive empty batches
        while (consecutiveEmptyBatches < requiredEmptyBatches) {
            runIndexing(indexingDbHelper.getInteger('lastIndexedBlock', -1));
            if (!hadSomethingToIndex) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
        console.log('Indexing complete - no more blocks to process');
        blocksDb.close();
        indexingDb.close();
    } else {
        // Run continuously
        while (true) {
            runIndexing(indexingDbHelper.getInteger('lastIndexedBlock', -1));
            if (!hadSomethingToIndex) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }
    }
} 
