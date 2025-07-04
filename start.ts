
import cluster from 'node:cluster';
import fs from 'node:fs';
import path from 'node:path';
import { BlockDB } from './blockFetcher/BlockDB';
import { startFetchingLoop } from './blockFetcher/startFetchingLoop';
import { BatchRpc } from './blockFetcher/BatchRpc';
import { RPC_URL, CHAIN_ID, DATA_DIR, RPS, REQUEST_BATCH_SIZE, MAX_CONCURRENT, BLOCKS_PER_BATCH, DEBUG_RPC_AVAILABLE, TEST_KILL_INDEXER_WHEN_DONE } from './config';
import { createApiServer } from './server';
import { startIndexer } from './indexer';


const blocksDbPath = path.join(DATA_DIR, CHAIN_ID, DEBUG_RPC_AVAILABLE ? 'blocks.db' : 'blocks_no_dbg.db');
const indexingDbPath = path.join(path.dirname(blocksDbPath), DEBUG_RPC_AVAILABLE ? 'indexing.db' : 'indexing_no_dbg.db');
if (!fs.existsSync(blocksDbPath)) {
    fs.mkdirSync(path.dirname(blocksDbPath), { recursive: true });
}



if (cluster.isPrimary) {
    const roles = process.env['ROLES']?.split(',') || ['fetcher', 'api', 'indexer'];

    // spawn one writer, one reader, one misc-job worker
    for (const role of roles) {
        cluster.fork({ ROLE: role });
    }

    // Kill all workers when parent exits
    const killAllWorkers = () => {
        for (const id in cluster.workers) {
            cluster.workers[id]?.kill();
        }
    };

    // Handle various exit scenarios
    process.on('SIGINT', () => {
        console.log('SIGINT received, killing all workers...');
        killAllWorkers();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('SIGTERM received, killing all workers...');
        killAllWorkers();
        process.exit(0);
    });

    process.on('exit', () => {
        killAllWorkers();
    });

    // If any worker dies, kill the entire process
    cluster.on('exit', (worker, code, signal) => {
        console.error(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
        console.error('Terminating primary process as a worker has died');
        killAllWorkers();
        process.exit(1);
    });
} else {
    if (process.env['ROLE'] === 'fetcher') {
        const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: false, hasDebug: DEBUG_RPC_AVAILABLE });
        const batchRpc = new BatchRpc({
            rpcUrl: RPC_URL,
            batchSize: REQUEST_BATCH_SIZE,
            maxConcurrent: MAX_CONCURRENT,
            rps: RPS,
            enableBatchSizeGrowth: false,
            rpcSupportsDebug: DEBUG_RPC_AVAILABLE,
        });
        startFetchingLoop(blocksDb, batchRpc, BLOCKS_PER_BATCH);
    } else if (process.env['ROLE'] === 'api') {
        //awaits both files as it is read only for both
        await awaitFileExists(indexingDbPath);
        await awaitFileExists(blocksDbPath);

        const apiServer = createApiServer(blocksDbPath, indexingDbPath);
        apiServer.start(3000);
    } else if (process.env['ROLE'] === 'indexer') {
        await awaitFileExists(blocksDbPath);

        await startIndexer({
            blocksDbPath,
            indexingDbPath,
            exitWhenDone: TEST_KILL_INDEXER_WHEN_DONE
        });

        if (TEST_KILL_INDEXER_WHEN_DONE) {
            process.exit(0);
        }
    } else {
        throw new Error('unknown role');
    }
}


async function awaitFileExists(path: string, maxMs: number = 3 * 1000, intervalMs: number = 100) {
    const startTime = Date.now();
    while (true) {
        if (fs.existsSync(path)) {
            return;
        }
        await new Promise(resolve => setTimeout(resolve, intervalMs));
        if (Date.now() - startTime > maxMs) {
            throw new Error(`File ${path} did not exist after ${maxMs} ms`);
        }
    }
}
