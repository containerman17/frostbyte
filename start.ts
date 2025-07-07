import cluster, { Worker } from 'node:cluster';
import fs from 'node:fs';
import path from 'node:path';
import { BlockDB } from './blockFetcher/BlockDB';
import { startFetchingLoop } from './blockFetcher/startFetchingLoop';
import { BatchRpc } from './blockFetcher/BatchRpc';
import { DATA_DIR, CHAIN_CONFIGS, getCurrentChainConfig } from './config';
import { createApiServer } from './server';
import { startSingleIndexer, getAvailableIndexers } from './indexer';
import { getBlocksDbPath } from './lib/dbPaths';

if (cluster.isPrimary) {
    const roles = process.env['ROLES']?.split(',') || ['fetcher', 'api', 'indexer'];

    for (let config of CHAIN_CONFIGS) {
        // Spawn workers based on roles
        for (const role of roles) {
            if (role === 'indexer') {
                // Discover all available indexers and spawn one worker for each
                const availableIndexers = await getAvailableIndexers();
                console.log(`Discovered ${availableIndexers.length} indexers: ${availableIndexers.join(', ')}`);

                for (const indexerName of availableIndexers) {
                    const worker = cluster.fork({
                        ROLE: 'indexer',
                        INDEXER_NAME: indexerName,
                        CHAIN_ID: config.blockchainId,
                    });
                    console.log(`Spawned worker for indexer: ${indexerName}`);
                }
            } else {
                // Spawn single worker for other roles
                cluster.fork({ ROLE: role, CHAIN_ID: config.blockchainId });
            }
        }
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

    // If any worker dies, kill everything
    cluster.on('exit', (worker, code, signal) => {
        console.error(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`);
        console.error('Terminating primary process as a worker has died');
        killAllWorkers();
        process.exit(1);
    });
} else {
    if (process.env['ROLE'] === 'fetcher') {
        const chainConfig = getCurrentChainConfig();
        const blocksDbPath = getBlocksDbPath(chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug);
        const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: false, hasDebug: chainConfig.rpcConfig.rpcSupportsDebug });
        const batchRpc = new BatchRpc(chainConfig.rpcConfig);
        startFetchingLoop(blocksDb, batchRpc, chainConfig.rpcConfig.blocksPerBatch);
    } else if (process.env['ROLE'] === 'api') {
        //awaits both files as it is read only for both
        const chainConfig = getCurrentChainConfig();
        const blocksDbPath = getBlocksDbPath(chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug);
        await awaitFileExists(blocksDbPath);

        // Wait for at least one indexer database to exist
        // The API server will handle missing databases gracefully
        const indexingDbBaseDir = path.dirname(blocksDbPath);
        await awaitIndexerDatabases(indexingDbBaseDir, chainConfig.rpcConfig.rpcSupportsDebug);

        const apiServer = await createApiServer(blocksDbPath, chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug);
        apiServer.start(3000);
    } else if (process.env['ROLE'] === 'indexer') {
        const chainConfig = getCurrentChainConfig();
        const blocksDbPath = getBlocksDbPath(chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug);
        await awaitFileExists(blocksDbPath);

        // Each indexer worker must have a specific indexer name
        const indexerName = process.env['INDEXER_NAME'];
        if (!indexerName) {
            throw new Error('INDEXER_NAME environment variable is required for indexer workers');
        }

        console.log(`Starting indexer worker for: ${indexerName}`);
        await startSingleIndexer({
            blocksDbPath,
            chainId: chainConfig.blockchainId,
            indexerName,
            exitWhenDone: false,
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug
        });
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

async function awaitIndexerDatabases(baseDir: string, debugEnabled: boolean, maxMs: number = 30 * 1000, intervalMs: number = 500) {
    const startTime = Date.now();

    // Get list of available indexers to know what databases to expect
    const availableIndexers = await getAvailableIndexers();
    console.log(`API worker waiting for ${availableIndexers.length} indexer databases...`);

    while (true) {
        // Check if directory exists
        if (!fs.existsSync(baseDir)) {
            fs.mkdirSync(baseDir, { recursive: true });
        }

        // Look for any indexer database files
        const files = fs.readdirSync(baseDir);
        const indexerDbPattern = debugEnabled
            ? /^indexing_.*_v\d+\.db$/
            : /^indexing_.*_v\d+_no_dbg\.db$/;

        const indexerDbs = files.filter(f => indexerDbPattern.test(f));

        if (indexerDbs.length > 0) {
            console.log(`Found ${indexerDbs.length} indexer database(s): ${indexerDbs.join(', ')}`);
            return;
        }

        await new Promise(resolve => setTimeout(resolve, intervalMs));
        if (Date.now() - startTime > maxMs) {
            throw new Error(`No indexer databases found in ${baseDir} after ${maxMs} ms`);
        }
    }
}
