import cluster, { Worker } from 'node:cluster';
import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper';
import { startFetchingLoop } from './blockFetcher/startFetchingLoop.js';
import { BatchRpc } from './blockFetcher/BatchRpc.js';
import { CHAIN_CONFIGS, getCurrentChainConfig, getSqliteDb } from './config.js';
import { createApiServer } from './api.js';
import { startSingleIndexer, getAvailableIndexers } from './indexer.js';

// Log any uncaught exceptions or promise rejections to aid debugging of worker crashes
process.on('unhandledRejection', reason => {
    console.error('Unhandled promise rejection:', reason);
});
process.on('uncaughtException', error => {
    console.error('Uncaught exception:', error);
});

if (cluster.isPrimary) {
    const roles = process.env['ROLES']?.split(',') || ['fetcher', 'api', 'indexer'];
    let apiStarted = false;
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
                    console.log(`Spawned worker for indexer: ${indexerName}, PID: ${worker.process.pid}, Chain ID: ${config.blockchainId}`);
                }
            } else if (role === 'fetcher') {
                // Spawn single worker for other roles
                const worker = cluster.fork({ ROLE: role, CHAIN_ID: config.blockchainId });
                console.log(`Spawned worker for role: ${role}, PID: ${worker.process.pid}, Chain ID: ${config.blockchainId}`);
            } else if (role === 'api') {
                // API would be started as one process for all chains
                if (!apiStarted) {
                    const worker = cluster.fork({ ROLE: role });
                    console.log(`Spawned worker for role: ${role}, PID: ${worker.process.pid}`);
                    apiStarted = true;
                }
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
    cluster.on('exit', (worker: Worker, code, signal) => {
        console.error(`Terminating primary process as a worker ${worker.process.pid} has died with code ${code} and signal ${signal}`);
        killAllWorkers();
        process.exit(1);
    });
} else {
    if (process.env['ROLE'] === 'fetcher') {
        const chainConfig = getCurrentChainConfig();
        const pool = await getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "blocks",
            chainId: chainConfig.blockchainId,
        });
        const blocksDb = await BlocksDBHelper.createFromPool(pool, {
            isReadonly: false,
            hasDebug: chainConfig.rpcConfig.rpcSupportsDebug
        });
        const batchRpc = new BatchRpc(chainConfig.rpcConfig);
        startFetchingLoop(blocksDb, batchRpc, chainConfig.rpcConfig.blocksPerBatch, chainConfig.chainName);
    } else if (process.env['ROLE'] === 'api') {
        const apiServer = await createApiServer(CHAIN_CONFIGS);
        const port = parseInt(process.env['PORT'] || '3080', 10);
        await apiServer.start(port);
    } else if (process.env['ROLE'] === 'indexer') {
        const chainConfig = getCurrentChainConfig();
        const pool = await getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "blocks",
            chainId: chainConfig.blockchainId,
        });

        // Each indexer worker must have a specific indexer name
        const indexerName = process.env['INDEXER_NAME'];
        if (!indexerName) {
            throw new Error('INDEXER_NAME environment variable is required for indexer workers');
        }

        console.log(`Starting indexer worker for: ${indexerName}`);
        await startSingleIndexer({
            pool,
            chainId: chainConfig.blockchainId,
            indexerName,
            exitWhenDone: false,
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug
        });
    } else {
        throw new Error('unknown role');
    }
}
