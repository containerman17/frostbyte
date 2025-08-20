import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper.js';
import { loadIndexingPlugins } from './lib/plugins.js';
import { getIntValue, setIntValue } from './lib/dbHelper.js';
import { IndexingPlugin } from './lib/types.js';
import { getCurrentChainConfig, getSqliteDb, getPluginDirs, ChainConfig } from './config.js';
import sqlite3 from 'better-sqlite3';
import Piscina from 'piscina';
import os from 'os';

const cpuThreads = os.cpus().length;

// Dynamic lookahead configuration
let currentLookaheadBatches = 0; // Start at middle
const MIN_LOOKAHEAD = 0;
const MAX_LOOKAHEAD = 10;

// Simple resource monitor
function startResourceMonitor() {
    let prevCpus = os.cpus();

    setInterval(() => {
        // Calculate CPU usage
        const currCpus = os.cpus();
        let totalIdle = 0;
        let totalTick = 0;

        for (let i = 0; i < currCpus.length; i++) {
            const prevCpu = prevCpus[i]!;
            const currCpu = currCpus[i]!;

            const idleDelta = currCpu.times.idle - prevCpu.times.idle;
            let totalDelta = 0;

            for (const type in currCpu.times) {
                totalDelta += currCpu.times[type as keyof typeof currCpu.times] -
                    prevCpu.times[type as keyof typeof prevCpu.times];
            }

            totalIdle += idleDelta;
            totalTick += totalDelta;
        }

        const cpuUsage = 1 - (totalIdle / totalTick);
        prevCpus = currCpus;


        // Calculate memory usage
        const memUsage = (os.totalmem() - os.freemem()) / os.totalmem();

        // Adjust lookahead batches
        const oldValue = currentLookaheadBatches;
        if (cpuUsage > 0.9 || memUsage > 0.9) {
            currentLookaheadBatches = Math.max(MIN_LOOKAHEAD, currentLookaheadBatches - 1);
        } else {
            currentLookaheadBatches = Math.min(MAX_LOOKAHEAD, currentLookaheadBatches + 1);
        }

        if (oldValue !== currentLookaheadBatches) {
            console.log(`Lookahead batches: ${oldValue} → ${currentLookaheadBatches} (CPU: ${(cpuUsage * 100).toFixed(1)}%, Mem: ${(memUsage * 100).toFixed(1)}%)`);
        }
    }, 500 + 500 * Math.random()); // Check every 0.5-1 seconds
}

// Start monitoring
startResourceMonitor();

const piscina = new Piscina({
    filename: new URL('./indexer_worker.ts', import.meta.url).toString(),
    maxThreads: cpuThreads,
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

interface BatchJob {
    fromTx: number;
    toTx: number;
    promise: Promise<{ extractedData: any, indexedTxs: number }>;
}

class BatchQueue {
    private queue: BatchJob[] = [];
    private readonly batchSize: number;
    private readonly piscina: Piscina;
    private readonly chainConfig: ChainConfig;
    private readonly indexerName: string;
    private readonly indexerVersion: number;

    constructor(
        batchSize: number,
        piscina: Piscina,
        chainConfig: ChainConfig,
        indexerName: string,
        indexerVersion: number
    ) {
        this.batchSize = batchSize;
        this.piscina = piscina;
        this.chainConfig = chainConfig;
        this.indexerName = indexerName;
        this.indexerVersion = indexerVersion;
    }

    async getNextBatch(lastIndexedTx: number, totalTxCount: number): Promise<{
        batch: { extractedData: any, indexedTxs: number },
        processedToTx: number
    } | null> {
        // Remove any outdated batches from the front of the queue
        while (this.queue.length > 0 && this.queue[0]?.fromTx !== lastIndexedTx) {
            this.queue.shift();
        }

        // Check if we need to wait for new data
        const nextToTx = Math.min(totalTxCount, lastIndexedTx + this.batchSize);
        if (nextToTx <= lastIndexedTx) {
            return null;
        }

        // Fill the queue with new batches
        this.fillQueue(lastIndexedTx, totalTxCount);

        // Get the first batch if available
        if (this.queue.length === 0) {
            // No batches available, create one on demand
            const promise = this.createBatch(lastIndexedTx, nextToTx);
            const batch = await promise;
            return { batch, processedToTx: nextToTx };
        }

        const job = this.queue.shift()!;
        const batch = await job.promise;
        return { batch, processedToTx: job.toTx };
    }

    private fillQueue(lastIndexedTx: number, totalTxCount: number): void {
        // Calculate the next batch start based on queue state
        let nextFromTx = lastIndexedTx;
        if (this.queue.length > 0) {
            const lastQueued = this.queue[this.queue.length - 1];
            if (lastQueued) {
                nextFromTx = lastQueued.toTx;
            }
        }

        // Fill the queue up to current dynamic limit
        while (this.queue.length < currentLookaheadBatches) {
            const toTx = Math.min(totalTxCount, nextFromTx + this.batchSize);

            // Skip empty batches
            if (toTx <= nextFromTx) {
                break;
            }

            const promise = this.createBatch(nextFromTx, toTx);
            this.queue.push({ fromTx: nextFromTx, toTx, promise });

            nextFromTx = toTx;
        }
    }

    private createBatch(fromTx: number, toTx: number): Promise<{ extractedData: any, indexedTxs: number }> {
        return this.piscina.run({
            chainConfig: this.chainConfig,
            pluginName: this.indexerName,
            pluginVersion: this.indexerVersion,
            fromTx: fromTx,
            toTx: toTx
        });
    }

    getQueueSize(): number {
        return this.queue.length;
    }
}

const startTime = performance.now();
async function startSingleIndexer(indexer: IndexingPlugin<any>, db: sqlite3.Database, blocksDb: BlocksDBHelper) {
    const chainConfig = getCurrentChainConfig();

    // Create batch queue
    const batchQueue = new BatchQueue(
        TXS_PER_LOOP,
        piscina,
        chainConfig,
        indexer.name,
        indexer.version
    );

    // Main indexing loop
    while (true) {
        // Get last indexed transaction from db (outside of transaction)
        const lastIndexedTx = getIntValue(db, `lastIndexedTx_${indexer.name}`, -1);

        // Fetch data from BlocksDBHelper (async operation)
        const getStart = performance.now();

        const totalTxCount = blocksDb.getTxCount();

        // Get next batch from queue

        if (lastIndexedTx === totalTxCount) {
            console.log(`[${indexer.name} - ${chainConfig.chainName}] No more transactions to process`);
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        const result = await batchQueue.getNextBatch(lastIndexedTx, totalTxCount);

        if (!result) {
            // No more transactions to process
            await new Promise(resolve => setTimeout(resolve, SLEEP_TIME));
            continue;
        }

        const { batch, processedToTx } = result;
        const { extractedData, indexedTxs } = batch;

        const indexingStart = performance.now();

        // Save extracted data in SQLite transaction
        const saveDataTransaction = db.transaction(() => {
            indexer.saveExtractedData(db, blocksDb, extractedData);
            setIntValue(db, `lastIndexedTx_${indexer.name}`, processedToTx);
        });

        saveDataTransaction();

        const indexingFinish = performance.now();

        // Get progress information (async operations outside transaction)
        const lastStoredBlock = blocksDb.getLastStoredBlockNumber();
        const indexingPercentage = ((lastIndexedTx / lastStoredBlock) * 100).toFixed(2);

        if (indexedTxs > 0) {
            console.log(
                `[${indexer.name} - ${chainConfig.chainName}] Retrieved ${indexedTxs} txs in ${Math.round(indexingStart - getStart)}ms`,
                `Indexed ${indexedTxs} txs in ${Math.round(indexingFinish - indexingStart)}ms`,
                `(${indexingPercentage}% - tx ${lastIndexedTx}/${totalTxCount}, queue: ${batchQueue.getQueueSize()}/${currentLookaheadBatches})`,
                `Total time: ${Math.round((performance.now() - startTime) / 1000)}s`
            );
        }
    }
}
