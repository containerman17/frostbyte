import type { IndexerModule } from "../lib/types";
import { initializeBatchProcessor, processBatch, BatchProcessorState } from "./glacierMetrics_lib/processors";
import { registerMetricsRoutes } from "./glacierMetrics_lib/routes";

// Global state for batch processing
let batchProcessorState: BatchProcessorState | null = null;

// Lifecycle hooks
const wipe: IndexerModule["wipe"] = (db) => {
    // Drop all metrics tables to start fresh
    db.exec(`DROP TABLE IF EXISTS metrics`);
    db.exec(`DROP TABLE IF EXISTS active_senders`);
    db.exec(`DROP TABLE IF EXISTS active_addresses`);

    // Reset state
    batchProcessorState = null;
};

const initialize: IndexerModule["initialize"] = (db) => {
    // Create the main metrics table
    db.exec(`
        CREATE TABLE IF NOT EXISTS metrics (
            timeInterval INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            metric INTEGER NOT NULL,
            value INTEGER NOT NULL,
            PRIMARY KEY (timeInterval, timestamp, metric)
        ) WITHOUT ROWID
    `);

    // Create table to track active senders per time period
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_senders (
            timeInterval INTEGER NOT NULL,
            periodTimestamp INTEGER NOT NULL,
            address TEXT NOT NULL,
            PRIMARY KEY (timeInterval, periodTimestamp, address)
        ) WITHOUT ROWID
    `);

    // Create index for efficient counting
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_active_senders_period 
        ON active_senders(timeInterval, periodTimestamp)
    `);

    // Create table to track active addresses (both from and to) per time period
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_addresses (
            timeInterval INTEGER NOT NULL,
            periodTimestamp INTEGER NOT NULL,
            address TEXT NOT NULL,
            PRIMARY KEY (timeInterval, periodTimestamp, address)
        ) WITHOUT ROWID
    `);

    // Create index for efficient counting
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_active_addresses_period 
        ON active_addresses(timeInterval, periodTimestamp)
    `);

    // Initialize batch processor state
    batchProcessorState = initializeBatchProcessor(db);
};

// Batch processor (required)
const handleTxBatch: IndexerModule["handleTxBatch"] = (db, blocksDb, batch) => {
    // Ensure state is initialized
    if (!batchProcessorState) {
        batchProcessorState = initializeBatchProcessor(db);
    }

    // Process the batch
    processBatch(db, blocksDb, batch, batchProcessorState);
};

// Optional HTTP surface
const registerRoutes: IndexerModule["registerRoutes"] = (app, db, blocksDb) => {
    registerMetricsRoutes(app, db, blocksDb);
};

const module: IndexerModule = {
    name: "glacierMetrics",
    version: 1,
    usesTraces: true, // We use traces for contract deployment counting
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
