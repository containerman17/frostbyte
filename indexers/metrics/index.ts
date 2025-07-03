import SQLite from "better-sqlite3";
import { BlockDB } from "../../blockFetcher/BlockDB";
import { CreateIndexerFunction, Indexer } from "../types";
import { LazyTx } from "../../blockFetcher/lazy/LazyTx";
import { LazyBlock } from "../../blockFetcher/lazy/LazyBlock";
import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import { LazyTraces } from "../../blockFetcher/lazy/LazyTrace";

// Import schemas
import { MetricQuerySchema, MetricResponseSchema } from "./schemas";

// Import utils and constants
import {
    TIME_INTERVAL_HOUR,
    TIME_INTERVAL_DAY,
    TIME_INTERVAL_WEEK,
    TIME_INTERVAL_MONTH,
    METRIC_txCount,
    METRIC_cumulativeContracts,
    METRIC_cumulativeTxCount,
    METRIC_activeSenders,
    METRIC_activeAddresses,
    isCumulativeMetric,
    normalizeTimestamp,
    getTimeIntervalId
} from "./utils";

// Import query handlers
import { handleIncrementalMetricQuery, handleCumulativeMetricQuery } from "./query";

// Import metric handlers
import { countContractDeployments } from "./handlers/ContractMetrics";
import { countTransactions } from "./handlers/TxMetrics";

// Define available metrics
const METRICS = {
    txCount: METRIC_txCount,
    cumulativeContracts: METRIC_cumulativeContracts,
    cumulativeTxCount: METRIC_cumulativeTxCount,
    activeSenders: METRIC_activeSenders,
    activeAddresses: METRIC_activeAddresses,
} as const;

class MetricsIndexer implements Indexer {
    private cumulativeContractCount = 0;
    private cumulativeTxCount = 0;

    constructor(private blocksDb: BlockDB, private indexingDb: SQLite.Database) { }

    initialize(): void {
        this.indexingDb.exec(`
            CREATE TABLE IF NOT EXISTS metrics (
                timeInterval INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                metric INTEGER NOT NULL,
                value INTEGER NOT NULL,
                PRIMARY KEY (timeInterval, timestamp, metric)
            ) WITHOUT ROWID
        `);

        // Create table to track active senders per time period
        this.indexingDb.exec(`
            CREATE TABLE IF NOT EXISTS active_senders (
                timeInterval INTEGER NOT NULL,
                periodTimestamp INTEGER NOT NULL,
                address TEXT NOT NULL,
                PRIMARY KEY (timeInterval, periodTimestamp, address)
            ) WITHOUT ROWID
        `);

        // Create index for efficient counting
        this.indexingDb.exec(`
            CREATE INDEX IF NOT EXISTS idx_active_senders_period 
            ON active_senders(timeInterval, periodTimestamp)
        `);

        // Create table to track active addresses (both from and to) per time period
        this.indexingDb.exec(`
            CREATE TABLE IF NOT EXISTS active_addresses (
                timeInterval INTEGER NOT NULL,
                periodTimestamp INTEGER NOT NULL,
                address TEXT NOT NULL,
                PRIMARY KEY (timeInterval, periodTimestamp, address)
            ) WITHOUT ROWID
        `);

        // Create index for efficient counting
        this.indexingDb.exec(`
            CREATE INDEX IF NOT EXISTS idx_active_addresses_period 
            ON active_addresses(timeInterval, periodTimestamp)
        `);

        // Initialize cumulative contract count from existing data
        const contractResult = this.indexingDb.prepare(`
            SELECT MAX(value) as maxValue 
            FROM metrics 
            WHERE metric = ? AND timeInterval = ?
        `).get(METRIC_cumulativeContracts, TIME_INTERVAL_DAY) as { maxValue: number | null };

        this.cumulativeContractCount = contractResult?.maxValue || 0;

        // Initialize cumulative tx count from existing data
        const txResult = this.indexingDb.prepare(`
            SELECT MAX(value) as maxValue 
            FROM metrics 
            WHERE metric = ? AND timeInterval = ?
        `).get(METRIC_cumulativeTxCount, TIME_INTERVAL_DAY) as { maxValue: number | null };

        this.cumulativeTxCount = txResult?.maxValue || 0;
    }

    indexBlocks(blocks: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[]): void {
        const startTime = performance.now();
        const timings = {
            dataExtraction: 0,
            senderProcessing: 0,
            addressProcessing: 0,
            transaction: 0,
            incrementalUpdates: 0,
            cumulativeUpdates: 0,
            activeSendersDb: 0,
            activeAddressesDb: 0,
        };

        // Maps to accumulate metric updates
        const incrementalUpdates = new Map<string, number>(); // key = "timeInterval,timestamp,metric"
        const cumulativeUpdates = new Map<string, number>(); // key = "timeInterval,timestamp,metric"

        // Maps to track unique senders and addresses per time period
        const activeSendersMap = new Map<string, Set<string>>(); // key = "timeInterval,periodTimestamp"
        const activeAddressesMap = new Map<string, Set<string>>(); // key = "timeInterval,periodTimestamp"

        const timeIntervals = [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH];

        // Process all blocks and accumulate updates
        const dataExtractionStart = performance.now();
        for (const { block, txs, traces } of blocks) {
            const blockTimestamp = block.timestamp;
            const txCount = countTransactions(txs);

            // Accumulate incremental metrics (txCount)
            for (const timeInterval of timeIntervals) {
                const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                const key = `${timeInterval},${normalizedTimestamp},${METRIC_txCount}`;
                incrementalUpdates.set(key, (incrementalUpdates.get(key) || 0) + txCount);
            }

            // Extract and accumulate active senders
            const senderStart = performance.now();
            const uniqueSenders = this.extractUniqueSenders(txs);
            for (const sender of uniqueSenders) {
                for (const timeInterval of timeIntervals) {
                    const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                    const key = `${timeInterval},${periodTimestamp}`;
                    if (!activeSendersMap.has(key)) {
                        activeSendersMap.set(key, new Set<string>());
                    }
                    activeSendersMap.get(key)!.add(sender);
                }
            }
            timings.senderProcessing += performance.now() - senderStart;

            // Extract and accumulate active addresses
            const addressStart = performance.now();
            const uniqueAddresses = this.extractUniqueAddresses(txs);
            for (const address of uniqueAddresses) {
                for (const timeInterval of timeIntervals) {
                    const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                    const key = `${timeInterval},${periodTimestamp}`;
                    if (!activeAddressesMap.has(key)) {
                        activeAddressesMap.set(key, new Set<string>());
                    }
                    activeAddressesMap.get(key)!.add(address);
                }
            }
            timings.addressProcessing += performance.now() - addressStart;

            // Update cumulative transaction count
            this.cumulativeTxCount += txCount;

            // Accumulate cumulative metrics (cumulativeTxCount)
            for (const timeInterval of timeIntervals) {
                const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                const key = `${timeInterval},${normalizedTimestamp},${METRIC_cumulativeTxCount}`;
                cumulativeUpdates.set(key, this.cumulativeTxCount);
            }

            // Count contract deployments in this block
            const contractCount = countContractDeployments(txs, traces);
            this.cumulativeContractCount += contractCount;

            // Accumulate cumulative metrics (cumulativeContracts)
            for (const timeInterval of timeIntervals) {
                const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                const key = `${timeInterval},${normalizedTimestamp},${METRIC_cumulativeContracts}`;
                cumulativeUpdates.set(key, this.cumulativeContractCount);
            }
        }
        timings.dataExtraction = performance.now() - dataExtractionStart - timings.senderProcessing - timings.addressProcessing;

        // Now apply all updates in a single transaction
        const transactionStart = performance.now();
        const transaction = this.indexingDb.transaction(() => {
            // Prepare statements for batch operations
            const incrementalStmt = this.indexingDb.prepare(`
                INSERT INTO metrics (timeInterval, timestamp, metric, value) 
                VALUES (?, ?, ?, ?)
                ON CONFLICT(timeInterval, timestamp, metric) 
                DO UPDATE SET value = value + ?
            `);

            const cumulativeStmt = this.indexingDb.prepare(`
                INSERT INTO metrics (timeInterval, timestamp, metric, value) 
                VALUES (?, ?, ?, ?)
                ON CONFLICT(timeInterval, timestamp, metric) 
                DO UPDATE SET value = ?
            `);

            const activeSenderStmt = this.indexingDb.prepare(`
                INSERT OR IGNORE INTO active_senders (timeInterval, periodTimestamp, address) 
                VALUES (?, ?, ?)
            `);

            const activeAddressStmt = this.indexingDb.prepare(`
                INSERT OR IGNORE INTO active_addresses (timeInterval, periodTimestamp, address) 
                VALUES (?, ?, ?)
            `);

            // Prepare count queries for active senders/addresses
            const countSendersStmt = this.indexingDb.prepare(`
                SELECT COUNT(DISTINCT address) as count 
                FROM active_senders 
                WHERE timeInterval = ? AND periodTimestamp = ?
            `);

            const countAddressesStmt = this.indexingDb.prepare(`
                SELECT COUNT(DISTINCT address) as count 
                FROM active_addresses 
                WHERE timeInterval = ? AND periodTimestamp = ?
            `);

            // Apply incremental updates
            const incrementalStart = performance.now();
            for (const [key, value] of incrementalUpdates) {
                const [timeInterval, timestamp, metric] = key.split(',').map(Number);
                incrementalStmt.run(timeInterval, timestamp, metric, value, value);
            }
            timings.incrementalUpdates = performance.now() - incrementalStart;

            // Apply cumulative updates
            const cumulativeStart = performance.now();
            for (const [key, value] of cumulativeUpdates) {
                const [timeInterval, timestamp, metric] = key.split(',').map(Number);
                cumulativeStmt.run(timeInterval, timestamp, metric, value, value);
            }
            timings.cumulativeUpdates = performance.now() - cumulativeStart;

            // Insert active senders and update metrics
            const activeSendersStart = performance.now();
            for (const [key, senders] of activeSendersMap) {
                const [timeInterval, periodTimestamp] = key.split(',').map(Number);

                // Insert all unique senders for this period
                for (const address of senders) {
                    activeSenderStmt.run(timeInterval, periodTimestamp, address);
                }

                // Count total unique senders (including existing ones)
                const countResult = countSendersStmt.get(timeInterval, periodTimestamp) as { count: number };

                // Update the metric with the total count
                cumulativeStmt.run(timeInterval, periodTimestamp, METRIC_activeSenders, countResult.count, countResult.count);
            }
            timings.activeSendersDb = performance.now() - activeSendersStart;

            // Insert active addresses and update metrics
            const activeAddressesStart = performance.now();
            for (const [key, addresses] of activeAddressesMap) {
                const [timeInterval, periodTimestamp] = key.split(',').map(Number);

                // Insert all unique addresses for this period
                for (const address of addresses) {
                    activeAddressStmt.run(timeInterval, periodTimestamp, address);
                }

                // Count total unique addresses (including existing ones)
                const countResult = countAddressesStmt.get(timeInterval, periodTimestamp) as { count: number };

                // Update the metric with the total count
                cumulativeStmt.run(timeInterval, periodTimestamp, METRIC_activeAddresses, countResult.count, countResult.count);
            }
            timings.activeAddressesDb = performance.now() - activeAddressesStart;
        });

        transaction();
        timings.transaction = performance.now() - transactionStart;

        const totalTime = performance.now() - startTime;
        console.log(`MetricsIndexer batch (${blocks.length} blocks) took ${totalTime.toFixed(2)}ms:`);
        console.log(`  - Data extraction: ${timings.dataExtraction.toFixed(2)}ms`);
        console.log(`  - Sender processing: ${timings.senderProcessing.toFixed(2)}ms`);
        console.log(`  - Address processing: ${timings.addressProcessing.toFixed(2)}ms`);
        console.log(`  - Transaction total: ${timings.transaction.toFixed(2)}ms`);
        console.log(`    - Incremental updates: ${timings.incrementalUpdates.toFixed(2)}ms`);
        console.log(`    - Cumulative updates: ${timings.cumulativeUpdates.toFixed(2)}ms`);
        console.log(`    - Active senders DB: ${timings.activeSendersDb.toFixed(2)}ms`);
        console.log(`    - Active addresses DB: ${timings.activeAddressesDb.toFixed(2)}ms`);
    }

    private extractUniqueSenders(txs: LazyTx[]): Set<string> {
        const uniqueSenders = new Set<string>();
        const TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

        for (const tx of txs) {
            // Add transaction sender
            uniqueSenders.add(tx.from.toLowerCase());

            // Check logs for Transfer events
            for (const log of tx.logs) {
                if (log.topics.length >= 2 && log.topics[0] === TRANSFER_EVENT_SIGNATURE && log.topics[1]) {
                    // Extract 'from' address from topics[1] (remove 0x prefix and padding)
                    const fromAddress = "0x" + log.topics[1].slice(-40);
                    uniqueSenders.add(fromAddress.toLowerCase());
                }
            }
        }

        return uniqueSenders;
    }

    private extractUniqueAddresses(txs: LazyTx[]): Set<string> {
        const uniqueAddresses = new Set<string>();
        const TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

        for (const tx of txs) {
            // Add transaction sender (from)
            uniqueAddresses.add(tx.from.toLowerCase());

            // Add transaction recipient (to) if it exists
            if (tx.to) {
                uniqueAddresses.add(tx.to.toLowerCase());
            }

            // Check logs for Transfer events
            for (const log of tx.logs) {
                if (log.topics.length >= 3 && log.topics[0] === TRANSFER_EVENT_SIGNATURE) {
                    // Extract 'from' address from topics[1] (remove 0x prefix and padding)
                    if (log.topics[1]) {
                        const fromAddress = "0x" + log.topics[1].slice(-40);
                        uniqueAddresses.add(fromAddress.toLowerCase());
                    }

                    // Extract 'to' address from topics[2] (remove 0x prefix and padding)
                    if (log.topics[2]) {
                        const toAddress = "0x" + log.topics[2].slice(-40);
                        uniqueAddresses.add(toAddress.toLowerCase());
                    }
                }
            }
        }

        return uniqueAddresses;
    }

    registerRoutes(app: OpenAPIHono): void {
        // Create a separate route for each metric
        for (const [metricName, metricId] of Object.entries(METRICS)) {
            this.createMetricRoute(app, metricName, metricId);
        }
    }

    private createMetricRoute(app: OpenAPIHono, metricName: string, metricId: number): void {
        const route = createRoute({
            method: 'get',
            path: `/metrics/${metricName}`,
            request: {
                query: MetricQuerySchema,
            },
            responses: {
                200: {
                    content: {
                        'application/json': {
                            schema: MetricResponseSchema
                        }
                    },
                    description: `${metricName} metric data`
                },
                400: {
                    description: 'Bad request (invalid parameters)'
                }
            },
            tags: ['Metrics'],
            summary: `Get ${metricName} data`,
            description: `Retrieve ${metricName} blockchain metric with optional filtering and pagination`
        });

        app.openapi(route, (c) => {
            const {
                startTimestamp,
                endTimestamp,
                timeInterval = 'hour',
                pageSize = 10,
                pageToken
            } = c.req.valid('query');

            // Map time interval to constant
            const timeIntervalId = getTimeIntervalId(timeInterval);
            if (timeIntervalId === -1) {
                return c.json({ error: `Invalid timeInterval: ${timeInterval}` }, 400);
            }

            // Validate pageSize
            const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

            // Route to appropriate handler based on metric type
            if (isCumulativeMetric(metricId)) {
                const result = handleCumulativeMetricQuery(
                    this.indexingDb, metricId, timeIntervalId, startTimestamp, endTimestamp,
                    validPageSize, pageToken
                );
                return c.json(result);
            } else {
                const result = handleIncrementalMetricQuery(
                    this.indexingDb, metricId, timeIntervalId, startTimestamp, endTimestamp,
                    validPageSize, pageToken
                );
                return c.json(result);
            }
        });
    }
}

export const createMetricsIndexer: CreateIndexerFunction = (blocksDb: BlockDB, indexingDb: SQLite.Database) => {
    return new MetricsIndexer(blocksDb, indexingDb);
}
