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

    indexBlock(block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined): void {
        const blockTimestamp = block.timestamp;
        const txCount = countTransactions(txs);

        // Update incremental metrics (txCount)
        this.updateIncrementalMetric(TIME_INTERVAL_HOUR, blockTimestamp, METRIC_txCount, txCount);
        this.updateIncrementalMetric(TIME_INTERVAL_DAY, blockTimestamp, METRIC_txCount, txCount);
        this.updateIncrementalMetric(TIME_INTERVAL_WEEK, blockTimestamp, METRIC_txCount, txCount);
        this.updateIncrementalMetric(TIME_INTERVAL_MONTH, blockTimestamp, METRIC_txCount, txCount);

        // Process active senders - store unique addresses per time period
        this.processActiveSenders(txs, blockTimestamp);

        // Process active addresses (both from and to) - store unique addresses per time period
        this.processActiveAddresses(txs, blockTimestamp);

        // Update cumulative transaction count
        this.cumulativeTxCount += txCount;

        // Update cumulative metrics (cumulativeTxCount) - all intervals
        this.updateCumulativeMetric(TIME_INTERVAL_HOUR, blockTimestamp, METRIC_cumulativeTxCount, this.cumulativeTxCount);
        this.updateCumulativeMetric(TIME_INTERVAL_DAY, blockTimestamp, METRIC_cumulativeTxCount, this.cumulativeTxCount);
        this.updateCumulativeMetric(TIME_INTERVAL_WEEK, blockTimestamp, METRIC_cumulativeTxCount, this.cumulativeTxCount);
        this.updateCumulativeMetric(TIME_INTERVAL_MONTH, blockTimestamp, METRIC_cumulativeTxCount, this.cumulativeTxCount);

        // Count contract deployments in this block
        const contractCount = countContractDeployments(txs, traces);
        this.cumulativeContractCount += contractCount;

        // Update cumulative metrics (cumulativeContracts) - only day interval supported
        this.updateCumulativeMetric(TIME_INTERVAL_DAY, blockTimestamp, METRIC_cumulativeContracts, this.cumulativeContractCount);
    }

    private updateIncrementalMetric(timeInterval: number, timestamp: number, metric: number, increment: number): void {
        const normalizedTimestamp = normalizeTimestamp(timestamp, timeInterval);

        this.indexingDb.prepare(`
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = value + ?
        `).run(timeInterval, normalizedTimestamp, metric, increment, increment);
    }

    private updateCumulativeMetric(timeInterval: number, timestamp: number, metric: number, totalValue: number): void {
        const normalizedTimestamp = normalizeTimestamp(timestamp, timeInterval);

        this.indexingDb.prepare(`
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = ?
        `).run(timeInterval, normalizedTimestamp, metric, totalValue, totalValue);
    }

    private processActiveSenders(txs: LazyTx[], blockTimestamp: number): void {
        // Extract unique senders from this block
        const uniqueSenders = this.extractUniqueSenders(txs);

        if (uniqueSenders.size === 0) return;

        // Process for each time interval
        const timeIntervals = [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH];

        // Prepare statements outside the loop for better performance
        const insertStmt = this.indexingDb.prepare(`
            INSERT OR IGNORE INTO active_senders (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
        `);

        const countStmt = this.indexingDb.prepare(`
            SELECT COUNT(DISTINCT address) as count 
            FROM active_senders 
            WHERE timeInterval = ? AND periodTimestamp = ?
        `);

        const updateMetricStmt = this.indexingDb.prepare(`
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = ?
        `);

        for (const timeInterval of timeIntervals) {
            const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);

            // Batch insert all unique senders for this period
            const insertBatch = this.indexingDb.transaction(() => {
                for (const address of uniqueSenders) {
                    insertStmt.run(timeInterval, periodTimestamp, address);
                }
            });
            insertBatch();

            // Count total unique senders for this period
            const countResult = countStmt.get(timeInterval, periodTimestamp) as { count: number };

            // Update the metric with the total count
            updateMetricStmt.run(timeInterval, periodTimestamp, METRIC_activeSenders, countResult.count, countResult.count);
        }
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

    private processActiveAddresses(txs: LazyTx[], blockTimestamp: number): void {
        // Extract unique addresses (both from and to) from this block
        const uniqueAddresses = this.extractUniqueAddresses(txs);

        if (uniqueAddresses.size === 0) return;

        // Process for each time interval
        const timeIntervals = [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH];

        // Prepare statements outside the loop for better performance
        const insertStmt = this.indexingDb.prepare(`
            INSERT OR IGNORE INTO active_addresses (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
        `);

        const countStmt = this.indexingDb.prepare(`
            SELECT COUNT(DISTINCT address) as count 
            FROM active_addresses 
            WHERE timeInterval = ? AND periodTimestamp = ?
        `);

        const updateMetricStmt = this.indexingDb.prepare(`
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = ?
        `);

        for (const timeInterval of timeIntervals) {
            const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);

            // Batch insert all unique addresses for this period
            const insertBatch = this.indexingDb.transaction(() => {
                for (const address of uniqueAddresses) {
                    insertStmt.run(timeInterval, periodTimestamp, address);
                }
            });
            insertBatch();

            // Count total unique addresses for this period
            const countResult = countStmt.get(timeInterval, periodTimestamp) as { count: number };

            // Update the metric with the total count
            updateMetricStmt.run(timeInterval, periodTimestamp, METRIC_activeAddresses, countResult.count, countResult.count);
        }
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
                pageSize = 100,
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
