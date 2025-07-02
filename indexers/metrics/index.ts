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
