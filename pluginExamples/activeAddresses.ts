import type { IndexerModule } from "../lib/types";
import { normalizeTimestamp, TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH, getPreviousTimestamp, getTimeIntervalFromString } from "../lib/dateUtils";
import { extractTransferAddresses } from "./lib/evmUtils";
import { prepQueryCached } from "../lib/prep";
import { FastifyInstance } from "fastify";
import { Database as SqliteDatabase } from "better-sqlite3";

// Wipe function - reset all data
const wipe: IndexerModule["wipe"] = (db) => {
    db.exec(`DROP TABLE IF EXISTS active_addresses`);
    db.exec(`DROP TABLE IF EXISTS active_addresses_count`);
};

// Initialize function - set up tables
const initialize: IndexerModule["initialize"] = (db) => {
    // Table to track unique addresses per time period
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_addresses (
            time_interval INTEGER NOT NULL,
            period_timestamp INTEGER NOT NULL,
            address TEXT NOT NULL,
            PRIMARY KEY (time_interval, period_timestamp, address)
        ) WITHOUT ROWID
    `);

    // Index for efficient counting
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_active_addresses_period 
        ON active_addresses(time_interval, period_timestamp)
    `);

    // Pre-aggregated counts for performance
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_addresses_count (
            time_interval INTEGER NOT NULL,
            period_timestamp INTEGER NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (time_interval, period_timestamp)
        ) WITHOUT ROWID
    `);
};

// Handle transaction batch
const handleTxBatch: IndexerModule["handleTxBatch"] = (db, _blocksDb, batch) => {
    // Temporary table for batch operations
    db.exec(`
        CREATE TEMP TABLE IF NOT EXISTS temp_addresses (
            time_interval INTEGER,
            period_timestamp INTEGER,
            address TEXT,
            PRIMARY KEY (time_interval, period_timestamp, address)
        ) WITHOUT ROWID
    `);


    const tempStmt = prepQueryCached(db, `
        INSERT OR IGNORE INTO temp_addresses (time_interval, period_timestamp, address) 
        VALUES (?, ?, ?)
    `);

    // Collect addresses per time period
    const periodAddresses = new Map<string, Set<string>>();

    for (const tx of batch.txs) {
        const blockTimestamp = tx.blockTs;

        // Collect all addresses (from and to)
        const addresses = new Set<string>();
        addresses.add(tx.tx.from);
        if (tx.tx.to) {
            addresses.add(tx.tx.to);
        }

        // Extract Transfer event addresses
        const transferAddresses = extractTransferAddresses(tx.receipt.logs);
        for (const address of transferAddresses.addresses) {
            addresses.add(address);
        }

        // Add to period maps
        for (const timeInterval of [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH]) {
            const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${periodTimestamp}`;

            if (!periodAddresses.has(key)) {
                periodAddresses.set(key, new Set<string>());
            }

            for (const address of addresses) {
                periodAddresses.get(key)!.add(address);
                tempStmt.run(timeInterval, periodTimestamp, address);
            }
        }
    }

    // Bulk insert from temp table to main table
    db.exec(`
        INSERT OR IGNORE INTO active_addresses (time_interval, period_timestamp, address)
        SELECT time_interval, period_timestamp, address FROM temp_addresses
    `);

    // Update counts
    db.exec(`
        INSERT OR REPLACE INTO active_addresses_count (time_interval, period_timestamp, count)
        SELECT 
            time_interval,
            period_timestamp,
            COUNT(DISTINCT address)
        FROM active_addresses
        WHERE (time_interval, period_timestamp) IN (
            SELECT DISTINCT time_interval, period_timestamp FROM temp_addresses
        )
        GROUP BY time_interval, period_timestamp
    `);

    // Clean up temp table
    db.exec(`DROP TABLE temp_addresses`);
};

// Register routes
const registerRoutes: IndexerModule["registerRoutes"] = (app, dbCtx) => {
    // JSON Schemas
    const paramsSchema = {
        type: 'object',
        properties: {
            evmChainId: { type: 'number' }
        },
        required: ['evmChainId']
    };

    const querySchema = {
        type: 'object',
        properties: {
            startTimestamp: { type: 'number' },
            endTimestamp: { type: 'number' },
            timeInterval: {
                type: 'string',
                enum: ['hour', 'day', 'week', 'month'],
                default: 'hour'
            },
            pageSize: {
                type: 'number',
                default: 10
            },
            pageToken: { type: 'string' }
        }
    };

    const responseSchema = {
        type: 'object',
        properties: {
            results: {
                type: 'array',
                items: {
                    type: 'object',
                    properties: {
                        timestamp: { type: 'number' },
                        value: { type: 'number' }
                    },
                    required: ['timestamp', 'value']
                }
            },
            nextPageToken: { type: 'string' }
        },
        required: ['results']
    };

    app.get('/:evmChainId/metrics/activeAddresses', {
        schema: {
            description: 'Get active addresses data',
            tags: ['Metrics'],
            summary: 'Get active addresses metric data',
            params: paramsSchema,
            querystring: querySchema,
            response: {
                200: responseSchema,
                400: {
                    type: 'object',
                    properties: {
                        error: { type: 'string' }
                    }
                }
            }
        }
    }, async (request, reply) => {
        const { evmChainId } = request.params as { evmChainId: number };
        const db = dbCtx.indexerDbFactory(evmChainId);

        const {
            startTimestamp,
            endTimestamp,
            timeInterval = 'hour',
            pageSize = 10,
            pageToken
        } = request.query as any;

        const timeIntervalId = getTimeIntervalFromString(timeInterval);
        if (timeIntervalId === -1) {
            return reply.code(400).send({ error: `Invalid timeInterval: ${timeInterval}` });
        }

        const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

        // Build query
        let query = `SELECT period_timestamp as timestamp, count as value FROM active_addresses_count WHERE time_interval = ?`;
        const params: any[] = [timeIntervalId];

        if (startTimestamp) {
            query += ` AND period_timestamp >= ?`;
            params.push(startTimestamp);
        }

        if (endTimestamp) {
            query += ` AND period_timestamp <= ?`;
            params.push(endTimestamp);
        }

        if (pageToken) {
            query += ` AND period_timestamp < ?`;
            params.push(parseInt(pageToken));
        }

        query += ` ORDER BY period_timestamp DESC LIMIT ?`;
        params.push(validPageSize + 1);

        const results = prepQueryCached(db, query).all(...params) as Array<{ timestamp: number; value: number }>;

        const hasNextPage = results.length > validPageSize;
        if (hasNextPage) {
            results.pop();
        }

        // Backfill missing periods with zeros
        const backfilled = backfillResults(results, timeIntervalId, startTimestamp, endTimestamp);

        return {
            results: backfilled.slice(0, validPageSize),
            nextPageToken: hasNextPage && backfilled.length > 0
                ? backfilled[validPageSize - 1]?.timestamp.toString()
                : undefined
        };
    });
};

// Helper function to backfill missing time periods with zeros
function backfillResults(
    results: Array<{ timestamp: number; value: number }>,
    timeInterval: number,
    startTimestamp?: number,
    _endTimestamp?: number
): Array<{ timestamp: number; value: number }> {
    if (results.length === 0) return results;

    const backfilled: Array<{ timestamp: number; value: number }> = [];
    const resultMap = new Map(results.map(r => [r.timestamp, r.value]));

    const oldest = results[results.length - 1]!.timestamp;
    const newest = results[0]!.timestamp;

    const start = startTimestamp ? Math.max(startTimestamp, oldest) : oldest;

    let current = newest;
    while (current >= start) {
        const value = resultMap.get(current);
        backfilled.push({
            timestamp: current,
            value: value !== undefined ? value : 0
        });
        current = getPreviousTimestamp(current, timeInterval);
    }

    return backfilled;
}

const module: IndexerModule = {
    name: "activeAddresses",
    version: 1,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
