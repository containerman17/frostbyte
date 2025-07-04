import type { IndexerModule } from "../lib/types";
import { normalizeTimestamp, TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH, getPreviousTimestamp, getTimeIntervalFromString } from "../lib/dateUtils";
import { extractTransferAddresses } from "../lib/evmUtils";
import { createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prepQueryCached } from "../lib/prep";

// Wipe function - reset all data
const wipe: IndexerModule["wipe"] = (db) => {
    db.exec(`DROP TABLE IF EXISTS active_senders`);
    db.exec(`DROP TABLE IF EXISTS active_senders_count`);
};

// Initialize function - set up tables
const initialize: IndexerModule["initialize"] = (db) => {
    // Table to track unique senders per time period
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_senders (
            time_interval INTEGER NOT NULL,
            period_timestamp INTEGER NOT NULL,
            address TEXT NOT NULL,
            PRIMARY KEY (time_interval, period_timestamp, address)
        ) WITHOUT ROWID
    `);

    // Index for efficient counting
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_active_senders_period 
        ON active_senders(time_interval, period_timestamp)
    `);

    // Pre-aggregated counts for performance
    db.exec(`
        CREATE TABLE IF NOT EXISTS active_senders_count (
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
        CREATE TEMP TABLE IF NOT EXISTS temp_senders (
            time_interval INTEGER,
            period_timestamp INTEGER,
            address TEXT,
            PRIMARY KEY (time_interval, period_timestamp, address)
        ) WITHOUT ROWID
    `);

    const tempStmt = prepQueryCached(db, `
        INSERT OR IGNORE INTO temp_senders (time_interval, period_timestamp, address) 
        VALUES (?, ?, ?)
    `);

    // Collect senders per time period
    const periodSenders = new Map<string, Set<string>>();

    for (const tx of batch.txs) {
        const blockTimestamp = tx.blockTs;

        // Collect transaction sender
        const senders = new Set<string>();
        senders.add(tx.tx.from);

        // Extract Transfer event senders
        const transferAddresses = extractTransferAddresses(tx.receipt.logs);
        for (const sender of transferAddresses.senders) {
            senders.add(sender);
        }

        // Add to period maps
        for (const timeInterval of [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH]) {
            const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${periodTimestamp}`;

            if (!periodSenders.has(key)) {
                periodSenders.set(key, new Set<string>());
            }

            for (const sender of senders) {
                periodSenders.get(key)!.add(sender);
                tempStmt.run(timeInterval, periodTimestamp, sender);
            }
        }
    }

    // Bulk insert from temp table to main table
    db.exec(`
        INSERT OR IGNORE INTO active_senders (time_interval, period_timestamp, address)
        SELECT time_interval, period_timestamp, address FROM temp_senders
    `);

    // Update counts
    db.exec(`
        INSERT OR REPLACE INTO active_senders_count (time_interval, period_timestamp, count)
        SELECT 
            time_interval,
            period_timestamp,
            COUNT(DISTINCT address)
        FROM active_senders
        WHERE (time_interval, period_timestamp) IN (
            SELECT DISTINCT time_interval, period_timestamp FROM temp_senders
        )
        GROUP BY time_interval, period_timestamp
    `);

    // Clean up temp table
    db.exec(`DROP TABLE temp_senders`);
};

// Register routes
const registerRoutes: IndexerModule["registerRoutes"] = (app, db) => {
    // Query schema
    const QuerySchema = z.object({
        startTimestamp: z.coerce.number().optional(),
        endTimestamp: z.coerce.number().optional(),
        timeInterval: z.enum(['hour', 'day', 'week', 'month']).optional().default('hour'),
        pageSize: z.coerce.number().optional().default(10),
        pageToken: z.string().optional()
    });

    // Response schema
    const ResponseSchema = z.object({
        results: z.array(z.object({
            timestamp: z.number(),
            value: z.number()
        })),
        nextPageToken: z.string().optional()
    });

    const route = createRoute({
        method: 'get',
        path: '/metrics/activeSenders',
        request: {
            query: QuerySchema,
        },
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: ResponseSchema
                    }
                },
                description: 'Active senders metric data'
            },
            400: {
                description: 'Bad request'
            }
        },
        tags: ['Metrics'],
        summary: 'Get active senders data'
    });

    app.openapi(route, (c) => {
        const { startTimestamp, endTimestamp, timeInterval = 'hour', pageSize = 10, pageToken } = c.req.valid('query');

        const timeIntervalId = getTimeIntervalFromString(timeInterval);
        if (timeIntervalId === -1) {
            return c.json({ error: `Invalid timeInterval: ${timeInterval}` }, 400);
        }

        const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

        // Build query
        let query = `SELECT period_timestamp as timestamp, count as value FROM active_senders_count WHERE time_interval = ?`;
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

        return c.json({
            results: backfilled.slice(0, validPageSize),
            nextPageToken: hasNextPage && backfilled.length > 0
                ? backfilled[validPageSize - 1]?.timestamp.toString()
                : undefined
        });
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
    name: "activeSenders",
    version: 1,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
