import type { IndexerModule } from "../lib/types";
import { normalizeTimestamp, TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH, getPreviousTimestamp, getTimeIntervalFromString } from "../lib/dateUtils";
import { createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prepQueryCached } from "../lib/prep";

// Wipe function - reset all data
const wipe: IndexerModule["wipe"] = (db) => {
    db.exec(`DROP TABLE IF EXISTS tx_counts`);
};

// Initialize function - set up tables
const initialize: IndexerModule["initialize"] = (db) => {
    db.exec(`
        CREATE TABLE IF NOT EXISTS tx_counts (
            time_interval INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (time_interval, timestamp)
        ) WITHOUT ROWID
    `);
};

// Handle transaction batch
const handleTxBatch: IndexerModule["handleTxBatch"] = (db, _blocksDb, batch) => {
    const stmt = prepQueryCached(db, `
            INSERT INTO tx_counts (time_interval, timestamp, count)
            VALUES (?, ?, ?)
            ON CONFLICT(time_interval, timestamp)
            DO UPDATE SET count = count + ?
        `);

    // Count transactions per time period
    const periodCounts = new Map<string, number>();

    for (const tx of batch.txs) {
        const blockTimestamp = tx.blockTs;

        // Update counts for each time interval
        for (const timeInterval of [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH]) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${normalizedTimestamp}`;
            periodCounts.set(key, (periodCounts.get(key) || 0) + 1);
        }
    }

    // Apply all updates
    for (const [key, count] of periodCounts) {
        const [timeInterval, timestamp] = key.split(',').map(Number);
        stmt.run(timeInterval, timestamp, count, count);
    }

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
        path: '/metrics/txCount',
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
                description: 'Transaction count metric data'
            },
            400: {
                description: 'Bad request'
            }
        },
        tags: ['Metrics'],
        summary: 'Get transaction count data'
    });

    app.openapi(route, (c) => {
        const { startTimestamp, endTimestamp, timeInterval = 'hour', pageSize = 10, pageToken } = c.req.valid('query');

        const timeIntervalId = getTimeIntervalFromString(timeInterval);
        if (timeIntervalId === -1) {
            return c.json({ error: `Invalid timeInterval: ${timeInterval}` }, 400);
        }

        const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

        // Build query
        let query = `SELECT timestamp, count as value FROM tx_counts WHERE time_interval = ?`;
        const params: any[] = [timeIntervalId];

        if (startTimestamp) {
            query += ` AND timestamp >= ?`;
            params.push(startTimestamp);
        }

        if (endTimestamp) {
            query += ` AND timestamp <= ?`;
            params.push(endTimestamp);
        }

        if (pageToken) {
            query += ` AND timestamp < ?`;
            params.push(parseInt(pageToken));
        }

        query += ` ORDER BY timestamp DESC LIMIT ?`;
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
    name: "txCount",
    version: 1,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
