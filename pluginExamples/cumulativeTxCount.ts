import type { IndexerModule } from "../lib/types";
import { normalizeTimestamp, TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH, getPreviousTimestamp, getTimeIntervalFromString } from "../lib/dateUtils";
import { createRoute } from "@hono/zod-openapi";
import { z } from "@hono/zod-openapi";
import { prepQueryCached } from "../lib/prep";

// Wipe function - reset all data
const wipe: IndexerModule["wipe"] = (db) => {
    db.exec(`DROP TABLE IF EXISTS cumulative_tx_counts`);
};

// Initialize function - set up tables
const initialize: IndexerModule["initialize"] = (db) => {
    db.exec(`
        CREATE TABLE IF NOT EXISTS cumulative_tx_counts (
            time_interval INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            count INTEGER NOT NULL,
            PRIMARY KEY (time_interval, timestamp)
        ) WITHOUT ROWID
    `);
};

// Handle transaction batch
const handleTxBatch: IndexerModule["handleTxBatch"] = (db, _blocksDb, batch) => {
    const batchTxCount = batch.txs.length;

    // If no transactions in this batch, skip processing
    if (batchTxCount === 0) {
        return;
    }

    // Get current cumulative count for each interval
    const getCurrentStmt = prepQueryCached(db, `
        SELECT MAX(count) as max_count 
        FROM cumulative_tx_counts 
        WHERE time_interval = ?
    `);

    const updateStmt = prepQueryCached(db, `
        INSERT INTO cumulative_tx_counts (time_interval, timestamp, count)
        VALUES (?, ?, ?)
        ON CONFLICT(time_interval, timestamp)
        DO UPDATE SET count = ?
    `);

    // Collect unique timestamps for this batch
    const timestampsByInterval = new Map<number, Set<number>>();

    for (const tx of batch.txs) {
        const blockTimestamp = tx.blockTs;

        for (const timeInterval of [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH]) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);

            if (!timestampsByInterval.has(timeInterval)) {
                timestampsByInterval.set(timeInterval, new Set());
            }
            timestampsByInterval.get(timeInterval)!.add(normalizedTimestamp);
        }
    }

    // Update cumulative counts for each interval
    for (const [timeInterval, timestamps] of timestampsByInterval) {
        const result = getCurrentStmt.get(timeInterval) as { max_count: number | null };
        const currentCount = result?.max_count || 0;
        const newCount = currentCount + batchTxCount;

        for (const timestamp of timestamps) {
            updateStmt.run(timeInterval, timestamp, newCount, newCount);
        }
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
        path: '/metrics/cumulativeTxCount',
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
                description: 'Cumulative transaction count metric data'
            },
            400: {
                description: 'Bad request'
            }
        },
        tags: ['Metrics'],
        summary: 'Get cumulative transaction count data'
    });

    app.openapi(route, (c) => {
        const { startTimestamp, endTimestamp, timeInterval = 'hour', pageSize = 10, pageToken } = c.req.valid('query');

        const timeIntervalId = getTimeIntervalFromString(timeInterval);
        if (timeIntervalId === -1) {
            return c.json({ error: `Invalid timeInterval: ${timeInterval}` }, 400);
        }

        const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

        // Get the current cumulative value
        const currentResult = prepQueryCached(db, `
            SELECT MAX(count) as max_count 
            FROM cumulative_tx_counts 
            WHERE time_interval = ?
        `).get(timeIntervalId) as { max_count: number | null };

        const currentCumulativeValue = currentResult?.max_count || 0;

        // Build query
        let query = `SELECT timestamp, count as value FROM cumulative_tx_counts WHERE time_interval = ?`;
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

        // Backfill cumulative values
        const backfilled = backfillCumulativeResults(results, timeIntervalId, currentCumulativeValue, startTimestamp, endTimestamp);

        return c.json({
            results: backfilled.slice(0, validPageSize),
            nextPageToken: hasNextPage && backfilled.length > 0
                ? backfilled[validPageSize - 1]?.timestamp.toString()
                : undefined
        });
    });
};

// Helper function to backfill cumulative values
function backfillCumulativeResults(
    results: Array<{ timestamp: number; value: number }>,
    timeInterval: number,
    currentCumulativeValue: number,
    startTimestamp?: number,
    endTimestamp?: number
): Array<{ timestamp: number; value: number }> {
    // If no results and no cumulative value, return empty
    if (results.length === 0 && currentCumulativeValue === 0) {
        return [];
    }

    // If no results but we have a cumulative value, generate synthetic results
    if (results.length === 0) {
        const now = Math.floor(Date.now() / 1000);
        const end = endTimestamp || now;
        const start = startTimestamp || end - (24 * 60 * 60); // Default to 1 day range

        const syntheticResults: Array<{ timestamp: number; value: number }> = [];
        let current = normalizeTimestamp(end, timeInterval);
        const normalizedStart = normalizeTimestamp(start, timeInterval);

        while (current >= normalizedStart) {
            syntheticResults.push({
                timestamp: current,
                value: currentCumulativeValue
            });
            current = getPreviousTimestamp(current, timeInterval);
        }

        return syntheticResults;
    }

    const backfilled: Array<{ timestamp: number; value: number }> = [];
    const resultMap = new Map(results.map(r => [r.timestamp, r.value]));

    const oldest = results[results.length - 1]!.timestamp;
    const newest = results[0]!.timestamp;

    const start = startTimestamp ? Math.max(startTimestamp, oldest) : oldest;

    // Find the last known value
    let lastKnownValue = currentCumulativeValue;
    for (const result of results) {
        if (result.value > 0) {
            lastKnownValue = result.value;
            break;
        }
    }

    let current = newest;
    while (current >= start) {
        const value = resultMap.get(current);
        if (value !== undefined && value > 0) {
            lastKnownValue = value;
            backfilled.push({
                timestamp: current,
                value: value
            });
        } else {
            // Use last known value for gaps
            backfilled.push({
                timestamp: current,
                value: lastKnownValue
            });
        }
        current = getPreviousTimestamp(current, timeInterval);
    }

    return backfilled;
}

const module: IndexerModule = {
    name: "cumulativeTxCount",
    version: 1,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
