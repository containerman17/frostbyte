import type { IndexerModule } from "../lib/types";
import { normalizeTimestamp, TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH, getPreviousTimestamp, getTimeIntervalFromString } from "../lib/dateUtils";
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

    app.get('/:evmChainId/metrics/txCount', {
        schema: {
            description: 'Get transaction count data',
            tags: ['Metrics'],
            summary: 'Get transaction count data',
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
    name: "txCount",
    version: 1,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};

export default module;
