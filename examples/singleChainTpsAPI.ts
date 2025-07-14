import type { ApiPlugin } from "../index";

type DailyTpsDataPoint = {
    timestamp: number;  // Unix timestamp for the end of the 24h period
    txs: number;        // Total transactions in the period
    tps: number;        // Average TPS for the period
}

const module: ApiPlugin = {
    name: "single_chain_tps",
    requiredIndexers: ['minute_tx_counter'],

    registerRoutes: (app, dbCtx) => {
        app.get<{
            Params: { chainId: string };
            Querystring: { count?: number }
        }>('/api/:chainId/stats/tps', {
            schema: {
                params: {
                    type: 'object',
                    properties: {
                        chainId: { type: 'string' }
                    },
                    required: ['chainId']
                },
                querystring: {
                    type: 'object',
                    properties: {
                        count: { type: 'number', minimum: 1, maximum: 100, default: 30 }
                    },
                    required: []
                },
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                timestamp: { type: 'number' },
                                txs: { type: 'number' },
                                tps: { type: 'number' }
                            },
                            required: ['timestamp', 'txs', 'tps']
                        }
                    },
                    404: {
                        type: 'object',
                        properties: {
                            error: { type: 'string' }
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const chainId = parseInt(request.params.chainId);
            const count = request.query.count || 30;

            // Validate chain exists
            const chainConfig = dbCtx.getAllChainConfigs().find(c => c.evmChainId === chainId);
            if (!chainConfig) {
                return reply.code(404).send({ error: `Chain ${chainId} not found` });
            }

            const db = dbCtx.indexerDbFactory(chainId, 'minute_tx_counter');
            const results: DailyTpsDataPoint[] = [];

            // Get current timestamp in seconds
            const now = Math.floor(Date.now() / 1000);
            const dayInSeconds = 86400;

            // Calculate data points for each day going back
            for (let i = 0; i < count; i++) {
                // Calculate the time range for this 24h period
                const periodEnd = now - (i * dayInSeconds);
                const periodStart = periodEnd - dayInSeconds;

                // Query minute_tx_counts table for this 24h period
                const stmt = db.prepare(`
                    SELECT SUM(tx_count) as total_txs
                    FROM minute_tx_counts
                    WHERE minute_ts >= ? AND minute_ts < ?
                `);

                const result = stmt.get(periodStart, periodEnd) as { total_txs: number | null };

                const txCount = result.total_txs || 0;
                const tps = txCount / dayInSeconds;

                results.push({
                    timestamp: periodEnd,
                    txs: txCount,
                    tps: Number(tps.toFixed(6))
                });
            }

            return reply.send(results);
        });

        // Cumulative transactions endpoint
        app.get<{
            Params: { chainId: string };
            Querystring: { timestamp?: number }
        }>('/api/:chainId/stats/cumulative-txs', {
            schema: {
                params: {
                    type: 'object',
                    properties: {
                        chainId: { type: 'string' }
                    },
                    required: ['chainId']
                },
                querystring: {
                    type: 'object',
                    properties: {
                        timestamp: { type: 'number', description: 'Unix timestamp to get cumulative count at. If not provided, returns latest.' }
                    },
                    required: []
                },
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            timestamp: { type: 'number' },
                            cumulativeTxs: { type: 'number' }
                        },
                        required: ['timestamp', 'cumulativeTxs']
                    },
                    404: {
                        type: 'object',
                        properties: {
                            error: { type: 'string' }
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const chainId = parseInt(request.params.chainId);
            const queryTimestamp = request.query.timestamp;

            // Validate chain exists
            const chainConfig = dbCtx.getAllChainConfigs().find(c => c.evmChainId === chainId);
            if (!chainConfig) {
                return reply.code(404).send({ error: `Chain ${chainId} not found` });
            }

            const db = dbCtx.indexerDbFactory(chainId, 'minute_tx_counter');

            let result: { minute_ts: number; cumulative_count: number } | undefined;

            if (queryTimestamp) {
                // Get cumulative count at or before the specified timestamp
                const minuteTs = Math.floor(queryTimestamp / 60) * 60;
                const stmt = db.prepare(`
                    SELECT minute_ts, cumulative_count
                    FROM cumulative_tx_counts
                    WHERE minute_ts <= ?
                    ORDER BY minute_ts DESC
                    LIMIT 1
                `);
                result = stmt.get(minuteTs) as { minute_ts: number; cumulative_count: number } | undefined;
            } else {
                // Get the latest cumulative count
                const stmt = db.prepare(`
                    SELECT minute_ts, cumulative_count
                    FROM cumulative_tx_counts
                    ORDER BY minute_ts DESC
                    LIMIT 1
                `);
                result = stmt.get() as { minute_ts: number; cumulative_count: number } | undefined;
            }

            if (!result) {
                return reply.send({
                    timestamp: queryTimestamp || Math.floor(Date.now() / 1000),
                    cumulativeTxs: 0
                });
            }

            return reply.send({
                timestamp: result.minute_ts,
                cumulativeTxs: result.cumulative_count
            });
        });
    }
};

export default module; 
