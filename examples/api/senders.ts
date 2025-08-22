import type { ApiPlugin } from "../../index.ts";

const module: ApiPlugin = {
    name: "senders_api",
    requiredIndexers: ['daily_unique_senders'],

    registerRoutes: (app, dbCtx) => {

        app.get('/api/global/stats/tx-senders', {
            schema: {
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            uniqueSenders: { type: 'number' }
                        },
                        required: ['uniqueSenders']
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
            console.time('tx-senders global');

            const chains = dbCtx.getAllChainConfigs();
            const senders = new Set<string>();
            for (const chainConfig of chains) {

                const indexerConn = dbCtx.getIndexerDbConnection(chainConfig.evmChainId, 'daily_unique_senders');
                const result = await indexerConn.prepare(`
                    SELECT DISTINCT sender
                    FROM daily_unique_senders;
                `).all() as { sender: string }[];
                for (const sender of result) {
                    senders.add(sender.sender);
                }
            }
            console.timeEnd('tx-senders global');
            reply.status(200).send({ uniqueSenders: senders.size });
        });

        // Get unique senders for a specific chain
        app.get<{
            Params: { evmChainId: string };
        }>('/api/:evmChainId/stats/tx-senders', {
            schema: {
                params: {
                    type: 'object',
                    properties: {
                        evmChainId: { type: 'string' }
                    },
                    required: ['evmChainId']
                },
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            uniqueSenders: { type: 'number' }
                        },
                        required: ['uniqueSenders']
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
            console.time('tx-senders chain');
            const { evmChainId } = request.params;
            const evmChainIdNumber = parseInt(evmChainId);
            const db = await dbCtx.getIndexerDbConnection(evmChainIdNumber, 'daily_unique_senders');

            const query = `
                SELECT COUNT(DISTINCT sender) AS unique_senders
                FROM daily_unique_senders;
            `;

            const result = await db.prepare(query).get() as { unique_senders: number };
            console.timeEnd('tx-senders chain');
            reply.status(200).send({ uniqueSenders: result.unique_senders });
        });

    }


};

export default module;
