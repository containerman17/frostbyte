import type { ApiPlugin } from "../index";

type ReplicationChain = {
    chainName: string;
    blockchainId: string;
    evmChainId: number;
    rpcConfig: {
        rpcUrl: string;
        requestBatchSize: number;
        maxConcurrentRequests: number;
        rps: number;
        rpcSupportsDebug: boolean;
        enableBatchSizeGrowth: boolean;
        blocksPerBatch: number;
    };
}

const module: ApiPlugin = {
    name: "replicationChains",
    requiredIndexers: [],

    registerRoutes: (app, dbCtx) => {
        app.get('/api/replication/chains.json', {
            schema: {
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                chainName: { type: 'string' },
                                blockchainId: { type: 'string' },
                                evmChainId: { type: 'number' },
                                rpcConfig: {
                                    type: 'object',
                                    properties: {
                                        rpcUrl: { type: 'string' },
                                        requestBatchSize: { type: 'number' },
                                        maxConcurrentRequests: { type: 'number' },
                                        rps: { type: 'number' },
                                        rpcSupportsDebug: { type: 'boolean' },
                                        enableBatchSizeGrowth: { type: 'boolean' },
                                        blocksPerBatch: { type: 'number' }
                                    },
                                    required: ['rpcUrl', 'requestBatchSize', 'maxConcurrentRequests', 'rps', 'rpcSupportsDebug', 'enableBatchSizeGrowth', 'blocksPerBatch']
                                }
                            },
                            required: ['chainName', 'blockchainId', 'evmChainId', 'rpcConfig']
                        }
                    }
                }
            }
        }, (request, reply) => {
            const configs = dbCtx.getAllChainConfigs();
            const result: ReplicationChain[] = [];

            // Get the host from the request
            const host = request.hostname;
            const protocol = request.protocol;
            const port = request.socket.localPort;

            // Construct base URL
            let baseUrl = `${protocol}://${host}`;
            if ((protocol === 'http' && port !== 80) || (protocol === 'https' && port !== 443)) {
                baseUrl += `:${port}`;
            }

            for (const config of configs) {
                result.push({
                    chainName: config.chainName,
                    blockchainId: config.blockchainId,
                    evmChainId: config.evmChainId,
                    rpcConfig: {
                        rpcUrl: `${baseUrl}/api/${config.evmChainId}/rpc`,
                        requestBatchSize: 20,
                        maxConcurrentRequests: 300,
                        rps: 1000,
                        rpcSupportsDebug: config.rpcConfig.rpcSupportsDebug,
                        enableBatchSizeGrowth: false,
                        blocksPerBatch: 100,
                    }
                });
            }

            return reply.send(result);
        });
    }
};

export default module;
