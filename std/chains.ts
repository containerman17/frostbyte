import type { IndexerModule } from "../index";

type ChainStatus = {
    evmChainId: number;
    chainName: string;
    blockchainId: string;
    hasDebug: boolean;
    lastStoredBlockNumber: number;
    latestRemoteBlockNumber: number;
    txCount: number;
    projectedTxCount: number;
}

const module: IndexerModule = {
    name: "chains",
    version: 1,
    usesTraces: false,

    wipe: (db) => { },
    initialize: (db) => { },
    handleTxBatch: (db, blocksDb, batch) => { },

    registerRoutes: (app, dbCtx) => {
        app.get('/chains', {
            schema: {
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                evmChainId: { type: 'number' },
                                chainName: { type: 'string' },
                                blockchainId: { type: 'string' },
                                hasDebug: { type: 'boolean' },
                                lastStoredBlockNumber: { type: 'number' },
                                latestRemoteBlockNumber: { type: 'number' },
                                txCount: { type: 'number' },
                                projectedTxCount: { type: 'number' }
                            },
                            required: ['evmChainId', 'chainName', 'blockchainId', 'hasDebug', 'lastStoredBlockNumber', 'latestRemoteBlockNumber', 'txCount', 'projectedTxCount']
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const configs = dbCtx.getAllChainConfigs();
            const result: ChainStatus[] = [];
            for (const config of configs) {
                const blocksDb = dbCtx.blocksDbFactory(config.evmChainId);

                const lastStoredBlockNumber = blocksDb.getLastStoredBlockNumber();
                const latestRemoteBlockNumber = blocksDb.getBlockchainLatestBlockNum();
                const txCount = blocksDb.getTxCount();

                // Calculate projected transaction count based on block ratio
                let projectedTxCount = txCount;
                if (lastStoredBlockNumber > 0 && latestRemoteBlockNumber > lastStoredBlockNumber) {
                    const blockRatio = latestRemoteBlockNumber / lastStoredBlockNumber;
                    projectedTxCount = Math.round(txCount * blockRatio);
                }

                result.push({
                    evmChainId: blocksDb.getEvmChainId(),
                    chainName: config.chainName,
                    blockchainId: config.blockchainId,
                    hasDebug: blocksDb.getHasDebug() === 1,
                    lastStoredBlockNumber,
                    latestRemoteBlockNumber,
                    txCount,
                    projectedTxCount,
                });
            }
            return reply.send(result);
        });
    }
};

export default module;
