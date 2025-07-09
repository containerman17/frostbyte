import type { ApiPlugin } from "../index";

type TpsStats = {
    blockchainId: string;
    evmChainId: number;
    txs: number;
    tps: number;
}

const module: ApiPlugin = {
    name: "tps_stats",
    requiredIndexers: [],

    registerRoutes: (app, dbCtx) => {
        app.get('/stats/tps/day', {
            schema: {
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                blockchainId: { type: 'string' },
                                evmChainId: { type: 'number' },
                                txs: { type: 'number' },
                                tps: { type: 'number' }
                            },
                            required: ['blockchainId', 'evmChainId', 'txs', 'tps']
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const configs = dbCtx.getAllChainConfigs();
            const results: TpsStats[] = [];
            
            // Get current timestamp in seconds
            const now = Math.floor(Date.now() / 1000);
            const twentyFourHoursAgo = now - 86400; // 24 hours = 86400 seconds
            
            for (const config of configs) {
                try {
                    const blocksDb = dbCtx.blocksDbFactory(config.evmChainId);
                    
                    // Get the latest block number
                    const latestBlockNum = blocksDb.getLastStoredBlockNumber();
                    if (latestBlockNum < 0) {
                        // No blocks stored yet
                        results.push({
                            blockchainId: config.blockchainId,
                            evmChainId: config.evmChainId,
                            txs: 0,
                            tps: 0
                        });
                        continue;
                    }
                    
                    let txCount = 0;
                    let currentBlockNum = latestBlockNum;
                    let oldestBlockTimestamp = now;
                    
                    // Iterate backwards through blocks until we find one older than 24 hours
                    // We'll sample blocks to find the approximate range first
                    const blocksToCheck = Math.min(latestBlockNum + 1, 50000); // Limit iterations
                    const sampleSize = Math.min(100, Math.max(10, Math.floor(blocksToCheck / 100)));
                    
                    // Binary search to find approximate block from 24 hours ago
                    let lowBlock = Math.max(0, latestBlockNum - blocksToCheck);
                    let highBlock = latestBlockNum;
                    let targetBlock = lowBlock;
                    
                    // Do a few binary search iterations to narrow down the range
                    for (let i = 0; i < 10 && lowBlock < highBlock - sampleSize; i++) {
                        const midBlock = Math.floor((lowBlock + highBlock) / 2);
                        const block = blocksDb.slow_getBlockWithTransactions(midBlock);
                        
                        if (block) {
                            const blockTimestamp = parseInt(block.timestamp, 16);
                            if (blockTimestamp > twentyFourHoursAgo) {
                                highBlock = midBlock;
                            } else {
                                lowBlock = midBlock;
                                targetBlock = midBlock;
                            }
                        } else {
                            // Block not found, adjust range
                            highBlock = midBlock - 1;
                        }
                    }
                    
                    // Now count transactions from targetBlock to latestBlockNum
                    // We'll process in chunks to avoid loading too many blocks
                    const chunkSize = 100;
                    
                    for (let blockNum = targetBlock; blockNum <= latestBlockNum; blockNum += chunkSize) {
                        const endBlock = Math.min(blockNum + chunkSize - 1, latestBlockNum);
                        
                        for (let b = blockNum; b <= endBlock; b++) {
                            const block = blocksDb.slow_getBlockWithTransactions(b);
                            if (block) {
                                const blockTimestamp = parseInt(block.timestamp, 16);
                                
                                // Only count transactions from blocks within the last 24 hours
                                if (blockTimestamp >= twentyFourHoursAgo) {
                                    txCount += block.transactions.length;
                                    oldestBlockTimestamp = Math.min(oldestBlockTimestamp, blockTimestamp);
                                }
                            }
                        }
                    }
                    
                    // Calculate actual time span in seconds
                    const actualTimeSpan = Math.max(1, now - oldestBlockTimestamp);
                    const tps = txCount / actualTimeSpan;
                    
                    results.push({
                        blockchainId: config.blockchainId,
                        evmChainId: config.evmChainId,
                        txs: txCount,
                        tps: Number(tps.toFixed(6)) // Round to 6 decimal places
                    });
                } catch (error) {
                    console.error(`Error getting TPS stats for chain ${config.chainName}:`, error);
                    // Include chain with 0 values if error occurs
                    results.push({
                        blockchainId: config.blockchainId,
                        evmChainId: config.evmChainId,
                        txs: 0,
                        tps: 0
                    });
                }
            }
            
            // Sort by TPS descending
            results.sort((a, b) => b.tps - a.tps);
            
            return reply.send(results);
        });
    }
};

export default module;