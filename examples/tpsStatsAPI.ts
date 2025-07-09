import type { ApiPlugin } from "../index";
import { decompress as zstdDecompress } from 'zstd-napi';

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
                    const db = blocksDb.getDatabase();
                    
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
                    
                    // First, find the approximate block range for the last 24 hours
                    // We'll sample a few blocks to find the timestamp range
                    const sampleStmt = db.prepare(`
                        SELECT number, data, codec
                        FROM blocks 
                        WHERE number IN (
                            SELECT number FROM blocks 
                            WHERE number % ? = 0 
                            ORDER BY number DESC 
                            LIMIT 100
                        )
                        ORDER BY number DESC
                    `);
                    
                    // Sample every 100th block
                    const sampledBlocks = sampleStmt.all(100) as Array<{ number: number; data: Buffer; codec: number }>;
                    
                    let startBlockNum = 0;
                    let oldestTimestampInRange = now;
                    
                    // Find the first block older than 24 hours ago
                    for (const row of sampledBlocks) {
                        let decompressedData: Buffer;
                        if (row.codec === 0) {
                            decompressedData = zstdDecompress(row.data);
                        } else {
                            // Skip dict-compressed blocks for now
                            continue;
                        }
                        
                        const block = JSON.parse(decompressedData.toString());
                        const blockTimestamp = parseInt(block.timestamp, 16);
                        
                        if (blockTimestamp < twentyFourHoursAgo) {
                            // Found a block older than 24 hours, use the next sampled block as start
                            const prevIndex = sampledBlocks.indexOf(row) - 1;
                            if (prevIndex >= 0 && sampledBlocks[prevIndex]) {
                                startBlockNum = sampledBlocks[prevIndex].number;
                            } else {
                                startBlockNum = row.number + 1;
                            }
                            break;
                        }
                        
                        oldestTimestampInRange = Math.min(oldestTimestampInRange, blockTimestamp);
                    }
                    
                    // If all sampled blocks are within 24 hours, start from block 0
                    if (startBlockNum === 0 && sampledBlocks.length > 0) {
                        const lastSampledBlock = sampledBlocks[sampledBlocks.length - 1];
                        startBlockNum = Math.max(0, (lastSampledBlock?.number ?? 0) - 100);
                    }
                    
                    // Now count transactions in the range
                    const countStmt = db.prepare(`
                        SELECT COUNT(*) as tx_count
                        FROM txs
                        WHERE block_num >= ? AND block_num <= ?
                    `);
                    
                    const result = countStmt.get(startBlockNum, latestBlockNum) as { tx_count: number };
                    const txCount = result.tx_count;
                    
                    // Calculate TPS based on actual time range
                    const actualTimeSpan = Math.max(1, now - oldestTimestampInRange);
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