import { BatchRpc } from "./BatchRpc.js";
import { BlocksDBHelper } from "./BlocksDBHelper.js"

const NO_BLOCKS_PAUSE_TIME = 3 * 1000;
const ERROR_PAUSE_TIME = 10 * 1000;

export async function startFetchingLoop(blockDB: BlocksDBHelper, batchRpc: BatchRpc, blocksPerBatch: number, chainName: string) {
    let latestRemoteBlock = blockDB.getBlockchainLatestBlockNum()
    const needsChainId = (blockDB.getEvmChainId()) === -1;

    while (true) {//Kinda wait for readiness
        try {
            const newLatestRemoteBlock = await batchRpc.getCurrentBlockNumber();
            blockDB.setBlockchainLatestBlockNum(newLatestRemoteBlock);
            latestRemoteBlock = newLatestRemoteBlock;

            // Also get chain ID if needed
            if (needsChainId) {
                const newEvmChainId = await batchRpc.getEvmChainId();
                blockDB.setEvmChainId(newEvmChainId);
            }

            break;
        } catch (error) {
            console.error(error);
            await new Promise(resolve => setTimeout(resolve, ERROR_PAUSE_TIME));
        }
    }

    let lastStoredBlock = blockDB.getLastStoredBlockNumber();

    while (true) {
        // Check if we've caught up to the latest remote block
        if (lastStoredBlock >= latestRemoteBlock) {
            const newLatestRemoteBlock = await batchRpc.getCurrentBlockNumber();
            if (newLatestRemoteBlock === latestRemoteBlock) {
                console.log(`[${chainName}] No new blocks, pause before checking again ${NO_BLOCKS_PAUSE_TIME / 1000}s`)

                await new Promise(resolve => setTimeout(resolve, NO_BLOCKS_PAUSE_TIME));
                continue;
            }
            // Update latest remote block and continue fetching
            latestRemoteBlock = newLatestRemoteBlock;
            console.log(`[${chainName}] Updated latest remote block to ${latestRemoteBlock}`);
            try {
                blockDB.setBlockchainLatestBlockNum(latestRemoteBlock);
            } catch (error) {
                console.error(`[${chainName}] Failed to update blockchain latest block number:`, error);
                // Continue - we can still fetch blocks even if we can't update this metadata
            }
        }

        const startBlock = lastStoredBlock + 1;
        const endBlock = Math.min(startBlock + blocksPerBatch - 1, latestRemoteBlock);
        try {
            const start = performance.now();
            const blockNumbers = Array.from({ length: endBlock - startBlock + 1 }, (_, i) => startBlock + i);
            const blocksPromise = batchRpc.getBlocksWithReceipts(blockNumbers);
            await new Promise(resolve => setTimeout(resolve, 100));
            blockDB.performCompressionMaintenance();
            await new Promise(resolve => setTimeout(resolve, 100));
            blockDB.performBlockCompressionMaintenance();

            //now actually await
            const blocks = await blocksPromise;

            blockDB.storeBlocks(blocks);
            lastStoredBlock = endBlock;

            const end = performance.now();
            const blocksLeft = latestRemoteBlock - endBlock;
            const blocksPerSecond = blocks.length / (end - start) * 1000;
            const secondsLeft = blocksLeft / blocksPerSecond;
            console.log(`[${chainName}] Fetched ${blocks.length} blocks in ${Math.round(end - start)}ms, that's ~${Math.round(blocksPerSecond)} blocks/s, ${blocksLeft.toLocaleString()} blocks left, ~${formatSeconds(secondsLeft)} left`);
        } catch (error) {
            console.error(`[${chainName}] Error fetching/storing blocks ${startBlock}-${endBlock}:`, error);
            // Re-fetch the actual last stored block from database after error
            lastStoredBlock = blockDB.getLastStoredBlockNumber();
            console.log(`[${chainName}] Reset lastStoredBlock to ${lastStoredBlock} after error`);
            await new Promise(resolve => setTimeout(resolve, ERROR_PAUSE_TIME));
        }
    }
}

function formatSeconds(seconds: number) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = Math.round(seconds) % 60;
    if (days > 0) return `${days}d${hours}h${minutes}m${remainingSeconds}s`;
    if (hours > 0) return `${hours}h${minutes}m${remainingSeconds}s`;
    if (minutes > 0) return `${minutes}m${remainingSeconds}s`;
    return `${hours}h${minutes}m${remainingSeconds}s`;
}
