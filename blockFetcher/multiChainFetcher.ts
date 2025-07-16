import { BlockDB } from './BlockDB.js';
import { BatchRpc } from './BatchRpc.js';
import { startFetchingLoop } from './startFetchingLoop.js';
import { getBlocksDbPath } from '../lib/dbPaths.js';
import { ChainConfig } from '../config.js';

export async function startMultiChainFetcher(chainConfigs: ChainConfig[]): Promise<void> {
    console.log(`Starting multi-chain fetcher for ${chainConfigs.length} chains`);

    // Start a fetching loop for each chain concurrently
    const fetchingPromises = chainConfigs.map(async (chainConfig) => {
        try {
            const blocksDbPath = getBlocksDbPath(chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug);

            // Create BlockDB instance for this chain
            const blocksDb = new BlockDB({
                path: blocksDbPath,
                isReadonly: false,
                hasDebug: chainConfig.rpcConfig.rpcSupportsDebug,
                chainId: chainConfig.blockchainId
            });

            await blocksDb.init();

            // Create BatchRpc instance for this chain
            const batchRpc = new BatchRpc(chainConfig.rpcConfig);

            console.log(`[${chainConfig.chainName}] Starting fetcher for chain ${chainConfig.evmChainId}`);

            // Start the fetching loop for this chain
            await startFetchingLoop(
                blocksDb,
                batchRpc,
                chainConfig.rpcConfig.blocksPerBatch,
                chainConfig.chainName
            );
        } catch (error) {
            console.error(`[${chainConfig.chainName}] Fatal error in fetcher:`, error);
            throw error; // Re-throw to crash the process and trigger restart
        }
    });

    // Wait for all chains (though they should run indefinitely)
    await Promise.all(fetchingPromises);
} 
