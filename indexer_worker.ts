import { BlocksDBHelper } from "./blockFetcher/BlocksDBHelper";
import { ChainConfig, getPluginDirs, getSqliteDb } from "./config";
import { loadIndexingPlugins } from "./lib/plugins";
import { IndexingPlugin } from "./lib/types.js";

const pluginsPromise = loadIndexingPlugins(getPluginDirs());

const blocksDbCache = new Map<string, BlocksDBHelper>();
function getBlocksDb(chainConfig: ChainConfig) {
    const key = `${chainConfig.blockchainId}-${chainConfig.rpcConfig.rpcSupportsDebug}`;
    if (!blocksDbCache.has(key)) {
        blocksDbCache.set(key, new BlocksDBHelper(getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "blocks",
            chainId: chainConfig.blockchainId,
            readonly: true,
        }),
            true,
            chainConfig.rpcConfig.rpcSupportsDebug
        ));
    }
    return blocksDbCache.get(key)!;
}

export default async function executeIndexingTask<ExtractedDataType>(chainConfig: ChainConfig, pluginName: string, pluginVersion: number, fromTx: number, toTx: number): Promise<{
    extractedData: ExtractedDataType,
    hadSomeData: boolean,
    lastIndexedTx: number | undefined,
    lastIndexedBlock: number | undefined,
    indexedTxs: number
}> {
    const plugins = await pluginsPromise;
    const plugin = plugins.find(p => p.name === pluginName && p.version === pluginVersion);
    if (!plugin) {
        throw new Error(`Plugin ${pluginName} v${pluginVersion} not found`);
    }

    const blocksDb = getBlocksDb(chainConfig);

    const transactions = blocksDb.getTxBatch(fromTx, toTx, plugin.usesTraces, plugin.filterEvents);

    const extractedData = plugin.extractData(transactions);
    const hadSomeData = transactions.txs.length > 0;

    return {
        extractedData,
        hadSomeData,
        lastIndexedTx: transactions.txs[transactions.txs.length - 1]?.txNum,
        lastIndexedBlock: transactions.txs[transactions.txs.length - 1]?.receipt.blockNumber
            ? parseInt(transactions.txs[transactions.txs.length - 1]!.receipt.blockNumber)
            : undefined,
        indexedTxs: transactions.txs.length,
    };
}
