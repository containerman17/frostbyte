import { BlocksDBHelper } from "./blockFetcher/BlocksDBHelper.js";
import { ChainConfig, getPluginDirs, getSqliteDb } from "./config.js";
import { loadIndexingPlugins } from "./lib/plugins.js";
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

export default async function executeIndexingTask<ExtractedDataType>(args: {
    chainConfig: ChainConfig,
    pluginName: string,
    pluginVersion: number,
    fromTx: number,
    toTx: number
}): Promise<{
    extractedData: ExtractedDataType,
    indexedTxs: number
}> {
    const { chainConfig, pluginName, pluginVersion, fromTx, toTx } = args;
    const plugins = await pluginsPromise;
    const plugin = plugins.find(p => p.name === pluginName);
    if (!plugin) {
        throw new Error(`Plugin ${pluginName} v${pluginVersion} not found. Available plugins: ${plugins.map(p => `${p.name}`).join(", ")}`);
    }

    const blocksDb = getBlocksDb(chainConfig);

    const transactions = blocksDb.getTxBatch(fromTx, toTx, plugin.usesTraces, plugin.filterEvents);

    const extractedData = plugin.extractData(transactions);

    return {
        extractedData,
        indexedTxs: transactions.txs.length,
    };
}
