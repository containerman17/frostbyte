import { type IndexingPlugin, evmTypes } from "../index.js";

const module: IndexingPlugin = {
    name: "nothing",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,
    filterEvents: [
        // "0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498",
        //  "0xa25801b4a623dc96869bc01ae2ec7d763ba5e8012407b9d7f96fd65b8a2ce1ba", 
        evmTypes.CONTRACT_CREATION_TOPIC
    ],

    // Initialize tables
    initialize: (db) => {

    },

    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        const topicStats: Record<string, number> = {};
        console.log(`Got batch ${batch.txs.length} txs from blocks ${Number(batch.txs[0]!.receipt.blockNumber)} to ${Number(batch.txs[batch.txs.length - 1]!.receipt.blockNumber)}`);
        for (let tx of batch.txs) {
            console.log(tx)
            for (let log of tx.receipt.logs) {
                const topic = log.topics[0];
                if (!topic) {
                    continue;
                }
                topicStats[topic] = (topicStats[topic] || 0) + 1;
            }
        }
        console.log(topicStats);
    }
};

export default module;
