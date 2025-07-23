import { type IndexingPlugin, evmTypes } from "../index.js";

const module: IndexingPlugin = {
    name: "nothing",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,
    filterEvents: [
        "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d",
    ],

    // Initialize tables
    initialize: (db) => {

    },

    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        const topicStats: Record<string, number> = {};
        console.log(`Got batch ${batch.txs.length} txs from blocks ${Number(batch.txs[0]!.receipt.blockNumber)} to ${Number(batch.txs[batch.txs.length - 1]!.receipt.blockNumber)}`);
        for (let tx of batch.txs) {
            const txTopics = new Set<string>();
            for (let log of tx.receipt.logs) {
                const topic = log.topics[0];
                if (!topic) {
                    continue;
                }
                txTopics.add(topic);
            }
            for (let topic of txTopics) {
                topicStats[topic] = (topicStats[topic] || 0) + 1;
            }
        }
    }
};

export default module;
