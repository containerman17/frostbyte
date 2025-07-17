import { type IndexingPlugin } from "../index.js";

const module: IndexingPlugin = {
    name: "nothing",
    version: 1,
    usesTraces: false,

    // Initialize tables
    initialize: (db) => {

    },

    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        console.log(`Got batch ${batch.txs.length} txs from blocks ${Number(batch.txs[0]!.receipt.blockNumber)} to ${Number(batch.txs[batch.txs.length - 1]!.receipt.blockNumber)}`);
    }
};

export default module;
