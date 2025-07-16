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
        console.log(`Got batch ${batch.txs.length} txs`);
    }
};

export default module;
