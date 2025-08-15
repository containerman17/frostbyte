import { type IndexingPlugin, evmTypes } from "../index.js";

const module: IndexingPlugin = {
    name: "nothing",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,
    // filterEvents: [
    //     "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d",
    // ],

    // Initialize tables
    initialize: (db) => {
        //This table exists by default: 
        // CREATE TABLE IF NOT EXISTS kv_int (
        //     key   TEXT PRIMARY KEY,
        //     value INTEGER NOT NULL
        // )
    },

    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        // const topicStats: Record<string, number> = {};
        // console.log(`Got batch ${batch.txs.length} txs from blocks ${Number(batch.txs[0]!.receipt.blockNumber)} to ${Number(batch.txs[batch.txs.length - 1]!.receipt.blockNumber)}`);
        // for (let tx of batch.txs) {
        //     const txTopics = new Set<string>();
        //     for (let log of tx.receipt.logs) {
        //         const topic = log.topics[0];
        //         if (!topic) {
        //             continue;
        //         }
        //         txTopics.add(topic);
        //     }
        //     for (let topic of txTopics) {
        //         topicStats[topic] = (topicStats[topic] || 0) + 1;
        //     }
        // }

        //txs atrt with 1
        let lastRecordedTxNum = (db.prepare("SELECT value FROM kv_int WHERE key = 'last_recorded_tx_num'").get() as { value: number } | undefined)?.value || 0;

        for (let { txNum } of batch.txs) {
            if (lastRecordedTxNum !== txNum - 1) {
                throw new Error(`Tx num mismatch: ${lastRecordedTxNum} !== ${txNum - 1}`);
            }
            lastRecordedTxNum = txNum;
        }

        db.prepare("INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)").run('last_recorded_tx_num', lastRecordedTxNum);
        console.log(`Recorded tx num: ${lastRecordedTxNum}`);
    }
};

export default module;
