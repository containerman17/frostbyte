import { type IndexingPlugin, evmTypes } from "../../index.js";

type MinuteTxSenderRow = {
    minute: number;
    sender: string;
}

const module: IndexingPlugin<MinuteTxSenderRow[]> = {
    name: "minute_tx_sender",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,
    // filterEvents: [
    //     "0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d",
    // ],

    // Initialize tables
    initialize: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS minute_tx_sender (
                minute INTEGER,
                sender TEXT,
                PRIMARY KEY (minute, sender)
            )
        `);
    },

    // Process transactions
    extractData: (batch): MinuteTxSenderRow[] => {
        const uniquePairs = new Set<string>();
        const result: MinuteTxSenderRow[] = [];

        for (let tx of batch.txs) {
            const sender = tx.receipt.from;
            const minute = Math.floor(tx.blockTs / 60) * 60;
            const key = `${minute}:${sender}`;

            if (!uniquePairs.has(key)) {
                uniquePairs.add(key);
                result.push({ minute, sender });
            }
        }

        return result;
    },

    saveExtractedData: (db, blocksDb, data) => {
        const stmt = db.prepare(`
            INSERT INTO minute_tx_sender (minute, sender)
            VALUES (?, ?)
            ON CONFLICT(minute, sender) DO NOTHING
        `);
        for (let row of data) {
            stmt.run(row.minute, row.sender);
        }
    }
};

export default module;
