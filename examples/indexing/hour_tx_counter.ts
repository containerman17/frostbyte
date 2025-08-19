import { type IndexingPlugin } from "../../index.ts";

const module: IndexingPlugin<Record<string, number>> = {
    name: "tx_counter",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,

    // Initialize tables
    initialize: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS hour_tx_counter (
                hour INTEGER,
                txCount INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (hour)
            )
        `);
    },

    // Process transactions
    extractData: (batch): Record<string, number> => {
        const hourCount = new Map<number, number>();
        for (let tx of batch.txs) {
            const hour = Math.floor(tx.blockTs / 3600) * 3600;
            hourCount.set(hour, (hourCount.get(hour) || 0) + 1);
        }
        return Object.fromEntries(hourCount.entries());
    },

    saveExtractedData: (db, blocksDb, data) => {
        const stmt = db.prepare(`
            INSERT INTO hour_tx_counter (hour, txCount)
            VALUES (?, ?)
            ON CONFLICT(hour) DO UPDATE SET txCount = txCount + excluded.txCount
        `);
        for (let [hour, txCount] of Object.entries(data)) {
            stmt.run(hour, txCount);
        }
    }
};

export default module;
