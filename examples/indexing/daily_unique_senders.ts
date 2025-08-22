import { type IndexingPlugin } from "../../index.ts";

type DailyUniqueSendersData = Record<number, string[]>;

const module: IndexingPlugin<DailyUniqueSendersData> = {
    name: "daily_unique_senders",
    version: 1,
    usesTraces: false,

    // Initialize tables
    initialize: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS daily_unique_senders (
                day INTEGER,
                sender TEXT,
                PRIMARY KEY (day, sender)
            )
        `);
    },

    // Process transactions
    extractData: (batch): DailyUniqueSendersData => {
        const uniqueSenders: DailyUniqueSendersData = {};
        for (let tx of batch.txs) {
            const day = Math.floor(tx.blockTs / 86400) * 86400;
            if (!uniqueSenders[day]) {
                uniqueSenders[day] = [];
            }
            uniqueSenders[day].push(tx.tx.from);
        }
        return uniqueSenders;
    },

    saveExtractedData: (db, blocksDb, data: DailyUniqueSendersData) => {
        const stmt = db.prepare(`
            INSERT INTO daily_unique_senders (day, sender)
            VALUES (?, ?)
            ON CONFLICT(day, sender) DO NOTHING
        `);
        for (let [day, senders] of Object.entries(data)) {
            for (let sender of senders) {
                stmt.run(day, sender);
            }
        }
    }
};

export default module;
