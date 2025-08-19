import { type IndexingPlugin } from "../../index.ts";

const module: IndexingPlugin<{ firstTx: number, lastTx: number } | undefined> = {
    name: "sanity_check",
    version: Math.floor(Math.random() * 1000000),
    usesTraces: false,

    // Initialize tables
    initialize: (db) => {
        //TODO: create table if not exists
    },

    // Process transactions
    extractData: (batch): { firstTx: number, lastTx: number } | undefined => {
        if (batch.txs.length === 0) return undefined;
        return {
            firstTx: batch.txs[0].txNum,
            lastTx: batch.txs[batch.txs.length - 1].txNum,
        };
    },

    saveExtractedData: (db, blocksDb, data) => {
        if (!data) {
            return;
        }
        db.exec(`
            CREATE TABLE IF NOT EXISTS sanity_check (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
        `);
        const prev = db.prepare(`SELECT value FROM sanity_check WHERE key = ?`).get("lastTx") as { value: number } | undefined;
        if (prev) {
            const prevLastTx = prev.value;
            if (data.firstTx !== prevLastTx + 1) {
                console.error(`Sanity check failed: firstTx (${data.firstTx}) != previous lastTx (${prevLastTx}) + 1`);
                process.exit(1);
            }
        }
        const stmt = db.prepare(`
            INSERT INTO sanity_check (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        `);
        stmt.run("firstTx", data.firstTx);
        stmt.run("lastTx", data.lastTx);
    }
};

export default module;
