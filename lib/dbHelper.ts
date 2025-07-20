import sqlite3 from 'better-sqlite3';

export function initializeIndexingDB(db: sqlite3.Database): void {
    db.exec(`
        CREATE TABLE IF NOT EXISTS kv_int (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )
    `);
}

export function getIntValue(db: sqlite3.Database, key: string, defaultValue: number): number {
    const value = db.prepare(`SELECT value FROM kv_int WHERE key = ?`).get(key) as { value: number } | undefined;
    return value?.value ?? defaultValue;
}

export function setIntValue(db: sqlite3.Database, key: string, value: number): void {
    db.prepare(`INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)`).run(key, value);
}
