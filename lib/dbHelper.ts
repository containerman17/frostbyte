import sqlite3 from 'better-sqlite3';

export function initializeIndexingDB(db: sqlite3.Database): Promise<void> {
    db.exec(`
        CREATE TABLE IF NOT EXISTS kv_int (
            \`key\`   VARCHAR(255) PRIMARY KEY,
            \`value\` BIGINT NOT NULL
        )
    `);
}

export function getIntValue(db: sqlite3.Database, key: string, defaultValue: number): number {
    const value = db.prepare(`SELECT value FROM kv_int WHERE key = ?`).get(key) as { value: number } | undefined;
    return value?.value ?? defaultValue;
}

export function setIntValue(db: sqlite3.Database, key: string, value: number): void {
    db.prepare(`INSERT INTO kv_int (key, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)`).run(key, value);
}
