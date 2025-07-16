import { Database } from "better-sqlite3";

export function initializeIndexingDB({ db, isReadonly }: { db: Database, isReadonly: boolean }): void {
    if (isReadonly) {
        // Readonly: optimize for fast reads
        db.pragma('mmap_size = 53687091200'); // 50GB - map entire database
        db.pragma('cache_size = -32000'); // 32MB cache
        db.pragma('synchronous = OFF'); // Fastest, safe for readonly
        db.pragma('temp_store = MEMORY');
        // Don't set journal_mode - it's already set by writer and requires write access
    } else {
        // Writer: optimize for fast writes while preventing corruption
        db.pragma('mmap_size = 53687091200'); // 50GB - map entire database
        db.pragma('cache_size = -64000'); // 64MB cache
        db.pragma('synchronous = NORMAL'); // Fast but prevents corruption
        db.pragma('journal_mode = WAL'); // Only writer sets this
        db.pragma('temp_store = MEMORY');
        // db.pragma('locking_mode = EXCLUSIVE'); // Single writer optimization

        // Create kv_int table for integer key-value storage
        db.exec(`
            CREATE TABLE IF NOT EXISTS kv_int (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            ) WITHOUT ROWID;
        `);
    }
}

export function getIntValue(db: Database, key: string, defaultValue: number): number {
    const stmt = db.prepare('SELECT value FROM kv_int WHERE key = ?');
    const result = stmt.get(key) as { value: number } | undefined;
    return result?.value ?? defaultValue;
}

export function setIntValue(db: Database, key: string, value: number): void {
    const stmt = db.prepare('INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)');
    stmt.run(key, value);
}
