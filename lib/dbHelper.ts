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

        // Step 1: Initialize incremental vacuum if needed
        const isCaughtUp = getIntValue(db, 'is_caught_up', -1);
        if (isCaughtUp === -1) {
            // First time setup - enable incremental vacuum
            db.pragma('auto_vacuum = INCREMENTAL');
            db.pragma('VACUUM'); // builds the page map needed later
            console.log('IndexingDB: Initial setup with incremental vacuum enabled');
        }

        // Set pragmas based on catch-up state
        if (isCaughtUp === 1) {
            // Step 2: Post-catch-up optimized settings
            db.pragma('journal_size_limit = 67108864'); // 64 MB WAL limit
            db.pragma('wal_autocheckpoint = 10000');    // merge every ~40 MB
            console.log('IndexingDB: Using post-catch-up optimized settings');
        } else {
            // Pre-catch-up: larger WAL for bulk writes
            db.pragma('wal_autocheckpoint = 20000');    // ~80 MB before checkpoint pause
        }
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

/**
 * Step 2: Post-catch-up maintenance for indexing databases
 */
export function performIndexingPostCatchUpMaintenance(db: Database): void {
    console.log('IndexingDB: Starting post-catch-up maintenance...');
    const start = performance.now();
    
    // Flush & zero the big WAL
    db.pragma('wal_checkpoint(TRUNCATE)');
    
    // Set future WAL limits
    db.pragma('journal_size_limit = 67108864'); // 64 MB
    db.pragma('wal_autocheckpoint = 10000');    // merge every ~40 MB
    
    // Mark as caught up
    setIntValue(db, 'is_caught_up', 1);
    
    const end = performance.now();
    console.log(`IndexingDB: Post-catch-up maintenance completed in ${Math.round(end - start)}ms`);
}

/**
 * Step 3: Periodic maintenance for indexing databases during idle gaps
 */
export function performIndexingPeriodicMaintenance(db: Database): void {
    const isCaughtUp = getIntValue(db, 'is_caught_up', -1);
    if (isCaughtUp !== 1) return; // Only do this after catch-up
    
    const start = performance.now();
    
    // Reclaim ≤ 4 MB; finishes in ms
    db.pragma('incremental_vacuum(1000)');
    
    const end = performance.now();
    if (end - start > 10) { // Only log if it took more than 10ms
        console.log(`IndexingDB: Periodic maintenance completed in ${Math.round(end - start)}ms`);
    }
}

