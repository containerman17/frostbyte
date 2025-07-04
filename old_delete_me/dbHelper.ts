import { Database, Statement } from "better-sqlite3";

export class IndexingDbHelper {
    private db: Database;

    constructor(db: Database) {
        this.db = db;
        this.initSystemTables();
    }

    private preppedQueries = new Map<string, Statement>();
    public prepQuery(query: string): Statement {
        if (this.preppedQueries.has(query)) {
            return this.preppedQueries.get(query)!;
        }
        const prepped = this.db.prepare(query);
        this.preppedQueries.set(query, prepped);
        return prepped;
    }

    initSystemTables() {
        this.prepQuery(`
    CREATE TABLE IF NOT EXISTS kv_int (
        key   TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    ) WITHOUT ROWID;
    `).run();
    }

    setInteger(key: string, value: number) {
        this.prepQuery(`INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)`).run(key, value);
    }

    getInteger(key: string, defaultValue: number): number {
        const result = this.prepQuery(`SELECT value FROM kv_int WHERE key = ?`).get(key) as { value: number } | undefined;
        return result ? result.value : defaultValue;
    }
}



const helpersCache = new Map<Database, IndexingDbHelper>();
export function getIndexingDbHelper(db: Database): IndexingDbHelper {
    if (!helpersCache.has(db)) {
        helpersCache.set(db, new IndexingDbHelper(db));
    }
    return helpersCache.get(db)!;
}
