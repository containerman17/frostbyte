import { Database, Statement } from "better-sqlite3";

const preppedQueries = new WeakMap<Database, Map<string, Statement>>();
export function prepQueryCached(db: Database, query: string): Statement {
    if (!preppedQueries.has(db)) {
        preppedQueries.set(db, new Map());
    }
    const preppedQueriesForDb = preppedQueries.get(db)!;
    if (preppedQueriesForDb.has(query)) {
        return preppedQueriesForDb.get(query)!;
    } else {
        const prepped = db.prepare(query);
        preppedQueriesForDb.set(query, prepped);
        return prepped;
    }
}
