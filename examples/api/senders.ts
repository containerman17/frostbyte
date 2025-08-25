import type { ApiPlugin } from "../../index.ts";
import type sqlite3 from 'better-sqlite3';

const module: ApiPlugin = {
    name: "senders_api",
    version: 6,  // Bump this when cache logic changes
    requiredIndexers: ['daily_unique_senders'],

    initializeCacheDb: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS global_senders (
                day INTEGER,
                sender TEXT,
                PRIMARY KEY (day, sender)
            );
            
            CREATE TABLE IF NOT EXISTS chain_sync_state (
                chain_id INTEGER PRIMARY KEY,
                last_synced_rowid INTEGER NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_global_senders ON global_senders(sender);
        `);
    },

    registerRoutes: (app, dbCtx) => {

        // Shared function to sync global senders from all chains
        const syncGlobalSenders = (cacheDb: sqlite3.Database) => {
            const chains = dbCtx.getAllChainConfigs();
            let totalNewPairs = 0;

            for (const chain of chains) {
                const indexerDb = dbCtx.getIndexerDbConnection(chain.evmChainId, 'daily_unique_senders');

                // Get last sync point
                const syncState = cacheDb.prepare(
                    'SELECT last_synced_rowid FROM chain_sync_state WHERE chain_id = ?'
                ).get(chain.evmChainId) as { last_synced_rowid: number } | undefined;

                const lastRowId = syncState?.last_synced_rowid ?? 0;

                // Get new day-sender pairs since last sync
                const newPairs = indexerDb.prepare(`
                    SELECT day, sender, rowid
                    FROM daily_unique_senders
                    WHERE rowid > ?
                    ORDER BY rowid
                `).all(lastRowId) as { day: number, sender: string, rowid: number }[];

                if (newPairs.length > 0) {
                    const insertStmt = cacheDb.prepare('INSERT OR IGNORE INTO global_senders VALUES (?, ?)');
                    const updateStmt = cacheDb.prepare('INSERT OR REPLACE INTO chain_sync_state VALUES (?, ?)');

                    const maxRowId = Math.max(...newPairs.map(p => p.rowid));

                    cacheDb.transaction(() => {
                        for (const { day, sender } of newPairs) {
                            insertStmt.run(day, sender);
                        }
                        updateStmt.run(chain.evmChainId, maxRowId);
                    })();

                    totalNewPairs += newPairs.length;
                    console.log(`[tx-senders] Synced ${newPairs.length} new day-sender pairs from chain ${chain.evmChainId}`);
                }
            }

            return totalNewPairs;
        };

        // Method 1: Using cache table with sync
        app.get('/api/global/stats/tx-senders-last-year-cached', {
            schema: {
                description: 'Get total unique senders across all chains for the last year using cache',
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            uniqueSenders: { type: 'number', description: 'Total unique senders in last year' }
                        },
                        required: ['uniqueSenders']
                    },
                    404: {
                        type: 'object',
                        properties: {
                            error: { type: 'string' }
                        }
                    }
                }
            }
        }, async (request, reply) => {
            console.time('tx-senders last year cached');

            if (!dbCtx.getCacheDb) {
                throw new Error('Cache database not available');
            }

            const cacheDb = dbCtx.getCacheDb();

            // Sync new data from all chains
            syncGlobalSenders(cacheDb);

            // Calculate last year range
            const now = Math.floor(Date.now() / 1000);
            const todayStart = Math.floor(now / 86400) * 86400;
            const oneYearAgo = todayStart - (364 * 86400); // 364 days back + today = 365 days

            // Get unique senders count for last year from cache
            const result = cacheDb.prepare(`
                SELECT COUNT(DISTINCT sender) as count 
                FROM global_senders 
                WHERE day >= ? AND day <= ?
            `).get(oneYearAgo, todayStart) as { count: number };

            console.timeEnd('tx-senders last year cached');
            reply.status(200).send({ uniqueSenders: result.count });
        });

        // Method 2: Calculate on JS side without cache
        app.get('/api/global/stats/tx-senders-last-year', {
            schema: {
                description: 'Get total unique senders across all chains for the last year calculated on JS side',
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            uniqueSenders: { type: 'number', description: 'Total unique senders in last year' }
                        },
                        required: ['uniqueSenders']
                    },
                    404: {
                        type: 'object',
                        properties: {
                            error: { type: 'string' }
                        }
                    }
                }
            }
        }, async (request, reply) => {
            console.time('tx-senders last year js');

            const chains = dbCtx.getAllChainConfigs();
            const uniqueSenders = new Set<string>();

            // Calculate last year range
            const now = Math.floor(Date.now() / 1000);
            const todayStart = Math.floor(now / 86400) * 86400;
            const oneYearAgo = todayStart - (364 * 86400); // 364 days back + today = 365 days

            // Query each chain and collect all unique senders
            for (const chain of chains) {
                const indexerDb = dbCtx.getIndexerDbConnection(chain.evmChainId, 'daily_unique_senders');

                const senders = indexerDb.prepare(`
                    SELECT DISTINCT sender 
                    FROM daily_unique_senders 
                    WHERE day >= ? AND day <= ?
                `).all(oneYearAgo, todayStart) as { sender: string }[];

                for (const { sender } of senders) {
                    uniqueSenders.add(sender);
                }
            }

            console.timeEnd('tx-senders last year js');
            reply.status(200).send({ uniqueSenders: uniqueSenders.size });
        });

    }


};

export default module;
