import fs from 'node:fs';
import path from 'node:path';

export async function createPluginTemplate(name: string, pluginsDir: string) {
    const pluginPath = path.join(pluginsDir, `${name}.ts`);

    // Create plugins directory if it doesn't exist
    if (!fs.existsSync(pluginsDir)) {
        fs.mkdirSync(pluginsDir, { recursive: true });
    }

    // Check if plugin already exists
    if (fs.existsSync(pluginPath)) {
        throw new Error(`Plugin ${name} already exists at ${pluginPath}`);
    }

    const template = `import { type IndexerModule, prepQueryCached } from "frostbyte-sdk";

const module: IndexerModule = {
    name: "${name}",
    version: 1,
    usesTraces: false,
    
    // Reset all plugin data
    wipe: (db) => {
        db.exec(\`DROP TABLE IF EXISTS ${name}_data\`);
    },
    
    // Initialize tables
    initialize: (db) => {
        db.exec(\`
            CREATE TABLE IF NOT EXISTS ${name}_data (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_count INTEGER NOT NULL DEFAULT 0
            )
        \`);

        // Initialize with 0 if not exists
        db.exec(\`INSERT OR IGNORE INTO ${name}_data (id, total_count) VALUES (1, 0)\`);
    },
    
    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        const txCount = batch.txs.length;
        prepQueryCached(db, \`
            UPDATE ${name}_data SET total_count = total_count + ? WHERE id = 1
        \`).run(txCount);
    },
    
    // Add API endpoints
    registerRoutes: (app, dbCtx) => {
        app.get('/:evmChainId/${name}/total', {
            schema: {
                params: {
                    type: 'object',
                    properties: {
                        evmChainId: { type: 'number' }
                    },
                    required: ['evmChainId']
                },
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            totalCount: { type: 'number' }
                        },
                        required: ['totalCount']
                    }
                }
            }
        }, async (request, reply) => {
            const { evmChainId } = request.params as { evmChainId: number };
            const db = dbCtx.indexerDbFactory(evmChainId);

            const result = prepQueryCached(db, \`
                SELECT total_count FROM ${name}_data WHERE id = 1
            \`).get() as { total_count: number } | undefined;

            return {
                totalCount: result?.total_count || 0
            };
        });
    }
};

export default module;
`;

    fs.writeFileSync(pluginPath, template);
    console.log(`✅ Created plugin: ${pluginPath}`);
    console.log(`\nNext steps:`);
    console.log(`1. Edit the plugin to add your indexing logic`);
    console.log(`2. Run: frostbyte run --plugins-dir ${pluginsDir}`);
} 
