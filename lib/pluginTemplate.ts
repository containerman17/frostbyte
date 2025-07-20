import fs from 'node:fs';
import path from 'node:path';

export async function createPluginTemplate(name: string, pluginsDir: string, type: 'indexing' | 'api' = 'indexing') {
    const pluginPath = path.join(pluginsDir, `${name}.ts`);

    // Create plugins directory if it doesn't exist
    if (!fs.existsSync(pluginsDir)) {
        fs.mkdirSync(pluginsDir, { recursive: true });
    }

    // Check if plugin already exists
    if (fs.existsSync(pluginPath)) {
        throw new Error(`Plugin ${name} already exists at ${pluginPath}`);
    }

    let template: string;

    if (type === 'indexing') {
        template = `import { type IndexingPlugin, prepQueryCached } from "frostbyte-sdk";

const module: IndexingPlugin = {
    name: "${name}",
    version: 1,
    usesTraces: false,
    
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
    }
};

export default module;
`;
    } else {
        template = `import { type ApiPlugin, prepQueryCached } from "frostbyte-sdk";

const module: ApiPlugin = {
    name: "${name}",
    requiredIndexers: ["${name}-indexer"], // List the indexers this API needs
    
    // Add API endpoints
    registerRoutes: (app, dbCtx) => {
        app.get('/:evmChainId/${name}/stats', {
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
        }, (request, reply) => {
            const { evmChainId } = request.params as { evmChainId: number };
            // Get the database for the indexer we depend on
            const db = dbCtx.getIndexerDbConnection(evmChainId, "${name}-indexer");

            const result = prepQueryCached(db, \`
                SELECT total_count FROM ${name}_indexer_data WHERE id = 1
            \`).get() as { total_count: number } | undefined;

            return {
                totalCount: result?.total_count || 0
            };
        });
    }
};

export default module;
`;
    }

    fs.writeFileSync(pluginPath, template);
    console.log(`✅ Created ${type} plugin: ${pluginPath}`);
    console.log(`\nNext steps:`);
    if (type === 'indexing') {
        console.log(`1. Edit the plugin to add your indexing logic`);
        console.log(`2. Run: frostbyte run --plugins-dir ${pluginsDir}`);
    } else {
        console.log(`1. Edit the plugin to specify which indexers it needs`);
        console.log(`2. Update the API endpoints to query the indexer databases`);
        console.log(`3. Run: frostbyte run --plugins-dir ${pluginsDir}`);
    }
} 
