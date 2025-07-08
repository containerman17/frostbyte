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

    const template = `import type { IndexerModule } from "frostbyte";

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
                block_number INTEGER PRIMARY KEY,
                -- Add your columns here
                value TEXT
            )
        \`);
    },
    
    // Process transactions
    handleTxBatch: (db, blocksDb, batch) => {
        // Your indexing logic here
        for (const tx of batch.txs) {
            // Process each transaction
            console.log(\`Processing block \${tx.block.number}, tx \${tx.tx.hash}\`);
        }
    },
    
    // Optional: Add API endpoints
    registerRoutes: (app, dbCtx) => {
        app.get('/:evmChainId/${name}/status', async (request, reply) => {
            const { evmChainId } = request.params as { evmChainId: number };
            const db = dbCtx.indexerDbFactory(evmChainId);
            
            // Your API logic here
            return { status: "ok" };
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
