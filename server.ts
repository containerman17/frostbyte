import { BlockDB } from './blockFetcher/BlockDB';
import Fastify, { FastifyInstance } from 'fastify';
import Database from 'better-sqlite3';
import { DEBUG_RPC_AVAILABLE } from './config';
import { initializeIndexingDB } from './lib/dbHelper';
import { loadPlugins } from './lib/plugins';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';

const docsPage = `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>Elements in HTML</title>
  
    <script src="https://unpkg.com/@stoplight/elements/web-components.min.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/@stoplight/elements/styles.min.css">
  </head>
  <body>

    <elements-api
      apiDescriptionUrl="/api/openapi.json"
      router="hash"
    />

  </body>
</html>
`;

// Helper function to find the database for a specific indexer
function findIndexerDatabase(baseDir: string, indexerName: string, indexerVersion: number): string | null {
    const dbName = DEBUG_RPC_AVAILABLE
        ? `indexing_${indexerName}_v${indexerVersion}.db`
        : `indexing_${indexerName}_v${indexerVersion}_no_dbg.db`;
    const dbPath = path.join(baseDir, dbName);

    if (fs.existsSync(dbPath)) {
        return dbPath;
    }

    return null;
}

export async function createApiServer(blocksDbPath: string, indexingDbPath: string) {
    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: DEBUG_RPC_AVAILABLE });

    // indexingDbPath is now used as the base directory for indexer databases
    const indexingDbBaseDir = path.dirname(indexingDbPath);

    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const indexers = await loadPlugins([path.join(__dirname, 'pluginExamples')]);

    const app: FastifyInstance = Fastify({ logger: false });

    // Register swagger plugin
    await app.register(import('@fastify/swagger'), {
        openapi: {
            info: {
                title: 'Blockchain Indexer API',
                description: 'API for querying blockchain data and metrics',
                version: '1.0.0',
            },
            servers: [
                {
                    url: 'http://localhost:3000',
                    description: 'Local development server'
                }
            ],
            tags: [
                { name: 'Metrics', description: 'Blockchain metrics endpoints' },
                { name: 'RPC', description: 'JSON-RPC endpoints' },
                { name: 'Teleporter Metrics', description: 'Teleporter specific metrics' }
            ]
        }
    });

    await app.register(import('@fastify/swagger-ui'), {
        routePrefix: '/documentation',
        uiConfig: {
            docExpansion: 'list',
            deepLinking: false
        }
    });

    // Keep track of opened databases for cleanup
    const openedDatabases: Database.Database[] = [];

    for (const indexer of indexers) {
        // Find the database for this indexer
        const indexerDbPath = findIndexerDatabase(indexingDbBaseDir, indexer.name, indexer.version);

        if (!indexerDbPath) {
            console.warn(`[API Server] Database not found for indexer ${indexer.name} v${indexer.version}. Skipping route registration.`);
            continue;
        }

        // Open the indexer's database
        const indexerDb = new Database(indexerDbPath, { readonly: true });
        initializeIndexingDB({ db: indexerDb, isReadonly: true });
        openedDatabases.push(indexerDb);

        console.log(`[API Server] Registering routes for ${indexer.name} using ${path.basename(indexerDbPath)}`);
        indexer.registerRoutes(app, indexerDb, blocksDb);
    }

    // Add route to serve OpenAPI JSON
    app.get('/api/openapi.json', async (request, reply) => {
        return reply.send(app.swagger());
    });

    // Add root route
    app.get('/', async (request, reply) => {
        return reply.type('text/html').send(`<a href="/docs">OpenAPI documentation</a>`);
    });

    // Add docs route
    app.get('/docs', async (request, reply) => {
        return reply.type('text/html').send(docsPage);
    });

    return {
        app,
        start: async (port = 3000) => {
            try {
                await app.listen({ port, host: '0.0.0.0' });
                console.log(`Starting server on http://localhost:${port}/`);
            } catch (err) {
                app.log.error(err);
                process.exit(1);
            }
        },
        close: async () => {
            await app.close();
            blocksDb.close();
            for (const db of openedDatabases) {
                db.close();
            }
        }
    };
} 
