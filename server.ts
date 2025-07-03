import { BlockDB } from './blockFetcher/BlockDB';
import { createRPCIndexer } from './indexers/rpc';
import { OpenAPIHono } from '@hono/zod-openapi';
import Database from 'better-sqlite3';
import { executePragmas } from './indexers/dbHelper';
import { createMetricsIndexer } from './indexers/metrics/index';
import { serve } from '@hono/node-server';
import { createTeleporterMetricsIndexer } from './indexers/teleporterMetrics';
import { createInfoIndexer } from './indexers/info';
import { createSanityChecker } from './indexers/sanityChecker';
import { IS_DEVELOPMENT, DEBUG_RPC_AVAILABLE } from './config';

const indexerFactories = [
    createRPCIndexer,
    createMetricsIndexer,
    createTeleporterMetricsIndexer,
    createInfoIndexer,
];
if (IS_DEVELOPMENT) {
    indexerFactories.push(createSanityChecker);
}

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

export function createApiServer(blocksDbPath: string, indexingDbPath: string) {
    const blocksDb = new BlockDB({ path: blocksDbPath, isReadonly: true, hasDebug: DEBUG_RPC_AVAILABLE });
    const indexingDb = new Database(indexingDbPath, { readonly: true });

    executePragmas({ db: indexingDb, isReadonly: true });

    const app = new OpenAPIHono();

    for (const indexerFactory of indexerFactories) {
        const indexer = indexerFactory(blocksDb, indexingDb);
        indexer.registerRoutes(app);
    }

    // Add OpenAPI documentation endpoint
    app.doc('/api/openapi.json', {
        openapi: '3.0.0',
        info: {
            version: '1.0.0',
            title: 'Blockchain Indexer API',
            description: 'API for querying blockchain data and metrics'
        },
        servers: [
            {
                url: 'http://localhost:3000',
                description: 'Local development server'
            }
        ]
    });

    app.get('/', (c) => c.html(`<a href="/docs">OpenAPI documentation</a>`));
    app.get('/docs', (c) => c.html(docsPage));

    return {
        app,
        start: (port = 3000) => {
            console.log(`Starting server on http://localhost:${port}/`);
            return serve({
                fetch: app.fetch,
                port
            });
        },
        close: () => {
            blocksDb.close();
            indexingDb.close();
        }
    };
} 
