import { BlockDB } from './blockFetcher/BlockDB';
import Fastify, { FastifyInstance } from 'fastify';
import Sqlite3 from 'better-sqlite3';
import { initializeIndexingDB } from './lib/dbHelper';
import { loadPlugins } from './lib/plugins';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getBlocksDbPath, getIndexerDbPath } from './lib/dbPaths';
import { ChainConfig } from './config';
import Database from 'better-sqlite3';
import { getPluginDirs } from './config';

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

export async function createApiServer(chainConfigs: ChainConfig[]) {
    const indexers = await loadPlugins(getPluginDirs());

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


    const blocksDbCache = new Map<number, BlockDB>();
    function getBlocksDb(evmChainId: number): BlockDB {
        if (blocksDbCache.has(evmChainId)) {
            return blocksDbCache.get(evmChainId)!;
        }
        const chainConfig = chainConfigs.find(c => c.evmChainId === evmChainId);
        if (!chainConfig) {
            throw new Error(`Chain config not found for evmChainId: ${evmChainId}`);
        }
        const blocksDb = new BlockDB({ path: getBlocksDbPath(chainConfig.blockchainId, chainConfig.rpcConfig.rpcSupportsDebug), isReadonly: true, hasDebug: chainConfig.rpcConfig.rpcSupportsDebug });
        blocksDbCache.set(evmChainId, blocksDb);
        return blocksDb;
    }

    const indexerDbCache = new Map<string, Sqlite3.Database>();
    function getIndexerDb(evmChainId: number, indexerName: string, indexerVersion: number): Sqlite3.Database {
        const key = `${evmChainId}-${indexerName}-${indexerVersion}`;
        if (indexerDbCache.has(key)) {
            return indexerDbCache.get(key)!;
        }
        const chainConfig = chainConfigs.find(c => c.evmChainId === evmChainId);
        if (!chainConfig) {
            throw new Error(`Chain config not found for evmChainId: ${evmChainId}`);
        }
        const indexerDbPath = getIndexerDbPath(chainConfig.blockchainId, indexerName, indexerVersion, chainConfig.rpcConfig.rpcSupportsDebug);
        const indexingDb = new Database(indexerDbPath, { readonly: false });
        initializeIndexingDB({ db: indexingDb, isReadonly: true });
        indexerDbCache.set(key, indexingDb);
        return indexingDb;
    }

    for (const indexer of indexers) {
        indexer.registerRoutes(app, {
            blocksDbFactory: getBlocksDb,
            indexerDbFactory: (evmChainId: number) => getIndexerDb(evmChainId, indexer.name, indexer.version)
        });
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
        }
    };
} 
