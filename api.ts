import { BlocksDBHelper } from './blockFetcher/BlocksDBHelper';
import Fastify, { FastifyInstance } from 'fastify';
import { loadApiPlugins, loadIndexingPlugins } from './lib/plugins.js';
import { getSqliteDb, ChainConfig } from './config.js';
import { getPluginDirs } from './config.js';
import fs from 'node:fs';
import path from 'node:path';
import chainsApiPlugin from './standardPlugins/chainsApi.js';
import rpcApiPlugin from './standardPlugins/rpcApi.js';
import { ASSETS_DIR } from './config.js';
import sqlite3 from 'better-sqlite3';

const docsPage = `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>Frostbyte API explorer</title>
  
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
    // Load both types of plugins
    const loadedApiPlugins = await loadApiPlugins(getPluginDirs());
    const indexingPlugins = await loadIndexingPlugins(getPluginDirs());

    // Standard API plugins are always included
    const standardApiPlugins = [chainsApiPlugin, rpcApiPlugin];

    // Combine standard and loaded API plugins
    const apiPlugins = [...standardApiPlugins, ...loadedApiPlugins];

    console.log(`Loaded ${standardApiPlugins.length} standard API plugins and ${loadedApiPlugins.length} custom API plugins`);

    // Create a map of available indexers for validation
    const availableIndexers = new Map<string, number>();
    for (const indexer of indexingPlugins) {
        availableIndexers.set(indexer.name, indexer.version);
    }

    const app: FastifyInstance = Fastify({
        logger: {
            level: 'info',
            transport: {
                target: 'pino-pretty',
                options: {
                    translateTime: 'HH:MM:ss Z',
                    ignore: 'pid,hostname',
                    colorize: true,
                    singleLine: true
                }
            }
        }
    });

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
                    url: '/',
                    description: 'This server'
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

    // Validate ASSETS_DIR configuration if provided
    if (ASSETS_DIR) {
        const indexPath = path.join(ASSETS_DIR, 'index.html');
        if (!fs.existsSync(indexPath)) {
            throw new Error(`ASSETS_DIR is configured but index.html not found at ${indexPath}. This is a misconfiguration.`);
        }

        // Register static file serving
        await app.register(import('@fastify/static'), {
            root: ASSETS_DIR,
            prefix: '/',
            wildcard: false
        });
    }

    const chainConfigCache = new Map<string, ChainConfig>();
    const getChainConfig = (evmChainIdOrBlockchainId: number | string): ChainConfig | undefined => {
        const idString = String(evmChainIdOrBlockchainId);
        if (chainConfigCache.has(idString)) {
            return chainConfigCache.get(idString)!;
        }
        const chainConfig = chainConfigs.find(c => String(c.evmChainId) === idString || c.blockchainId === idString);
        if (!chainConfig) {
            return undefined;
        }
        chainConfigCache.set(idString, chainConfig);
        return chainConfig;
    }

    const blocksDbCache = new Map<number, BlocksDBHelper>();
    const getBlocksDbHelper = (evmChainId: number): BlocksDBHelper => {
        if (blocksDbCache.has(evmChainId)) {
            return blocksDbCache.get(evmChainId)!;
        }
        const chainConfig = getChainConfig(evmChainId);
        if (!chainConfig) {
            throw new Error(`Chain config not found for evmChainId: ${evmChainId}`);
        }
        const db = getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "blocks",
            chainId: chainConfig.blockchainId,
        });
        const blocksDb = new BlocksDBHelper(db, true, chainConfig.rpcConfig.rpcSupportsDebug);
        blocksDbCache.set(evmChainId, blocksDb);
        return blocksDb;
    }

    const getIndexerDbConnection = (evmChainId: number, indexerName: string): sqlite3.Database => {
        const indexerVersion = availableIndexers.get(indexerName);
        if (indexerVersion === undefined) {
            throw new Error(`Indexer "${indexerName}" not found in available indexers`);
        }

        const chainConfig = getChainConfig(evmChainId);
        if (!chainConfig) {
            throw new Error(`Chain config not found for evmChainId: ${evmChainId}`);
        }
        const db = getSqliteDb({
            debugEnabled: chainConfig.rpcConfig.rpcSupportsDebug,
            type: "plugin",
            indexerName: indexerName,
            chainId: chainConfig.blockchainId,
            pluginVersion: indexerVersion,
        });
        return db;
    }


    // Validate and register API plugins
    for (const apiPlugin of apiPlugins) {
        console.log(`Validating API plugin "${apiPlugin.name}"`);

        // Check that all required indexers are available
        for (const requiredIndexer of apiPlugin.requiredIndexers) {
            if (!availableIndexers.has(requiredIndexer)) {
                throw new Error(`API plugin "${apiPlugin.name}" requires indexer "${requiredIndexer}" which is not available`);
            }
        }

        console.log(`Registering routes for API plugin "${apiPlugin.name}"`);
        try {
            apiPlugin.registerRoutes(app, {
                getBlocksDbHelper: getBlocksDbHelper,
                getIndexerDbConnection: getIndexerDbConnection,
                getChainConfig: getChainConfig,
                getAllChainConfigs: () => [...chainConfigs]
            });
        } catch (error) {
            console.error(`Failed to register routes for API plugin "${apiPlugin.name}":`, error);
            throw new Error(`API plugin "${apiPlugin.name}" route registration failed: ${error instanceof Error ? error.message : String(error)}`);
        }
        console.log(`Routes registered for API plugin "${apiPlugin.name}"`);
    }

    // Add route to serve OpenAPI JSON
    app.get('/api/openapi.json', async (request, reply) => {
        return reply.send(app.swagger());
    });

    // Add root route only if not already registered by indexers and no ASSETS_DIR
    const rootRouteExists = app.hasRoute({ method: 'GET', url: '/' });

    if (!rootRouteExists && !ASSETS_DIR) {
        app.get('/', {
            schema: {
                hide: true
            }
        }, async (request, reply) => {
            return reply.type('text/html').send(`<a href="/api/docs">OpenAPI documentation</a>`);
        });
    }

    // Add docs route
    app.get('/api/docs', {
        schema: {
            hide: true
        }
    }, async (request, reply) => {
        return reply.type('text/html').send(docsPage);
    });

    // SPA fallback route - must be registered last
    if (ASSETS_DIR) {
        app.get('/*', {
            schema: {
                hide: true
            }
        }, async (request, reply) => {
            // Return 404 for /assets and /api paths
            if (request.url.startsWith('/assets/') || request.url.startsWith('/api/')) {
                return reply.code(404).send({ error: 'Not Found' });
            }
            return reply.sendFile('index.html');
        });
    }

    return {
        app,
        start: async (port: number) => {
            try {
                await app.listen({ port, host: '0.0.0.0' });
                console.log(`Server started on http://0.0.0.0:${port}/`);
            } catch (err) {
                console.error('Failed to start API server:', err);
                process.exit(1);
            }
        },
        close: async () => {
            await app.close();
        }
    };
} 
