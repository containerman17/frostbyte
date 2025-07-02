process.env.NODE_ENV = "dev"
process.env.RPC_URL = "http://localhost:3333/rpc" //Points to itself
process.env.CHAIN_ID = "e2e"
process.env.DATA_DIR = "./database"
process.env.RPS = "1000"
process.env.REQUEST_BATCH_SIZE = "100"
process.env.MAX_CONCURRENT = "50"
process.env.BLOCKS_PER_BATCH = "1000"
process.env.DEBUG_RPC_AVAILABLE = "false"
process.env.REVERSE_PROXY_PREFIX = "/"

import test from 'node:test';
import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import * as jsondiffpatch from 'jsondiffpatch';
import * as consoleFormatter from 'jsondiffpatch/formatters/console';

test('API Specs Validation', async (t) => {
    // Dynamic imports after env vars are set
    const { createApiServer } = await import('../server');
    const { startIndexer } = await import('../indexer');
    const { CHAIN_ID, DATA_DIR, DEBUG_RPC_AVAILABLE } = await import('../config');

    // Setup database paths
    const blocksDbPath = path.join(DATA_DIR, CHAIN_ID, DEBUG_RPC_AVAILABLE ? 'blocks.db' : 'blocks_no_dbg.db');
    const indexingDbPath = path.join(path.dirname(blocksDbPath), DEBUG_RPC_AVAILABLE ? 'indexing.db' : 'indexing_no_dbg.db');

    // Check if databases exist
    if (!fs.existsSync(blocksDbPath)) {
        throw new Error(`Blocks database not found at ${blocksDbPath}. Run the fetcher first.`);
    }

    console.log('Running indexer until all blocks are processed...');
    await startIndexer({
        blocksDbPath,
        indexingDbPath,
        exitWhenDone: true
    });

    console.log('Starting API server...');
    const apiServer = createApiServer(blocksDbPath, indexingDbPath);
    const server = apiServer.start(3333);

    try {
        // Find all YAML files in specs folder
        const specsDir = path.join(process.cwd(), 'specs');
        const yamlFiles = fs.readdirSync(specsDir)
            .filter(file => file.endsWith('.yaml') || file.endsWith('.yml'));

        console.log(`Found ${yamlFiles.length} spec file(s): ${yamlFiles.join(', ')}`);

        // Test each YAML file
        for (const yamlFile of yamlFiles) {
            await t.test(yamlFile, async (t) => {
                const yamlPath = path.join(specsDir, yamlFile);
                const yamlContent = fs.readFileSync(yamlPath, 'utf8');

                // Parse multi-document YAML
                const blocks = yamlContent.split('---\n').filter(block => block.trim());
                if (blocks.length === 0) {
                    throw new Error(`No blocks found in ${yamlFile}`);
                }

                console.log(`\n✓ Processing ${blocks.length} endpoint specs`);

                // Test each endpoint spec
                for (let i = 0; i < blocks.length; i++) {
                    const block = YAML.parse(blocks[i]);
                    if (!block.path) continue;

                    await t.test(`${block.path}`, async () => {
                        const url = `http://localhost:3333${block.path}`;

                        const response = await fetch(url);

                        // Check status code
                        const expectedStatus = block.status || 200;
                        assert.strictEqual(response.status, expectedStatus,
                            `Status code mismatch: expected ${expectedStatus}, got ${response.status}`);

                        // Check expectedBody - fail if not present or empty
                        if (!block.expectedBody || block.expectedBody === "" || block.expectedBody === "TODO: add body") {
                            throw new Error(`expectedBody is missing or empty for endpoint ${block.path}. Run the fill script first!`);
                        }

                        const actualBody = await response.json();
                        const expectedBody = typeof block.expectedBody === 'string'
                            ? JSON.parse(block.expectedBody)
                            : block.expectedBody;

                        const delta = jsondiffpatch.diff(expectedBody, actualBody);

                        if (delta) {
                            console.log('\n❌ Response differences found for ' + url + ':');
                            console.log(consoleFormatter.format(delta));
                            console.log('\nExpected:');
                            console.log(JSON.stringify(expectedBody, null, 2));
                            console.log('\nActual:');
                            console.log(JSON.stringify(actualBody, null, 2));
                            throw new Error(`Response body mismatch`);
                        } else {
                            console.log('✓ Response matches expected');
                        }
                    });
                }
            });
        }
    } finally {
        // Clean up
        console.log('\nStopping server...');
        server.close();
        apiServer.close();
    }
});


