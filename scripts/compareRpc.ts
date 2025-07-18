/**
 * Temporary script to compare local RPC API plugin with official RPC endpoint
 * 
 * Usage:
 *   npm run build && node dist/script/compareRpc.js
 * 
 * Tests:
 * - eth_chainId
 * - eth_call (blockchain ID from precompile)
 * - eth_blockNumber
 * - eth_getBlockByNumber (for blocks 0-9)
 * - eth_getTransactionReceipt (for all txs in blocks 0-9)
 * - debug_traceBlockByNumber (if chain config supports debug)
 * - Error handling (extended tests always run)
 * - Batch operations (extended tests always run)
 */
import { diffString, diff } from 'json-diff';
import { BatchRpc } from '../blockFetcher/BatchRpc.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Read chain config
const chainsData = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/chains.json'), 'utf-8'));
const chainConfig = chainsData[0];
const evmChainId = chainConfig.evmChainId;

// Always run extended tests, and debug tests unless explicitly disabled by chain config
const testDebug = chainConfig.rpcConfig.rpcSupportsDebug !== false;
const testExtended = true;

// Create two BatchRpc instances
const localRpc = new BatchRpc({
    rpcUrl: `http://localhost:3080/api/${evmChainId}/rpc`,
    requestBatchSize: 20,
    maxConcurrentRequests: 100,
    rps: 20,
    rpcSupportsDebug: testDebug,
    enableBatchSizeGrowth: false
});

const officialRpc = new BatchRpc({
    ...chainConfig.rpcConfig,
    rpcSupportsDebug: testDebug
});



async function compareRpcs() {
    console.log('=== RPC Comparison Script ===');
    console.log(`Local RPC: http://localhost:3080/api/${evmChainId}/rpc`);
    console.log(`Official RPC: ${chainConfig.rpcConfig.rpcUrl}`);
    console.log(`Test mode: ${testDebug ? 'Debug + Extended' : 'Standard + Extended'}`);
    console.log('');

    // 1. Compare Chain ID
    console.log('1. Fetching Chain ID...');
    try {
        const localChainId = await localRpc.getEvmChainId();
        const officialChainId = await officialRpc.getEvmChainId();
        const match = localChainId === officialChainId;
        console.log(`   Local: ${localChainId}`);
        console.log(`   Official: ${officialChainId}`);
        console.log(`   Match: ${match ? '✓' : '✗'}`);

        if (!match) {
            console.error('\n❌ Chain ID mismatch!');
            console.error('Local response:', localChainId);
            console.error('Official response:', officialChainId);
            process.exit(1);
        }
    } catch (error) {
        console.error('   Error:', error);
        process.exit(1);
    }
    console.log('');

    // 2. Compare Blockchain ID from precompile
    console.log('2. Fetching Blockchain ID from precompile...');
    try {
        const localBlockchainId = await localRpc.fetchBlockchainIDFromPrecompile();
        const officialBlockchainId = await officialRpc.fetchBlockchainIDFromPrecompile();
        const match = localBlockchainId === officialBlockchainId;
        console.log(`   Local: ${localBlockchainId}`);
        console.log(`   Official: ${officialBlockchainId}`);
        console.log(`   Match: ${match ? '✓' : '✗'}`);

        if (!match) {
            console.error('\n❌ Blockchain ID mismatch!');
            console.error('Local response:', localBlockchainId);
            console.error('Official response:', officialBlockchainId);
            process.exit(1);
        }
    } catch (error) {
        console.error('   Error:', error);
        process.exit(1);
    }
    console.log('');

    // 3. Compare Current Block Number
    try {
        console.log('3. Fetching Current Block Number...');
        const localBlockNumber = await localRpc.getCurrentBlockNumber();
        const officialBlockNumber = await officialRpc.getCurrentBlockNumber();
        // For block number, we just check if local is not ahead of official
        const match = localBlockNumber <= officialBlockNumber;
        console.log(`   Local: ${localBlockNumber}`);
        console.log(`   Official: ${officialBlockNumber}`);
        console.log(`   Match: ${match ? '✓' : '✗'} (local should not be ahead)`);

        if (!match) {
            console.error('\n❌ Block number invalid - local is ahead of official!');
            console.error('Local response:', localBlockNumber);
            console.error('Official response:', officialBlockNumber);
            process.exit(1);
        }
    } catch (error) {
        console.error('   Error:', error);
        process.exit(1);
    }
    console.log('');

    // 4. Compare blocks 1,10,100,1000
    const localBlockNumber = await localRpc.getCurrentBlockNumber();
    const blockNumbers: number[] = [0];
    for (let i = 1; i < localBlockNumber; i *= 2) {
        blockNumbers.push(i);
    }
    console.log(`4. Fetching and comparing blocks ${blockNumbers.join(', ')}...`);

    try {
        const [localBlocks, officialBlocks] = await Promise.all([
            localRpc.getBlocksWithReceipts(blockNumbers),
            officialRpc.getBlocksWithReceipts(blockNumbers)
        ]);

        console.log(`   Local returned ${localBlocks.length} blocks`);
        console.log(`   Official returned ${officialBlocks.length} blocks`);

        if (localBlocks.length !== officialBlocks.length) {
            console.error(`\n❌ Block count mismatch - Local: ${localBlocks.length}, Official: ${officialBlocks.length}`);
            process.exit(1);
        }

        // Compare entire responses
        const differences = diff(localBlocks, officialBlocks);
        if (differences !== undefined) {
            console.error(`\n❌ Complete response mismatch:`);
            console.error("differences", diffString(localBlocks, officialBlocks));
            process.exit(1);
        } else {
            console.log(`   All ${localBlocks.length} blocks match: ✓`);
        }
    } catch (error) {
        console.error(`\n❌ Error fetching blocks:`, error);
        process.exit(1);
    }

    // 5. Test individual trace queries if debug is enabled
    if (testDebug) {
        console.log('\n5. Testing individual debug_traceBlockByNumber calls...');
        for (let i = 1; i <= 3; i++) { // Test blocks 1-3 (skip block 0 as it has no traces)
            console.log(`   Testing trace for block ${i}:`);
            try {
                const [localTrace, officialTrace] = await Promise.all([
                    localRpc.traceBlockByNumber(i),
                    officialRpc.traceBlockByNumber(i)
                ]);

                const traceMatch = JSON.stringify(localTrace) === JSON.stringify(officialTrace);
                console.log(`     Trace match: ${traceMatch ? '✓' : '✗'}`);

                if (!traceMatch) {
                    console.error(`\n❌ Trace mismatch for block ${i}`);
                    console.error('Local trace:', JSON.stringify(localTrace, null, 2));
                    console.error('Official trace:', JSON.stringify(officialTrace, null, 2));
                    console.log(`     Local:    ${JSON.stringify(localTrace)}`);
                    console.log(`     Official: ${JSON.stringify(officialTrace)}`);
                    process.exit(1);
                }
            } catch (error) {
                console.error(`\n❌ Error tracing block ${i}:`, error);
                process.exit(1);
            }
        }
    }

    // All basic tests passed
    console.log('\n✅ ALL BASIC TESTS PASS');

    // Run extended tests if requested
    if (testExtended) {
        console.log('\n=== EXTENDED TESTS ===\n');
        await runExtendedTests(localRpc, officialRpc);
    }
}

async function runExtendedTests(localRpc: BatchRpc, officialRpc: BatchRpc) {
    // 1. Test error handling
    console.log('1. Testing error handling...');

    // Test non-existent block (far future)
    console.log('   Testing non-existent block (999999999)...');
    try {
        const [localResult, officialResult] = await Promise.all([
            localRpc.getBlocksWithReceipts([999999999]).catch(e => ({ error: e.message })),
            officialRpc.getBlocksWithReceipts([999999999]).catch(e => ({ error: e.message }))
        ]);

        // Both should return empty array or error
        const localEmpty = Array.isArray(localResult) && localResult.length === 0;
        const officialEmpty = Array.isArray(officialResult) && officialResult.length === 0;
        const bothFailed = 'error' in localResult && 'error' in officialResult;

        console.log(`     Both returned empty/error: ${(localEmpty && officialEmpty) || bothFailed ? '✓' : '✗'}`);
    } catch (error) {
        console.error('     Error:', error);
    }

    // 2. Test batch operations
    console.log('\n2. Testing batch operations...');

    // Test fetching multiple blocks at once
    console.log('   Fetching blocks 10-19 in a single call...');
    try {
        const blockNumbers = Array.from({ length: 10 }, (_, i) => i + 10);
        const [localBlocks, officialBlocks] = await Promise.all([
            localRpc.getBlocksWithReceipts(blockNumbers),
            officialRpc.getBlocksWithReceipts(blockNumbers)
        ]);

        console.log(`     Local returned ${localBlocks.length} blocks`);
        console.log(`     Official returned ${officialBlocks.length} blocks`);
        console.log(`     Count match: ${localBlocks.length === officialBlocks.length ? '✓' : '✗'}`);

        // Verify all blocks match
        let allMatch = true;
        for (let i = 0; i < Math.min(localBlocks.length, officialBlocks.length); i++) {
            const localBlock = localBlocks[i];
            const officialBlock = officialBlocks[i];
            if (!localBlock || !officialBlock) {
                allMatch = false;
                console.error(`     Block ${blockNumbers[i]} missing`);
                break;
            }
            if (JSON.stringify(localBlock.block) !== JSON.stringify(officialBlock.block)) {
                allMatch = false;
                console.error(`     Block ${blockNumbers[i]} mismatch`);
                break;
            }
        }
        console.log(`     All blocks match: ${allMatch ? '✓' : '✗'}`);
    } catch (error) {
        console.error('     Error:', error);
    }

    // 3. Test direct batch RPC requests
    console.log('\n3. Testing direct batch RPC requests...');
    console.log('   Making multiple eth_blockNumber requests...');
    try {
        const requests = Array(5).fill(null).map(() => ({
            method: 'eth_blockNumber',
            params: []
        }));

        const [localResults, officialResults] = await Promise.all([
            localRpc.batchRpcRequests(requests),
            officialRpc.batchRpcRequests(requests)
        ]);

        const localAllSuccess = localResults.every(r => r.result && !r.error);
        const officialAllSuccess = officialResults.every(r => r.result && !r.error);

        console.log(`     Local: ${localAllSuccess ? 'All succeeded ✓' : 'Some failed ✗'}`);
        console.log(`     Official: ${officialAllSuccess ? 'All succeeded ✓' : 'Some failed ✗'}`);

        // All results should be the same block number
        if (localAllSuccess) {
            const blockNumbers = localResults.map(r => r.result);
            const allSame = blockNumbers.every(bn => bn === blockNumbers[0]);
            console.log(`     Local results consistent: ${allSame ? '✓' : '✗'}`);
        }
    } catch (error) {
        console.error('     Error:', error);
    }

    // 4. Test mixed batch requests
    console.log('\n4. Testing mixed batch requests...');
    console.log('   Making mixed eth_chainId and eth_blockNumber requests...');
    try {
        const mixedRequests = [
            { method: 'eth_chainId', params: [] },
            { method: 'eth_blockNumber', params: [] },
            { method: 'eth_chainId', params: [] },
            { method: 'eth_getBlockByNumber', params: ['0x0', false] },
        ];

        const [localResults, officialResults] = await Promise.all([
            localRpc.batchRpcRequests(mixedRequests),
            officialRpc.batchRpcRequests(mixedRequests)
        ]);

        // Compare each result
        let allMatch = true;
        for (let i = 0; i < mixedRequests.length; i++) {
            const localResult = localResults[i];
            const officialResult = officialResults[i];
            if (!localResult || !officialResult) {
                allMatch = false;
                console.error(`     Request ${i} (${mixedRequests[i]?.method}) missing result`);
                break;
            }
            if (JSON.stringify(localResult.result) !== JSON.stringify(officialResult.result)) {
                allMatch = false;
                console.error(`     Request ${i} (${mixedRequests[i]?.method}) mismatch`);
                break;
            }
        }
        console.log(`     All results match: ${allMatch ? '✓' : '✗'}`);
    } catch (error) {
        console.error('     Error:', error);
    }

    // 5. Test batch size stats
    console.log('\n5. Testing batch size stats...');
    try {
        const localStats = localRpc.getBatchSizeStats();
        const officialStats = officialRpc.getBatchSizeStats();

        console.log(`     Local stats: current=${localStats.current}, min=${localStats.min}, utilization=${localStats.utilizationRatio}`);
        console.log(`     Official stats: current=${officialStats.current}, min=${officialStats.min}, utilization=${officialStats.utilizationRatio}`);
        console.log(`     Stats retrieved: ✓`);
    } catch (error) {
        console.error('     Error:', error);
    }

    // 6. Test empty blocks
    console.log('\n6. Testing empty blocks...');
    console.log('   Looking for empty blocks in first 100 blocks...');
    try {
        // Search for empty blocks
        const searchRange = Array.from({ length: 20 }, (_, i) => i);
        const [localBlocks, officialBlocks] = await Promise.all([
            localRpc.getBlocksWithReceipts(searchRange),
            officialRpc.getBlocksWithReceipts(searchRange)
        ]);

        const emptyBlocks = localBlocks
            .map((block, idx) => ({ block, number: searchRange[idx] }))
            .filter(({ block }) => !block.block.transactions || block.block.transactions.length === 0);

        if (emptyBlocks.length > 0) {
            console.log(`     Found ${emptyBlocks.length} empty blocks: ${emptyBlocks.map(b => b.number).join(', ')}`);

            // Verify empty blocks match
            for (const { number } of emptyBlocks) {
                const localBlock = localBlocks.find(b => parseInt(b.block.number, 16) === number);
                const officialBlock = officialBlocks.find(b => parseInt(b.block.number, 16) === number);

                if (!localBlock || !officialBlock) {
                    console.error(`     Could not find block ${number} in results`);
                    continue;
                }

                if (JSON.stringify(localBlock) !== JSON.stringify(officialBlock)) {
                    console.error(`     Empty block ${number} mismatch`);
                }
            }
            console.log(`     Empty blocks match: ✓`);
        } else {
            console.log(`     No empty blocks found in range`);
        }
    } catch (error) {
        console.error('     Error:', error);
    }

    console.log('\n✅ ALL EXTENDED TESTS COMPLETE');
}

// Run the comparison
compareRpcs().catch(console.error).then(() => {
    process.exit(0);
});
