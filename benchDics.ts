import { BlockDB } from "./blockFetcher/BlockDB"
import { LazyTx, lazyTxToReceipt } from "./blockFetcher/lazy/LazyTx";
import { LazyBlock, lazyBlockToBlock } from "./blockFetcher/lazy/LazyBlock";
import { LazyTraces } from "./blockFetcher/lazy/LazyTrace";
import { RpcBlock, RpcTxReceipt } from "./blockFetcher/evmTypes";
import * as fs from 'fs';
import * as path from 'path';
import { tmpdir } from 'os';
import { execSync } from 'child_process';
import * as zstd from 'zstd-napi';
import { Table } from 'console-table-printer';

const BENCHMARK_BLOCKS = 20000;
const DICT_PERCENTAGE = 0.50;
const COMP_LEVEL = 1;

const otherDbPath = "./database/e2e_zerooneMainnet/blocks.db"
const otherDb = new BlockDB({ path: otherDbPath, isReadonly: true, hasDebug: true })



const dbPath = "./database/C-Chain/blocks_no_dbg.db"
const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: false })
const lastIndexedBlock = db.getLastStoredBlockNumber();
let lastBlock = 0;

const maxBlocks = Math.min(lastIndexedBlock, BENCHMARK_BLOCKS);

const testBlocks: RpcBlock[] = [];
const testReceipts: RpcTxReceipt[] = [];

// Add training data from other chain
const otherChainBlocks: RpcBlock[] = [];
const otherChainReceipts: RpcTxReceipt[] = [];

console.log('Loading test data...');
const readingStart = performance.now();
while (lastBlock < maxBlocks) {
    const batch: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[] = db.getBlocks(lastBlock, 1000);
    if (batch.length === 0) {
        break;
    }
    lastBlock = batch[batch.length - 1]!.block.number;

    for (const block of batch) {
        const rawBlock = lazyBlockToBlock(block.block, block.txs);
        testBlocks.push(rawBlock);
        const rawReceipts = block.txs.map(tx => lazyTxToReceipt(tx));
        testReceipts.push(...rawReceipts);
    }
}

// Load training data from other chain
console.log('Loading training data from other chain...');
let otherLastBlock = 0;
const otherLastIndexedBlock = otherDb.getLastStoredBlockNumber();
const maxOtherBlocks = Math.min(otherLastIndexedBlock, Math.floor(BENCHMARK_BLOCKS * DICT_PERCENTAGE));

while (otherLastBlock < maxOtherBlocks) {
    const batch: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[] = otherDb.getBlocks(otherLastBlock, 1000);
    if (batch.length === 0) {
        break;
    }
    otherLastBlock = batch[batch.length - 1]!.block.number;

    for (const block of batch) {
        const rawBlock = lazyBlockToBlock(block.block, block.txs);
        otherChainBlocks.push(rawBlock);
        const rawReceipts = block.txs.map(tx => lazyTxToReceipt(tx));
        otherChainReceipts.push(...rawReceipts);
    }
}
const readingEnd = performance.now();

console.log(`Got ${testBlocks.length} blocks and ${testReceipts.length} receipts from C-Chain in ${Math.round(readingEnd - readingStart)}ms`);
console.log(`Got ${otherChainBlocks.length} blocks and ${otherChainReceipts.length} receipts from e2e_zerooneMainnet for training`);

// Dictionary size configurations
const DICT_SIZES = [
    { name: 'no dict', size: 0 },
    { name: '63KB', size: 63 * 1024 },
    { name: '64KB', size: 64 * 1024 },
    { name: '65KB', size: 65 * 1024 },
];

// Helper function to train dictionary using zstd CLI
function trainDictionary(samples: any[], name: string, transformer: (item: any) => string | Buffer, dictSize: number): Buffer {
    if (dictSize === 0) {
        return Buffer.alloc(0); // No dictionary
    }

    const tempDir = path.join(tmpdir(), 'dict-training-' + Date.now());
    fs.mkdirSync(tempDir, { recursive: true });

    const sampleDir = path.join(tempDir, name);
    fs.mkdirSync(sampleDir, { recursive: true });

    // Use first 50% of data for training
    const availableSamples = Math.floor(samples.length * DICT_PERCENTAGE);
    const TARGET_SAMPLES = Math.min(1280 / (64 * 1024) * dictSize, availableSamples);

    // Write samples to individual files
    for (let i = 0; i < TARGET_SAMPLES; i++) {
        const sampleData = transformer(samples[i]);
        fs.writeFileSync(path.join(sampleDir, `sample_${i}`), sampleData);
    }

    // Train dictionary using zstd CLI
    const dictPath = path.join(tempDir, `${name}.dict`);
    try {
        execSync(`zstd --train ${sampleDir}/* -o ${dictPath} --maxdict=${dictSize}`, {
            stdio: 'pipe' // Suppress output
        });
        const dict = fs.readFileSync(dictPath);

        // Clean up temp directory
        fs.rmSync(tempDir, { recursive: true, force: true });

        return dict;
    } catch (err) {
        console.error(`Failed to train dictionary for ${name}:`, err);
        // Clean up temp directory
        fs.rmSync(tempDir, { recursive: true, force: true });
        return Buffer.alloc(0);
    }
}

interface BenchmarkResult {
    dictSize: string;
    actualDictSizeKB: number;
    totalSizeMB: number;
    avgSizeKB: number;
    encodeTimeMs: number;
    decodeTimeMs: number;
    compressionRatio: number;
}

function benchmarkDataset(datasetName: string, data: any[]): BenchmarkResult[] {
    console.log(`\n=== Benchmarking ${datasetName} (${data.length} items) ===`);

    const results: BenchmarkResult[] = [];

    // Get baseline size (JSON without compression)
    const baselineSize = data.reduce((sum, item) => sum + Buffer.from(JSON.stringify(item)).length, 0);

    for (const dictConfig of DICT_SIZES) {
        console.log(`Training dictionary: ${dictConfig.name}...`);

        // Train dictionary
        const dictionary = trainDictionary(
            data,
            `${datasetName}-${dictConfig.name}`,
            item => Buffer.from(JSON.stringify(item)),
            dictConfig.size
        );

        // Create compressor/decompressor
        const compressor = new zstd.Compressor();
        const decompressor = new zstd.Decompressor();
        compressor.setParameters({ compressionLevel: COMP_LEVEL });

        if (dictionary.length > 0) {
            compressor.loadDictionary(dictionary);
            decompressor.loadDictionary(dictionary);
        }

        // Encode all items
        const encodedItems: Buffer[] = [];
        const encodeStart = performance.now();
        for (const item of data) {
            const jsonString = JSON.stringify(item);
            encodedItems.push(compressor.compress(Buffer.from(jsonString)));
        }
        const encodeTime = performance.now() - encodeStart;

        // Calculate total size
        const totalSize = encodedItems.reduce((sum, item) => sum + item.length, 0);
        const avgSize = totalSize / encodedItems.length;

        // Decode all items
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            const decompressed = decompressor.decompress(encoded);
            JSON.parse(decompressed.toString()); // Parse to complete the process
        }
        const decodeTime = performance.now() - decodeStart;

        results.push({
            dictSize: dictConfig.name,
            actualDictSizeKB: dictionary.length / 1024,
            totalSizeMB: totalSize / 1024 / 1024,
            avgSizeKB: avgSize / 1024,
            encodeTimeMs: encodeTime,
            decodeTimeMs: decodeTime,
            compressionRatio: baselineSize / totalSize
        });

        console.log(`  ${dictConfig.name}: ${(totalSize / 1024 / 1024).toFixed(2)}MB, ${(avgSize / 1024).toFixed(2)}KB avg, ${(baselineSize / totalSize).toFixed(1)}x compression`);
    }

    return results;
}

// Add cross-chain benchmark function
function benchmarkCrossChain(datasetName: string, trainingData: any[], testData: any[]): BenchmarkResult[] {
    console.log(`\n=== Cross-Chain Benchmarking ${datasetName} ===`);
    console.log(`Training on e2e_zerooneMainnet (${trainingData.length} items), testing on C-Chain (${testData.length} items)`);

    const results: BenchmarkResult[] = [];

    // Get baseline size (JSON without compression)
    const baselineSize = testData.reduce((sum, item) => sum + Buffer.from(JSON.stringify(item)).length, 0);

    for (const dictConfig of DICT_SIZES) {
        console.log(`Training cross-chain dictionary: ${dictConfig.name}...`);

        // Train dictionary on other chain data
        const dictionary = trainDictionary(
            trainingData,
            `cross-${datasetName}-${dictConfig.name}`,
            item => Buffer.from(JSON.stringify(item)),
            dictConfig.size
        );

        // Create compressor/decompressor
        const compressor = new zstd.Compressor();
        const decompressor = new zstd.Decompressor();
        compressor.setParameters({ compressionLevel: COMP_LEVEL });

        if (dictionary.length > 0) {
            compressor.loadDictionary(dictionary);
            decompressor.loadDictionary(dictionary);
        }

        // Encode all test items
        const encodedItems: Buffer[] = [];
        const encodeStart = performance.now();
        for (const item of testData) {
            const jsonString = JSON.stringify(item);
            encodedItems.push(compressor.compress(Buffer.from(jsonString)));
        }
        const encodeTime = performance.now() - encodeStart;

        // Calculate total size
        const totalSize = encodedItems.reduce((sum, item) => sum + item.length, 0);
        const avgSize = totalSize / encodedItems.length;

        // Decode all items
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            const decompressed = decompressor.decompress(encoded);
            JSON.parse(decompressed.toString()); // Parse to complete the process
        }
        const decodeTime = performance.now() - decodeStart;

        results.push({
            dictSize: dictConfig.name,
            actualDictSizeKB: dictionary.length / 1024,
            totalSizeMB: totalSize / 1024 / 1024,
            avgSizeKB: avgSize / 1024,
            encodeTimeMs: encodeTime,
            decodeTimeMs: decodeTime,
            compressionRatio: baselineSize / totalSize
        });

        console.log(`  ${dictConfig.name}: ${(totalSize / 1024 / 1024).toFixed(2)}MB, ${(avgSize / 1024).toFixed(2)}KB avg, ${(baselineSize / totalSize).toFixed(1)}x compression`);
    }

    return results;
}

// Test both blocks and receipts
const datasets = [
    { name: 'blocks', data: testBlocks },
    { name: 'receipts', data: testReceipts }
];

const allResults: { [key: string]: BenchmarkResult[] } = {};

for (const dataset of datasets) {
    allResults[dataset.name] = benchmarkDataset(dataset.name, dataset.data);
}

// Run cross-chain benchmarks
const crossChainDatasets = [
    { name: 'blocks', trainingData: otherChainBlocks, testData: testBlocks },
    { name: 'receipts', trainingData: otherChainReceipts, testData: testReceipts }
];

const crossChainResults: { [key: string]: BenchmarkResult[] } = {};

for (const dataset of crossChainDatasets) {
    crossChainResults[dataset.name] = benchmarkCrossChain(dataset.name, dataset.trainingData, dataset.testData);
}

// Print detailed results
for (const [datasetName, results] of Object.entries(allResults)) {
    console.log(`\n--- ${datasetName.toUpperCase()} Results ---`);

    const table = new Table({
        title: `${datasetName.toUpperCase()} Dictionary Size Comparison`,
        columns: [
            { name: 'dictSize', title: 'Dict Size', alignment: 'left' },
            { name: 'actualDictSizeKB', title: 'Actual (KB)', alignment: 'right' },
            { name: 'totalSizeMB', title: 'Total (MB)', alignment: 'right' },
            { name: 'avgSizeKB', title: 'Avg (KB)', alignment: 'right' },
            { name: 'compressionRatio', title: 'Compression', alignment: 'right' },
            { name: 'encodeTimeMs', title: 'Encode (ms)', alignment: 'right' },
            { name: 'decodeTimeMs', title: 'Decode (ms)', alignment: 'right' }
        ]
    });

    results.forEach(result => {
        table.addRow({
            dictSize: result.dictSize,
            actualDictSizeKB: result.actualDictSizeKB.toFixed(1),
            totalSizeMB: result.totalSizeMB.toFixed(2),
            avgSizeKB: result.avgSizeKB.toFixed(2),
            compressionRatio: result.compressionRatio.toFixed(1) + 'x',
            encodeTimeMs: result.encodeTimeMs.toFixed(2),
            decodeTimeMs: result.decodeTimeMs.toFixed(2)
        });
    });

    table.printTable();
}

// Add cross-chain detailed results tables
for (const [datasetName, results] of Object.entries(crossChainResults)) {
    console.log(`\n--- CROSS-CHAIN ${datasetName.toUpperCase()} Results ---`);

    const table = new Table({
        title: `CROSS-CHAIN ${datasetName.toUpperCase()} Dictionary Size Comparison`,
        columns: [
            { name: 'dictSize', title: 'Dict Size', alignment: 'left' },
            { name: 'actualDictSizeKB', title: 'Actual (KB)', alignment: 'right' },
            { name: 'totalSizeMB', title: 'Total (MB)', alignment: 'right' },
            { name: 'avgSizeKB', title: 'Avg (KB)', alignment: 'right' },
            { name: 'compressionRatio', title: 'Compression', alignment: 'right' },
            { name: 'encodeTimeMs', title: 'Encode (ms)', alignment: 'right' },
            { name: 'decodeTimeMs', title: 'Decode (ms)', alignment: 'right' }
        ]
    });

    results.forEach(result => {
        table.addRow({
            dictSize: result.dictSize,
            actualDictSizeKB: result.actualDictSizeKB.toFixed(1),
            totalSizeMB: result.totalSizeMB.toFixed(2),
            avgSizeKB: result.avgSizeKB.toFixed(2),
            compressionRatio: result.compressionRatio.toFixed(1) + 'x',
            encodeTimeMs: result.encodeTimeMs.toFixed(2),
            decodeTimeMs: result.decodeTimeMs.toFixed(2)
        });
    });

    table.printTable();
}

// Summary comparison
console.log('\n=== SUMMARY: Dictionary Size Impact ===');

for (const [datasetName, results] of Object.entries(allResults)) {
    console.log(`\n${datasetName.toUpperCase()} - Best compression vs performance trade-offs:`);

    const noDictResult = results.find(r => r.dictSize === 'no dict')!;

    results.forEach(result => {
        if (result.dictSize === 'no dict') return;

        const sizeImprovement = ((noDictResult.totalSizeMB - result.totalSizeMB) / noDictResult.totalSizeMB * 100);
        const timeOverhead = ((result.decodeTimeMs - noDictResult.decodeTimeMs) / noDictResult.decodeTimeMs * 100);

        console.log(`  ${result.dictSize.padEnd(8)}: ${sizeImprovement.toFixed(1)}% smaller, ${timeOverhead.toFixed(1)}% slower decode`);
    });
}

// Fix the linter errors in cross-chain comparison
console.log('\n=== CROSS-CHAIN DICTIONARY EFFECTIVENESS ===');
console.log('Comparing same-chain vs cross-chain dictionary performance:');

for (const datasetName of ['blocks', 'receipts']) {
    console.log(`\n${datasetName.toUpperCase()}:`);

    const sameChainResults = allResults[datasetName];
    const crossResults = crossChainResults[datasetName];

    if (!sameChainResults || !crossResults) {
        console.log(`  No results available for ${datasetName}`);
        continue;
    }

    DICT_SIZES.forEach((dictConfig, idx) => {
        if (dictConfig.size === 0) return; // Skip no dict

        const sameChain = sameChainResults[idx];
        const crossChain = crossResults[idx];

        if (!sameChain || !crossChain) return;

        const compressionDiff = ((crossChain.compressionRatio - sameChain.compressionRatio) / sameChain.compressionRatio * 100);

        console.log(`  ${dictConfig.name.padEnd(8)}: Same-chain ${sameChain.compressionRatio.toFixed(2)}x, Cross-chain ${crossChain.compressionRatio.toFixed(2)}x (${compressionDiff > 0 ? '+' : ''}${compressionDiff.toFixed(1)}%)`);
    });
}

console.log('\n=== Dictionary Size Benchmark Complete ===');
process.exit(0);
