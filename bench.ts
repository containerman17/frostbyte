import { BlockDB } from "./blockFetcher/BlockDB"
import { LazyTx, lazyTxToReceipt } from "./blockFetcher/lazy/LazyTx";
import { LazyBlock, lazyBlockToBlock } from "./blockFetcher/lazy/LazyBlock";
import { LazyTraces } from "./blockFetcher/lazy/LazyTrace";
import { RpcBlock, RpcTxReceipt } from "./blockFetcher/evmTypes";
import { pack, unpack } from 'msgpackr';
import { compressSync as lz4CompressSync, uncompressSync as lz4UncompressSync } from 'lz4-napi';
import * as fs from 'fs';
import * as path from 'path';
import { tmpdir } from 'os';
import { execSync } from 'child_process';
import { zstdCompressSync, zstdDecompressSync, constants as zlibConstants } from 'node:zlib';

const dbPath = "./database/e2e_zerooneMainnet/blocks.db"
const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: true })
const lastIndexedBlock = db.getLastStoredBlockNumber();
let lastBlock = 0;


const BENCHMARK_BLOCKS = 10000;
const maxBlocks = Math.min(lastIndexedBlock, BENCHMARK_BLOCKS);

const testBlocks: RpcBlock[] = [];
const testReceipts: RpcTxReceipt[] = [];

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
const readingEnd = performance.now();

console.log(`Got ${testBlocks.length} blocks and ${testReceipts.length} receipts in ${Math.round(readingEnd - readingStart)}ms. Starting benchmark...`);

// Train LZ4 dictionaries from sample data using zstd
console.log('\n=== Training LZ4 Dictionaries ===');
const dictStart = performance.now();

// Dictionary size target: 64KB
const DICT_SIZE = 64 * 1024;
// Assuming ~5KB per sample, we need 100x samples: 6.4MB / 5KB = 1280 samples
const TARGET_SAMPLES = 1280;

// Create temp directory for training samples
const tempDir = path.join(tmpdir(), 'dict-training-' + Date.now());
fs.mkdirSync(tempDir, { recursive: true });

// Helper function to train dictionary using zstd
function trainDictionary(samples: any[], name: string, transformer: (item: any) => string | Buffer): Buffer {
    const sampleDir = path.join(tempDir, name);
    fs.mkdirSync(sampleDir, { recursive: true });

    // Use first 10% of data, but select TARGET_SAMPLES unique items
    const availableSamples = Math.floor(samples.length * 0.1);
    const samplesToUse = Math.min(TARGET_SAMPLES, availableSamples);

    // Write samples to individual files
    for (let i = 0; i < samplesToUse; i++) {
        const sampleData = transformer(samples[i]);
        fs.writeFileSync(path.join(sampleDir, `sample_${i}`), sampleData);
    }

    // Train dictionary using zstd CLI
    const dictPath = path.join(tempDir, `${name}.dict`);
    try {
        execSync(`zstd --train ${sampleDir}/* -o ${dictPath} --maxdict=${DICT_SIZE}`, {
            stdio: 'pipe' // Suppress output
        });
        return fs.readFileSync(dictPath);
    } catch (err) {
        console.error(`Failed to train dictionary for ${name}:`, err);
        // Return empty buffer as fallback
        return Buffer.alloc(0);
    }
}

// Train dictionaries for each format
const jsonBlockDict = trainDictionary(testBlocks, 'json-blocks', b => JSON.stringify(b));
const jsonReceiptDict = trainDictionary(testReceipts, 'json-receipts', r => JSON.stringify(r));
const msgpackBlockDict = trainDictionary(testBlocks, 'msgpack-blocks', b => Buffer.from(pack(b)));
const msgpackReceiptDict = trainDictionary(testReceipts, 'msgpack-receipts', r => Buffer.from(pack(r)));

// Clean up temp directory
fs.rmSync(tempDir, { recursive: true, force: true });

const dictEnd = performance.now();
console.log(`Dictionary training completed in ${Math.round(dictEnd - dictStart)}ms`);
console.log(`  Target samples per dictionary: ${TARGET_SAMPLES}`);
console.log(`  Actual block samples: ${Math.min(TARGET_SAMPLES, Math.floor(testBlocks.length * 0.1))}`);
console.log(`  Actual receipt samples: ${Math.min(TARGET_SAMPLES, Math.floor(testReceipts.length * 0.1))}`);
console.log(`  JSON block dict: ${(jsonBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  JSON receipt dict: ${(jsonReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Msgpack block dict: ${(msgpackBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Msgpack receipt dict: ${(msgpackReceiptDict.length / 1024).toFixed(1)} KB`);

console.log('\n=== Individual Object Encoding/Decoding Benchmark ===');
console.log('(Simulating database row storage)\n');

interface Method {
    name: string;
    encode: (obj: any, datasetName: string) => Buffer | Uint8Array;
    decode: (data: Buffer | Uint8Array, datasetName: string) => any;
}

const methods: Method[] = [
    {
        name: 'JSON plain',
        encode: (obj) => Buffer.from(JSON.stringify(obj)),
        decode: (data) => JSON.parse(data.toString())
    },
    {
        name: 'JSON + lz4',
        encode: (obj) => lz4CompressSync(Buffer.from(JSON.stringify(obj))),
        decode: (data) => JSON.parse(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data)).toString())
    },
    {
        name: 'JSON + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? jsonBlockDict : jsonReceiptDict;
            return lz4CompressSync(Buffer.from(JSON.stringify(obj)), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? jsonBlockDict : jsonReceiptDict;
            return JSON.parse(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict).toString());
        }
    },
    {
        name: 'JSON + zstd',
        encode: (obj) => zstdCompressSync(Buffer.from(JSON.stringify(obj)), {
            params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
        }),
        decode: (data) => JSON.parse(zstdDecompressSync(data).toString())
    },
    {
        name: 'msgpackr plain',
        encode: (obj) => pack(obj),
        decode: (data) => unpack(data)
    },
    {
        name: 'msgpackr + lz4',
        encode: (obj) => lz4CompressSync(Buffer.from(pack(obj))),
        decode: (data) => unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data)))
    },
    {
        name: 'msgpackr + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? msgpackBlockDict : msgpackReceiptDict;
            return lz4CompressSync(Buffer.from(pack(obj)), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? msgpackBlockDict : msgpackReceiptDict;
            return unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict));
        }
    },
    {
        name: 'msgpackr + zstd',
        encode: (obj) => zstdCompressSync(Buffer.from(pack(obj)), {
            params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
        }),
        decode: (data) => unpack(zstdDecompressSync(data))
    }
];

// Test both blocks and receipts
const datasets = [
    { name: 'blocks', data: testBlocks },
    { name: 'receipts', data: testReceipts }
];

for (const dataset of datasets) {
    console.log(`\n--- Testing ${dataset.name} (${dataset.data.length} items) ---`);

    for (const method of methods) {
        console.log(`\n${method.name}:`);

        // Encode all objects individually
        const encodedItems: (Buffer | Uint8Array)[] = [];
        const encodeStart = performance.now();
        for (const item of dataset.data) {
            encodedItems.push(method.encode(item, dataset.name));
        }
        const encodeTime = performance.now() - encodeStart;

        // Calculate total size
        const totalSize = encodedItems.reduce((sum, item) => sum + item.length, 0);
        const avgSize = totalSize / encodedItems.length;

        // Decode all objects individually
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            method.decode(encoded, dataset.name);
        }
        const decodeTime = performance.now() - decodeStart;

        // Display results
        console.log(`  Total size:     ${(totalSize / 1024 / 1024).toFixed(2)} MB`);
        console.log(`  Avg size/item:  ${(avgSize / 1024).toFixed(2)} KB`);
        console.log(`  Encode time:    ${encodeTime.toFixed(2)}ms (${(encodeTime / dataset.data.length * 1000).toFixed(2)}μs per item)`);
        console.log(`  Decode time:    ${decodeTime.toFixed(2)}ms (${(decodeTime / dataset.data.length * 1000).toFixed(2)}μs per item)`);
    }

    // Calculate relative comparisons
    console.log('\n📊 Size Comparison (vs JSON plain):');

    // Get actual JSON plain total size
    let jsonPlainTotalSize = 0;
    for (const item of dataset.data) {
        jsonPlainTotalSize += methods[0]!.encode(item, dataset.name).length;
    }

    for (let i = 0; i < methods.length; i++) {
        const method = methods[i]!;
        let totalSize = 0;
        for (const item of dataset.data) {
            totalSize += method.encode(item, dataset.name).length;
        }
        const sizeReduction = ((jsonPlainTotalSize - totalSize) / jsonPlainTotalSize * 100).toFixed(1);
        const sizeRatio = (totalSize / jsonPlainTotalSize).toFixed(2);
        if (i === 0) {
            console.log(`  ${method.name.padEnd(15)} - Baseline (100%)`);
        } else {
            console.log(`  ${method.name.padEnd(15)} - ${sizeRatio}x size (${sizeReduction}% reduction)`);
        }
    }
}

console.log('\n=== Benchmark Complete ===');

process.exit(0);
