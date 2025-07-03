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
import * as zstd from 'zstd-napi';

const HEX_RE = /^0x[0-9a-f]+$/i;


const dbPath = "./database/e2e_zerooneMainnet/blocks.db"
const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: true })
const lastIndexedBlock = db.getLastStoredBlockNumber();
let lastBlock = 0;


const BENCHMARK_BLOCKS = 20000;
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

const compactBlockDict = trainDictionary(
    testBlocks,
    'compact-blocks',
    b => Buffer.from(pack(hex2bin(b)))
);
const compactReceiptDict = trainDictionary(
    testReceipts,
    'compact-receipts',
    r => Buffer.from(pack(hex2bin(r)))
);

// Train zstd-napi dictionaries
const zstdJsonBlockDict = trainDictionary(testBlocks, 'zstd-json-blocks', b => JSON.stringify(b));
const zstdJsonReceiptDict = trainDictionary(testReceipts, 'zstd-json-receipts', r => JSON.stringify(r));
const zstdMsgpackBlockDict = trainDictionary(testBlocks, 'zstd-msgpack-blocks', b => Buffer.from(pack(b)));
const zstdMsgpackReceiptDict = trainDictionary(testReceipts, 'zstd-msgpack-receipts', r => Buffer.from(pack(r)));
const zstdCompactBlockDict = trainDictionary(testBlocks, 'zstd-compact-blocks', b => Buffer.from(pack(hex2bin(b))));
const zstdCompactReceiptDict = trainDictionary(testReceipts, 'zstd-compact-receipts', r => Buffer.from(pack(hex2bin(r))));

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
console.log(`  Compact block dict: ${(compactBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Compact receipt dict: ${(compactReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd JSON block dict: ${(zstdJsonBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd JSON receipt dict: ${(zstdJsonReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Msgpack block dict: ${(zstdMsgpackBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Msgpack receipt dict: ${(zstdMsgpackReceiptDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Compact block dict: ${(zstdCompactBlockDict.length / 1024).toFixed(1)} KB`);
console.log(`  Zstd Compact receipt dict: ${(zstdCompactReceiptDict.length / 1024).toFixed(1)} KB`);

console.log('\n=== Individual Object Encoding/Decoding Benchmark ===');
console.log('(Simulating database row storage)\n');

interface Method {
    name: string;
    encode: (obj: any, datasetName: string) => Buffer | Uint8Array;
    decode: (data: Buffer | Uint8Array, datasetName: string) => any;
    setup?: (datasetName: string) => void;
    cleanup?: () => void;
}

// Create reusable compressor/decompressor instances
const zstdCompressors: Record<string, zstd.Compressor> = {};
const zstdDecompressors: Record<string, zstd.Decompressor> = {};

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
        name: 'JSON + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdJsonBlockDict : zstdJsonReceiptDict;
            const key = `json-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `json-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(JSON.stringify(obj)));
        },
        decode: (data, datasetName) => {
            const key = `json-${datasetName}`;
            return JSON.parse(zstdDecompressors[key]!.decompress(data).toString());
        }
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
    },
    {
        name: 'msgpackr + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdMsgpackBlockDict : zstdMsgpackReceiptDict;
            const key = `msgpack-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `msgpack-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(obj)));
        },
        decode: (data, datasetName) => {
            const key = `msgpack-${datasetName}`;
            return unpack(zstdDecompressors[key]!.decompress(data));
        }
    },
    {
        name: 'compactMsgpackr plain',
        encode: obj => Buffer.from(pack(hex2bin(obj))),
        decode: data => bin2hex(unpack(data))
    },
    {
        name: 'compactMsgpackr + lz4',
        encode: obj => lz4CompressSync(Buffer.from(pack(hex2bin(obj)))),
        decode: data =>
            bin2hex(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data))))
    },
    {
        name: 'compactMsgpackr + lz4+dict',
        encode: (obj, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            return lz4CompressSync(Buffer.from(pack(hex2bin(obj))), dict);
        },
        decode: (data, datasetName) => {
            const dict = datasetName === 'blocks' ? compactBlockDict : compactReceiptDict;
            return bin2hex(unpack(lz4UncompressSync(Buffer.isBuffer(data) ? data : Buffer.from(data), dict)));
        }
    },
    {
        name: 'compactMsgpackr + zstd',
        encode: obj =>
            zstdCompressSync(Buffer.from(pack(hex2bin(obj))), {
                params: { [zlibConstants.ZSTD_c_compressionLevel]: 3 }
            }),
        decode: data => bin2hex(unpack(zstdDecompressSync(data)))
    },


    {
        name: 'compactMsgpackr + zstdNapi+dict',
        setup: (datasetName) => {
            const dict = datasetName === 'blocks' ? zstdCompactBlockDict : zstdCompactReceiptDict;
            const key = `compact-${datasetName}`;
            if (!zstdCompressors[key]) {
                const compressor = new zstd.Compressor();
                compressor.setParameters({ compressionLevel: 3 });
                compressor.loadDictionary(dict);
                zstdCompressors[key] = compressor;
            }
            if (!zstdDecompressors[key]) {
                const decompressor = new zstd.Decompressor();
                decompressor.loadDictionary(dict);
                zstdDecompressors[key] = decompressor;
            }
        },
        encode: (obj, datasetName) => {
            const key = `compact-${datasetName}`;
            return zstdCompressors[key]!.compress(Buffer.from(pack(hex2bin(obj))));
        },
        decode: (data, datasetName) => {
            const key = `compact-${datasetName}`;
            return bin2hex(unpack(zstdDecompressors[key]!.decompress(data)));
        }
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

        // Setup method if needed
        if (method.setup) {
            method.setup(dataset.name);
        }

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
    console.log('\n📊 Size & Decode Time Comparison (vs JSON plain):');

    // Get actual JSON plain total size and decode time
    let jsonPlainTotalSize = 0;
    for (const item of dataset.data) {
        jsonPlainTotalSize += methods[0]!.encode(item, dataset.name).length;
    }

    // Get JSON plain decode time
    const jsonPlainEncodedItems: (Buffer | Uint8Array)[] = [];
    for (const item of dataset.data) {
        jsonPlainEncodedItems.push(methods[0]!.encode(item, dataset.name));
    }
    const jsonPlainDecodeStart = performance.now();
    for (const encoded of jsonPlainEncodedItems) {
        methods[0]!.decode(encoded, dataset.name);
    }
    const jsonPlainDecodeTime = performance.now() - jsonPlainDecodeStart;

    // Calculate extrapolation factor for 100M items
    const ITEMS_TARGET = 100_000_000;
    const extrapolationFactor = ITEMS_TARGET / dataset.data.length;

    for (let i = 0; i < methods.length; i++) {
        const method = methods[i]!;
        let totalSize = 0;
        const encodedItems: (Buffer | Uint8Array)[] = [];
        for (const item of dataset.data) {
            const encoded = method.encode(item, dataset.name);
            encodedItems.push(encoded);
            totalSize += encoded.length;
        }

        // Measure decode time
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            method.decode(encoded, dataset.name);
        }
        const decodeTime = performance.now() - decodeStart;

        const sizeReduction = ((jsonPlainTotalSize - totalSize) / jsonPlainTotalSize * 100).toFixed(1);
        const sizeRatio = (totalSize / jsonPlainTotalSize).toFixed(2);
        const timeRatio = (decodeTime / jsonPlainDecodeTime).toFixed(2);

        // Extrapolate to 100M items
        const extrapolatedSizeGB = (totalSize * extrapolationFactor) / (1024 * 1024 * 1024);
        const extrapolatedTimeMinutes = (decodeTime * extrapolationFactor) / (1000 * 60);

        if (i === 0) {
            console.log(`  ${method.name.padEnd(20)} - Baseline: ${extrapolatedSizeGB.toFixed(1)}GB, ${extrapolatedTimeMinutes.toFixed(1)}min`);
        } else {
            console.log(`  ${method.name.padEnd(20)} - ${sizeRatio}x size ${timeRatio}x time: ${extrapolatedSizeGB.toFixed(1)}GB, ${extrapolatedTimeMinutes.toFixed(1)}min (${sizeReduction}% smaller)`);
        }
    }
}

console.log('\n=== Benchmark Complete ===');

process.exit(0);


function hex2bin<T>(x: T): T {
    if (typeof x === 'string' && HEX_RE.test(x)) {
        return Buffer.from(x.slice(2), 'hex') as unknown as T;
    }
    if (Array.isArray(x)) return x.map(hex2bin) as unknown as T;
    if (x && typeof x === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(x)) out[k] = hex2bin(v);
        return out as T;
    }
    return x;
}

function bin2hex<T>(x: T): T {
    if (Buffer.isBuffer(x)) return ('0x' + x.toString('hex')) as unknown as T;
    if (Array.isArray(x)) return x.map(bin2hex) as unknown as T;
    if (x && typeof x === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(x)) out[k] = bin2hex(v);
        return out as T;
    }
    return x;
}
