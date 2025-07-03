import { BlockDB } from "./blockFetcher/BlockDB"
import { LazyTx, lazyTxToReceipt } from "./blockFetcher/lazy/LazyTx";
import { LazyBlock, lazyBlockToBlock } from "./blockFetcher/lazy/LazyBlock";
import { LazyTraces } from "./blockFetcher/lazy/LazyTrace";
import { RpcBlock, RpcTxReceipt } from "./blockFetcher/evmTypes";
import { pack, unpack } from 'msgpackr';
import { compressSync as lz4CompressSync, uncompressSync as lz4UncompressSync } from 'lz4-napi';

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

console.log('\n=== Individual Object Encoding/Decoding Benchmark ===');
console.log('(Simulating database row storage)\n');

interface Method {
    name: string;
    encode: (obj: any) => Buffer | Uint8Array;
    decode: (data: Buffer | Uint8Array) => any;
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
        decode: (data) => JSON.parse(lz4UncompressSync(data).toString())
    },
    {
        name: 'msgpackr plain',
        encode: (obj) => pack(obj),
        decode: (data) => unpack(data)
    },
    {
        name: 'msgpackr + lz4',
        encode: (obj) => lz4CompressSync(pack(obj)),
        decode: (data) => unpack(lz4UncompressSync(data))
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
            encodedItems.push(method.encode(item));
        }
        const encodeTime = performance.now() - encodeStart;

        // Calculate total size
        const totalSize = encodedItems.reduce((sum, item) => sum + item.length, 0);
        const avgSize = totalSize / encodedItems.length;

        // Decode all objects individually
        const decodeStart = performance.now();
        for (const encoded of encodedItems) {
            method.decode(encoded);
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
        jsonPlainTotalSize += methods[0]!.encode(item).length;
    }

    for (let i = 0; i < methods.length; i++) {
        const method = methods[i]!;
        let totalSize = 0;
        for (const item of dataset.data) {
            totalSize += method.encode(item).length;
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
