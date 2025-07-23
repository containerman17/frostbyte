import { Compressor, Decompressor } from 'zstd-napi';
import { compressSync, uncompressSync } from 'lz4-napi';
import fs from 'fs';
import { promises as fsAsync } from 'fs';
import readline from 'readline';
import { pack, unpack } from 'msgpackr';

const TEST_ROWS = 10000;
const ZSTD_COMPRESSION_LEVEL = 1;
const DICT_SIZE = 64 * 1000;
const SAMPLE_EVERY_NTH_LINE = 10;
const ACCESS_TESTS = 1000; // Number of random accesses to test
const BLOCK_SIZES = [10]; // Block sizes to test

interface DecodingResult {
    method: string;
    format: 'json' | 'msgpack';
    blockSize?: number;
    encodedSize: number;
    avgDecodeTimeMs: number;
    p95DecodeTimeMs: number;
    p99DecodeTimeMs: number;
}

async function trainDictionary(lines: string[]): Promise<Buffer> {
    // Create a temporary directory for sample files
    const tempDir = `/tmp/zstd_dict_training_${Date.now()}`;
    await fsAsync.mkdir(tempDir, { recursive: true });

    // Write individual sample files
    let sampleCount = 0;
    for (let i = 0; i < lines.length; i += SAMPLE_EVERY_NTH_LINE) {
        await fsAsync.writeFile(`${tempDir}/sample_${sampleCount}.json`, lines[i]!);
        sampleCount++;
    }

    const dictPath = `/tmp/zstd_dict_${Date.now()}.dict`;
    const { execSync } = await import('child_process');
    execSync(`zstd --train "${tempDir}"/* -o "${dictPath}" --maxdict=${DICT_SIZE}`, { stdio: 'pipe' });

    const dict = await fsAsync.readFile(dictPath);

    // Cleanup
    await fsAsync.rm(tempDir, { recursive: true, force: true });
    await fsAsync.unlink(dictPath);

    return dict;
}

async function readTestData(filePath: string): Promise<string[]> {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const lines: string[] = [];
    for await (const line of rl) {
        lines.push(line);
        if (lines.length >= TEST_ROWS) break;
    }

    return lines;
}

// Block packing implementation with configurable block size
class BlockPacker {
    private compressor: Compressor;
    private decompressor: Decompressor;
    private blocks: Buffer[] = [];
    private format: 'json' | 'msgpack';
    private blockSize: number;
    private cachedBlockIndex: number = -1;
    private cachedDecompressedBlock: Buffer | null = null;

    constructor(blockSize: number, dict?: Buffer, format: 'json' | 'msgpack' = 'json') {
        this.compressor = new Compressor();
        this.decompressor = new Decompressor();
        this.compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
        this.format = format;
        this.blockSize = blockSize;

        if (dict) {
            this.compressor.loadDictionary(dict);
            this.decompressor.loadDictionary(dict);
        }
    }

    encode(lines: string[]): Buffer {
        this.blocks = [];

        for (let i = 0; i < lines.length; i += this.blockSize) {
            const group = lines.slice(i, Math.min(i + this.blockSize, lines.length));
            const blockBuffer = this.encodeBlock(group);
            this.blocks.push(blockBuffer);
        }

        return Buffer.concat(this.blocks);
    }

    private encodeBlock(items: string[]): Buffer {
        const count = items.length;
        const headerSize = 1 + count * 8; // 1 byte count + 8 bytes per item (offset + length)

        // Encode items
        const encodedItems: Buffer[] = [];
        for (const item of items) {
            const data = this.format === 'json'
                ? Buffer.from(item)
                : Buffer.from(pack(JSON.parse(item)));
            encodedItems.push(data);
        }

        // Calculate total uncompressed size
        const dataSize = encodedItems.reduce((sum, buf) => sum + buf.length, 0);
        const uncompressedBuffer = Buffer.allocUnsafe(headerSize + dataSize);

        // Write header
        uncompressedBuffer.writeUInt8(count, 0);

        let currentOffset = 0;
        for (let i = 0; i < count; i++) {
            const offsetPos = 1 + i * 8;
            const lengthPos = offsetPos + 4;

            uncompressedBuffer.writeUInt32LE(currentOffset, offsetPos);
            uncompressedBuffer.writeUInt32LE(encodedItems[i]!.length, lengthPos);

            currentOffset += encodedItems[i]!.length;
        }

        // Write data
        let dataOffset = headerSize;
        for (const item of encodedItems) {
            item.copy(uncompressedBuffer, dataOffset);
            dataOffset += item.length;
        }

        // Compress the entire block
        return this.compressor.compress(uncompressedBuffer);
    }

    decode(index: number): any {
        const blockIndex = Math.floor(index / this.blockSize);
        const itemIndex = index % this.blockSize;

        if (blockIndex >= this.blocks.length) {
            throw new Error(`Index ${index} out of bounds`);
        }

        // Check if we need to decompress a new block
        let decompressed: Buffer;
        if (this.cachedBlockIndex === blockIndex && this.cachedDecompressedBlock) {
            // Use cached decompressed block
            decompressed = this.cachedDecompressedBlock;
        } else {
            // Decompress the block and cache it
            decompressed = this.decompressor.decompress(this.blocks[blockIndex]!);
            this.cachedBlockIndex = blockIndex;
            this.cachedDecompressedBlock = decompressed;
        }

        // Read header
        const count = decompressed.readUInt8(0);
        if (itemIndex >= count) {
            throw new Error(`Item index ${itemIndex} out of bounds in block with ${count} items`);
        }

        // Read offset and length
        const offsetPos = 1 + itemIndex * 8;
        const lengthPos = offsetPos + 4;

        const offset = decompressed.readUInt32LE(offsetPos);
        const length = decompressed.readUInt32LE(lengthPos);

        // Extract data
        const headerSize = 1 + count * 8;
        const dataStart = headerSize + offset;
        const data = decompressed.slice(dataStart, dataStart + length);

        // Decode
        if (this.format === 'json') {
            return JSON.parse(new TextDecoder().decode(data));
        } else {
            return unpack(data);
        }
    }

    getEncodedSize(): number {
        return this.blocks.reduce((sum, block) => sum + block.length, 0);
    }
}

// Line-by-line compression (for comparison with dictionary)
class LineByLineCompressor {
    private compressor: Compressor;
    private decompressor: Decompressor;
    private compressedLines: Buffer[] = [];
    private format: 'json' | 'msgpack';

    constructor(dict?: Buffer, format: 'json' | 'msgpack' = 'json') {
        this.compressor = new Compressor();
        this.decompressor = new Decompressor();
        this.compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
        this.format = format;

        if (dict) {
            this.compressor.loadDictionary(dict);
            this.decompressor.loadDictionary(dict);
        }
    }

    encode(lines: string[]): Buffer {
        this.compressedLines = [];
        const allBuffers: Buffer[] = [];

        for (const line of lines) {
            const data = this.format === 'json'
                ? Buffer.from(line)
                : Buffer.from(pack(JSON.parse(line)));

            const compressed = this.compressor.compress(data);
            this.compressedLines.push(compressed);

            // Store with 4-byte length prefix
            const sizeBuffer = Buffer.allocUnsafe(4);
            sizeBuffer.writeUInt32LE(compressed.length, 0);
            allBuffers.push(sizeBuffer);
            allBuffers.push(compressed);
        }

        return Buffer.concat(allBuffers);
    }

    decode(index: number): any {
        if (index >= this.compressedLines.length) {
            throw new Error(`Index ${index} out of bounds`);
        }

        const decompressed = this.decompressor.decompress(this.compressedLines[index]!);

        if (this.format === 'json') {
            return JSON.parse(new TextDecoder().decode(decompressed));
        } else {
            return unpack(decompressed);
        }
    }

    getEncodedSize(): number {
        return this.compressedLines.reduce((sum, line) => sum + 4 + line.length, 0);
    }
}

// LZ4 Block packing implementation with configurable block size
class LZ4BlockPacker {
    private blocks: Buffer[] = [];
    private format: 'json' | 'msgpack';
    private blockSize: number;
    private dict?: Buffer | undefined;
    private cachedBlockIndex: number = -1;
    private cachedDecompressedBlock: Buffer | null = null;

    constructor(blockSize: number, dict?: Buffer, format: 'json' | 'msgpack' = 'json') {
        this.format = format;
        this.blockSize = blockSize;
        this.dict = dict as any;
    }

    encode(lines: string[]): Buffer {
        this.blocks = [];

        for (let i = 0; i < lines.length; i += this.blockSize) {
            const group = lines.slice(i, Math.min(i + this.blockSize, lines.length));
            const blockBuffer = this.encodeBlock(group);
            this.blocks.push(blockBuffer);
        }

        return Buffer.concat(this.blocks);
    }

    private encodeBlock(items: string[]): Buffer {
        const count = items.length;
        const headerSize = 1 + count * 8; // 1 byte count + 8 bytes per item (offset + length)

        // Encode items
        const encodedItems: Buffer[] = [];
        for (const item of items) {
            const data = this.format === 'json'
                ? Buffer.from(item)
                : Buffer.from(pack(JSON.parse(item)));
            encodedItems.push(data);
        }

        // Calculate total uncompressed size
        const dataSize = encodedItems.reduce((sum, buf) => sum + buf.length, 0);
        const uncompressedBuffer = Buffer.allocUnsafe(headerSize + dataSize);

        // Write header
        uncompressedBuffer.writeUInt8(count, 0);

        let currentOffset = 0;
        for (let i = 0; i < count; i++) {
            const offsetPos = 1 + i * 8;
            const lengthPos = offsetPos + 4;

            uncompressedBuffer.writeUInt32LE(currentOffset, offsetPos);
            uncompressedBuffer.writeUInt32LE(encodedItems[i]!.length, lengthPos);

            currentOffset += encodedItems[i]!.length;
        }

        // Write data
        let dataOffset = headerSize;
        for (const item of encodedItems) {
            item.copy(uncompressedBuffer, dataOffset);
            dataOffset += item.length;
        }

        // Compress the entire block with LZ4
        return compressSync(uncompressedBuffer, this.dict || undefined);
    }

    decode(index: number): any {
        const blockIndex = Math.floor(index / this.blockSize);
        const itemIndex = index % this.blockSize;

        if (blockIndex >= this.blocks.length) {
            throw new Error(`Index ${index} out of bounds`);
        }

        // Check if we need to decompress a new block
        let decompressed: Buffer;
        if (this.cachedBlockIndex === blockIndex && this.cachedDecompressedBlock) {
            // Use cached decompressed block
            decompressed = this.cachedDecompressedBlock;
        } else {
            // Decompress the block and cache it
            decompressed = uncompressSync(this.blocks[blockIndex]!, this.dict || undefined);
            this.cachedBlockIndex = blockIndex;
            this.cachedDecompressedBlock = decompressed;
        }

        // Read header
        const count = decompressed.readUInt8(0);
        if (itemIndex >= count) {
            throw new Error(`Item index ${itemIndex} out of bounds in block with ${count} items`);
        }

        // Read offset and length
        const offsetPos = 1 + itemIndex * 8;
        const lengthPos = offsetPos + 4;

        const offset = decompressed.readUInt32LE(offsetPos);
        const length = decompressed.readUInt32LE(lengthPos);

        // Extract data
        const headerSize = 1 + count * 8;
        const dataStart = headerSize + offset;
        const data = decompressed.slice(dataStart, dataStart + length);

        // Decode
        if (this.format === 'json') {
            return JSON.parse(new TextDecoder().decode(data));
        } else {
            return unpack(data);
        }
    }

    getEncodedSize(): number {
        return this.blocks.reduce((sum, block) => sum + block.length, 0);
    }
}

// LZ4 Line-by-line compression (for comparison with dictionary)
class LZ4LineByLineCompressor {
    private compressedLines: Buffer[] = [];
    private format: 'json' | 'msgpack';
    private dict?: Buffer | undefined;

    constructor(dict?: Buffer, format: 'json' | 'msgpack' = 'json') {
        this.format = format;
        this.dict = dict as any;
    }

    encode(lines: string[]): Buffer {
        this.compressedLines = [];
        const allBuffers: Buffer[] = [];

        for (const line of lines) {
            const data = this.format === 'json'
                ? Buffer.from(line)
                : Buffer.from(pack(JSON.parse(line)));

            const compressed = compressSync(data, this.dict || undefined);
            this.compressedLines.push(compressed);

            // Store with 4-byte length prefix
            const sizeBuffer = Buffer.allocUnsafe(4);
            sizeBuffer.writeUInt32LE(compressed.length, 0);
            allBuffers.push(sizeBuffer);
            allBuffers.push(compressed);
        }

        return Buffer.concat(allBuffers);
    }

    decode(index: number): any {
        if (index >= this.compressedLines.length) {
            throw new Error(`Index ${index} out of bounds`);
        }

        const decompressed = uncompressSync(this.compressedLines[index]!, this.dict || undefined);

        if (this.format === 'json') {
            return JSON.parse(new TextDecoder().decode(decompressed));
        } else {
            return unpack(decompressed);
        }
    }

    getEncodedSize(): number {
        return this.compressedLines.reduce((sum, line) => sum + 4 + line.length, 0);
    }
}

function benchmarkDecoding(name: string, decoder: { decode(index: number): any }, indices: number[]): DecodingResult {
    const times: number[] = [];

    // Warmup
    for (let i = 0; i < 10; i++) {
        decoder.decode(indices[i]!);
    }

    // Actual benchmark
    for (const index of indices) {
        const start = process.hrtime.bigint();
        decoder.decode(index);
        const end = process.hrtime.bigint();

        const timeNs = Number(end - start);
        times.push(timeNs / 1_000_000); // Convert to milliseconds
    }

    // Calculate statistics
    times.sort((a, b) => a - b);
    const avg = times.reduce((sum, t) => sum + t, 0) / times.length;
    const p95 = times[Math.floor(times.length * 0.95)]!;
    const p99 = times[Math.floor(times.length * 0.99)]!;

    return {
        method: name,
        format: 'json', // Will be set by caller
        encodedSize: 0, // Will be set by caller
        avgDecodeTimeMs: avg,
        p95DecodeTimeMs: p95,
        p99DecodeTimeMs: p99
    };
}

async function main() {
    console.log('FrostByte Decoding Speed Benchmark');
    console.log('==================================\n');
    console.log(`Testing with ${TEST_ROWS} rows, ${ACCESS_TESTS} random accesses\n`);

    // Read test data
    const dataFile = 'data/dict_test/txs.jsonl';
    console.log('Reading test data...');
    const lines = await readTestData(dataFile);
    console.log(`Loaded ${lines.length} lines\n`);

    // Train dictionaries for each format
    console.log('Training dictionaries...');
    const dictJson = await trainDictionary(lines);
    const dictPack = await trainDictionary(
        lines.map(l => pack(JSON.parse(l)).toString("binary"))
    );
    console.log(`JSON dictionary size: ${(dictJson.length / 1024).toFixed(2)} KB`);
    console.log(`MessagePack dictionary size: ${(dictPack.length / 1024).toFixed(2)} KB\n`);

    // Generate random access indices
    const indices: number[] = [];
    for (let i = 0; i < ACCESS_TESTS; i++) {
        indices.push(Math.floor(Math.random() * lines.length));
    }

    const results: DecodingResult[] = [];

    // Test all combinations
    const formats: Array<'json' | 'msgpack'> = ['json', 'msgpack'];

    for (const format of formats) {
        console.log(`\nTesting with ${format.toUpperCase()} format:`);
        console.log('='.repeat(40));

        for (const blockSize of BLOCK_SIZES) {
            console.log(`\nTesting with block size: ${blockSize}`);
            console.log('='.repeat(40));

            // Select appropriate dictionary for format
            const dict = format === 'json' ? dictJson : dictPack;

            // ZSTD Tests
            console.log('\n--- ZSTD Tests ---');

            // 1. ZSTD Block packing with dictionary
            {
                console.log(`\n1. ZSTD Block packing with dictionary (${blockSize}x)...`);
                const packer = new BlockPacker(blockSize, dict, format);
                const encoded = packer.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`ZSTD Block packing (dict, ${blockSize}x)`, packer, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = packer.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 2. ZSTD Line-by-line with dictionary
            {
                console.log(`\n2. ZSTD Line-by-line with dictionary (${blockSize})...`);
                const compressor = new LineByLineCompressor(dict, format);
                const encoded = compressor.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`ZSTD Line-by-line (dict, ${blockSize})`, compressor, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = compressor.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 3. ZSTD Block packing without dictionary (plain zstd)
            {
                console.log(`\n3. ZSTD Block packing plain (${blockSize})...`);
                const packer = new BlockPacker(blockSize, undefined, format);
                const encoded = packer.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`ZSTD Block packing (plain, ${blockSize})`, packer, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = packer.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 4. ZSTD Line-by-line without dictionary (plain zstd)
            {
                console.log(`\n4. ZSTD Line-by-line plain (${blockSize})...`);
                const compressor = new LineByLineCompressor(undefined, format);
                const encoded = compressor.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`ZSTD Line-by-line (plain, ${blockSize})`, compressor, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = compressor.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // LZ4 Tests
            console.log('\n--- LZ4 Tests ---');

            // 5. LZ4 Block packing with dictionary
            {
                console.log(`\n5. LZ4 Block packing with dictionary (${blockSize}x)...`);
                const packer = new LZ4BlockPacker(blockSize, dict, format);
                const encoded = packer.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`LZ4 Block packing (dict, ${blockSize}x)`, packer, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = packer.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 6. LZ4 Line-by-line with dictionary
            {
                console.log(`\n6. LZ4 Line-by-line with dictionary (${blockSize})...`);
                const compressor = new LZ4LineByLineCompressor(dict, format);
                const encoded = compressor.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`LZ4 Line-by-line (dict, ${blockSize})`, compressor, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = compressor.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 7. LZ4 Block packing without dictionary (plain LZ4)
            {
                console.log(`\n7. LZ4 Block packing plain (${blockSize})...`);
                const packer = new LZ4BlockPacker(blockSize, undefined, format);
                const encoded = packer.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`LZ4 Block packing (plain, ${blockSize})`, packer, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = packer.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }

            // 8. LZ4 Line-by-line without dictionary (plain LZ4)
            {
                console.log(`\n8. LZ4 Line-by-line plain (${blockSize})...`);
                const compressor = new LZ4LineByLineCompressor(undefined, format);
                const encoded = compressor.encode(lines);
                console.log(`   Encoded size: ${(encoded.length / 1024 / 1024).toFixed(2)} MB`);

                const result = benchmarkDecoding(`LZ4 Line-by-line (plain, ${blockSize})`, compressor, indices);
                result.format = format;
                result.blockSize = blockSize;
                result.encodedSize = compressor.getEncodedSize();
                results.push(result);

                console.log(`   Avg decode time: ${result.avgDecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P95 decode time: ${result.p95DecodeTimeMs.toFixed(3)} ms`);
                console.log(`   P99 decode time: ${result.p99DecodeTimeMs.toFixed(3)} ms`);
            }
        }
    }

    // Print summary table
    console.log('\n\nSummary');
    console.log('=======');
    console.log('Method                                 | Format  | Block | Size (MB) | Avg/10k (ms) | P95/10k (ms) | P99/10k (ms)');
    console.log('---------------------------------------|---------|-------|-----------|--------------|--------------|--------------');

    for (const result of results) {
        const blockSizeStr = result.blockSize ? result.blockSize.toString().padStart(5) : '    -';
        console.log(
            `${result.method.padEnd(38)} | ${result.format.padEnd(7)} | ${blockSizeStr} | ${(result.encodedSize / 1024 / 1024).toFixed(1).padStart(9)} | ${(result.avgDecodeTimeMs * 10000).toFixed(1).padStart(11)
            } | ${(result.p95DecodeTimeMs * 10000).toFixed(1).padStart(11)} | ${(result.p99DecodeTimeMs * 10000).toFixed(1).padStart(11)}`
        );
    }

    // Group results by format and find best performers
    console.log('\n\nBest Performers by Format:');
    console.log('==========================');

    const formatTypes: Array<'json' | 'msgpack'> = ['json', 'msgpack'];
    for (const format of formatTypes) {
        const formatResults = results.filter(r => r.format === format);
        const fastestInFormat = formatResults.reduce((a, b) => a.avgDecodeTimeMs < b.avgDecodeTimeMs ? a : b);
        const smallestInFormat = formatResults.reduce((a, b) => a.encodedSize < b.encodedSize ? a : b);

        console.log(`\n${format.toUpperCase()}:`);
        console.log(`  Fastest: ${fastestInFormat.method} (block size: ${fastestInFormat.blockSize}) - ${fastestInFormat.avgDecodeTimeMs.toFixed(3)} ms avg`);
        console.log(`  Smallest: ${smallestInFormat.method} (block size: ${smallestInFormat.blockSize}) - ${(smallestInFormat.encodedSize / 1024 / 1024).toFixed(2)} MB`);
    }

    // Find best by speed
    const bestSpeed = results.reduce((a, b) => a.avgDecodeTimeMs < b.avgDecodeTimeMs ? a : b);
    console.log(`\nOverall fastest decoding: ${bestSpeed.method} (${bestSpeed.format}, block size: ${bestSpeed.blockSize}) - ${bestSpeed.avgDecodeTimeMs.toFixed(3)} ms avg`);

    // Find best by size
    const bestSize = results.reduce((a, b) => a.encodedSize < b.encodedSize ? a : b);
    console.log(`Overall smallest size: ${bestSize.method} (${bestSize.format}, block size: ${bestSize.blockSize}) - ${(bestSize.encodedSize / 1024 / 1024).toFixed(2)} MB`);
}

main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
}); 
