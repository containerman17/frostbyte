
// Summary
// =======
// Method                    | Original Size   | Compressed Size | Ratio  | Time
// --------------------------|-----------------|-----------------|--------|-------
// zstd CLI (whole file)     | 456.32 MB       | 41.47 MB        | 11.00x | 380ms
// Line by line (no dict)    | 456.32 MB       | 112.01 MB       |  4.07x | 1512ms
// Groups of 10 (no dict)    | 456.32 MB       | 57.43 MB        |  7.94x | 985ms
// Line by line with dict    | 456.32 MB       | 53.13 MB        |  8.59x | 1029ms
// Groups of 10 with dict    | 456.32 MB       | 45.98 MB        |  9.92x | 890ms


import { Compressor } from 'zstd-napi';
import fs from 'fs';
import { promises as fsAsync } from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import os from 'os';
import readline from 'readline';

const ZSTD_COMPRESSION_LEVEL = 1;
const DICT_SIZE = 110 * 1000;
const SAMPLE_EVERY_NTH_LINE = 10;

interface CompressionResult {
    method: string;
    originalSize: number;
    compressedSize: number;
    ratio: number;
    timeMs: number;
}

function formatBytes(bytes: number): string {
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }

    return `${size.toFixed(2)} ${units[unitIndex]}`;
}

async function getFileSize(filePath: string): Promise<number> {
    const stats = await fsAsync.stat(filePath);
    return stats.size;
}

async function compressWithCLI(inputPath: string, outputPath: string, level: number = ZSTD_COMPRESSION_LEVEL): Promise<void> {
    execSync(`zstd -${level} -f -o "${outputPath}" "${inputPath}"`, { stdio: 'pipe' });
}

async function trainDictionary(dataPath: string, dictPath: string, maxDictSize: number = DICT_SIZE): Promise<void> {
    // Use zstd's built-in block splitting feature instead of manual sampling
    // -B option splits the file into blocks which is better for dictionary training
    const blockSize = 1024; // 1KB blocks - good size for JSON lines

    const tempDir = await fsAsync.mkdtemp(path.join(os.tmpdir(), 'zstd-dict-'));

    // Remove tempDir if it exists, then recreate
    await fsAsync.rm(tempDir, { recursive: true, force: true });
    await fsAsync.mkdir(tempDir, { recursive: true });

    // Read lines and create many more samples
    const fileStream = fs.createReadStream(dataPath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineCount = 0;

    for await (const line of rl) {
        lineCount++;
        if (lineCount % SAMPLE_EVERY_NTH_LINE === 0) {
            fs.writeFileSync(path.join(tempDir, `sample_${lineCount}.json`), line);
        }
    }

    // Train dictionary with manual samples
    execSync(`zstd --train "${tempDir}"/* -o "${dictPath}" --maxdict=${maxDictSize}`, { stdio: 'pipe' });
    // Cleanup temp dir
    await fsAsync.rm(tempDir, { recursive: true, force: true });
}

async function compressLineByLine(
    inputPath: string,
    outputPath: string,
    dict?: Buffer
): Promise<{ totalOriginal: number; totalCompressed: number }> {
    const fileStream = fs.createReadStream(inputPath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const writeStream = fs.createWriteStream(outputPath);
    let totalOriginal = 0;
    let totalCompressed = 0;

    // Create compressor and configure it
    const compressor = new Compressor();
    compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });

    if (dict) {
        compressor.loadDictionary(dict);
    }

    for await (const line of rl) {
        const lineBuffer = Buffer.from(line + '\n');
        totalOriginal += lineBuffer.length;

        // Compress the line
        const compressed = compressor.compress(lineBuffer);

        // Write compressed size (4 bytes) + compressed data
        const sizeBuffer = Buffer.allocUnsafe(4);
        sizeBuffer.writeUInt32LE(compressed.length, 0);
        writeStream.write(sizeBuffer);
        writeStream.write(compressed);

        totalCompressed += 4 + compressed.length;
    }

    await new Promise((resolve) => writeStream.end(resolve));

    return { totalOriginal, totalCompressed };
}

async function compressInGroups(
    inputPath: string,
    outputPath: string,
    groupSize: number = 10,
    dict?: Buffer
): Promise<{ totalOriginal: number; totalCompressed: number }> {
    const fileStream = fs.createReadStream(inputPath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const writeStream = fs.createWriteStream(outputPath);
    let totalOriginal = 0;
    let totalCompressed = 0;

    // Create compressor and configure it
    const compressor = new Compressor();
    compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });

    if (dict) {
        compressor.loadDictionary(dict);
    }

    let group: string[] = [];

    for await (const line of rl) {
        group.push(line);

        if (group.length === groupSize) {
            // Compress the group
            const groupText = group.join('\n') + '\n';
            const groupBuffer = Buffer.from(groupText);
            totalOriginal += groupBuffer.length;

            const compressed = compressor.compress(groupBuffer);

            // Write compressed size (4 bytes) + compressed data
            const sizeBuffer = Buffer.allocUnsafe(4);
            sizeBuffer.writeUInt32LE(compressed.length, 0);
            writeStream.write(sizeBuffer);
            writeStream.write(compressed);

            totalCompressed += 4 + compressed.length;

            // Reset group
            group = [];
        }
    }

    // Handle remaining lines if any
    if (group.length > 0) {
        const groupText = group.join('\n') + '\n';
        const groupBuffer = Buffer.from(groupText);
        totalOriginal += groupBuffer.length;

        const compressed = compressor.compress(groupBuffer);

        // Write compressed size (4 bytes) + compressed data
        const sizeBuffer = Buffer.allocUnsafe(4);
        sizeBuffer.writeUInt32LE(compressed.length, 0);
        writeStream.write(sizeBuffer);
        writeStream.write(compressed);

        totalCompressed += 4 + compressed.length;
    }

    await new Promise((resolve) => writeStream.end(resolve));

    return { totalOriginal, totalCompressed };
}

async function main() {
    const dataFile = 'data/dict_test/txs.jsonl';
    const results: CompressionResult[] = [];

    console.log('FrostByte Compression Benchmark');
    console.log('================================\n');

    const originalSize = await getFileSize(dataFile);
    console.log(`Original file size: ${formatBytes(originalSize)}\n`);

    // 1. Compress entire file with zstd CLI (no dictionary needed for large files)
    {
        console.log('1. Compressing entire file with zstd CLI...');
        const outputPath = 'data/dict_test/txs_cli.jsonl.zst';
        const start = Date.now();

        await compressWithCLI(dataFile, outputPath);

        const compressedSize = await getFileSize(outputPath);
        const timeMs = Date.now() - start;

        results.push({
            method: 'zstd CLI (whole file)',
            originalSize,
            compressedSize,
            ratio: originalSize / compressedSize,
            timeMs
        });

        console.log(`   Compressed size: ${formatBytes(compressedSize)}`);
        console.log(`   Compression ratio: ${(originalSize / compressedSize).toFixed(2)}x\n`);
    }

    // 2. Compress line by line without dictionary
    {
        console.log('2. Compressing line by line without dictionary...');
        const outputPath = 'data/dict_test/txs_line_by_line.zst';
        const start = Date.now();

        const { totalOriginal, totalCompressed } = await compressLineByLine(dataFile, outputPath);

        const timeMs = Date.now() - start;

        results.push({
            method: 'Line by line (no dict)',
            originalSize: totalOriginal,
            compressedSize: totalCompressed,
            ratio: totalOriginal / totalCompressed,
            timeMs
        });

        console.log(`   Compressed size: ${formatBytes(totalCompressed)}`);
        console.log(`   Compression ratio: ${(totalOriginal / totalCompressed).toFixed(2)}x\n`);
    }

    // 3. Compress in groups of 10 without dictionary
    {
        console.log('3. Compressing in groups of 10 without dictionary...');
        const outputPath = 'data/dict_test/txs_groups.zst';
        const start = Date.now();

        const { totalOriginal, totalCompressed } = await compressInGroups(dataFile, outputPath, 10);

        const timeMs = Date.now() - start;

        results.push({
            method: 'Groups of 10 (no dict)',
            originalSize: totalOriginal,
            compressedSize: totalCompressed,
            ratio: totalOriginal / totalCompressed,
            timeMs
        });

        console.log(`   Compressed size: ${formatBytes(totalCompressed)}`);
        console.log(`   Compression ratio: ${(totalOriginal / totalCompressed).toFixed(2)}x\n`);
    }

    // 4. Train dictionary and compress line by line with it
    {
        console.log('4. Training dictionary on txs.jsonl...');
        const dictPath = 'data/dict_test/zstd.dict';

        await trainDictionary(dataFile, dictPath);
        const dictSize = await getFileSize(dictPath);
        console.log(`   Dictionary size: ${formatBytes(dictSize)}`);

        console.log('\n   Compressing line by line with dictionary...');
        const outputPath = 'data/dict_test/txs_line_by_line_dict.zst';
        const dictBuffer = await fsAsync.readFile(dictPath);

        const start = Date.now();
        const { totalOriginal, totalCompressed } = await compressLineByLine(dataFile, outputPath, dictBuffer);

        const timeMs = Date.now() - start;

        results.push({
            method: 'Line by line with dict',
            originalSize: totalOriginal,
            compressedSize: totalCompressed + dictSize, // Include dict size
            ratio: totalOriginal / (totalCompressed + dictSize),
            timeMs
        });

        console.log(`   Compressed size: ${formatBytes(totalCompressed)} (+ ${formatBytes(dictSize)} dict)`);
        console.log(`   Total size: ${formatBytes(totalCompressed + dictSize)}`);
        console.log(`   Compression ratio: ${(totalOriginal / (totalCompressed + dictSize)).toFixed(2)}x\n`);
    }

    // 5. Compress in groups of 10 with dictionary
    {
        console.log('5. Compressing in groups of 10 with dictionary...');
        const outputPath = 'data/dict_test/txs_groups_dict.zst';
        const dictPath = 'data/dict_test/zstd.dict';
        const dictBuffer = await fsAsync.readFile(dictPath);
        const dictSize = await getFileSize(dictPath);
        const start = Date.now();

        const { totalOriginal, totalCompressed } = await compressInGroups(dataFile, outputPath, 10, dictBuffer);

        const timeMs = Date.now() - start;

        results.push({
            method: 'Groups of 10 with dict',
            originalSize: totalOriginal,
            compressedSize: totalCompressed + dictSize, // Include dict size
            ratio: totalOriginal / (totalCompressed + dictSize),
            timeMs
        });

        console.log(`   Compressed size: ${formatBytes(totalCompressed)} (+ ${formatBytes(dictSize)} dict)`);
        console.log(`   Total size: ${formatBytes(totalCompressed + dictSize)}`);
        console.log(`   Compression ratio: ${(totalOriginal / (totalCompressed + dictSize)).toFixed(2)}x\n`);
    }

    // Print summary
    console.log('\nSummary');
    console.log('=======');
    console.log('Method                    | Original Size   | Compressed Size | Ratio  | Time');
    console.log('--------------------------|-----------------|-----------------|--------|-------');

    for (const result of results) {
        console.log(
            `${result.method.padEnd(25)} | ${formatBytes(result.originalSize).padEnd(15)} | ${formatBytes(result.compressedSize).padEnd(15)} | ${result.ratio.toFixed(2).padStart(5)}x | ${result.timeMs}ms`
        );
    }

    // Find best method
    const best = results.reduce((a, b) => a.compressedSize < b.compressedSize ? a : b);
    console.log(`\nBest compression: ${best.method} (${formatBytes(best.compressedSize)}, ${best.ratio.toFixed(2)}x)`);
}

main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
});
