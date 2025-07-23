import sqlite3 from 'better-sqlite3';
import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { StoredTx } from '../blockFetcher/evmTypes';
import { Compressor, Decompressor } from 'zstd-napi';
import { execSync } from 'child_process';
import os from 'os';

const MAX_LINES = 150000;
const BENCHMARK_ROWS = 10000;
const DICT_SIZE = 110 * 1000;
const SAMPLE_EVERY_NTH_LINE = 10;
const COMPRESSION_LEVEL = 1;

// Storage directories
const testDir = '/mnt/btrfsdev';
const dictPath = path.join(testDir, 'zstd.dict');

// Database paths
const dictDbPath = path.join(testDir, 'dict_compressed.db');
const zstdDbPath = path.join(testDir, 'zstd_compressed.db');
const uncompressedDbPath = path.join(testDir, 'uncompressed.db');

interface BenchmarkResult {
    method: string;
    dbSize: number;
    readTimeMs: number;
    objectsRead: number;
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

async function trainDictionary(dataPath: string): Promise<void> {
    console.log('Training zstd dictionary...');

    const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'zstd-dict-'));

    try {
        // Sample lines for dictionary training
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

        // Train dictionary
        execSync(`zstd --train "${tempDir}"/* -o "${dictPath}" --maxdict=${DICT_SIZE}`, { stdio: 'pipe' });

        const dictSize = fs.statSync(dictPath).size;
        console.log(`Dictionary trained: ${formatBytes(dictSize)}`);

    } finally {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    }
}

async function setupDatabases(): Promise<void> {
    console.log('Setting up databases...');

    // Clean up existing databases
    [dictDbPath, zstdDbPath, uncompressedDbPath].forEach(dbPath => {
        if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    });

    // Create databases
    const dictDb = sqlite3(dictDbPath);
    const zstdDb = sqlite3(zstdDbPath);
    const uncompressedDb = sqlite3(uncompressedDbPath);

    // Create tables - all store compressed/uncompressed data as BLOB
    const createTableSql = `
    CREATE TABLE txs (
        txId INTEGER PRIMARY KEY AUTOINCREMENT,
            data BLOB NOT NULL
        )
    `;

    dictDb.exec(createTableSql);
    zstdDb.exec(createTableSql);
    uncompressedDb.exec(createTableSql);

    dictDb.close();
    zstdDb.close();
    uncompressedDb.close();
}

async function insertData(): Promise<void> {
    console.log('Inserting data into databases...');

    const filePath = path.join('data', 'dict_test', 'txs.jsonl');

    // Open databases
    const dictDb = sqlite3(dictDbPath);
    const zstdDb = sqlite3(zstdDbPath);
    const uncompressedDb = sqlite3(uncompressedDbPath);

    // Prepare statements
    const dictStmt = dictDb.prepare('INSERT INTO txs (data) VALUES (?)');
    const zstdStmt = zstdDb.prepare('INSERT INTO txs (data) VALUES (?)');
    const uncompressedStmt = uncompressedDb.prepare('INSERT INTO txs (data) VALUES (?)');

    // Setup compressors
    const dictCompressor = new Compressor();
    dictCompressor.setParameters({ compressionLevel: COMPRESSION_LEVEL });
    const dictBuffer = fs.readFileSync(dictPath);
    dictCompressor.loadDictionary(dictBuffer);

    const zstdCompressor = new Compressor();
    zstdCompressor.setParameters({ compressionLevel: COMPRESSION_LEVEL });

    // Start transactions
    dictDb.exec('BEGIN');
    zstdDb.exec('BEGIN');
    uncompressedDb.exec('BEGIN');

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineCount = 0;
    for await (const line of rl) {
        if (lineCount >= MAX_LINES) break;
        if (!line.trim()) continue;

        const lineBuffer = Buffer.from(line, 'utf8');

        // Insert with dictionary compression
        const dictCompressed = dictCompressor.compress(lineBuffer);
        dictStmt.run(dictCompressed);

        // Insert with plain zstd compression
        const zstdCompressed = zstdCompressor.compress(lineBuffer);
        zstdStmt.run(zstdCompressed);

        // Insert uncompressed
        uncompressedStmt.run(lineBuffer);

        lineCount++;
    }

    // Commit transactions
    dictDb.exec('COMMIT');
    zstdDb.exec('COMMIT');
    uncompressedDb.exec('COMMIT');

    console.log(`Inserted ${lineCount} records into each database`);

    // Vacuum all databases
    console.log('Vacuuming databases...');
    dictDb.exec('VACUUM');
    zstdDb.exec('VACUUM');
    uncompressedDb.exec('VACUUM');

    dictDb.close();
    zstdDb.close();
    uncompressedDb.close();
}

async function benchmarkRead(dbPath: string, useDict: boolean, useZstd: boolean): Promise<{ timeMs: number; objectsRead: number }> {
    const db = sqlite3(dbPath);

    // Setup decompressor if needed
    let decompressor: Decompressor | null = null;
    if (useDict || useZstd) {
        decompressor = new Decompressor();
        if (useDict) {
            const dictBuffer = fs.readFileSync(dictPath);
            decompressor.loadDictionary(dictBuffer);
        }
    }

    const start = Date.now();

    const rows = db.prepare(`SELECT data FROM txs LIMIT ${BENCHMARK_ROWS}`).all() as { data: Buffer }[];

    const objects: StoredTx[] = [];
    for (const row of rows) {
        let jsonString: string;

        if (decompressor) {
            // Decompress the data
            const decompressed = decompressor.decompress(row.data);
            jsonString = decompressed.toString('utf8');
        } else {
            // Use data directly
            jsonString = row.data.toString('utf8');
        }

        // Parse JSON
        const obj: StoredTx = JSON.parse(jsonString);
        objects.push(obj);
    }

    const timeMs = Date.now() - start;

    db.close();

    return { timeMs, objectsRead: objects.length };
}

async function runBenchmarks(): Promise<BenchmarkResult[]> {
    console.log('\nRunning benchmarks...');

    const results: BenchmarkResult[] = [];

    // Get database sizes
    const dictSize = fs.statSync(dictDbPath).size;
    const zstdSize = fs.statSync(zstdDbPath).size;
    const uncompressedSize = fs.statSync(uncompressedDbPath).size;

    console.log('\nDatabase sizes:');
    console.log(`Dictionary compressed: ${formatBytes(dictSize)}`);
    console.log(`Plain zstd compressed: ${formatBytes(zstdSize)}`);
    console.log(`Uncompressed: ${formatBytes(uncompressedSize)}`);

    // Benchmark dictionary compressed
    console.log('\nBenchmarking dictionary compressed read...');
    const dictResult = await benchmarkRead(dictDbPath, true, true);
    results.push({
        method: 'Dictionary + zstd',
        dbSize: dictSize,
        readTimeMs: dictResult.timeMs,
        objectsRead: dictResult.objectsRead
    });

    // Benchmark plain zstd compressed
    console.log('Benchmarking plain zstd compressed read...');
    const zstdResult = await benchmarkRead(zstdDbPath, false, true);
    results.push({
        method: 'Plain zstd',
        dbSize: zstdSize,
        readTimeMs: zstdResult.timeMs,
        objectsRead: zstdResult.objectsRead
    });

    // Benchmark uncompressed
    console.log('Benchmarking uncompressed read...');
    const uncompressedResult = await benchmarkRead(uncompressedDbPath, false, false);
    results.push({
        method: 'Uncompressed',
        dbSize: uncompressedSize,
        readTimeMs: uncompressedResult.timeMs,
        objectsRead: uncompressedResult.objectsRead
    });

    return results;
}

async function runMultipleIterations(): Promise<void> {
    console.log('\n--- Running 3 iterations for average performance ---');

    const iterations = 3;
    const dictTimes: number[] = [];
    const zstdTimes: number[] = [];
    const uncompressedTimes: number[] = [];

    for (let i = 0; i < iterations; i++) {
        console.log(`\nIteration ${i + 1}/${iterations}`);

        // Dictionary compressed
        const dictResult = await benchmarkRead(dictDbPath, true, true);
        dictTimes.push(dictResult.timeMs);

        // Plain zstd compressed  
        const zstdResult = await benchmarkRead(zstdDbPath, false, true);
        zstdTimes.push(zstdResult.timeMs);

        // Uncompressed
        const uncompressedResult = await benchmarkRead(uncompressedDbPath, false, false);
        uncompressedTimes.push(uncompressedResult.timeMs);
    }

    console.log('\nAverage performance (3 iterations):');
    const avgDict = dictTimes.reduce((a, b) => a + b, 0) / dictTimes.length;
    const avgZstd = zstdTimes.reduce((a, b) => a + b, 0) / zstdTimes.length;
    const avgUncompressed = uncompressedTimes.reduce((a, b) => a + b, 0) / uncompressedTimes.length;

    console.log(`Dictionary + zstd: ${avgDict.toFixed(2)}ms`);
    console.log(`Plain zstd: ${avgZstd.toFixed(2)}ms`);
    console.log(`Uncompressed: ${avgUncompressed.toFixed(2)}ms`);
}

async function main(): Promise<void> {
    try {
        console.log('FrostByte Storage Comparison Benchmark');
        console.log('=====================================\n');

        const dataFile = 'data/dict_test/txs.jsonl';
        if (!fs.existsSync(dataFile)) {
            throw new Error(`Data file not found: ${dataFile}`);
        }

        // Train dictionary
        await trainDictionary(dataFile);

        // Setup databases
        await setupDatabases();

        // Insert data
        await insertData();

        // Run initial benchmarks
        const results = await runBenchmarks();

        // Run multiple iterations for average
        await runMultipleIterations();

        // Print summary
        console.log('\n\nSummary');
        console.log('=======');
        console.log('Method              | DB Size     | Read Time | Objects | Size Ratio | Speed Ratio');
        console.log('--------------------|-------------|-----------|---------|------------|------------');

        const uncompressedResult = results.find(r => r.method === 'Uncompressed');
        if (!uncompressedResult) {
            throw new Error('Uncompressed result not found');
        }

        const baseSize = uncompressedResult.dbSize;
        const baseTime = uncompressedResult.readTimeMs;

        results.forEach(result => {
            const sizeRatio = (baseSize / result.dbSize).toFixed(2);
            const speedRatio = (result.readTimeMs / baseTime).toFixed(2);

            console.log(
                `${result.method.padEnd(19)} | ${formatBytes(result.dbSize).padEnd(11)} | ${result.readTimeMs.toString().padEnd(9)}ms | ${result.objectsRead.toString().padEnd(7)} | ${sizeRatio.padStart(8)}x | ${speedRatio.padStart(9)}x`
            );
        });

        // Best compression
        const bestCompression = results.reduce((a, b) => a.dbSize < b.dbSize ? a : b);
        console.log(`\nBest compression: ${bestCompression.method} (${formatBytes(bestCompression.dbSize)})`);

        // Fastest read
        const fastestRead = results.reduce((a, b) => a.readTimeMs < b.readTimeMs ? a : b);
        console.log(`Fastest read: ${fastestRead.method} (${fastestRead.readTimeMs}ms)`);

    } catch (error) {
        console.error('Fatal error:', error);
        process.exit(1);
    }
}

main();
