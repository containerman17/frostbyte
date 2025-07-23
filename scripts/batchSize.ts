import { Compressor } from 'zstd-napi';
import fs from 'fs';
import { promises as fsAsync } from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import os from 'os';
import readline from 'readline';

const ZSTD_COMPRESSION_LEVEL = 1;
const DICT_SIZES = [110, 256]; // KB
const SAMPLE_SIZES = [50000, 100000, 200000];
const DATA_FILE = './data/dict_test/txs.jsonl';
const OUTPUT_DIR = './data/dict_test';

interface BenchmarkResult {
    dictSizeKB: number;
    sampleSize: number;
    originalSize: number;
    compressedSize: number;
    compressionRatio: number;
    dictActualSize: number;
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

async function readAllLines(filePath: string): Promise<string[]> {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const lines: string[] = [];
    for await (const line of rl) {
        lines.push(line);
    }

    return lines;
}

function sampleLines(lines: string[], sampleSize: number): string[] {
    if (lines.length <= sampleSize) {
        return lines;
    }

    // Use systematic sampling to get even distribution
    const step = lines.length / sampleSize;
    const sampled: string[] = [];

    for (let i = 0; i < sampleSize; i++) {
        const index = Math.floor(i * step);
        sampled.push(lines[index]!);
    }

    return sampled;
}

async function trainDictionary(
    samples: string[],
    dictSizeKB: number
): Promise<{ dictPath: string; actualSize: number }> {
    const tempDir = await fsAsync.mkdtemp(path.join(os.tmpdir(), 'zstd-dict-'));
    const dictPath = path.join(OUTPUT_DIR, `dict_${dictSizeKB}kb_${samples.length}samples.dict`);

    try {
        // Use sparse sampling if we have too many items (40k limit)
        const MAX_FILES = 40000;
        const step = Math.ceil(samples.length / MAX_FILES);
        const actualSamples = step > 1 ?
            samples.filter((_, index) => index % step === 0) :
            samples;

        console.log(`    Using ${actualSamples.length} files (step: ${step}) for training...`);

        // Write samples to individual files for training
        for (let i = 0; i < actualSamples.length; i++) {
            await fsAsync.writeFile(path.join(tempDir, `sample_${i}.json`), actualSamples[i]!);
        }

        // Train dictionary
        const maxDictSize = dictSizeKB * 1000;
        execSync(`zstd --train "${tempDir}"/* -o "${dictPath}" --maxdict=${maxDictSize}`, {
            stdio: 'pipe'
        });

        // Get actual dictionary size
        const stats = await fsAsync.stat(dictPath);
        const actualSize = stats.size;

        return { dictPath, actualSize };
    } finally {
        // Cleanup temp directory
        await fsAsync.rm(tempDir, { recursive: true, force: true });
    }
}

async function compressWithDictionary(
    lines: string[],
    dictPath: string
): Promise<{ originalSize: number; compressedSize: number }> {
    const compressor = new Compressor();
    compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });

    // Load dictionary
    const dictBuffer = await fsAsync.readFile(dictPath);
    compressor.loadDictionary(dictBuffer);

    let originalSize = 0;
    let compressedSize = 0;

    // Compress each line individually (simulating line-by-line access pattern)
    for (const line of lines) {
        const lineBuffer = Buffer.from(line + '\n');
        originalSize += lineBuffer.length;

        const compressed = compressor.compress(lineBuffer);
        compressedSize += compressed.length + 4; // +4 for size header
    }

    return { originalSize, compressedSize };
}

async function runBenchmark(
    allLines: string[],
    dictSizeKB: number,
    sampleSize: number
): Promise<BenchmarkResult> {
    console.log(`  Testing dict ${dictSizeKB}KB with ${sampleSize} samples...`);

    // Sample lines for dictionary training
    const samples = sampleLines(allLines, sampleSize);

    // Train dictionary
    const { dictPath, actualSize } = await trainDictionary(samples, dictSizeKB);

    // Compress all data with the trained dictionary
    const { originalSize, compressedSize } = await compressWithDictionary(allLines, dictPath);

    const compressionRatio = originalSize / compressedSize;

    console.log(`    Dict size: ${formatBytes(actualSize)}, Compressed: ${formatBytes(compressedSize)}, Ratio: ${compressionRatio.toFixed(2)}x`);

    return {
        dictSizeKB,
        sampleSize,
        originalSize,
        compressedSize,
        compressionRatio,
        dictActualSize: actualSize
    };
}

async function main() {
    console.log('FrostByte Dictionary Size Benchmark');
    console.log('===================================\n');

    // Ensure output directory exists
    await fsAsync.mkdir(OUTPUT_DIR, { recursive: true });

    // Read all lines from the data file
    console.log(`Reading data from ${DATA_FILE}...`);
    const allLines = await readAllLines(DATA_FILE);
    console.log(`Loaded ${allLines.length} lines\n`);

    if (allLines.length === 0) {
        console.error('No data found in file!');
        process.exit(1);
    }

    // Run benchmarks for all combinations
    const results: BenchmarkResult[] = [];

    for (const dictSizeKB of DICT_SIZES) {
        console.log(`\nTesting ${dictSizeKB}KB dictionaries:`);
        console.log('='.repeat(40));

        for (const sampleSize of SAMPLE_SIZES) {
            if (sampleSize > allLines.length) {
                console.log(`  Skipping ${sampleSize} samples (only ${allLines.length} lines available)`);
                continue;
            }

            try {
                const result = await runBenchmark(allLines, dictSizeKB, sampleSize);
                results.push(result);
            } catch (error) {
                console.error(`  Error with dict ${dictSizeKB}KB, samples ${sampleSize}:`, error);
            }
        }
    }

    // Print results matrix
    console.log('\n\nResults Matrix');
    console.log('==============');
    console.log('Dict Size | Sample Size | Dict Actual | Compressed Size | Compression Ratio');
    console.log('----------|-------------|-------------|-----------------|------------------');

    for (const result of results) {
        console.log(
            `${result.dictSizeKB.toString().padStart(8)}K | ` +
            `${result.sampleSize.toString().padStart(10)} | ` +
            `${formatBytes(result.dictActualSize).padStart(10)} | ` +
            `${formatBytes(result.compressedSize).padStart(14)} | ` +
            `${result.compressionRatio.toFixed(2).padStart(16)}x`
        );
    }

    // Find best results
    console.log('\n\nBest Results');
    console.log('============');

    // Best compression ratio overall
    const bestRatio = results.reduce((a, b) => a.compressionRatio > b.compressionRatio ? a : b);
    console.log(`Best compression ratio: ${bestRatio.compressionRatio.toFixed(2)}x (${bestRatio.dictSizeKB}KB dict, ${bestRatio.sampleSize} samples)`);

    // Best by dict size
    for (const dictSize of DICT_SIZES) {
        const dictResults = results.filter(r => r.dictSizeKB === dictSize);
        if (dictResults.length > 0) {
            const best = dictResults.reduce((a, b) => a.compressionRatio > b.compressionRatio ? a : b);
            console.log(`Best for ${dictSize}KB dict: ${best.compressionRatio.toFixed(2)}x (${best.sampleSize} samples)`);
        }
    }

    // Analysis: Does smaller sample size help?
    console.log('\n\nSample Size Analysis');
    console.log('===================');

    for (const dictSize of DICT_SIZES) {
        const dictResults = results.filter(r => r.dictSizeKB === dictSize).sort((a, b) => a.sampleSize - b.sampleSize);
        if (dictResults.length > 1) {
            console.log(`\n${dictSize}KB Dictionary:`);
            let bestRatio = 0;
            let bestSampleSize = 0;

            for (const result of dictResults) {
                const indicator = result.compressionRatio > bestRatio ? ' ← BEST' : '';
                console.log(`  ${result.sampleSize.toString().padStart(6)} samples: ${result.compressionRatio.toFixed(3)}x${indicator}`);

                if (result.compressionRatio > bestRatio) {
                    bestRatio = result.compressionRatio;
                    bestSampleSize = result.sampleSize;
                }
            }

            const smallestSample = Math.min(...dictResults.map(r => r.sampleSize));
            if (bestSampleSize === smallestSample) {
                console.log(`  → Smallest sample size (${smallestSample}) is optimal!`);
            } else {
                console.log(`  → Optimal sample size is ${bestSampleSize}, not the smallest (${smallestSample})`);
            }
        }
    }

    // Save detailed results to JSON
    const resultsPath = path.join(OUTPUT_DIR, 'benchmark_results.json');
    await fsAsync.writeFile(resultsPath, JSON.stringify(results, null, 2));
    console.log(`\nDetailed results saved to: ${resultsPath}`);

    console.log('\n✅ Benchmark complete!');
}

main().catch((error) => {
    console.error('Error:', error);
    process.exit(1);
});
