import { BlockDB } from "./blockFetcher/BlockDB"
import { LazyBlock, lazyBlockToBlock } from "./blockFetcher/lazy/LazyBlock"
import { LazyTx } from "./blockFetcher/lazy/LazyTx"
import { LazyTraces } from "./blockFetcher/lazy/LazyTrace"
import { RpcBlock } from "./blockFetcher/evmTypes"
import { pack as msgpackrPack, unpack as msgpackrUnpack, Packr, Unpackr } from 'msgpackr'
import { encode as msgpackEncode, decode as msgpackDecode } from '@msgpack/msgpack'
import * as zstd from 'zstd-napi'
import * as fs from 'fs'
import * as path from 'path'
import { tmpdir } from 'os'
import { execSync } from 'child_process'
import { Table } from 'console-table-printer'

const BLOCKS_PER_CHAIN = 10000
const TRAIN_PERCENTAGE = 0.20
const DICT_SIZES = [8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024]
const COMPRESSION_LEVEL = 3

interface EncodingMethod {
    name: string
    encode: (obj: any) => Buffer
    decode: (data: Buffer) => any
}

// Create msgpackr instances with options
const packrWithRecords = new Packr({ useRecords: true })
const unpackrWithRecords = new Unpackr({ useRecords: true })

const packrStream = new Packr({
    structuredClone: true,
    bundleStrings: true,
    useMaps: false
})
const unpackrStream = new Unpackr({
    structuredClone: true,
    bundleStrings: true,
    useMaps: false
})

const encodingMethods: EncodingMethod[] = [
    {
        name: 'JSON',
        encode: (obj) => Buffer.from(JSON.stringify(obj)),
        decode: (data) => JSON.parse(data.toString())
    },
    {
        name: 'msgpackr',
        encode: (obj) => Buffer.from(msgpackrPack(obj)),
        decode: (data) => msgpackrUnpack(data)
    },
    {
        name: 'msgpackr-records',
        encode: (obj) => Buffer.from(packrWithRecords.encode(obj)),
        decode: (data) => unpackrWithRecords.decode(data)
    },
    {
        name: 'msgpackr-stream',
        encode: (obj) => Buffer.from(packrStream.encode(obj)),
        decode: (data) => unpackrStream.decode(data)
    },
    {
        name: '@msgpack/msgpack',
        encode: (obj) => Buffer.from(msgpackEncode(obj)),
        decode: (data) => msgpackDecode(data)
    }
]

// Load blocks from all chains
async function loadAllChainBlocks(): Promise<RpcBlock[]> {
    const databaseDir = './database'
    const allBlocks: RpcBlock[] = []

    const folders = fs.readdirSync(databaseDir).filter(f => {
        const fullPath = path.join(databaseDir, f)
        return fs.statSync(fullPath).isDirectory()
    }).slice(0, 1)


    console.log(`Found ${folders.length} chain folders`)

    for (const folder of folders) {
        const dbPath = path.join(databaseDir, folder, 'blocks_no_dbg.db')

        if (!fs.existsSync(dbPath)) {
            console.log(`  Skipping ${folder} - no blocks_no_dbg.db found`)
            continue
        }

        try {
            const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: false })
            const lastBlock = db.getLastStoredBlockNumber()

            if (lastBlock < BLOCKS_PER_CHAIN) {
                console.log(`  ${folder}: Only ${lastBlock} blocks available`)
            }

            // Just get 1000 blocks in one call
            const batch = db.getBlocks(0, BLOCKS_PER_CHAIN)

            const blocks = batch.map(item => lazyBlockToBlock(item.block, item.txs))

            console.log(`  ${folder}: Loaded ${blocks.length} blocks`)
            allBlocks.push(...blocks)

            db.close()
        } catch (err) {
            console.log(`  ${folder}: Error loading blocks - ${err}`)
        }
    }

    return allBlocks
}

// Train dictionary using zstd CLI
function trainDictionary(samples: Buffer[], dictSize: number): Buffer {
    if (dictSize === 0 || samples.length === 0) {
        return Buffer.alloc(0)
    }

    const tempDir = path.join(tmpdir(), 'dict-training-' + Date.now())
    fs.mkdirSync(tempDir, { recursive: true })

    // Write samples to files
    samples.forEach((sample, i) => {
        fs.writeFileSync(path.join(tempDir, `sample_${i}`), sample)
    })

    const dictPath = path.join(tempDir, 'trained.dict')

    try {
        execSync(`zstd --train ${tempDir}/* -o ${dictPath} --maxdict=${dictSize}`, {
            stdio: 'pipe'
        })
        const dict = fs.readFileSync(dictPath)
        fs.rmSync(tempDir, { recursive: true, force: true })
        return dict
    } catch (err) {
        console.error(`Failed to train dictionary: ${err}`)
        fs.rmSync(tempDir, { recursive: true, force: true })
        return Buffer.alloc(0)
    }
}

// Calculate median of an array
function median(values: number[]): number {
    if (values.length === 0) return 0
    const sorted = [...values].sort((a, b) => a - b)
    const mid = Math.floor(sorted.length / 2)
    return sorted.length % 2 === 0
        ? (sorted[mid - 1]! + sorted[mid]!) / 2
        : sorted[mid]!
}

// Fisher-Yates shuffle
function shuffle<T>(array: T[]): T[] {
    const result = [...array]
    for (let i = result.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [result[i], result[j]] = [result[j]!, result[i]!]
    }
    return result
}

async function runBenchmark() {
    console.log('Loading blocks from all chains...')
    const allBlocks = await loadAllChainBlocks()

    if (allBlocks.length === 0) {
        console.error('No blocks loaded!')
        return
    }

    console.log(`\nTotal blocks loaded: ${allBlocks.length}`)

    // Sort blocks by hash for pseudo-random mixing
    console.log('Mixing blocks by hash...')
    allBlocks.sort((a, b) => a.hash.localeCompare(b.hash))

    // Split 20/80
    const trainCount = Math.floor(allBlocks.length * TRAIN_PERCENTAGE)
    const trainBlocks = allBlocks.slice(0, trainCount)
    const testBlocks = allBlocks.slice(trainCount)

    console.log(`Training blocks: ${trainBlocks.length}`)
    console.log(`Testing blocks: ${testBlocks.length}`)

    // Results storage
    const results: Map<string, number[]> = new Map()
    const compressionRatios: Map<string, number> = new Map()

    // Pre-train all dictionaries once
    console.log('\nPre-training dictionaries...')
    const dictionaries: Map<string, Buffer> = new Map()

    for (const encoding of encodingMethods) {
        for (const dictSize of DICT_SIZES) {
            const key = `${encoding.name}-${dictSize}`
            const trainingSamples = trainBlocks.map(block => encoding.encode(block))
            const dictionary = trainDictionary(trainingSamples, dictSize)

            if (dictionary.length === 0) {
                console.error(`Failed to train ${dictSize / 1024}KB dictionary for ${encoding.name}`)
            } else {
                dictionaries.set(key, dictionary)
                console.log(`  Trained ${dictSize / 1024}KB dictionary for ${encoding.name}`)
            }
        }
    }

    // Calculate baseline sizes for compression ratio
    console.log('\nCalculating baseline sizes...')
    let jsonUncompressedSize = 0
    for (const block of testBlocks) {
        jsonUncompressedSize += Buffer.from(JSON.stringify(block)).length
    }

    let iteration = 0

    // Main benchmark loop
    while (true) {
        iteration++
        console.log(`\n=== Iteration ${iteration} ===`)

        // Create all test configurations
        const testConfigs: Array<{
            key: string
            encoding: EncodingMethod
            dictSize: number | null
        }> = []

        for (const encoding of encodingMethods) {
            // No dictionary config
            testConfigs.push({
                key: `${encoding.name} (no dict)`,
                encoding,
                dictSize: null
            })

            // Dictionary configs
            for (const dictSize of DICT_SIZES) {
                testConfigs.push({
                    key: `${encoding.name} (${dictSize / 1024}KB dict)`,
                    encoding,
                    dictSize
                })
            }
        }

        // Randomize execution order
        const shuffledConfigs = shuffle(testConfigs)

        for (const config of shuffledConfigs) {
            if (!results.has(config.key)) results.set(config.key, [])

            if (config.dictSize === null) {
                // Test without dictionary
                const compressor = new zstd.Compressor()
                const decompressor = new zstd.Decompressor()
                compressor.setParameters({ compressionLevel: COMPRESSION_LEVEL })

                // Compress test blocks
                const compressedBlocks: Buffer[] = []
                let totalCompressedSize = 0
                for (const block of testBlocks) {
                    const encoded = config.encoding.encode(block)
                    const compressed = compressor.compress(encoded)
                    compressedBlocks.push(compressed)
                    totalCompressedSize += compressed.length
                }

                // Store compression ratio
                compressionRatios.set(config.key, jsonUncompressedSize / totalCompressedSize)

                // Measure decompress + decode time
                const start = performance.now()
                for (const compressed of compressedBlocks) {
                    const decompressed = decompressor.decompress(compressed)
                    config.encoding.decode(decompressed)
                }
                const elapsed = performance.now() - start

                results.get(config.key)!.push(elapsed)
            } else {
                // Test with dictionary
                const dictKey = `${config.encoding.name}-${config.dictSize}`
                const dictionary = dictionaries.get(dictKey)

                if (!dictionary) {
                    continue
                }

                // Create new compressor/decompressor with dictionary
                const dictCompressor = new zstd.Compressor()
                const dictDecompressor = new zstd.Decompressor()
                dictCompressor.setParameters({ compressionLevel: COMPRESSION_LEVEL })
                dictCompressor.loadDictionary(dictionary)
                dictDecompressor.loadDictionary(dictionary)

                // Compress test blocks
                const dictCompressedBlocks: Buffer[] = []
                let totalCompressedSize = 0
                for (const block of testBlocks) {
                    const encoded = config.encoding.encode(block)
                    const compressed = dictCompressor.compress(encoded)
                    dictCompressedBlocks.push(compressed)
                    totalCompressedSize += compressed.length
                }

                // Store compression ratio
                compressionRatios.set(config.key, jsonUncompressedSize / totalCompressedSize)

                // Measure decompress + decode time
                const dictStart = performance.now()
                for (const compressed of dictCompressedBlocks) {
                    const decompressed = dictDecompressor.decompress(compressed)
                    config.encoding.decode(decompressed)
                }
                const dictElapsed = performance.now() - dictStart

                results.get(config.key)!.push(dictElapsed)
            }
        }

        // Display results table with medians
        console.log('\n📊 Current Results (Median time normalized to 10,000 iterations):')

        const table = new Table({
            columns: [
                { name: 'method', title: 'Method', alignment: 'left' },
                { name: 'medianMs', title: 'Time/10k (ms)', alignment: 'right' },
                { name: 'perBlockUs', title: 'Per Block (μs)', alignment: 'right' },
                { name: 'compression', title: 'Compression', alignment: 'right' },
                { name: 'samples', title: 'Samples', alignment: 'right' }
            ]
        })

        // Sort results by median time
        const sortedResults = Array.from(results.entries())
            .map(([method, times]) => ({
                method,
                median: median(times),
                times,
                compressionRatio: compressionRatios.get(method) || 1
            }))
            .sort((a, b) => a.median - b.median)

        for (const result of sortedResults) {
            const normalizedTime = (result.median / testBlocks.length) * 10000
            table.addRow({
                method: result.method,
                medianMs: normalizedTime.toFixed(2),
                perBlockUs: ((result.median * 1000) / testBlocks.length).toFixed(1),
                compression: result.compressionRatio.toFixed(1) + 'x',
                samples: result.times.length
            })
        }

        table.printTable()

        // Wait 1 second before next iteration
        await new Promise(resolve => setTimeout(resolve, 1000))
    }
}

// Run the benchmark
runBenchmark().catch(console.error) 
