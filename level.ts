import { Level } from 'level'
import { rmSync, statSync, readdirSync } from 'fs'
import { join } from 'path'
import sqlite3 from 'better-sqlite3'
import * as zstd from 'zstd-napi'
import { open } from 'lmdb'

export interface RpcBlockTransaction {
    hash: string;
    blockHash: string;
    blockNumber: string;
    transactionIndex: string;
    from: string;
    to: string | null;
    value: string;
    gas: string;
    gasPrice: string;
    input: string;
    nonce: string;
    type: string;
    chainId: string;
    v: string;
    r: string;
    s: string;
    maxFeePerGas?: string;
    maxPriorityFeePerGas?: string;
    accessList?: RpcAccessListEntry[];
    yParity?: string;
}

export interface RpcAccessListEntry {
    address: string;
    storageKeys: string[];
}

// Configuration
let BATCH_SIZE = 10000
let LOOP_COUNT = 4
let SEED = 12345 // Deterministic seed
let TX_PER_KEY = 10 // Add this line

// Delete existing databases
try {
    rmSync('benchmark-db', { recursive: true, force: true })
    rmSync('benchmark-db-10tx', { recursive: true, force: true })
    rmSync('benchmark-lmdb', { recursive: true, force: true })
    rmSync('benchmark.sqlite', { force: true })
    console.log('Deleted existing databases')
} catch (e) {
    // Databases don't exist, that's fine
}

// Create LevelDB
const db = new Level<Buffer, Buffer>('benchmark-db', {
    keyEncoding: 'buffer',
    valueEncoding: 'buffer', // Changed to buffer for compressed data
    compression: false
})

// Create LevelDB for 10tx batching test
const db10tx = new Level<Buffer, Buffer>('benchmark-db-10tx', {
    keyEncoding: 'buffer',
    valueEncoding: 'buffer',
    compression: false
})

// Create LMDB database
const lmdb = open({
    path: 'benchmark-lmdb',
    compression: false, // We'll handle compression ourselves
    mapSize: 1024 * 1024 * 1024, // 1GB map size
    noSync: true, // Disable sync for benchmarking (like SQLite OFF mode)
    noMetaSync: true, // Disable metadata sync
    encoding: 'binary' // Use binary encoding for raw buffers
})

// Create SQLite database
const sqlite = sqlite3('benchmark.sqlite')
sqlite.pragma('journal_mode = OFF')         // No journaling, fastest
sqlite.pragma('synchronous = OFF')          // No fsync, fastest
sqlite.pragma('locking_mode = EXCLUSIVE')   // Single-process, less locking overhead
sqlite.pragma('cache_size = 100000')        // Lower cache if memory is tight
sqlite.pragma('temp_store = memory')
sqlite.exec(`
    CREATE TABLE transactions (
        key BLOB PRIMARY KEY,
        value BLOB NOT NULL
    )
`)

// Prepare SQLite statements
const insertStmt = sqlite.prepare('INSERT INTO transactions (key, value) VALUES (?, ?)')
const selectStmt = sqlite.prepare('SELECT value FROM transactions WHERE key = ?')
const insertMany = sqlite.transaction((batch: Array<{ key: Buffer, value: Buffer }>) => {
    for (const item of batch) {
        insertStmt.run(item.key, item.value)
    }
})

// Deterministic counter for unique values
let counter = SEED

// Generate deterministic hex string of exact length
function generateDeterministicHex(length: number): string {
    const hex = (counter++).toString(16).padStart(length, '0').slice(-length)
    return '0x' + hex
}

// Generate deterministic RpcAccessListEntry
function generateDeterministicAccessListEntry(): RpcAccessListEntry {
    const storageKeyCount = 2 + (counter % 3) // 2-4 storage keys deterministically
    const storageKeys = []
    for (let i = 0; i < storageKeyCount; i++) {
        storageKeys.push(generateDeterministicHex(64))
    }

    return {
        address: generateDeterministicHex(40), // Always 40 hex chars (20 bytes)
        storageKeys
    }
}

// Generate deterministic RpcBlockTransaction
function generateDeterministicTransaction(): RpcBlockTransaction {
    const accessListLength = 3 + (counter % 5) // 3-7 access list entries deterministically
    const accessList = []
    for (let i = 0; i < accessListLength; i++) {
        accessList.push(generateDeterministicAccessListEntry())
    }

    // Vary input size deterministically between 1600-2400 bytes (800-1200 hex chars)
    const inputSize = 1600 + ((counter * 7) % 801) * 2 // Even numbers for hex pairs

    const tx: RpcBlockTransaction = {
        hash: generateDeterministicHex(64),           // 32 bytes
        blockHash: generateDeterministicHex(64),      // 32 bytes
        blockNumber: generateDeterministicHex(6 + (counter % 3)), // 3-4 bytes varied
        transactionIndex: generateDeterministicHex(2 + (counter % 3) * 2), // 1-3 bytes varied
        from: generateDeterministicHex(40),           // 20 bytes
        to: generateDeterministicHex(40),             // 20 bytes (always present, no null)
        value: generateDeterministicHex(16 + ((counter % 5) * 8)), // 8-24 bytes varied
        gas: generateDeterministicHex(6 + (counter % 3) * 2),     // 3-4 bytes varied
        gasPrice: generateDeterministicHex(12 + (counter % 5) * 2), // 6-10 bytes varied
        input: generateDeterministicHex(inputSize),    // 1600-2400 bytes varied
        nonce: generateDeterministicHex(4 + (counter % 4) * 2),   // 2-5 bytes varied
        type: generateDeterministicHex(2),            // 1 byte
        chainId: '0x1',                               // Fixed
        v: generateDeterministicHex(2),               // 1 byte
        r: generateDeterministicHex(64),              // 32 bytes
        s: generateDeterministicHex(64),              // 32 bytes
        accessList,
        maxFeePerGas: generateDeterministicHex(12 + (counter % 4) * 2), // 6-9 bytes varied
        maxPriorityFeePerGas: generateDeterministicHex(8 + (counter % 3) * 4), // 4-6 bytes varied
        yParity: generateDeterministicHex(2)          // 1 byte
    }

    return tx
}

// Generate deterministic 10-byte key
function generateDeterministicKey(): Buffer {
    const keyNum = counter++
    const keyBytes = Buffer.alloc(10)
    keyBytes.writeUInt32BE(keyNum, 0) // First 4 bytes
    keyBytes.writeUInt32BE(keyNum >> 8, 4) // Next 4 bytes
    keyBytes.writeUInt16BE(keyNum & 0xFFFF, 8) // Last 2 bytes
    return keyBytes
}

// Generate batch of key-value pairs
function generateBatch(size: number): Array<{ key: Buffer, value: RpcBlockTransaction }> {
    const batch = []
    for (let i = 0; i < size; i++) {
        batch.push({
            key: generateDeterministicKey(),
            value: generateDeterministicTransaction()
        })
    }
    return batch
}

// Generate batch for 10tx test - each key stores 10 transactions
function generate10TxBatch(batchCount: number): Array<{ key: Buffer, value: RpcBlockTransaction[] }> {
    const batch = []
    for (let i = 0; i < batchCount; i++) {
        const transactions = []
        for (let j = 0; j < 10; j++) {
            transactions.push(generateDeterministicTransaction())
        }

        // Key is just the batch number as 4 bytes
        const key = Buffer.alloc(4)
        key.writeUInt32BE(i, 0)

        batch.push({
            key,
            value: transactions
        })
    }
    return batch
}

// Compression helpers
function compressValue(value: any): Buffer {
    return zstd.compress(Buffer.from(JSON.stringify(value)))
}

function decompressValue(compressed: Buffer): any {
    return JSON.parse(zstd.decompress(compressed).toString())
}

// Benchmark function
async function runBenchmark() {
    console.log(`Starting benchmark with batch size: ${BATCH_SIZE}`)
    console.log(`Running for ${LOOP_COUNT} loops`)
    console.log(`Seed: ${SEED} (deterministic data generation)`)
    console.log(`TX_PER_KEY: ${TX_PER_KEY}`)
    console.log('Key size: 10 bytes, Value: RpcBlockTransaction strings (varied sizes, 3-7 access entries)')
    console.log('Comparing LevelDB vs SQLite vs LMDB vs LevelDB-batched (all with zstd compression)')
    console.log('---')

    let totalLevelWriteTime = 0
    let totalLevelReadTime = 0
    let totalSqliteWriteTime = 0
    let totalSqliteReadTime = 0
    let totalLmdbWriteTime = 0
    let totalLmdbReadTime = 0
    let totalLevelBatchedWriteTime = 0
    let totalLevelBatchedReadTime = 0

    for (let iteration = 1; iteration <= LOOP_COUNT; iteration++) {
        // Reset counter for deterministic generation across all tests
        counter = SEED + iteration * 1000000

        // Generate batch data for 1tx per key
        const batchData = generateBatch(BATCH_SIZE)

        // Reset counter and generate batched data using same generation logic
        counter = SEED + iteration * 1000000
        const batchedData: Array<{ key: Buffer, value: RpcBlockTransaction[] }> = []
        for (let i = 0; i < BATCH_SIZE / TX_PER_KEY; i++) {
            const transactions = []
            for (let j = 0; j < TX_PER_KEY; j++) {
                transactions.push(generateDeterministicTransaction())
            }

            // Key is just the batch number as 4 bytes  
            const key = Buffer.alloc(4)
            key.writeUInt32BE((iteration - 1) * (BATCH_SIZE / TX_PER_KEY) + i, 0)

            batchedData.push({
                key,
                value: transactions
            })
        }

        // Prepare LevelDB batch operations with compression
        const levelBatchOps = batchData.map(({ key, value }) => ({
            type: 'put' as const,
            key,
            value: compressValue(value)
        }))

        // Prepare SQLite batch data with compression
        const sqliteBatchData = batchData.map(({ key, value }) => ({
            key,
            value: compressValue(value)
        }))

        // Prepare LMDB batch data with compression
        const lmdbBatchData = batchData.map(({ key, value }) => ({
            key,
            value: compressValue(value)
        }))

        // Prepare LevelDB batched operations with compression
        const levelBatchedBatchOps = batchedData.map(({ key, value }) => ({
            type: 'put' as const,
            key,
            value: compressValue(value)
        }))

        // === LEVELDB OPERATIONS ===

        // Measure LevelDB write time
        const levelWriteStart = performance.now()
        await db.batch(levelBatchOps)
        const levelWriteEnd = performance.now()
        const levelWriteTime = levelWriteEnd - levelWriteStart
        totalLevelWriteTime += levelWriteTime

        // Measure LevelDB parallel read time
        const levelReadStart = performance.now()
        const levelReadPromises = batchData.map(async ({ key }) => {
            const compressed = await db.get(key)
            return decompressValue(compressed)
        })
        await Promise.all(levelReadPromises)
        const levelReadEnd = performance.now()
        const levelReadTime = levelReadEnd - levelReadStart
        totalLevelReadTime += levelReadTime

        // === SQLITE OPERATIONS ===

        // Measure SQLite write time
        const sqliteWriteStart = performance.now()
        insertMany(sqliteBatchData)
        const sqliteWriteEnd = performance.now()
        const sqliteWriteTime = sqliteWriteEnd - sqliteWriteStart
        totalSqliteWriteTime += sqliteWriteTime

        // Measure SQLite parallel read time (simulate with Promise.all)
        const sqliteReadStart = performance.now()
        const sqliteReadPromises = batchData.map(({ key }) =>
            Promise.resolve().then(() => {
                const result = selectStmt.get(key) as { value: Buffer }
                return decompressValue(result.value)
            })
        )
        await Promise.all(sqliteReadPromises)
        const sqliteReadEnd = performance.now()
        const sqliteReadTime = sqliteReadEnd - sqliteReadStart
        totalSqliteReadTime += sqliteReadTime

        // === LMDB OPERATIONS ===

        // Measure LMDB write time
        const lmdbWriteStart = performance.now()
        lmdb.transactionSync(() => {
            for (const { key, value } of lmdbBatchData) {
                lmdb.put(key, value)
            }
        })
        const lmdbWriteEnd = performance.now()
        const lmdbWriteTime = lmdbWriteEnd - lmdbWriteStart
        totalLmdbWriteTime += lmdbWriteTime

        // Measure LMDB parallel read time
        const lmdbReadStart = performance.now()
        const lmdbReadPromises = batchData.map(async ({ key }) => {
            const compressed = lmdb.get(key) as Buffer
            return decompressValue(compressed)
        })
        await Promise.all(lmdbReadPromises)
        const lmdbReadEnd = performance.now()
        const lmdbReadTime = lmdbReadEnd - lmdbReadStart
        totalLmdbReadTime += lmdbReadTime

        // === LEVELDB BATCHED OPERATIONS ===

        // Measure LevelDB batched write time
        const levelBatchedWriteStart = performance.now()
        await db10tx.batch(levelBatchedBatchOps)
        const levelBatchedWriteEnd = performance.now()
        const levelBatchedWriteTime = levelBatchedWriteEnd - levelBatchedWriteStart
        totalLevelBatchedWriteTime += levelBatchedWriteTime

        // Measure LevelDB batched parallel read time
        const levelBatchedReadStart = performance.now()
        const levelBatchedReadPromises = batchedData.map(async ({ key }) => {
            const compressed = await db10tx.get(key)
            return decompressValue(compressed)
        })
        await Promise.all(levelBatchedReadPromises)
        const levelBatchedReadEnd = performance.now()
        const levelBatchedReadTime = levelBatchedReadEnd - levelBatchedReadStart
        totalLevelBatchedReadTime += levelBatchedReadTime

        // Calculate rates
        const levelWriteRate = (BATCH_SIZE / levelWriteTime * 1000).toFixed(0)
        const levelReadRate = (BATCH_SIZE / levelReadTime * 1000).toFixed(0)
        const sqliteWriteRate = (BATCH_SIZE / sqliteWriteTime * 1000).toFixed(0)
        const sqliteReadRate = (BATCH_SIZE / sqliteReadTime * 1000).toFixed(0)
        const lmdbWriteRate = (BATCH_SIZE / lmdbWriteTime * 1000).toFixed(0)
        const lmdbReadRate = (BATCH_SIZE / lmdbReadTime * 1000).toFixed(0)
        const levelBatchedWriteRate = (BATCH_SIZE / levelBatchedWriteTime * 1000).toFixed(0)
        const levelBatchedReadRate = (BATCH_SIZE / levelBatchedReadTime * 1000).toFixed(0)

        // Calculate average transaction size (rough estimate)
        const sampleTx = batchData.length > 0 ? JSON.stringify(batchData[0]!.value) : '{}'
        const avgTxSize = sampleTx.length
        const sampleBatchedCompressed = batchedData.length > 0 ? compressValue(batchedData[0]!.value) : Buffer.alloc(0)

        console.log(`Iteration ${iteration}/${LOOP_COUNT}:`)
        console.log(`  Avg TX size: ~${avgTxSize} bytes, ${TX_PER_KEY}tx compressed: ${sampleBatchedCompressed.length} bytes`)
        console.log(`  LevelDB Write:    ${levelWriteTime.toFixed(2)}ms (${levelWriteRate} ops/sec)`)
        console.log(`  LevelDB Read:     ${levelReadTime.toFixed(2)}ms (${levelReadRate} ops/sec)`)
        console.log(`  SQLite Write:     ${sqliteWriteTime.toFixed(2)}ms (${sqliteWriteRate} ops/sec)`)
        console.log(`  SQLite Read:      ${sqliteReadTime.toFixed(2)}ms (${sqliteReadRate} ops/sec)`)
        console.log(`  LMDB Write:       ${lmdbWriteTime.toFixed(2)}ms (${lmdbWriteRate} ops/sec)`)
        console.log(`  LMDB Read:        ${lmdbReadTime.toFixed(2)}ms (${lmdbReadRate} ops/sec)`)
        console.log(`  LevelDB-${TX_PER_KEY}tx Write: ${levelBatchedWriteTime.toFixed(2)}ms (${levelBatchedWriteRate} ops/sec)`)
        console.log(`  LevelDB-${TX_PER_KEY}tx Read:  ${levelBatchedReadTime.toFixed(2)}ms (${levelBatchedReadRate} ops/sec)`)
        console.log('---')
    }

    // Calculate totals and averages
    const totalOps = BATCH_SIZE * LOOP_COUNT

    const avgLevelWriteRate = (totalOps / totalLevelWriteTime * 1000).toFixed(0)
    const avgLevelReadRate = (totalOps / totalLevelReadTime * 1000).toFixed(0)
    const avgSqliteWriteRate = (totalOps / totalSqliteWriteTime * 1000).toFixed(0)
    const avgSqliteReadRate = (totalOps / totalSqliteReadTime * 1000).toFixed(0)
    const avgLmdbWriteRate = (totalOps / totalLmdbWriteTime * 1000).toFixed(0)
    const avgLmdbReadRate = (totalOps / totalLmdbReadTime * 1000).toFixed(0)
    const avgLevelBatchedWriteRate = (totalOps / totalLevelBatchedWriteTime * 1000).toFixed(0)
    const avgLevelBatchedReadRate = (totalOps / totalLevelBatchedReadTime * 1000).toFixed(0)

    console.log('BENCHMARK TOTALS:')
    console.log(`  Total operations: ${totalOps} (${BATCH_SIZE} × ${LOOP_COUNT})`)
    console.log('')
    console.log('  LEVELDB (1tx per key):')
    console.log(`    Write time: ${totalLevelWriteTime.toFixed(2)}ms (avg ${avgLevelWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalLevelReadTime.toFixed(2)}ms (avg ${avgLevelReadRate} ops/sec)`)
    console.log(`    Total time: ${(totalLevelWriteTime + totalLevelReadTime).toFixed(2)}ms`)
    console.log('')
    console.log('  SQLITE:')
    console.log(`    Write time: ${totalSqliteWriteTime.toFixed(2)}ms (avg ${avgSqliteWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalSqliteReadTime.toFixed(2)}ms (avg ${avgSqliteReadRate} ops/sec)`)
    console.log(`    Total time: ${(totalSqliteWriteTime + totalSqliteReadTime).toFixed(2)}ms`)
    console.log('')
    console.log('  LMDB:')
    console.log(`    Write time: ${totalLmdbWriteTime.toFixed(2)}ms (avg ${avgLmdbWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalLmdbReadTime.toFixed(2)}ms (avg ${avgLmdbReadRate} ops/sec)`)
    console.log(`    Total time: ${(totalLmdbWriteTime + totalLmdbReadTime).toFixed(2)}ms`)
    console.log('')
    console.log(`  LEVELDB (${TX_PER_KEY}tx per key):`)
    console.log(`    Write time: ${totalLevelBatchedWriteTime.toFixed(2)}ms (avg ${avgLevelBatchedWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalLevelBatchedReadTime.toFixed(2)}ms (avg ${avgLevelBatchedReadRate} ops/sec)`)

    // Calculate adjusted read time for production usage (no LRU cache)
    const adjustedBatchedReadTime = totalLevelBatchedReadTime * TX_PER_KEY
    const adjustedBatchedReadRate = (totalOps / adjustedBatchedReadTime * 1000).toFixed(0)
    console.log(`    Read time (adjusted for single-tx access): ${adjustedBatchedReadTime.toFixed(2)}ms (avg ${adjustedBatchedReadRate} ops/sec)`)

    console.log(`    Total time: ${(totalLevelBatchedWriteTime + totalLevelBatchedReadTime).toFixed(2)}ms`)
    console.log(`    Total time (adjusted): ${(totalLevelBatchedWriteTime + adjustedBatchedReadTime).toFixed(2)}ms`)
    console.log('')
    console.log('  COMPARISON:')
    console.log(`    Write speed: SQLite vs LevelDB-1tx: ${(totalLevelWriteTime / totalSqliteWriteTime).toFixed(2)}x`)
    console.log(`    Write speed: LMDB vs LevelDB-1tx: ${(totalLevelWriteTime / totalLmdbWriteTime).toFixed(2)}x`)
    console.log(`    Write speed: LevelDB-${TX_PER_KEY}tx vs LevelDB-1tx: ${(totalLevelWriteTime / totalLevelBatchedWriteTime).toFixed(2)}x`)
    console.log(`    Read speed:  SQLite vs LevelDB-1tx: ${(totalLevelReadTime / totalSqliteReadTime).toFixed(2)}x`)
    console.log(`    Read speed:  LMDB vs LevelDB-1tx: ${(totalLevelReadTime / totalLmdbReadTime).toFixed(2)}x`)
    console.log(`    Read speed:  LevelDB-${TX_PER_KEY}tx vs LevelDB-1tx: ${(totalLevelReadTime / totalLevelBatchedReadTime).toFixed(2)}x`)
    console.log(`    Read speed (adjusted): LevelDB-${TX_PER_KEY}tx vs LevelDB-1tx: ${(totalLevelReadTime / adjustedBatchedReadTime).toFixed(2)}x`)

    // Close databases before checking sizes
    await db.close()
    await db10tx.close()
    await lmdb.close()
    sqlite.close()

    // Check database sizes
    try {
        const sqliteMainSize = statSync('benchmark.sqlite').size

        // Include WAL and SHM files in SQLite size calculation
        let sqliteWalSize = 0
        let sqliteShmSize = 0
        try {
            sqliteWalSize = statSync('benchmark.sqlite-wal').size
        } catch (e) { /* WAL file might not exist */ }
        try {
            sqliteShmSize = statSync('benchmark.sqlite-shm').size
        } catch (e) { /* SHM file might not exist */ }

        const sqliteTotalSize = sqliteMainSize + sqliteWalSize + sqliteShmSize
        const levelDbSize = getDirSize('benchmark-db')
        const levelDb10txSize = getDirSize('benchmark-db-10tx')
        const lmdbSize = getDirSize('benchmark-lmdb')

        console.log('')
        console.log('  DATABASE SIZES:')
        console.log(`    SQLite (main): ${(sqliteMainSize / 1024 / 1024).toFixed(2)} MB`)
        if (sqliteWalSize > 0) {
            console.log(`    SQLite (WAL):  ${(sqliteWalSize / 1024 / 1024).toFixed(2)} MB`)
        }
        if (sqliteShmSize > 0) {
            console.log(`    SQLite (SHM):  ${(sqliteShmSize / 1024 / 1024).toFixed(2)} MB`)
        }
        console.log(`    SQLite (total):     ${(sqliteTotalSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    LevelDB (1tx):      ${(levelDbSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    LMDB:               ${(lmdbSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    LevelDB (${TX_PER_KEY}tx):     ${(levelDb10txSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    Size comparison: SQLite/LevelDB-1tx: ${(sqliteTotalSize / levelDbSize).toFixed(2)}x`)
        console.log(`    Size comparison: LMDB/LevelDB-1tx: ${(lmdbSize / levelDbSize).toFixed(2)}x`)
        console.log(`    Size comparison: LevelDB-1tx/LevelDB-${TX_PER_KEY}tx: ${(levelDbSize / levelDb10txSize).toFixed(2)}x`)

    } catch (e) {
        console.log('    Could not determine database sizes')
    }

    console.log('Benchmark completed!')
}

// Helper function to get directory size recursively
function getDirSize(dirPath: string): number {
    let totalSize = 0
    try {
        const files = readdirSync(dirPath)
        for (const file of files) {
            const filePath = join(dirPath, file)
            const stats = statSync(filePath)
            if (stats.isDirectory()) {
                totalSize += getDirSize(filePath)
            } else {
                totalSize += stats.size
            }
        }
    } catch (e) {
        // Directory doesn't exist or can't be read
    }
    return totalSize
}

// Start the benchmark
runBenchmark().catch(console.error)
