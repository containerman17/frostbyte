import { Level } from 'level'
import { rmSync, statSync, readdirSync } from 'fs'
import { join } from 'path'
import sqlite3 from 'better-sqlite3'

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
let LOOP_COUNT = 10
let SEED = 12345 // Deterministic seed

// Delete existing databases
try {
    rmSync('benchmark-db', { recursive: true, force: true })
    rmSync('benchmark.sqlite', { force: true })
    console.log('Deleted existing databases')
} catch (e) {
    // Databases don't exist, that's fine
}


// Create LevelDB
const db = new Level<Buffer, string>('benchmark-db', {
    // db: rocksdb,
    keyEncoding: 'buffer',
    valueEncoding: 'utf8'
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
        value TEXT NOT NULL
    )
`)


// Prepare SQLite statements
const insertStmt = sqlite.prepare('INSERT INTO transactions (key, value) VALUES (?, ?)')
const selectStmt = sqlite.prepare('SELECT value FROM transactions WHERE key = ?')
const insertMany = sqlite.transaction((batch: Array<{ key: Buffer, value: string }>) => {
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

// Benchmark function
async function runBenchmark() {
    console.log(`Starting benchmark with batch size: ${BATCH_SIZE}`)
    console.log(`Running for ${LOOP_COUNT} loops`)
    console.log(`Seed: ${SEED} (deterministic data generation)`)
    console.log('Key size: 10 bytes, Value: RpcBlockTransaction strings (varied sizes, 3-7 access entries)')
    console.log('Comparing LevelDB vs SQLite (WAL mode) - both storing strings (fair comparison)')
    console.log('---')

    let totalLevelWriteTime = 0
    let totalLevelReadTime = 0
    let totalSqliteWriteTime = 0
    let totalSqliteReadTime = 0

    for (let iteration = 1; iteration <= LOOP_COUNT; iteration++) {
        // Generate batch data
        const batchData = generateBatch(BATCH_SIZE)

        // Prepare LevelDB batch operations
        const levelBatchOps = batchData.map(({ key, value }) => ({
            type: 'put' as const,
            key,
            value: JSON.stringify(value)
        }))

        // Prepare SQLite batch data
        const sqliteBatchData = batchData.map(({ key, value }) => ({
            key,
            value: JSON.stringify(value)
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
        const levelReadPromises = batchData.map(({ key }) => db.get(key))
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
            Promise.resolve(selectStmt.get(key))
        )
        await Promise.all(sqliteReadPromises)
        const sqliteReadEnd = performance.now()
        const sqliteReadTime = sqliteReadEnd - sqliteReadStart
        totalSqliteReadTime += sqliteReadTime

        // Calculate rates
        const levelWriteRate = (BATCH_SIZE / levelWriteTime * 1000).toFixed(0)
        const levelReadRate = (BATCH_SIZE / levelReadTime * 1000).toFixed(0)
        const sqliteWriteRate = (BATCH_SIZE / sqliteWriteTime * 1000).toFixed(0)
        const sqliteReadRate = (BATCH_SIZE / sqliteReadTime * 1000).toFixed(0)

        // Calculate average transaction size (rough estimate)
        const sampleTx = batchData.length > 0 ? JSON.stringify(batchData[0]!.value) : '{}'
        const avgTxSize = sampleTx.length

        console.log(`Iteration ${iteration}/${LOOP_COUNT}:`)
        console.log(`  Avg TX size: ~${avgTxSize} bytes`)
        console.log(`  LevelDB Write: ${levelWriteTime.toFixed(2)}ms (${levelWriteRate} ops/sec)`)
        console.log(`  LevelDB Read:  ${levelReadTime.toFixed(2)}ms (${levelReadRate} ops/sec)`)
        console.log(`  SQLite Write:  ${sqliteWriteTime.toFixed(2)}ms (${sqliteWriteRate} ops/sec)`)
        console.log(`  SQLite Read:   ${sqliteReadTime.toFixed(2)}ms (${sqliteReadRate} ops/sec)`)
        console.log('---')
    }

    // Calculate totals and averages
    const totalOps = BATCH_SIZE * LOOP_COUNT

    const avgLevelWriteRate = (totalOps / totalLevelWriteTime * 1000).toFixed(0)
    const avgLevelReadRate = (totalOps / totalLevelReadTime * 1000).toFixed(0)
    const avgSqliteWriteRate = (totalOps / totalSqliteWriteTime * 1000).toFixed(0)
    const avgSqliteReadRate = (totalOps / totalSqliteReadTime * 1000).toFixed(0)

    console.log('BENCHMARK TOTALS:')
    console.log(`  Total operations: ${totalOps} (${BATCH_SIZE} × ${LOOP_COUNT})`)
    console.log('')
    console.log('  LEVELDB:')
    console.log(`    Write time: ${totalLevelWriteTime.toFixed(2)}ms (avg ${avgLevelWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalLevelReadTime.toFixed(2)}ms (avg ${avgLevelReadRate} ops/sec)`)
    console.log(`    Total time: ${(totalLevelWriteTime + totalLevelReadTime).toFixed(2)}ms`)
    console.log('')
    console.log('  SQLITE:')
    console.log(`    Write time: ${totalSqliteWriteTime.toFixed(2)}ms (avg ${avgSqliteWriteRate} ops/sec)`)
    console.log(`    Read time:  ${totalSqliteReadTime.toFixed(2)}ms (avg ${avgSqliteReadRate} ops/sec)`)
    console.log(`    Total time: ${(totalSqliteWriteTime + totalSqliteReadTime).toFixed(2)}ms`)
    console.log('')
    console.log('  COMPARISON:')
    console.log(`    Write speed: SQLite is ${(totalLevelWriteTime / totalSqliteWriteTime).toFixed(2)}x ${totalLevelWriteTime > totalSqliteWriteTime ? 'slower' : 'faster'} than LevelDB`)
    console.log(`    Read speed:  SQLite is ${(totalLevelReadTime / totalSqliteReadTime).toFixed(2)}x ${totalLevelReadTime > totalSqliteReadTime ? 'slower' : 'faster'} than LevelDB`)

    // Close databases before checking sizes
    await db.close()
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

        console.log('')
        console.log('  DATABASE SIZES:')
        console.log(`    SQLite (main): ${(sqliteMainSize / 1024 / 1024).toFixed(2)} MB`)
        if (sqliteWalSize > 0) {
            console.log(`    SQLite (WAL):  ${(sqliteWalSize / 1024 / 1024).toFixed(2)} MB`)
        }
        if (sqliteShmSize > 0) {
            console.log(`    SQLite (SHM):  ${(sqliteShmSize / 1024 / 1024).toFixed(2)} MB`)
        }
        console.log(`    SQLite (total): ${(sqliteTotalSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    LevelDB:        ${(levelDbSize / 1024 / 1024).toFixed(2)} MB`)
        console.log(`    Size ratio: SQLite is ${(sqliteTotalSize / levelDbSize).toFixed(2)}x ${sqliteTotalSize > levelDbSize ? 'larger' : 'smaller'} than LevelDB`)
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
