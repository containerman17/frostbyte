import sqlite3 from 'better-sqlite3';
import { Compressor, Decompressor } from 'zstd-napi';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx, StoredRpcTxReceipt, CONTRACT_CREATION_TOPIC, RpcTraceCall } from './evmTypes.js';
import { StoredBlock } from './BatchRpc.js';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { execSync } from 'child_process';
const MAX_ROWS_WITH_FILTER = 10000;
const ZSTD_COMPRESSION_LEVEL = 1;

// Compression maintenance constants
const COMPRESSION_BATCH_SIZE = 100000; // Process 100k txs per maintenance call
const SAMPLE_EVERY_NTH_TX = 1; // Use every tx for dictionary training (no sampling)
const DICT_SIZE_KB = 110; // Dictionary size in KB
const CACHE_CLEAR_INTERVAL_MS = 60000; // Clear decompressor cache every minute

export class BlocksDBHelper {
    private db: sqlite3.Database;
    private isReadonly: boolean;
    private hasDebug: boolean;
    private statementCache = new Map<string, sqlite3.Statement>();
    private compressor: Compressor;
    private decompressor: Decompressor;

    // Dictionary-based decompression cache
    private dictDecompressorCache = new Map<string, Decompressor>();
    private blockDictDecompressorCache = new Map<number, Decompressor>();
    private cacheCleanupTimer: NodeJS.Timeout | null = null;

    constructor(db: sqlite3.Database, isReadonly: boolean, hasDebug: boolean) {
        this.db = db;
        this.isReadonly = isReadonly;
        this.hasDebug = hasDebug;

        // Initialize compression
        this.compressor = new Compressor();
        this.compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
        this.decompressor = new Decompressor();

        if (!isReadonly) {
            this.initSchema();
        }

        const storedHasDebug = this.getHasDebug()
        if (storedHasDebug === -1) {
            this.setHasDebug(hasDebug);
        } else if (storedHasDebug !== (hasDebug ? 1 : 0)) {
            throw new Error(`Database hasDebug mismatch: stored=${storedHasDebug}, provided=${hasDebug}`);
        }

        // Start cache cleanup timer for all instances (readers need it too)
        this.startCacheCleanupTimer();
    }

    private compressJson(data: any): Buffer {
        const jsonString = JSON.stringify(data);
        const jsonBuffer = Buffer.from(jsonString);
        return this.compressor.compress(jsonBuffer);
    }

    private decompressJson(compressedData: Buffer): any {
        try {
            const decompressedBuffer = this.decompressor.decompress(compressedData);
            const jsonString = decompressedBuffer.toString();
            return JSON.parse(jsonString);
        } catch (error) {
            console.error('[BlocksDBHelper] decompressJson error:', error);
            throw error;
        }
    }

    private decompressJsonWithDict(compressedData: Buffer, txNum: number, dictType: 'data' | 'traces' = 'data'): any {
        // Determine which batch this transaction belongs to (tx_num starts at 1)
        const batchNum = Math.floor((txNum - 1) / COMPRESSION_BATCH_SIZE);

        // Try to get a dictionary-enabled decompressor
        const dictDecompressor = this.getOrCreateDictDecompressor(batchNum, dictType);

        // Use dictionary decompressor if available, otherwise fall back to regular decompression
        const decompressor = dictDecompressor || this.decompressor;
        const decompressedBuffer = decompressor.decompress(compressedData);
        const jsonString = decompressedBuffer.toString();
        return JSON.parse(jsonString);
    }

    private decompressBlockWithDict(compressedData: Buffer, blockNumber: number): any {
        // Determine which batch this block belongs to
        const batchNum = Math.floor(blockNumber / COMPRESSION_BATCH_SIZE);

        // Try to get a dictionary-enabled decompressor for blocks
        const dictDecompressor = this.getOrCreateBlockDictDecompressor(batchNum);

        // Use dictionary decompressor if available, otherwise fall back to regular decompression
        const decompressor = dictDecompressor || this.decompressor;
        const decompressedBuffer = decompressor.decompress(compressedData);
        const jsonString = decompressedBuffer.toString();
        return JSON.parse(jsonString);
    }

    private getOrCreateBlockDictDecompressor(batchNum: number): Decompressor | null {
        // Check cache first
        let decompressor = this.blockDictDecompressorCache.get(batchNum);
        if (decompressor) {
            return decompressor;
        }

        // Load dictionary from database
        const dictRow = this.db.prepare(
            'SELECT dict FROM block_compression_dicts WHERE batch_num = ?'
        ).get(batchNum) as any;

        if (!dictRow) {
            return null;
        }

        // Create new decompressor with dictionary
        decompressor = new Decompressor();
        decompressor.loadDictionary(dictRow.dict);

        // Cache it
        this.blockDictDecompressorCache.set(batchNum, decompressor);

        return decompressor;
    }

    getEvmChainId(): number {
        return this.getIntValue('evm_chain_id', -1);
    }

    setEvmChainId(chainId: number): void {
        this.setIntValue('evm_chain_id', chainId);
    }

    getLastStoredBlockNumber(): number {
        return this.getIntValue('last_stored_block_number', -1);
    }

    setLastStoredBlockNumber(blockNumber: number): void {
        this.setIntValue('last_stored_block_number', blockNumber);
    }

    getTxCount(): number {
        return this.getIntValue('tx_count', 0);
    }

    storeBlocks(batch: StoredBlock[]): void {
        const start = performance.now();
        if (this.isReadonly) throw new Error('BlocksDBHelper is readonly');
        if (batch.length === 0) return;

        let lastStoredBlockNum = this.getLastStoredBlockNumber();
        let totalTxCount = 0;

        // Calculate total transactions in the batch
        for (const block of batch) {
            totalTxCount += block.block.transactions.length;
        }

        // Use SQLite transaction
        const storeBlocksTransaction = this.db.transaction(() => {
            for (let i = 0; i < batch.length; i++) {
                const storedBlock = batch[i]!;
                const expectedBlock = lastStoredBlockNum + 1;
                const actualBlock = Number(storedBlock.block.number);
                if (actualBlock !== expectedBlock) {
                    throw new Error(`Batch not sorted or has gaps: expected ${expectedBlock}, got ${actualBlock}. Batch size: ${batch.length}, current index: ${i}, lastStoredBlockNum: ${lastStoredBlockNum}`);
                }
                this.storeBlock(storedBlock);
                lastStoredBlockNum++;
            }

            // Update the last stored block number in the database
            this.setLastStoredBlockNumber(lastStoredBlockNum);

            // Update the transaction count once for the entire batch
            if (totalTxCount > 0) {
                const currentCount = this.getTxCount();
                this.setTxCount(currentCount + totalTxCount);
            }
        });

        storeBlocksTransaction();

        const elapsed = performance.now() - start;
        if (elapsed > 100) {
            console.log(`SQLite storeBlocks took ${elapsed}ms`);
        }
    }

    setBlockchainLatestBlockNum(blockNumber: number): void {
        if (this.isReadonly) throw new Error('BlocksDBHelper is readonly');
        this.setIntValue('blockchain_latest_block', blockNumber);
    }

    getBlockchainLatestBlockNum(): number {
        return this.getIntValue('blockchain_latest_block', -1);
    }

    public performCompressionMaintenance(): void {
        if (this.isReadonly) {
            console.log('Skipping compression maintenance in readonly mode');
            return;
        }

        const totalTxCount = this.getTxCount();
        const completeBatches = Math.floor(totalTxCount / COMPRESSION_BATCH_SIZE);

        if (completeBatches === 0) {
            console.log(`Not enough transactions for compression maintenance. Need ${COMPRESSION_BATCH_SIZE}, have ${totalTxCount}`);
            return;
        }

        const lastCompressedBatchNum = this.getIntValue('last_compressed_batch_num', -1);
        const nextBatchNum = lastCompressedBatchNum + 1;

        if (nextBatchNum >= completeBatches) {
            console.log('All complete batches are already compressed');
            return;
        }

        // tx_num starts at 1, so batch 0 covers tx_num 1-100000, batch 1 covers 100001-200000, etc.
        const batchStartNum = nextBatchNum * COMPRESSION_BATCH_SIZE + 1;
        const batchEndNum = batchStartNum + COMPRESSION_BATCH_SIZE - 1;

        console.log(`Starting compression maintenance for batch ${nextBatchNum} (tx_num ${batchStartNum} to ${batchEndNum})`);

        try {
            // Create temp directory for samples
            const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'frostbyte-dict-'));

            try {
                const start = performance.now();
                // Get the batch of transactions to recompress
                const stmt = this.db.prepare(
                    'SELECT tx_num, data FROM txs WHERE tx_num >= ? AND tx_num <= ? ORDER BY tx_num ASC'
                );
                const rows = stmt.all(batchStartNum, batchEndNum) as any[];

                if (rows.length === 0) {
                    throw new Error(`No transactions found in range ${batchStartNum} to ${batchEndNum}`);
                }

                console.log(`Processing ${rows.length} transactions (expected up to ${COMPRESSION_BATCH_SIZE})`);

                // Sample every Nth transaction for dictionary training
                let sampleCount = 0;
                for (let i = 0; i < rows.length; i += SAMPLE_EVERY_NTH_TX) {
                    const row = rows[i]!;
                    const txData = this.decompressJson(row.data);
                    const jsonStr = JSON.stringify(txData);
                    fs.writeFileSync(path.join(tempDir, `sample_data_${sampleCount}.json`), jsonStr);
                    sampleCount++;
                }

                console.log(`Wrote ${sampleCount} data samples for dictionary training`);

                // Train dictionary for transaction data
                const dataDictPath = path.join(tempDir, 'data_dict.zstd');
                const maxDictSize = DICT_SIZE_KB * 1024;

                // Create a file list to avoid "argument list too long" error
                const dataFileListPath = path.join(tempDir, 'data_files.txt');
                const dataFiles = fs.readdirSync(tempDir)
                    .filter(f => f.startsWith('sample_data_') && f.endsWith('.json'))
                    .map(f => path.join(tempDir, f));
                fs.writeFileSync(dataFileListPath, dataFiles.join('\n'));

                execSync(
                    `zstd --train -o "${dataDictPath}" --maxdict=${maxDictSize} --dictID=${nextBatchNum} --filelist="${dataFileListPath}"`,
                    { stdio: 'pipe' }
                );

                // Read the trained data dictionary
                const dataDictBuffer = fs.readFileSync(dataDictPath);
                console.log(`Trained data dictionary size: ${dataDictBuffer.length} bytes`);

                // If hasDebug, also train dictionary for traces
                let tracesDictBuffer: Buffer | null = null;
                if (this.hasDebug) {
                    // Get traces for sampled transactions
                    let traceSampleCount = 0;
                    const traceStmt = this.db.prepare(
                        'SELECT traces FROM txs WHERE tx_num >= ? AND tx_num <= ? AND traces IS NOT NULL ORDER BY tx_num ASC'
                    );
                    const traceRows = traceStmt.all(batchStartNum, batchEndNum) as any[];

                    for (let i = 0; i < traceRows.length; i += SAMPLE_EVERY_NTH_TX) {
                        const row = traceRows[i];
                        if (row && row.traces) {
                            const traceData = this.decompressJson(row.traces);
                            const jsonStr = JSON.stringify(traceData);
                            fs.writeFileSync(path.join(tempDir, `sample_traces_${traceSampleCount}.json`), jsonStr);
                            traceSampleCount++;
                        }
                    }

                    if (traceSampleCount > 0) {
                        console.log(`Wrote ${traceSampleCount} trace samples for dictionary training`);

                        // Train dictionary for traces
                        const tracesDictPath = path.join(tempDir, 'traces_dict.zstd');

                        // Create a file list to avoid "argument list too long" error
                        const tracesFileListPath = path.join(tempDir, 'traces_files.txt');
                        const tracesFiles = fs.readdirSync(tempDir)
                            .filter(f => f.startsWith('sample_traces_') && f.endsWith('.json'))
                            .map(f => path.join(tempDir, f));
                        fs.writeFileSync(tracesFileListPath, tracesFiles.join('\n'));

                        execSync(
                            `zstd --train -o "${tracesDictPath}" --maxdict=${maxDictSize} --dictID=${nextBatchNum} --filelist="${tracesFileListPath}"`,
                            { stdio: 'pipe' }
                        );

                        tracesDictBuffer = fs.readFileSync(tracesDictPath);
                        console.log(`Trained traces dictionary size: ${tracesDictBuffer.length} bytes`);
                    }
                }

                // Recompress all transactions in the batch with the new dictionaries
                const dataCompressor = new Compressor();
                dataCompressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
                dataCompressor.loadDictionary(dataDictBuffer);

                const tracesCompressor = tracesDictBuffer ? new Compressor() : null;
                if (tracesCompressor && tracesDictBuffer) {
                    tracesCompressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
                    tracesCompressor.loadDictionary(tracesDictBuffer);
                }

                const recompressedData: Array<{ tx_num: number; data: Buffer; traces?: Buffer }> = [];

                // Re-fetch all rows with traces if needed
                const fullStmt = this.hasDebug
                    ? this.db.prepare('SELECT tx_num, data, traces FROM txs WHERE tx_num >= ? AND tx_num <= ? ORDER BY tx_num ASC')
                    : this.db.prepare('SELECT tx_num, data FROM txs WHERE tx_num >= ? AND tx_num <= ? ORDER BY tx_num ASC');
                const fullRows = fullStmt.all(batchStartNum, batchEndNum) as any[];

                for (const row of fullRows) {
                    const txData = this.decompressJson(row.data);
                    const recompressed = dataCompressor.compress(Buffer.from(JSON.stringify(txData)));

                    const item: { tx_num: number; data: Buffer; traces?: Buffer } = {
                        tx_num: row.tx_num,
                        data: recompressed
                    };

                    if (this.hasDebug && row.traces && tracesCompressor) {
                        const traceData = this.decompressJson(row.traces);
                        item.traces = tracesCompressor.compress(Buffer.from(JSON.stringify(traceData)));
                    }

                    recompressedData.push(item);
                }

                // Start transaction to update everything atomically
                const updateTransaction = this.db.transaction(() => {
                    // Update all transaction data
                    const updateStmt = this.hasDebug
                        ? this.db.prepare('UPDATE txs SET data = ?, traces = ? WHERE tx_num = ?')
                        : this.db.prepare('UPDATE txs SET data = ? WHERE tx_num = ?');

                    for (const item of recompressedData) {
                        if (this.hasDebug && item.traces) {
                            updateStmt.run(item.data, item.traces, item.tx_num);
                        } else {
                            updateStmt.run(item.data, item.tx_num);
                        }
                    }

                    // Store the dictionaries
                    const insertDictStmt = this.db.prepare(
                        'INSERT INTO tx_compression_dicts (batch_num, dict_type, dict) VALUES (?, ?, ?)'
                    );
                    insertDictStmt.run(nextBatchNum, 'data', dataDictBuffer);

                    if (tracesDictBuffer) {
                        insertDictStmt.run(nextBatchNum, 'traces', tracesDictBuffer);
                    }

                    // Update progress
                    this.setIntValue('last_compressed_batch_num', nextBatchNum);
                });

                updateTransaction();

                console.log(`Successfully compressed batch ${nextBatchNum} with dictionary in ${Math.round(((performance.now() - start) / 1000))}s`);
            } finally {
                // Clean up temp directory
                fs.rmSync(tempDir, { recursive: true, force: true });
            }
        } catch (error) {
            console.error('Error during compression maintenance:', error);
            throw error;
        }
    }

    public performBlockCompressionMaintenance(): void {
        if (this.isReadonly) {
            console.log('Skipping block compression maintenance in readonly mode');
            return;
        }

        const lastStoredBlockNum = this.getLastStoredBlockNumber();
        if (lastStoredBlockNum < 0) {
            console.log('No blocks stored yet');
            return;
        }

        const totalBlocks = lastStoredBlockNum + 1;
        const completeBatches = Math.floor(totalBlocks / COMPRESSION_BATCH_SIZE);

        if (completeBatches === 0) {
            console.log(`Not enough blocks for compression maintenance. Need ${COMPRESSION_BATCH_SIZE}, have ${totalBlocks}`);
            return;
        }

        const lastCompressedBatchNum = this.getIntValue('last_compressed_batch_num', -1);
        const nextBatchNum = lastCompressedBatchNum + 1;

        if (nextBatchNum >= completeBatches) {
            console.log('All complete block batches are already compressed');
            return;
        }

        const batchStartNum = nextBatchNum * COMPRESSION_BATCH_SIZE;
        const batchEndNum = Math.min(batchStartNum + COMPRESSION_BATCH_SIZE - 1, lastStoredBlockNum);

        console.log(`Starting block compression maintenance for batch ${nextBatchNum} (block ${batchStartNum} to ${batchEndNum})`);

        try {
            // Create temp directory for samples
            const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'frostbyte-block-dict-'));

            try {
                const start = performance.now();
                // Get the batch of blocks to recompress
                const stmt = this.db.prepare(
                    'SELECT number, data FROM blocks WHERE number >= ? AND number <= ? ORDER BY number ASC'
                );
                const rows = stmt.all(batchStartNum, batchEndNum) as any[];

                console.log(`Processing ${rows.length} blocks`);

                // Sample every Nth block for dictionary training
                let sampleCount = 0;
                for (let i = 0; i < rows.length; i += SAMPLE_EVERY_NTH_TX) {
                    const row = rows[i]!;
                    const blockData = this.decompressJson(row.data);
                    const jsonStr = JSON.stringify(blockData);
                    fs.writeFileSync(path.join(tempDir, `sample_block_${sampleCount}.json`), jsonStr);
                    sampleCount++;
                }

                console.log(`Wrote ${sampleCount} block samples for dictionary training`);

                // Train dictionary for block data
                const dictPath = path.join(tempDir, 'block_dict.zstd');
                const maxDictSize = DICT_SIZE_KB * 1024;

                // Create a file list to avoid "argument list too long" error
                const fileListPath = path.join(tempDir, 'block_files.txt');
                const blockFiles = fs.readdirSync(tempDir)
                    .filter(f => f.startsWith('sample_block_') && f.endsWith('.json'))
                    .map(f => path.join(tempDir, f));
                fs.writeFileSync(fileListPath, blockFiles.join('\n'));

                execSync(
                    `zstd --train -o "${dictPath}" --maxdict=${maxDictSize} --dictID=${nextBatchNum} --filelist="${fileListPath}"`,
                    { stdio: 'pipe' }
                );

                // Read the trained dictionary
                const dictBuffer = fs.readFileSync(dictPath);
                console.log(`Trained block dictionary size: ${dictBuffer.length} bytes`);

                // Recompress all blocks in the batch with the new dictionary
                const compressor = new Compressor();
                compressor.setParameters({ compressionLevel: ZSTD_COMPRESSION_LEVEL });
                compressor.loadDictionary(dictBuffer);

                const recompressedData: Array<{ number: number; data: Buffer }> = [];

                for (const row of rows) {
                    const blockData = this.decompressJson(row.data);
                    const recompressed = compressor.compress(Buffer.from(JSON.stringify(blockData)));
                    recompressedData.push({
                        number: row.number,
                        data: recompressed
                    });
                }

                // Start transaction to update everything atomically
                const updateTransaction = this.db.transaction(() => {
                    // Update all block data
                    const updateStmt = this.db.prepare('UPDATE blocks SET data = ? WHERE number = ?');
                    for (const item of recompressedData) {
                        updateStmt.run(item.data, item.number);
                    }

                    // Store the dictionary
                    const insertDictStmt = this.db.prepare(
                        'INSERT INTO block_compression_dicts (batch_num, dict) VALUES (?, ?)'
                    );
                    insertDictStmt.run(nextBatchNum, dictBuffer);

                    // Update progress
                    this.setIntValue('last_compressed_batch_num', nextBatchNum);
                });

                updateTransaction();

                const elapsed = performance.now() - start;
                console.log(`Successfully compressed block batch ${nextBatchNum} with dictionary in ${Math.round(((elapsed) / 1000))}s`);
            } finally {
                // Clean up temp directory
                fs.rmSync(tempDir, { recursive: true, force: true });
            }
        } catch (error) {
            console.error('Error during block compression maintenance:', error);
            throw error;
        }
    }

    private startCacheCleanupTimer(): void {
        if (this.cacheCleanupTimer) {
            clearInterval(this.cacheCleanupTimer);
        }
        this.cacheCleanupTimer = setInterval(() => {
            this.dictDecompressorCache.clear();
            this.blockDictDecompressorCache.clear();
        }, CACHE_CLEAR_INTERVAL_MS);
    }

    private getOrCreateDictDecompressor(batchNum: number, dictType: 'data' | 'traces'): Decompressor | null {
        // Check cache first - use composite key
        const cacheKey = `${batchNum}_${dictType}`;
        let decompressor = this.dictDecompressorCache.get(cacheKey);
        if (decompressor) {
            return decompressor;
        }

        // Load dictionary from database
        const dictRow = this.db.prepare(
            'SELECT dict FROM tx_compression_dicts WHERE batch_num = ? AND dict_type = ?'
        ).get(batchNum, dictType) as any;

        if (!dictRow) {
            return null;
        }

        // Create new decompressor with dictionary
        decompressor = new Decompressor();
        decompressor.loadDictionary(dictRow.dict);

        // Cache it with composite key
        this.dictDecompressorCache.set(cacheKey, decompressor);

        return decompressor;
    }

    close(): void {
        // Clear cached statements (better-sqlite3 handles cleanup automatically)
        this.statementCache.clear();

        // Clear cache cleanup timer
        if (this.cacheCleanupTimer) {
            clearInterval(this.cacheCleanupTimer);
            this.cacheCleanupTimer = null;
        }

        this.db.close();
    }

    private storeBlock(storedBlock: StoredBlock): void {
        // Validate hasDebug flag vs traces presence
        if (this.hasDebug && storedBlock.traces === undefined) {
            throw new Error('hasDebug is true but StoredBlock.traces is undefined');
        }
        if (!this.hasDebug && storedBlock.traces !== undefined) {
            throw new Error('hasDebug is false but StoredBlock.traces is defined');
        }

        const blockNumber = Number(storedBlock.block.number);

        // Create a block without transactions for storage
        const blockWithoutTxs: Omit<RpcBlock, 'transactions' | 'logsBloom'> = {
            ...storedBlock.block,
        };
        delete (blockWithoutTxs as any).transactions;
        delete (blockWithoutTxs as any).logsBloom;

        // Store block data as compressed JSON
        const blockData = this.compressJson(blockWithoutTxs);

        // Extract block hash (remove '0x' prefix) and take first 5 bytes
        const blockHash = storedBlock.block.hash.slice(2);
        const blockHashPrefix = Buffer.from(blockHash, 'hex').slice(0, 5);

        const insertBlock = this.db.prepare(
            'INSERT INTO blocks (number, hash, data) VALUES (?, ?, ?)'
        );
        insertBlock.run(blockNumber, blockHashPrefix, blockData);

        // Prepare statements for transaction operations
        const insertTx = this.db.prepare(
            'INSERT INTO txs (hash, block_num, data, traces) VALUES (?, ?, ?, ?)'
        );
        const insertTopic = this.db.prepare(
            'INSERT INTO tx_topics (tx_num, topic_hash) VALUES (?, ?)'
        );

        // Store transactions with their receipts and traces
        for (let i = 0; i < storedBlock.block.transactions.length; ++i) {
            const tx = storedBlock.block.transactions[i]!;
            const receipt = storedBlock.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Prepare transaction data
            const receiptWithoutBloom: StoredRpcTxReceipt = { ...receipt };
            delete (receiptWithoutBloom as any).logsBloom;

            const txData = {
                tx: tx,
                receipt: receiptWithoutBloom,
                blockTs: Number(storedBlock.block.timestamp)
            };

            const txDataCompressed = this.compressJson(txData);
            const txHashPrefix = Buffer.from(tx.hash.slice(2), 'hex').slice(0, 5);

            // Find the corresponding trace for this transaction
            let traceDataCompressed: Buffer | null = null;
            if (this.hasDebug && storedBlock.traces) {
                const traces = storedBlock.traces.filter(t => t.txHash === tx.hash);
                if (traces.length === 0) {
                    throw new Error(`hasDebug is true but no trace found for tx ${tx.hash}`);
                }
                if (traces.length > 1) {
                    throw new Error(`Multiple traces found for tx ${tx.hash}: ${traces.length} traces`);
                }
                const trace = traces[0]!;
                traceDataCompressed = this.compressJson(trace);
            }

            const insertResult = insertTx.run(txHashPrefix, blockNumber, txDataCompressed, traceDataCompressed);
            const txNum = insertResult.lastInsertRowid as number;

            // Check if this transaction created a contract
            let isContractCreation = false;

            // Method 1: Check if tx.to is null (direct contract creation)
            if (!tx.to) {
                isContractCreation = true;
            }

            // Method 2: Check traces for internal contract creations (if hasDebug is enabled)
            if (!isContractCreation && this.hasDebug && storedBlock.traces) {
                const trace = storedBlock.traces.find(t => t.txHash === tx.hash);
                if (trace) {
                    // Recursively check for CREATE operations in the trace
                    const hasCreate = this.hasCreateOperation(trace.result);
                    if (hasCreate) {
                        isContractCreation = true;
                    }
                }
            }

            // If contract creation detected, add the special topic
            if (isContractCreation) {
                const contractCreationTopicPrefix = Buffer.from(CONTRACT_CREATION_TOPIC.slice(2), 'hex').slice(0, 5);
                insertTopic.run(txNum, contractCreationTopicPrefix);
            }

            // Index event topics
            const processedTopics = new Set<string>();
            for (const log of receipt.logs) {
                if (log.topics.length > 0 && log.topics[0]) {
                    const topic = log.topics[0];

                    // Only process each unique topic once per transaction
                    if (processedTopics.has(topic)) continue;
                    processedTopics.add(topic);

                    const topicHashPrefix = Buffer.from(topic.slice(2), 'hex').slice(0, 5);

                    // Insert tx_topics entry
                    insertTopic.run(txNum, topicHashPrefix);
                }
            }
        }
    }

    private setTxCount(count: number): void {
        this.setIntValue('tx_count', count);
    }

    private initSchema(): void {
        // SQLite doesn't have ENGINE=ROCKSDB, using standard SQLite tables
        const queries = [
            `CREATE TABLE IF NOT EXISTS blocks (
                number INTEGER PRIMARY KEY,
                hash BLOB NOT NULL,
                data BLOB NOT NULL
            )`,
            `CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash)`,

            `CREATE TABLE IF NOT EXISTS txs (
                tx_num INTEGER PRIMARY KEY AUTOINCREMENT,
                hash BLOB NOT NULL,
                block_num INTEGER NOT NULL,
                data BLOB NOT NULL,
                traces BLOB
            )`,
            `CREATE INDEX IF NOT EXISTS idx_txs_hash ON txs(hash)`,
            `CREATE INDEX IF NOT EXISTS idx_txs_block_num ON txs(block_num)`,

            `CREATE TABLE IF NOT EXISTS kv_int (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )`,

            `CREATE TABLE IF NOT EXISTS kv_string (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )`,

            `CREATE TABLE IF NOT EXISTS tx_topics (
                tx_num INTEGER NOT NULL,
                topic_hash BLOB NOT NULL
            )`,
            `CREATE INDEX IF NOT EXISTS idx_tx_topics_topic_hash_txnum ON tx_topics(topic_hash, tx_num)`,

            `CREATE TABLE IF NOT EXISTS tx_compression_dicts (
                batch_num INTEGER NOT NULL,
                dict_type TEXT NOT NULL, -- 'data' or 'traces'
                dict BLOB NOT NULL,
                PRIMARY KEY (batch_num, dict_type)
            )`,
            `CREATE INDEX IF NOT EXISTS idx_tx_compression_dicts_batch_num ON tx_compression_dicts(batch_num)`,

            `CREATE TABLE IF NOT EXISTS block_compression_dicts (
                batch_num INTEGER PRIMARY KEY,
                dict BLOB NOT NULL
            )`
        ];

        for (const query of queries) {
            this.db.exec(query);
        }
    }

    getHasDebug(): number {
        return this.getIntValue('hasDebug', -1);
    }

    setHasDebug(hasDebug: boolean): void {
        this.setIntValue('hasDebug', hasDebug ? 1 : 0);
    }

    getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean, filterEvents: string[] | undefined): { txs: StoredTx[], traces: RpcTraceResult[] | undefined } {
        // Ensure safe values to prevent SQL injection
        const txNumParam = Math.max(-1, greaterThanTxNum);
        let limitParam = Math.min(Math.max(1, limit), 100000);

        // When filtering events, hard cap to 1000 to avoid huge IN clauses
        if (filterEvents && filterEvents.length > 0) {
            limitParam = Math.min(limitParam, MAX_ROWS_WITH_FILTER);
        }

        if (filterEvents && filterEvents.length > 0) {
            // Use the index for efficient filtering
            const topicHashPrefixes = filterEvents.map(topic => Buffer.from(topic.slice(2), 'hex').slice(0, 5));

            const placeholders = topicHashPrefixes.map(() => '?').join(',');

            // First query: Get just the tx_nums using the indexed table (fast)
            const txNumQuery = `SELECT DISTINCT tt.tx_num
                               FROM tx_topics tt
                               WHERE tt.topic_hash IN (${placeholders})
                               AND tt.tx_num > ?
                               ORDER BY tt.tx_num ASC
                               LIMIT ?`;

            const startTime = performance.now();
            const txNumStmt = this.db.prepare(txNumQuery);
            const txNumRows = txNumStmt.all(...topicHashPrefixes, txNumParam, limitParam) as any[];
            const elapsed = performance.now() - startTime;
            if (elapsed > 100) {
                console.log(`SQLite tx_num query took ${elapsed}ms`);
            }

            if (txNumRows.length === 0) {
                return {
                    txs: [],
                    traces: undefined
                };
            }

            // Extract tx_nums
            const txNums = txNumRows.map(row => row.tx_num);

            // Second query: Fetch the actual data for these tx_nums
            const txNumPlaceholders = txNums.map(() => '?').join(',');
            const dataQuery = includeTraces && this.hasDebug
                ? `SELECT tx_num, data, traces FROM txs WHERE tx_num IN (${txNumPlaceholders}) ORDER BY tx_num ASC`
                : `SELECT tx_num, data FROM txs WHERE tx_num IN (${txNumPlaceholders}) ORDER BY tx_num ASC`;

            const dataStartTime = performance.now();
            const dataStmt = this.db.prepare(dataQuery);
            const dataRows = dataStmt.all(...txNums) as any[];
            const dataElapsed = performance.now() - dataStartTime;
            if (dataElapsed > 100) {
                console.log(`SQLite data query took ${dataElapsed}ms`);
            }

            const txs: StoredTx[] = [];
            const traces: RpcTraceResult[] = [];

            for (const row of dataRows) {
                const txData = this.decompressJsonWithDict(row.data, row.tx_num);
                const storedTx: StoredTx = {
                    txNum: row.tx_num,
                    ...txData
                };
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row.traces) {
                    const trace = this.decompressJsonWithDict(row.traces, row.tx_num, 'traces') as RpcTraceResult;
                    traces.push(trace);
                }
            }

            return {
                txs,
                traces: this.hasDebug && includeTraces ? traces : undefined
            };
        } else {
            // No filtering, use original query
            const query = includeTraces && this.hasDebug
                ? `SELECT tx_num, data, traces FROM txs WHERE tx_num > ? ORDER BY tx_num ASC LIMIT ?`
                : `SELECT tx_num, data FROM txs WHERE tx_num > ? ORDER BY tx_num ASC LIMIT ?`;

            const startTime = performance.now();
            const stmt = this.db.prepare(query);
            const rows = stmt.all(txNumParam, limitParam) as any[];
            const elapsed = performance.now() - startTime;
            if (elapsed > 100) {
                console.log(`SQLite query took ${elapsed}ms`);
            }

            const txs: StoredTx[] = [];
            const traces: RpcTraceResult[] = [];

            for (const row of rows) {
                const txData = this.decompressJsonWithDict(row.data, row.tx_num);
                const storedTx: StoredTx = {
                    txNum: row.tx_num,
                    ...txData
                };
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row.traces) {
                    const trace = this.decompressJsonWithDict(row.traces, row.tx_num, 'traces') as RpcTraceResult;
                    traces.push(trace);
                }
            }

            return {
                txs,
                traces: this.hasDebug && includeTraces ? traces : undefined
            };
        }
    }

    slow_getBlockWithTransactions(blockIdentifier: number | string): RpcBlock | null {
        let blockNumber: number = -1;
        let blockRow: any;

        if (typeof blockIdentifier === 'number') {
            // Query by block number
            blockNumber = blockIdentifier;
            const stmt = this.db.prepare('SELECT number, hash, data FROM blocks WHERE number = ?');
            blockRow = stmt.get(blockNumber);
        } else {
            // Query by block hash
            const hashStr = blockIdentifier.startsWith('0x') ? blockIdentifier.slice(2) : blockIdentifier;
            if (hashStr.length !== 64) {
                throw new Error(`Invalid block hash length: expected 64 hex chars, got ${hashStr.length}`);
            }
            const hashPrefix = Buffer.from(hashStr, 'hex').slice(0, 5);

            const stmt = this.db.prepare('SELECT number, hash, data FROM blocks WHERE hash = ?');
            const rows = stmt.all(hashPrefix) as any[];

            // Handle potential collisions by checking the full hash in the data
            for (const row of rows) {
                const storedData = this.decompressBlockWithDict(row.data, row.number) as Omit<RpcBlock, 'transactions'>;
                if (storedData.hash.toLowerCase() === ('0x' + hashStr).toLowerCase()) {
                    blockRow = row;
                    blockNumber = row.number;
                    break;
                }
            }

            if (!blockRow) {
                return null;
            }
        }

        if (!blockRow) {
            return null;
        }

        // Parse the block data (without transactions) using dictionary decompression
        const storedBlock = this.decompressBlockWithDict(blockRow.data, blockNumber) as Omit<RpcBlock, 'transactions'>;

        // Fetch all transactions for this block ordered by tx_num
        const txStmt = this.db.prepare('SELECT tx_num, data FROM txs WHERE block_num = ? ORDER BY tx_num ASC');
        const txRows = txStmt.all(blockNumber) as any[];

        // Reconstruct the transactions array
        const transactions: RpcBlockTransaction[] = [];

        for (const txRow of txRows) {
            // Use dictionary-aware decompression with tx_num
            const txData = this.decompressJsonWithDict(txRow.data, txRow.tx_num);
            transactions.push(txData.tx);
        }

        // Reassemble the complete RpcBlock
        const fullBlock: RpcBlock = {
            ...storedBlock,
            transactions
        };

        return fullBlock;
    }

    getTxReceipt(txHash: string): StoredRpcTxReceipt | null {
        const hashStr = txHash.replace(/^0x/, '');
        const hashPrefix = Buffer.from(hashStr, 'hex').slice(0, 5);
        const stmt = this.db.prepare('SELECT tx_num, data FROM txs WHERE hash = ?');
        const rows = stmt.all(hashPrefix) as any[];

        // Handle potential collisions by checking the full hash in the data
        for (const row of rows) {
            // Use dictionary-aware decompression with tx_num
            const txData = this.decompressJsonWithDict(row.data, row.tx_num);
            if (txData.tx.hash.toLowerCase() === ('0x' + hashStr).toLowerCase()) {
                return txData.receipt;
            }
        }

        return null;
    }

    slow_getBlockTraces(blockNumber: number): RpcTraceResult[] {
        const stmt = this.db.prepare('SELECT tx_num, traces FROM txs WHERE block_num = ? ORDER BY tx_num ASC');
        const rows = stmt.all(blockNumber) as any[];

        const traces: RpcTraceResult[] = [];
        for (const row of rows) {
            if (!row.traces) continue;
            // Use dictionary-aware decompression with tx_num for traces
            const trace = this.decompressJsonWithDict(row.traces, row.tx_num, 'traces') as RpcTraceResult;
            traces.push(trace);
        }
        return traces;
    }

    /**
     * Get direct access to the underlying database for custom queries.
     * USE WITH CAUTION: This bypasses all abstractions and safety checks.
     */
    getDatabase(): sqlite3.Database {
        return this.db;
    }

    /**
     * Cache and return a prepared statement. Automatically caches statements by SQL string.
     * Usage: this.cacheStatement('SELECT * FROM txs WHERE tx_num > ?').all(param)
     */
    private cacheStatement(sql: string): sqlite3.Statement {
        let stmt = this.statementCache.get(sql);
        if (!stmt) {
            stmt = this.db.prepare(sql);
            this.statementCache.set(sql, stmt);
        }
        return stmt;
    }

    /**
     * Recursively checks if a trace contains any CREATE operations
     */
    private hasCreateOperation(trace: RpcTraceCall): boolean {
        // Check if this trace is a CREATE operation
        if (trace.type === 'CREATE' || trace.type === 'CREATE2' || trace.type === 'CREATE3') {
            return true;
        }

        // Recursively check child calls
        if (trace.calls && Array.isArray(trace.calls)) {
            for (const call of trace.calls) {
                if (this.hasCreateOperation(call)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Get a value from kv_int table with a default fallback
     */
    private getIntValue(key: string, defaultValue: number): number {
        for (let attempt = 1; attempt <= 10; attempt++) {
            try {
                const row = this.cacheStatement('SELECT value FROM kv_int WHERE key = ?').get(key) as any;
                return row?.value ?? defaultValue;
            } catch (error: any) {
                if (error.code === 'SQLITE_ERROR' && error.message?.includes("no such table")) {
                    console.log(`Table kv_int doesn't exist, attempt ${attempt}/10. Waiting 1 second...`);
                    if (attempt === 10) {
                        console.error('Table kv_int still doesn\'t exist after 10 attempts. Exiting.');
                        process.exit(1);
                    }
                    // Sleep synchronously (blocking)
                    const start = Date.now();
                    while (Date.now() - start < 1000) {
                        // busy wait
                    }
                    continue;
                }
                throw error;
            }
        }
        // This should never be reached
        throw new Error('Unexpected end of retry loop');
    }

    /**
     * Set a value in kv_int table
     */
    private setIntValue(key: string, value: number): void {
        this.cacheStatement(
            'INSERT INTO kv_int (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
        ).run(key, value);
    }
}
