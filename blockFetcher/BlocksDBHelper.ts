import sqlite3 from 'better-sqlite3';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx, StoredRpcTxReceipt, CONTRACT_CREATION_TOPIC, RpcTraceCall } from './evmTypes.js';
import { StoredBlock } from './BatchRpc.js';
const MAX_ROWS_WITH_FILTER = 1000;
export class BlocksDBHelper {
    private db: sqlite3.Database;
    private isReadonly: boolean;
    private hasDebug: boolean;
    private statementCache = new Map<string, sqlite3.Statement>();

    constructor(db: sqlite3.Database, isReadonly: boolean, hasDebug: boolean) {
        this.db = db;
        this.isReadonly = isReadonly;
        this.hasDebug = hasDebug;

        if (!isReadonly) {
            this.initSchema();
        }

        const storedHasDebug = this.getHasDebug()
        if (storedHasDebug === -1) {
            this.setHasDebug(hasDebug);
        } else if (storedHasDebug !== (hasDebug ? 1 : 0)) {
            throw new Error(`Database hasDebug mismatch: stored=${storedHasDebug}, provided=${hasDebug}`);
        }
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

    getTxCount(): number {
        return this.getIntValue('tx_count', 0);
    }

    storeBlocks(batch: StoredBlock[]): void {
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
                if (Number(storedBlock.block.number) !== lastStoredBlockNum + 1) {
                    throw new Error(`Batch not sorted or has gaps: expected ${lastStoredBlockNum + 1}, got ${Number(storedBlock.block.number)}`);
                }
                this.storeBlock(storedBlock);
                lastStoredBlockNum++;
            }

            // Update the transaction count once for the entire batch
            if (totalTxCount > 0) {
                const currentCount = this.getTxCount();
                this.setTxCount(currentCount + totalTxCount);
            }
        });

        storeBlocksTransaction();
    }

    setBlockchainLatestBlockNum(blockNumber: number): void {
        if (this.isReadonly) throw new Error('BlocksDBHelper is readonly');
        this.setIntValue('blockchain_latest_block', blockNumber);
    }

    getBlockchainLatestBlockNum(): number {
        return this.getIntValue('blockchain_latest_block', -1);
    }

    close(): void {
        // Clear cached statements (better-sqlite3 handles cleanup automatically)
        this.statementCache.clear();
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

        // Store block data as JSON
        const blockData = JSON.stringify(blockWithoutTxs);

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

            const txDataJson = JSON.stringify(txData);
            const txHashPrefix = Buffer.from(tx.hash.slice(2), 'hex').slice(0, 5);

            // Find the corresponding trace for this transaction
            let traceDataJson: string | null = null;
            if (this.hasDebug && storedBlock.traces) {
                const traces = storedBlock.traces.filter(t => t.txHash === tx.hash);
                if (traces.length === 0) {
                    throw new Error(`hasDebug is true but no trace found for tx ${tx.hash}`);
                }
                if (traces.length > 1) {
                    throw new Error(`Multiple traces found for tx ${tx.hash}: ${traces.length} traces`);
                }
                const trace = traces[0]!;
                traceDataJson = JSON.stringify(trace);
            }

            const insertResult = insertTx.run(txHashPrefix, blockNumber, txDataJson, traceDataJson);
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
                data TEXT NOT NULL
            )`,
            `CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash)`,

            `CREATE TABLE IF NOT EXISTS txs (
                tx_num INTEGER PRIMARY KEY AUTOINCREMENT,
                hash BLOB NOT NULL,
                block_num INTEGER NOT NULL,
                data TEXT NOT NULL,
                traces TEXT
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
            `CREATE INDEX IF NOT EXISTS idx_tx_topics_topic_hash_txnum ON tx_topics(topic_hash, tx_num)`
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

    getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean, filterEvents: string[] | undefined): { txs: StoredTx[], traces: RpcTraceResult[] | undefined, maxTxNum: number } {
        // Ensure safe values to prevent SQL injection
        const txNumParam = Math.max(0, greaterThanTxNum);
        let limitParam = Math.min(Math.max(1, limit), 100000);

        // When filtering events, hard cap to 1000 to avoid huge IN clauses
        if (filterEvents && filterEvents.length > 0) {
            limitParam = Math.min(limitParam, MAX_ROWS_WITH_FILTER);
        }

        // Get the current maximum tx number (same as tx count since no deletions)
        const maxTxNum = this.getTxCount();

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
                    traces: undefined,
                    maxTxNum
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
                const txData = JSON.parse(row.data);
                const storedTx: StoredTx = {
                    txNum: row.tx_num,
                    ...txData
                };
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row.traces) {
                    const trace = JSON.parse(row.traces) as RpcTraceResult;
                    traces.push(trace);
                }
            }

            return {
                txs,
                traces: this.hasDebug && includeTraces ? traces : undefined,
                maxTxNum
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
                const txData = JSON.parse(row.data);
                const storedTx: StoredTx = {
                    txNum: row.tx_num,
                    ...txData
                };
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row.traces) {
                    const trace = JSON.parse(row.traces) as RpcTraceResult;
                    traces.push(trace);
                }
            }

            return {
                txs,
                traces: this.hasDebug && includeTraces ? traces : undefined,
                maxTxNum
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
                const storedData = JSON.parse(row.data) as Omit<RpcBlock, 'transactions'>;
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

        // Parse the block data (without transactions)
        const storedBlock = JSON.parse(blockRow.data) as Omit<RpcBlock, 'transactions'>;

        // Fetch all transactions for this block ordered by tx_num
        const txStmt = this.db.prepare('SELECT data FROM txs WHERE block_num = ? ORDER BY tx_num ASC');
        const txRows = txStmt.all(blockNumber) as any[];

        // Reconstruct the transactions array
        const transactions: RpcBlockTransaction[] = [];

        for (const txRow of txRows) {
            const txData = JSON.parse(txRow.data);
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
        const stmt = this.db.prepare('SELECT data FROM txs WHERE hash = ?');
        const rows = stmt.all(hashPrefix) as any[];

        // Handle potential collisions by checking the full hash in the data
        for (const row of rows) {
            const txData = JSON.parse(row.data);
            if (txData.tx.hash.toLowerCase() === ('0x' + hashStr).toLowerCase()) {
                return txData.receipt;
            }
        }

        return null;
    }

    slow_getBlockTraces(blockNumber: number): RpcTraceResult[] {
        const stmt = this.db.prepare('SELECT traces FROM txs WHERE block_num = ? ORDER BY tx_num ASC');
        const rows = stmt.all(blockNumber) as any[];

        const traces: RpcTraceResult[] = [];
        for (const row of rows) {
            if (!row.traces) continue;
            const trace = JSON.parse(row.traces) as RpcTraceResult;
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
