import mysql from 'mysql2/promise';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx, StoredRpcTxReceipt, CONTRACT_CREATION_TOPIC, RpcTraceCall } from './evmTypes.js';
import { StoredBlock } from './BatchRpc.js';

export class BlocksDBHelper {
    private pool: mysql.Pool;
    private isReadonly: boolean;
    private hasDebug: boolean;

    private constructor(pool: mysql.Pool, isReadonly: boolean, hasDebug: boolean) {
        this.pool = pool;
        this.isReadonly = isReadonly;
        this.hasDebug = hasDebug;
    }

    static async createFromPool(pool: mysql.Pool, options: { isReadonly: boolean, hasDebug: boolean }): Promise<BlocksDBHelper> {
        const blockDB = new BlocksDBHelper(pool, options.isReadonly, options.hasDebug);

        if (!options.isReadonly) {
            await blockDB.initSchema();
        }

        // Check and validate hasDebug setting
        try {
            const storedHasDebug = await blockDB.getHasDebug();
            console.log('storedHasDebug', storedHasDebug, 'hasDebug', options.hasDebug);
            if (storedHasDebug === -1) {
                // Never set before, set it now
                if (!options.isReadonly) {
                    await blockDB.setHasDebug(options.hasDebug);
                }
            } else {
                // Already set, must match
                const storedBool = storedHasDebug === 1;
                if (storedBool !== options.hasDebug) {
                    throw new Error(`Database hasDebug mismatch: stored=${storedBool}, provided=${options.hasDebug}`);
                }
            }
        } catch (err) {
            console.error('Failed to check hasDebug:', err);
            throw err;
        }

        return blockDB;
    }

    async getEvmChainId(): Promise<number> {
        return await this.getIntValue('evm_chain_id', -1);
    }

    async setEvmChainId(chainId: number): Promise<void> {
        await this.setIntValue('evm_chain_id', chainId);
    }

    async getLastStoredBlockNumber(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT MAX(number) as max_number FROM blocks'
        );
        return rows[0]?.['max_number'] ?? -1;
    }

    async getTxCount(): Promise<number> {
        return await this.getIntValue('tx_count', 0);
    }

    async storeBlocks(batch: StoredBlock[]): Promise<void> {
        if (this.isReadonly) throw new Error('BlocksDBHelper is readonly');
        if (batch.length === 0) return;

        let lastStoredBlockNum = await this.getLastStoredBlockNumber();
        let totalTxCount = 0;

        // Calculate total transactions in the batch
        for (const block of batch) {
            totalTxCount += block.block.transactions.length;
        }

        const connection = await this.pool.getConnection();
        try {
            await connection.beginTransaction();

            for (let i = 0; i < batch.length; i++) {
                const storedBlock = batch[i]!;
                if (Number(storedBlock.block.number) !== lastStoredBlockNum + 1) {
                    throw new Error(`Batch not sorted or has gaps: expected ${lastStoredBlockNum + 1}, got ${Number(storedBlock.block.number)}`);
                }
                await this.storeBlock(storedBlock, connection);
                lastStoredBlockNum++;
            }

            // Update the transaction count once for the entire batch
            if (totalTxCount > 0) {
                const currentCount = await this.getTxCount();
                await this.setTxCountWithConnection(currentCount + totalTxCount, connection);
            }

            await connection.commit();
        } catch (error) {
            await connection.rollback();
            throw error;
        } finally {
            connection.release();
        }
    }

    async setBlockchainLatestBlockNum(blockNumber: number): Promise<void> {
        if (this.isReadonly) throw new Error('BlocksDBHelper is readonly');
        await this.setIntValue('blockchain_latest_block', blockNumber);
    }

    async getBlockchainLatestBlockNum(): Promise<number> {
        return await this.getIntValue('blockchain_latest_block', -1);
    }

    async close(): Promise<void> {
        await this.pool.end();
    }

    private async storeBlock(storedBlock: StoredBlock, connection: mysql.PoolConnection): Promise<void> {
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

        // Store block data as JSON (RocksDB handles compression)
        const blockData = JSON.stringify(blockWithoutTxs);

        // Extract block hash (remove '0x' prefix) and take first 5 bytes
        const blockHash = storedBlock.block.hash.slice(2);
        const blockHashPrefix = Buffer.from(blockHash, 'hex').slice(0, 5);

        await connection.execute(
            'INSERT INTO blocks (number, hash, data) VALUES (?, ?, ?)',
            [blockNumber, blockHashPrefix, blockData]
        );

        // Store transactions with their receipts and traces
        for (let i = 0; i < storedBlock.block.transactions.length; ++i) {
            const tx = storedBlock.block.transactions[i]!;
            const receipt = storedBlock.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Prepare transaction data
            const receiptWithoutBloom: StoredRpcTxReceipt = { ...receipt };
            delete (receiptWithoutBloom as any).logsBloom;

            const txData: StoredTx = {
                txNum: 0, // Will be set by auto-increment
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

            const [insertResult] = await connection.execute<mysql.ResultSetHeader>(
                'INSERT INTO txs (hash, block_num, data, traces) VALUES (?, ?, ?, ?)',
                [txHashPrefix, blockNumber, txDataJson, traceDataJson]
            );

            const txNum = insertResult.insertId;

            // Update the stored transaction data with the actual tx_num
            txData.txNum = txNum;
            const updatedTxDataJson = JSON.stringify(txData);

            await connection.execute(
                'UPDATE txs SET data = ? WHERE tx_num = ?',
                [updatedTxDataJson, txNum]
            );

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
                await connection.execute(
                    'INSERT INTO tx_topics (tx_num, topic_hash) VALUES (?, ?)',
                    [txNum, contractCreationTopicPrefix]
                );
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
                    await connection.execute(
                        'INSERT INTO tx_topics (tx_num, topic_hash) VALUES (?, ?)',
                        [txNum, topicHashPrefix]
                    );
                }
            }
        }
    }

    private async setTxCountWithConnection(count: number, connection: mysql.PoolConnection): Promise<void> {
        await this.setIntValueWithConnection('tx_count', count, connection);
    }

    private async initSchema(): Promise<void> {
        const queries = [
            `CREATE TABLE IF NOT EXISTS blocks (
                number INT UNSIGNED PRIMARY KEY,
                hash BINARY(5) NOT NULL,
                data LONGTEXT NOT NULL,
                INDEX idx_hash (hash)
            ) ENGINE=ROCKSDB`,

            `CREATE TABLE IF NOT EXISTS txs (
                tx_num INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                hash BINARY(5) NOT NULL,
                block_num INT UNSIGNED NOT NULL,
                data LONGTEXT NOT NULL,
                traces LONGTEXT,
                INDEX idx_hash (hash),
                INDEX idx_block_num (block_num)
            ) ENGINE=ROCKSDB`,

            `CREATE TABLE IF NOT EXISTS kv_int (
                \`key\` VARCHAR(255) PRIMARY KEY,
                value BIGINT NOT NULL
            ) ENGINE=ROCKSDB`,

            `CREATE TABLE IF NOT EXISTS kv_string (
                \`key\` VARCHAR(255) PRIMARY KEY,
                value TEXT NOT NULL
            ) ENGINE=ROCKSDB`,

            `CREATE TABLE IF NOT EXISTS tx_topics (
                tx_num INT UNSIGNED NOT NULL,
                topic_hash BINARY(5) NOT NULL,
                INDEX idx_topic_hash (topic_hash)
            ) ENGINE=ROCKSDB`
        ];

        for (const query of queries) {
            try {
                await this.pool.execute(query);
            } catch (error: any) {
                // Ignore errors if tables already exist
                if (!error.message.includes('already exists')) {
                    throw error;
                }
            }
        }
    }

    async getHasDebug(): Promise<number> {
        return await this.getIntValue('hasDebug', -1);
    }

    async setHasDebug(hasDebug: boolean): Promise<void> {
        await this.setIntValue('hasDebug', hasDebug ? 1 : 0);
    }

    async getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean, filterEvents: string[] | undefined): Promise<{ txs: StoredTx[], traces: RpcTraceResult[] | undefined, maxTxNum: number }> {
        // Ensure safe values to prevent SQL injection
        const txNumParam = Math.max(0, greaterThanTxNum);
        const limitParam = Math.min(Math.max(1, limit), 100000);

        let query: string;

        // Get the current maximum tx number to help callers skip expensive
        // queries when no more results are available
        const [maxRows] = await this.pool.query<mysql.RowDataPacket[]>(
            'SELECT MAX(tx_num) AS max_tx_num FROM txs'
        );
        const maxTxNum = maxRows[0]?.['max_tx_num'] ?? -1;

        if (filterEvents && filterEvents.length > 0) {
            // Use the index for efficient filtering
            const topicHashPrefixes = filterEvents.map(topic => Buffer.from(topic.slice(2), 'hex').slice(0, 5));

            const placeholders = topicHashPrefixes.map(() => '?').join(',');

            // Query using the index directly without glossary lookup
            query = includeTraces && this.hasDebug
                ? `SELECT DISTINCT t.tx_num, t.data, t.traces 
                   FROM txs t 
                   INNER JOIN tx_topics tt ON t.tx_num = tt.tx_num 
                   WHERE tt.topic_hash IN (${placeholders}) 
                   AND t.tx_num > ${txNumParam} 
                   ORDER BY t.tx_num ASC 
                   LIMIT ${limitParam}`
                : `SELECT DISTINCT t.tx_num, t.data 
                   FROM txs t 
                   INNER JOIN tx_topics tt ON t.tx_num = tt.tx_num 
                   WHERE tt.topic_hash IN (${placeholders}) 
                   AND t.tx_num > ${txNumParam} 
                   ORDER BY t.tx_num ASC 
                   LIMIT ${limitParam}`;

            const mysqlGetStarted = performance.now();
            const [rows] = await this.pool.query<mysql.RowDataPacket[]>(query, topicHashPrefixes);
            const mysqlGetEnded = performance.now();
            if ((mysqlGetEnded - mysqlGetStarted) > 100) {
                console.log(`MySQL indexed query took ${mysqlGetEnded - mysqlGetStarted}ms`);
            }

            const txs: StoredTx[] = [];
            const traces: RpcTraceResult[] = [];

            for (const row of rows) {
                const storedTx = JSON.parse(row['data']) as StoredTx;
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row['traces']) {
                    const trace = JSON.parse(row['traces']) as RpcTraceResult;
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
            query = includeTraces && this.hasDebug
                ? `SELECT tx_num, data, traces FROM txs WHERE tx_num > ${txNumParam} ORDER BY tx_num ASC LIMIT ${limitParam}`
                : `SELECT tx_num, data FROM txs WHERE tx_num > ${txNumParam} ORDER BY tx_num ASC LIMIT ${limitParam}`;

            const mysqlGetStarted = performance.now();
            const [rows] = await this.pool.query<mysql.RowDataPacket[]>(query);
            const mysqlGetEnded = performance.now();
            if (mysqlGetEnded - mysqlGetStarted > 100) {
                console.log(`MySQL query took ${mysqlGetEnded - mysqlGetStarted}ms`);
            }

            const txs: StoredTx[] = [];
            const traces: RpcTraceResult[] = [];

            for (const row of rows) {
                const storedTx = JSON.parse(row['data']) as StoredTx;
                txs.push(storedTx);

                if (this.hasDebug && includeTraces && row['traces']) {
                    const trace = JSON.parse(row['traces']) as RpcTraceResult;
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

    async slow_getBlockWithTransactions(blockIdentifier: number | string): Promise<RpcBlock | null> {
        let blockNumber: number = -1;
        let blockRow: mysql.RowDataPacket | undefined;

        if (typeof blockIdentifier === 'number') {
            // Query by block number
            blockNumber = blockIdentifier;
            const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
                'SELECT number, hash, data FROM blocks WHERE number = ?',
                [blockNumber]
            );
            blockRow = rows[0];
        } else {
            // Query by block hash
            const hashStr = blockIdentifier.startsWith('0x') ? blockIdentifier.slice(2) : blockIdentifier;
            if (hashStr.length !== 64) {
                throw new Error(`Invalid block hash length: expected 64 hex chars, got ${hashStr.length}`);
            }
            const hashPrefix = Buffer.from(hashStr, 'hex').slice(0, 5);

            const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
                'SELECT number, hash, data FROM blocks WHERE hash = ?',
                [hashPrefix]
            );

            // Handle potential collisions by checking the full hash in the data
            for (const row of rows) {
                const storedData = JSON.parse(row['data']) as Omit<RpcBlock, 'transactions'>;
                if (storedData.hash.toLowerCase() === ('0x' + hashStr).toLowerCase()) {
                    blockRow = row;
                    blockNumber = row['number'];
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
        const storedBlock = JSON.parse(blockRow['data']) as Omit<RpcBlock, 'transactions'>;

        // Fetch all transactions for this block ordered by tx_idx
        const [txRows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT data FROM txs WHERE block_num = ? ORDER BY tx_num ASC',
            [blockNumber]
        );

        // Reconstruct the transactions array
        const transactions: RpcBlockTransaction[] = [];

        for (const txRow of txRows) {
            const storedTx = JSON.parse(txRow['data']) as StoredTx;
            transactions.push(storedTx.tx);
        }

        // Reassemble the complete RpcBlock
        const fullBlock: RpcBlock = {
            ...storedBlock,
            transactions
        };

        return fullBlock;
    }

    async getTxReceipt(txHash: string): Promise<StoredRpcTxReceipt | null> {
        const hashStr = txHash.replace(/^0x/, '');
        const hashPrefix = Buffer.from(hashStr, 'hex').slice(0, 5);
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT data FROM txs WHERE hash = ?',
            [hashPrefix]
        );

        // Handle potential collisions by checking the full hash in the data
        for (const row of rows) {
            const storedTx = JSON.parse(row['data']) as StoredTx;
            if (storedTx.tx.hash.toLowerCase() === ('0x' + hashStr).toLowerCase()) {
                return storedTx.receipt;
            }
        }

        return null;
    }

    async slow_getBlockTraces(blockNumber: number): Promise<RpcTraceResult[]> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT traces FROM txs WHERE block_num = ? ORDER BY tx_num ASC',
            [blockNumber]
        );

        const traces: RpcTraceResult[] = [];
        for (const row of rows) {
            if (!row['traces']) continue;
            const trace = JSON.parse(row['traces']) as RpcTraceResult;
            traces.push(trace);
        }
        return traces;
    }

    /**
     * Get direct access to the underlying pool for custom queries.
     * USE WITH CAUTION: This bypasses all abstractions and safety checks.
     */
    getDatabase(): mysql.Pool {
        return this.pool;
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
    private async getIntValue(key: string, defaultValue: number): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            [key]
        );
        return rows[0]?.['value'] ?? defaultValue;
    }

    /**
     * Set a value in kv_int table
     */
    private async setIntValue(key: string, value: number): Promise<void> {
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            [key, value]
        );
    }

    /**
     * Set a value in kv_int table using an existing connection
     */
    private async setIntValueWithConnection(key: string, value: number, connection: mysql.PoolConnection): Promise<void> {
        await connection.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            [key, value]
        );
    }
}
