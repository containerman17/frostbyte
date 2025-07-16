import mysql from 'mysql2/promise';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx } from './evmTypes.js';
import { StoredBlock } from './BatchRpc.js';

export class BlockDB {
    private pool: mysql.Pool;
    private isReadonly: boolean;
    private hasDebug: boolean;

    private constructor(pool: mysql.Pool, isReadonly: boolean, hasDebug: boolean) {
        this.pool = pool;
        this.isReadonly = isReadonly;
        this.hasDebug = hasDebug;
    }

    static async create({ path, isReadonly, hasDebug }: { path: string, isReadonly: boolean, hasDebug: boolean }): Promise<BlockDB> {
        // Parse connection string from path (format: mysql://user:pass@host:port/database)
        const url = new URL(path);
        const databaseName = url.pathname.slice(1); // Remove leading slash

        // First create the database if it doesn't exist
        const connection = await mysql.createConnection({
            host: url.hostname,
            port: parseInt(url.port) || 3306,
            user: url.username,
            password: url.password
        });

        try {
            // Create database if it doesn't exist
            await connection.execute(
                `CREATE DATABASE IF NOT EXISTS \`${databaseName}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`
            );
            console.log(`Database ${databaseName} ready`);
        } finally {
            await connection.end();
        }

        // Create the pool
        const pool = mysql.createPool({
            host: url.hostname,
            port: parseInt(url.port) || 3306,
            user: url.username,
            password: url.password,
            database: databaseName,
            waitForConnections: true,
            connectionLimit: isReadonly ? 5 : 10,
            queueLimit: 0,
            enableKeepAlive: true,
            keepAliveInitialDelay: 0
        });

        // Create the instance
        const blockDB = new BlockDB(pool, isReadonly, hasDebug);

        if (!isReadonly) {
            await blockDB.initSchema();
        }

        // Check and validate hasDebug setting
        try {
            const storedHasDebug = await blockDB.getHasDebug();
            console.log('storedHasDebug', storedHasDebug, 'hasDebug', hasDebug);
            if (storedHasDebug === -1) {
                // Never set before, set it now
                if (!isReadonly) {
                    await blockDB.setHasDebug(hasDebug);
                }
            } else {
                // Already set, must match
                const storedBool = storedHasDebug === 1;
                if (storedBool !== hasDebug) {
                    throw new Error(`Database hasDebug mismatch: stored=${storedBool}, provided=${hasDebug}`);
                }
            }
        } catch (err) {
            console.error('Failed to check hasDebug:', err);
            await pool.end();
            throw err;
        }

        return blockDB;
    }

    async getEvmChainId(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            ['evm_chain_id']
        );
        return rows[0]?.['value'] ?? -1;
    }

    async setEvmChainId(chainId: number): Promise<void> {
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['evm_chain_id', chainId]
        );
    }

    async getIsCaughtUp(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            ['is_caught_up']
        );
        return rows[0]?.['value'] ?? -1;
    }

    async setIsCaughtUp(isCaughtUp: boolean): Promise<void> {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['is_caught_up', isCaughtUp ? 1 : 0]
        );
    }

    async checkAndUpdateCatchUpStatus(): Promise<boolean> {
        if (this.isReadonly) return false;

        const lastStoredBlock = await this.getLastStoredBlockNumber();
        const latestBlockchain = await this.getBlockchainLatestBlockNum();
        const isCaughtUp = lastStoredBlock >= latestBlockchain;

        if (isCaughtUp && (await this.getIsCaughtUp()) !== 1) {
            console.log('BlockDB: Chain caught up!');
            await this.setIsCaughtUp(true);
            return true;
        }

        return false;
    }

    async performPeriodicMaintenance(): Promise<void> {
        // RocksDB handles all maintenance automatically
        return;
    }

    async getLastStoredBlockNumber(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT MAX(number) as max_number FROM blocks'
        );
        return rows[0]?.['max_number'] ?? -1;
    }

    async getTxCount(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            ['tx_count']
        );
        return rows[0]?.['value'] ?? 0;
    }

    private async setTxCount(count: number): Promise<void> {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['tx_count', count]
        );
    }

    async storeBlocks(batch: StoredBlock[]): Promise<void> {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
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
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['blockchain_latest_block', blockNumber]
        );
    }

    async getBlockchainLatestBlockNum(): Promise<number> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            ['blockchain_latest_block']
        );
        return rows[0]?.['value'] ?? -1;
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
        const blockWithoutTxs: Omit<RpcBlock, 'transactions'> = {
            ...storedBlock.block,
        };
        delete (blockWithoutTxs as any).transactions;

        // Store block data as JSON (RocksDB handles compression)
        const blockData = JSON.stringify(blockWithoutTxs);

        // Extract block hash (remove '0x' prefix)
        const blockHash = storedBlock.block.hash.slice(2);

        await connection.execute(
            'INSERT INTO blocks (number, hash, data) VALUES (?, ?, ?)',
            [blockNumber, Buffer.from(blockHash, 'hex'), blockData]
        );

        // Store transactions with their receipts and traces
        for (let i = 0; i < storedBlock.block.transactions.length; ++i) {
            const tx = storedBlock.block.transactions[i]!;
            const receipt = storedBlock.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Calculate tx_num using the formula: (block_num << 16) | tx_idx
            const tx_num = Number((BigInt(blockNumber) << 16n) | BigInt(i));

            // Prepare transaction data
            const txData: StoredTx = {
                txNum: tx_num,
                tx: tx,
                receipt: receipt,
                blockTs: Number(storedBlock.block.timestamp)
            };

            const txDataJson = JSON.stringify(txData);
            const txHash = Buffer.from(tx.hash.slice(2), 'hex');

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

            await connection.execute(
                'INSERT INTO txs (tx_num, hash, data, traces) VALUES (?, ?, ?, ?)',
                [tx_num, txHash, txDataJson, traceDataJson]
            );
        }
    }

    private async setTxCountWithConnection(count: number, connection: mysql.PoolConnection): Promise<void> {
        await connection.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['tx_count', count]
        );
    }

    private async initSchema(): Promise<void> {
        const queries = [
            `CREATE TABLE IF NOT EXISTS blocks (
                number BIGINT PRIMARY KEY,
                hash BINARY(32) NOT NULL UNIQUE,
                data LONGTEXT NOT NULL,
                INDEX idx_hash (hash)
            ) ENGINE=ROCKSDB`,

            `CREATE TABLE IF NOT EXISTS txs (
                tx_num BIGINT PRIMARY KEY,
                hash BINARY(32) NOT NULL UNIQUE,
                block_num BIGINT GENERATED ALWAYS AS (tx_num >> 16) STORED,
                tx_idx INT GENERATED ALWAYS AS (tx_num & 0xFFFF) STORED,
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
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT value FROM kv_int WHERE `key` = ?',
            ['hasDebug']
        );
        return rows[0]?.['value'] ?? -1;
    }

    async setHasDebug(hasDebug: boolean): Promise<void> {
        await this.pool.execute(
            'INSERT INTO kv_int (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)',
            ['hasDebug', hasDebug ? 1 : 0]
        );
    }

    async getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean): Promise<{ txs: StoredTx[], traces: RpcTraceResult[] | undefined }> {
        // Ensure safe values to prevent SQL injection
        const txNumParam = Math.max(0, greaterThanTxNum);
        const limitParam = Math.min(Math.max(1, limit), 100000);

        // Use direct query to avoid prepared statement issues with BIGINT columns
        const query = includeTraces && this.hasDebug
            ? `SELECT tx_num, data, traces FROM txs WHERE tx_num > ${txNumParam} ORDER BY tx_num ASC LIMIT ${limitParam}`
            : `SELECT tx_num, data FROM txs WHERE tx_num > ${txNumParam} ORDER BY tx_num ASC LIMIT ${limitParam}`;

        const [rows] = await this.pool.query<mysql.RowDataPacket[]>(query);

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
            traces: this.hasDebug && includeTraces ? traces : undefined
        };
    }

    async slow_getBlockWithTransactions(blockIdentifier: number | string): Promise<RpcBlock | null> {
        let blockNumber: number;
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
            const hashBuffer = Buffer.from(hashStr, 'hex');

            const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
                'SELECT number, hash, data FROM blocks WHERE hash = ?',
                [hashBuffer]
            );
            blockRow = rows[0];

            if (blockRow) {
                blockNumber = blockRow['number'];
            } else {
                return null;
            }
        }

        if (!blockRow) {
            return null;
        }

        // Parse the block data (without transactions)
        const storedBlock = JSON.parse(blockRow['data']) as Omit<RpcBlock, 'transactions'>;

        // Fetch all transactions for this block
        const minTxNum = Number(BigInt(blockNumber) << 16n);
        const maxTxNum = Number((BigInt(blockNumber) << 16n) | 0xFFFFn);

        const [txRows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT tx_num, data FROM txs WHERE tx_num >= ? AND tx_num <= ? ORDER BY tx_num ASC',
            [minTxNum, maxTxNum]
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

    async getTxReceipt(txHash: string): Promise<RpcTxReceipt | null> {
        const hashBuf = Buffer.from(txHash.replace(/^0x/, ''), 'hex');
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT data FROM txs WHERE hash = ?',
            [hashBuf]
        );

        if (rows.length === 0) return null;

        const storedTx = JSON.parse(rows[0]!['data']) as StoredTx;
        return storedTx.receipt;
    }

    async slow_getBlockTraces(blockNumber: number): Promise<RpcTraceResult[]> {
        const [rows] = await this.pool.execute<mysql.RowDataPacket[]>(
            'SELECT traces FROM txs WHERE block_num = ? ORDER BY tx_idx ASC',
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
}
