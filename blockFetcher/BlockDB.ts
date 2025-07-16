import { DuckDBInstance, DuckDBConnection, blobValue } from '@duckdb/node-api';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx } from './evmTypes.js';
import { StoredBlock } from './BatchRpc.js';

export class BlockDB {
    private instance!: DuckDBInstance;
    private connection!: DuckDBConnection;
    private isReadonly: boolean;
    private hasDebug: boolean;
    private path: string;
    private chainId: string;

    constructor({ path, isReadonly, hasDebug, chainId }: { path: string, isReadonly: boolean, hasDebug: boolean, chainId: string }) {
        this.isReadonly = isReadonly;
        this.hasDebug = hasDebug;
        this.path = path;
        this.chainId = chainId;
    }

    async init() {
        // Initialize DuckDB instance
        this.instance = await DuckDBInstance.fromCache(this.path, {
            access_mode: this.isReadonly ? 'READ_ONLY' : 'READ_WRITE'
        });

        // this.instance = await DuckDBInstance.create(this.path, {
        //     access_mode: this.isReadonly ? 'READ_ONLY' : 'READ_WRITE'
        // });
        this.connection = await this.instance.connect();

        if (!this.isReadonly) {
            await this.initSchema();
        }

        // Check and validate hasDebug setting
        const storedHasDebug = await this.getHasDebug();
        console.log('storedHasDebug', storedHasDebug, 'hasDebug', this.hasDebug);
        if (storedHasDebug === -1) {
            // Never set before, set it now
            if (!this.isReadonly) {
                await this.setHasDebug(this.hasDebug);
            }
        } else {
            // Already set, must match
            const storedBool = storedHasDebug === 1;
            if (storedBool !== this.hasDebug) {
                throw new Error(`Database hasDebug mismatch: stored=${storedBool}, provided=${this.hasDebug}`);
            }
        }
    }

    private async initSchema() {
        // Create tables with chain-specific prefixes
        await this.connection.run(`
            CREATE TABLE IF NOT EXISTS blocks_${this.chainId} (
                number BIGINT PRIMARY KEY,
                hash BLOB NOT NULL UNIQUE,
                data JSON NOT NULL
            );
        `);

        await this.connection.run(`
            CREATE TABLE IF NOT EXISTS txs_${this.chainId} (
                tx_num BIGINT PRIMARY KEY,
                hash BLOB NOT NULL UNIQUE,
                block_num BIGINT AS (tx_num >> 16),
                tx_idx INTEGER AS (tx_num & 65535),
                data JSON NOT NULL,
                traces JSON
            );
        `);

        await this.connection.run(`
            CREATE TABLE IF NOT EXISTS kv_int_${this.chainId} (
                key VARCHAR PRIMARY KEY,
                value BIGINT NOT NULL
            );
        `);

        await this.connection.run(`
            CREATE TABLE IF NOT EXISTS kv_string_${this.chainId} (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL
            );
        `);
    }

    // Generic key-value operations
    private async getInt(key: string, defaultValue: number = -1): Promise<number> {
        const result = await this.connection.runAndReadAll(
            `SELECT value FROM kv_int_${this.chainId} WHERE key = ?`,
            [key]
        );
        const rows = result.getRowObjects();
        if (rows.length > 0) {
            const value = rows[0]!['value'];
            // Convert bigint to number if needed
            return typeof value === 'bigint' ? Number(value) : value as number;
        }
        return defaultValue;
    }

    private async setInt(key: string, value: number): Promise<void> {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        await this.connection.run(
            `INSERT OR REPLACE INTO kv_int_${this.chainId} (key, value) VALUES (?, ?)`,
            [key, value]
        );
    }

    private async getString(key: string, defaultValue: string = ''): Promise<string> {
        const result = await this.connection.runAndReadAll(
            `SELECT value FROM kv_string_${this.chainId} WHERE key = ?`,
            [key]
        );
        const rows = result.getRowObjects();
        return rows.length > 0 ? rows[0]!['value'] as string : defaultValue;
    }

    private async setString(key: string, value: string): Promise<void> {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        await this.connection.run(
            `INSERT OR REPLACE INTO kv_string_${this.chainId} (key, value) VALUES (?, ?)`,
            [key, value]
        );
    }

    async getEvmChainId(): Promise<number> {
        return this.getInt('evm_chain_id');
    }

    async setEvmChainId(chainId: number): Promise<void> {
        await this.setInt('evm_chain_id', chainId);
    }

    async getIsCaughtUp(): Promise<number> {
        return this.getInt('is_caught_up');
    }

    async setIsCaughtUp(isCaughtUp: boolean): Promise<void> {
        await this.setInt('is_caught_up', isCaughtUp ? 1 : 0);
    }

    async checkAndUpdateCatchUpStatus(): Promise<boolean> {
        if (this.isReadonly) return false;

        const lastStoredBlock = await this.getLastStoredBlockNumber();
        const latestBlockchain = await this.getBlockchainLatestBlockNum();
        const isCaughtUp = lastStoredBlock >= latestBlockchain;

        if (isCaughtUp && await this.getIsCaughtUp() !== 1) {
            console.log('BlockDB: Chain caught up!');
            await this.setIsCaughtUp(true);
            return true;
        }

        return false;
    }

    async getLastStoredBlockNumber(): Promise<number> {
        const result = await this.connection.runAndReadAll(
            `SELECT MAX(number) as max_number FROM blocks_${this.chainId}`
        );
        const rows = result.getRowObjects();
        return Number(rows.length > 0 && rows[0]!['max_number'] != null
            ? rows[0]!['max_number']
            : -1);
    }

    async getTxCount(): Promise<number> {
        return this.getInt('tx_count', 0);
    }

    private async setTxCount(count: number): Promise<void> {
        await this.setInt('tx_count', count);
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

        // Begin transaction
        await this.connection.run('BEGIN TRANSACTION');

        try {
            for (const storedBlock of batch) {
                if (Number(storedBlock.block.number) !== lastStoredBlockNum + 1) {
                    throw new Error(`Batch not sorted or has gaps: expected ${lastStoredBlockNum + 1}, got ${Number(storedBlock.block.number)}`);
                }
                await this.storeBlock(storedBlock);
                lastStoredBlockNum++;
            }

            // Update the transaction count once for the entire batch
            if (totalTxCount > 0) {
                const currentCount = await this.getTxCount();
                await this.setTxCount(currentCount + totalTxCount);
            }

            await this.connection.run('COMMIT');
        } catch (error) {
            await this.connection.run('ROLLBACK');
            throw error;
        }
    }

    private async storeBlock(storedBlock: StoredBlock): Promise<void> {
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

        // Extract block hash (remove '0x' prefix and convert to blob)
        const blockHash = blobValue(Buffer.from(storedBlock.block.hash.slice(2), 'hex'));

        // Insert block
        await this.connection.run(
            `INSERT INTO blocks_${this.chainId}(number, hash, data) VALUES (?, ?, ?)`,
            [blockNumber, blockHash, JSON.stringify(blockWithoutTxs)]
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
                txNum: tx_num,//FIXME: coule be fetched from the column. double-storage here
                tx: tx,
                receipt: receipt,
                blockTs: Number(storedBlock.block.timestamp)
            };

            // Extract transaction hash (remove '0x' prefix and convert to blob)
            const txHash = blobValue(Buffer.from(tx.hash.slice(2), 'hex'));

            // Find the corresponding trace for this transaction
            let trace = null;
            if (this.hasDebug && storedBlock.traces) {
                // Find trace by matching txHash
                const traces = storedBlock.traces.filter(t => t.txHash === tx.hash);
                if (traces.length === 0) {
                    throw new Error(`hasDebug is true but no trace found for tx ${tx.hash}`);
                }
                if (traces.length > 1) {
                    throw new Error(`Multiple traces found for tx ${tx.hash}: ${traces.length} traces`);
                }
                trace = traces[0];
            }

            await this.connection.run(
                `INSERT INTO txs_${this.chainId}(tx_num, hash, data, traces) VALUES (?, ?, ?, ?)`,
                [tx_num, txHash, JSON.stringify(txData), trace ? JSON.stringify(trace) : null]
            );
        }
    }

    async getBlockchainLatestBlockNum(): Promise<number> {
        return this.getInt('blockchain_latest_block', -1);
    }

    async setBlockchainLatestBlockNum(blockNumber: number): Promise<void> {
        await this.setInt('blockchain_latest_block', blockNumber);
    }

    async getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean): Promise<{ txs: StoredTx[], traces: RpcTraceResult[] | undefined }> {
        const query = includeTraces && this.hasDebug
            ? `SELECT tx_num, data, traces FROM txs_${this.chainId} WHERE tx_num > ? ORDER BY tx_num ASC LIMIT ?`
            : `SELECT tx_num, data FROM txs_${this.chainId} WHERE tx_num > ? ORDER BY tx_num ASC LIMIT ?`;

        const result = await this.connection.runAndReadAll(query, [greaterThanTxNum, limit]);
        const rows = result.getRowObjects();

        const txs: StoredTx[] = [];
        const traces: RpcTraceResult[] = [];

        for (const row of rows) {
            // Parse JSON data
            const dataStr = row['data'] as string;
            const txData = JSON.parse(dataStr) as StoredTx;
            txs.push(txData);

            if (this.hasDebug && includeTraces && row['traces']) {
                const traceStr = row['traces'] as string;
                const traceData = JSON.parse(traceStr) as RpcTraceResult;
                traces.push(traceData);
            }
        }

        return {
            txs,
            traces: this.hasDebug && includeTraces ? traces : undefined
        };
    }

    async slow_getBlockWithTransactions(blockIdentifier: number | string): Promise<RpcBlock | null> {
        let blockNumber: number;
        let blockRow: any;

        if (typeof blockIdentifier === 'number') {
            // Query by block number
            blockNumber = blockIdentifier;
            const result = await this.connection.runAndReadAll(
                `SELECT number, hash, data FROM blocks_${this.chainId} WHERE number = ?`,
                [blockNumber]
            );
            blockRow = result.getRowObjects()[0];
        } else {
            // Query by block hash
            const hashStr = blockIdentifier.startsWith('0x') ? blockIdentifier.slice(2) : blockIdentifier;
            if (hashStr.length !== 64) {
                throw new Error(`Invalid block hash length: expected 64 hex chars, got ${hashStr.length}`);
            }
            const hashBuffer = blobValue(Buffer.from(hashStr, 'hex'));

            const result = await this.connection.runAndReadAll(
                `SELECT number, hash, data FROM blocks_${this.chainId} WHERE hash = ?`,
                [hashBuffer]
            );
            blockRow = result.getRowObjects()[0];

            if (blockRow) {
                blockNumber = blockRow['number'] as number;
            } else {
                return null;
            }
        }

        if (!blockRow) {
            return null;
        }

        // Parse JSON data
        const dataStr = blockRow['data'] as string;
        const storedBlock = JSON.parse(dataStr) as Omit<RpcBlock, 'transactions'>;

        // Fetch all transactions for this block
        const minTxNum = Number(BigInt(blockNumber) << 16n);
        const maxTxNum = Number((BigInt(blockNumber) << 16n) | 0xFFFFn);

        const txResult = await this.connection.runAndReadAll(
            `SELECT tx_num, data FROM txs_${this.chainId} WHERE tx_num >= ? AND tx_num <= ? ORDER BY tx_num ASC`,
            [minTxNum, maxTxNum]
        );

        const transactions: RpcBlockTransaction[] = [];
        for (const txRow of txResult.getRowObjects()) {
            const txDataStr = txRow['data'] as string;
            const storedTx = JSON.parse(txDataStr) as StoredTx;
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
        const hashBuf = blobValue(Buffer.from(txHash.replace(/^0x/, ''), 'hex'));
        const result = await this.connection.runAndReadAll(
            `SELECT data FROM txs_${this.chainId} WHERE hash = ?`,
            [hashBuf]
        );

        if (result.getRowObjects().length === 0) return null;

        const dataStr = result.getRowObjects()[0]!['data'] as string;
        const storedTx = JSON.parse(dataStr) as StoredTx;
        return storedTx.receipt;
    }

    async slow_getBlockTraces(blockNumber: number): Promise<RpcTraceResult[]> {
        const result = await this.connection.runAndReadAll(
            `SELECT traces FROM txs_${this.chainId} WHERE block_num = ? ORDER BY tx_idx ASC`,
            [blockNumber]
        );

        const traces: RpcTraceResult[] = [];
        for (const row of result.getRowObjects()) {
            if (row['traces']) {
                const traceStr = row['traces'] as string;
                const traceData = JSON.parse(traceStr) as RpcTraceResult;
                traces.push(traceData);
            }
        }
        return traces;
    }

    async getHasDebug(): Promise<number> {
        return this.getInt('hasDebug', -1);
    }

    async setHasDebug(hasDebug: boolean): Promise<void> {
        await this.setInt('hasDebug', hasDebug ? 1 : 0);
    }

    async close(): Promise<void> {
        // Properly close the connection first
        if (this.connection) {
            this.connection.closeSync();
            this.connection = null as any;
        }

        // Then close the instance
        if (this.instance) {
            await this.instance.closeSync();
            this.instance = null as any;
        }
    }

    /**
     * Get direct access to the underlying connection for custom queries.
     * USE WITH CAUTION: This bypasses all abstractions and safety checks.
     */
    getConnection(): DuckDBConnection {
        return this.connection;
    }
}
