import Database from 'better-sqlite3';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx } from './evmTypes';
import { compress as zstdCompress, decompress as zstdDecompress } from 'zstd-napi';
import { StoredBlock } from './BatchRpc';

export class BlockDB {
    private db: InstanceType<typeof Database>;
    private prepped: Map<string, any>;
    private isReadonly: boolean;
    private hasDebug: boolean;

    constructor({ path, isReadonly, hasDebug }: { path: string, isReadonly: boolean, hasDebug: boolean }) {
        this.db = new Database(path, {
            readonly: isReadonly,
        });
        this.isReadonly = isReadonly;
        this.initPragmas(isReadonly);
        if (!isReadonly) {
            this.initSchema();
        }
        this.prepped = new Map();

        // Check and validate hasDebug setting
        const storedHasDebug = this.getHasDebug();
        console.log('storedHasDebug', storedHasDebug, 'hasDebug', hasDebug);
        if (storedHasDebug === -1) {
            // Never set before, set it now
            if (!isReadonly) {
                this.setHasDebug(hasDebug);
            }
        } else {
            // Already set, must match
            const storedBool = storedHasDebug === 1;
            if (storedBool !== hasDebug) {
                throw new Error(`Database hasDebug mismatch: stored=${storedBool}, provided=${hasDebug}`);
            }
        }

        this.hasDebug = hasDebug;
    }

    getEvmChainId(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('evm_chain_id') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    setEvmChainId(chainId: number) {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('evm_chain_id', chainId, 0);
    }

    getLastStoredBlockNumber(): number {
        const selectMax = this.prepQuery('SELECT MAX(number) as max_number FROM blocks');
        const result = selectMax.get() as { max_number: number | null } | undefined;
        const result2 = result?.max_number ?? -1;
        return result2;
    }

    storeBlocks(batch: StoredBlock[]) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        if (batch.length === 0) return;

        let lastStoredBlockNum = this.getLastStoredBlockNumber();

        const insertMany = this.db.transaction((batch: StoredBlock[]) => {
            for (let i = 0; i < batch.length; i++) {
                const storedBlock = batch[i]!;
                if (Number(storedBlock.block.number) !== lastStoredBlockNum + 1) {
                    throw new Error(`Batch not sorted or has gaps: expected ${lastStoredBlockNum + 1}, got ${Number(storedBlock.block.number)}`);
                }
                this.storeBlock(storedBlock);
                lastStoredBlockNum++;
            }
        });
        insertMany(batch);
    }

    setBlockchainLatestBlockNum(blockNumber: number) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('blockchain_latest_block', blockNumber, 0);
    }

    getBlockchainLatestBlockNum(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('blockchain_latest_block') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    close() {
        this.db.close();
    }

    private prepQuery(query: string) {
        if (this.prepped.has(query)) return this.prepped.get(query)!;
        const prepped = this.db.prepare(query);
        this.prepped.set(query, prepped);
        return prepped;
    }

    private storeBlock(storedBlock: StoredBlock) {
        // Validate hasDebug flag vs traces presence
        if (this.hasDebug && storedBlock.traces === undefined) {
            throw new Error('hasDebug is true but StoredBlock.traces is undefined');
        }
        if (!this.hasDebug && storedBlock.traces !== undefined) {
            throw new Error('hasDebug is false but StoredBlock.traces is defined');
        }

        const insertBlock = this.prepQuery('INSERT INTO blocks(number, hash, data, codec) VALUES (?, ?, ?, ?)');
        const insertTx = this.prepQuery('INSERT INTO txs(tx_num, hash, data, traces, codec) VALUES (?, ?, ?, ?, ?)');

        const blockNumber = Number(storedBlock.block.number);

        // Create a block without transactions for storage
        const blockWithoutTxs: Omit<RpcBlock, 'transactions'> = {
            ...storedBlock.block,
        };
        delete (blockWithoutTxs as any).transactions;

        // Compress block data before storing
        const blockJsonStr = JSON.stringify(blockWithoutTxs);
        const compressedBlockData = zstdCompress(Buffer.from(blockJsonStr));

        // Extract block hash (remove '0x' prefix and convert to Buffer)
        const blockHash = Buffer.from(storedBlock.block.hash.slice(2), 'hex');

        insertBlock.run(blockNumber, blockHash, compressedBlockData, 0);

        // Store transactions with their receipts and traces
        for (let i = 0; i < storedBlock.block.transactions.length; ++i) {
            const tx = storedBlock.block.transactions[i]!;
            const receipt = storedBlock.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Calculate tx_num using the formula: (block_num << 16) | tx_idx
            const tx_num = (blockNumber << 16) | i;

            // Prepare transaction data
            const txData: StoredTx = {
                tx: tx,
                receipt: receipt,
                blockTs: Number(storedBlock.block.timestamp)
            };

            // Compress transaction data
            const txJsonStr = JSON.stringify(txData);
            const compressedTxData = zstdCompress(Buffer.from(txJsonStr));

            // Extract transaction hash (remove '0x' prefix and convert to Buffer)
            const txHash = Buffer.from(tx.hash.slice(2), 'hex');

            // Find the corresponding trace for this transaction
            let compressedTraceData: Buffer | null = null;
            if (this.hasDebug && storedBlock.traces) {
                // Find trace by matching txHash
                const traces = storedBlock.traces.filter(t => t.txHash === tx.hash);
                if (traces.length === 0) {
                    throw new Error(`hasDebug is true but no trace found for tx ${tx.hash}`);
                }
                if (traces.length > 1) {
                    throw new Error(`Multiple traces found for tx ${tx.hash}: ${traces.length} traces`);
                }
                const trace = traces[0]!;
                const traceJsonStr = JSON.stringify(trace);
                compressedTraceData = zstdCompress(Buffer.from(traceJsonStr));
            }

            insertTx.run(tx_num, txHash, compressedTxData, compressedTraceData, 0);
        }
    }

    private initSchema() {
        this.db.exec(`      
          -- Blocks ----------------------------------------------------------
          CREATE TABLE IF NOT EXISTS blocks (
            number INTEGER PRIMARY KEY,          -- block height
            hash   BLOB    NOT NULL UNIQUE,      -- 32-byte block hash
            data   BLOB    NOT NULL,
            codec  INTEGER NOT NULL DEFAULT 0
          ) WITHOUT ROWID;
      
          -- Transactions ----------------------------------------------------
          CREATE TABLE IF NOT EXISTS txs (
            tx_num    INTEGER PRIMARY KEY,                           -- (block_num<<16)|tx_idx
            hash      BLOB    NOT NULL UNIQUE,                       -- 32-byte tx hash
            block_num INTEGER GENERATED ALWAYS AS (tx_num >> 16) VIRTUAL,
            tx_idx    INTEGER GENERATED ALWAYS AS (tx_num & 0xFFFF) VIRTUAL,
            data      BLOB    NOT NULL,
            traces    BLOB,
            codec     INTEGER NOT NULL DEFAULT 0
          ) WITHOUT ROWID;
      
          -- KV store --------------------------------------------------------
          CREATE TABLE IF NOT EXISTS kv_int (
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL,
            codec INTEGER NOT NULL DEFAULT 0
          ) WITHOUT ROWID;
        `);
    }

    private initPragmas(isReadonly: boolean) {
        // 8 KiB pages = good balance for sequential writes & mmap reads
        this.db.pragma('page_size = 8192');

        if (!isReadonly) {
            // *** WRITER: fire-and-forget speed ***
            this.db.pragma('journal_mode      = WAL');         // enables concurrent readers
            this.db.pragma('synchronous       = OFF');         // lose at most one commit on crash
            this.db.pragma('wal_autocheckpoint = 20000');      // ~80 MB before checkpoint pause
            this.db.pragma('mmap_size         = 0');           // writer gains nothing from mmap
            this.db.pragma('cache_size        = -262144');     // 256 MiB page cache
            this.db.pragma('temp_store        = MEMORY');      // keep temp B-trees off disk
        } else {
            // *** READER: turbo random look-ups ***
            this.db.pragma('query_only         = TRUE');       // hard-lock to read-only
            this.db.pragma('read_uncommitted   = TRUE');       // skip commit window wait
            this.db.pragma('mmap_size          = 1099511627776'); // 1 TB
            this.db.pragma('cache_size         = -1048576');   // 1 GiB page cache
            this.db.pragma('busy_timeout       = 0');          // fail fast if writer stalls
        }
    }

    getHasDebug(): number {
        const select = this.prepQuery('SELECT value, codec FROM kv_int WHERE key = ?');
        const result = select.get('hasDebug') as { value: number, codec: number } | undefined;
        if (result && result.codec !== 0) throw new Error(`Unsupported codec ${result.codec} for hasDebug`);
        return result?.value ?? -1;
    }

    setHasDebug(hasDebug: boolean) {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('hasDebug', hasDebug ? 1 : 0, 0);
    }

    getTxBatch(greaterThanTxNum: number, limit: number): { txs: StoredTx[], traces: RpcTraceResult[] | undefined } {
        const selectTxs = this.prepQuery(`
            SELECT tx_num, data, traces, codec 
            FROM txs 
            WHERE tx_num > ? 
            ORDER BY tx_num ASC 
            LIMIT ?
        `);

        const rows = selectTxs.all(greaterThanTxNum, limit) as Array<{
            tx_num: number;
            data: Buffer;
            traces: Buffer | null;
            codec: number;
        }>;

        const txs: StoredTx[] = [];
        const traces: RpcTraceResult[] = [];

        for (const row of rows) {
            if (row.codec !== 0) {
                throw new Error(`Unsupported codec ${row.codec} for tx_num ${row.tx_num}`);
            }

            // Decompress and parse transaction data
            const decompressedTxData = zstdDecompress(row.data);
            const storedTx = JSON.parse(decompressedTxData.toString()) as StoredTx;
            txs.push(storedTx);

            // Handle traces if debug is enabled
            if (this.hasDebug) {
                if (!row.traces) {
                    throw new Error(`hasDebug is true but no trace found for tx_num ${row.tx_num}`);
                }
                const decompressedTraceData = zstdDecompress(row.traces);
                const trace = JSON.parse(decompressedTraceData.toString()) as RpcTraceResult;
                traces.push(trace);
            }
        }

        return {
            txs,
            traces: this.hasDebug ? traces : undefined
        };
    }

    /**
     * Fetches a complete block with all its transactions.
     * This method is marked as "slow" because it requires multiple database queries
     * and decompression operations to reassemble the full block structure.
     * 
     * @param blockIdentifier - Either a block number (number) or block hash (string with or without 0x prefix)
     * @returns The complete RpcBlock with all transactions, or null if block not found
     */
    slow_getBlockWithTransactions(blockIdentifier: number | string): RpcBlock | null {
        let blockNumber: number;
        let blockRow: { number: number; hash: Buffer; data: Buffer; codec: number } | undefined;

        if (typeof blockIdentifier === 'number') {
            // Query by block number
            blockNumber = blockIdentifier;
            const selectByNumber = this.prepQuery('SELECT number, hash, data, codec FROM blocks WHERE number = ?');
            blockRow = selectByNumber.get(blockNumber) as typeof blockRow;
        } else {
            // Query by block hash
            // Remove 0x prefix if present and convert to Buffer
            const hashStr = blockIdentifier.startsWith('0x') ? blockIdentifier.slice(2) : blockIdentifier;
            if (hashStr.length !== 64) {
                throw new Error(`Invalid block hash length: expected 64 hex chars, got ${hashStr.length}`);
            }
            const hashBuffer = Buffer.from(hashStr, 'hex');

            const selectByHash = this.prepQuery('SELECT number, hash, data, codec FROM blocks WHERE hash = ?');
            blockRow = selectByHash.get(hashBuffer) as typeof blockRow;

            if (blockRow) {
                blockNumber = blockRow.number;
            } else {
                // Block not found, return early
                return null;
            }
        }

        if (!blockRow) {
            return null;
        }

        if (blockRow.codec !== 0) {
            throw new Error(`Unsupported codec ${blockRow.codec} for block ${blockNumber}`);
        }

        // Decompress and parse the block data (without transactions)
        const decompressedBlockData = zstdDecompress(blockRow.data);
        const storedBlock = JSON.parse(decompressedBlockData.toString()) as Omit<RpcBlock, 'transactions'>;

        // Now fetch all transactions for this block
        // tx_num encoding: (block_num << 16) | tx_idx
        // For block N, tx_num range is [N << 16, (N << 16) | 0xFFFF]
        // This range covers all possible transaction indices (0 to 65535) for the block
        const minTxNum = blockNumber << 16;
        const maxTxNum = minTxNum | 0xFFFF;

        const selectTxs = this.prepQuery(`
            SELECT tx_num, data, codec 
            FROM txs 
            WHERE tx_num >= ? AND tx_num <= ?
            ORDER BY tx_num ASC
        `);

        const txRows = selectTxs.all(minTxNum, maxTxNum) as Array<{
            tx_num: number;
            data: Buffer;
            codec: number;
        }>;

        // Reconstruct the transactions array
        const transactions: RpcBlockTransaction[] = [];

        for (const txRow of txRows) {
            if (txRow.codec !== 0) {
                throw new Error(`Unsupported codec ${txRow.codec} for tx_num ${txRow.tx_num}`);
            }

            // Decompress and parse transaction data
            const decompressedTxData = zstdDecompress(txRow.data);
            const storedTx = JSON.parse(decompressedTxData.toString()) as StoredTx;

            // Extract the RpcBlockTransaction from StoredTx
            transactions.push(storedTx.tx);
        }

        // Reassemble the complete RpcBlock
        const fullBlock: RpcBlock = {
            ...storedBlock,
            transactions
        };

        return fullBlock;
    }
}
