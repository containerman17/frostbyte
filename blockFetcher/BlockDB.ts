import Database from 'better-sqlite3';
import { StoredBlock } from './BatchRpc';
import { encodeLazyBlock, LazyBlock } from './lazy/LazyBlock';
import { encodeLazyTx, LazyTx } from './lazy/LazyTx';
import { compressSync as lz4CompressSync, uncompressSync as lz4UncompressSync } from 'lz4-napi';
import { LazyTraces, encodeLazyTraces } from './lazy/LazyTrace';

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

    getBlocks(start: number, maxTransactions: number): { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[] {
        const totalStart = performance.now();

        // Fetch blocks with a reasonable limit
        const selectBlocks = this.prepQuery(`
            SELECT id, data, traces 
            FROM blocks 
            WHERE id >= ? 
            ORDER BY id 
            LIMIT 10010
        `);
        const blockRows = selectBlocks.all(start) as { id: number, data: Buffer, traces: Buffer | null }[];

        if (blockRows.length === 0) {
            return [];
        }

        const blockIds = blockRows.map(row => row.id);

        // Fetch all transactions for these blocks in one query
        const selectTxs = this.prepQuery(`
            SELECT block_id, tx_ix, data 
            FROM txs 
            WHERE block_id IN (${blockIds.map(() => '?').join(',')})
            ORDER BY block_id, tx_ix
        `);
        const txRows = selectTxs.all(...blockIds) as { block_id: number, tx_ix: number, data: Buffer }[];

        // Group transactions by block
        const txsByBlock = new Map<number, { tx_ix: number, data: Buffer }[]>();
        for (const tx of txRows) {
            if (!txsByBlock.has(tx.block_id)) {
                txsByBlock.set(tx.block_id, []);
            }
            txsByBlock.get(tx.block_id)!.push(tx);
        }

        // Process blocks in order, respecting transaction limit
        const result: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[] = [];
        let totalTxs = 0;
        console.log(`Queryies took ${performance.now() - totalStart}ms`);

        for (const blockRow of blockRows) {
            const blockTxs = txsByBlock.get(blockRow.id) || [];

            // Check if adding this block would exceed the transaction limit
            if (totalTxs + blockTxs.length > maxTransactions && result.length > 0) {
                break;
            }

            // Decode block
            const decompressedBlockData = lz4UncompressSync(blockRow.data);
            const block = new LazyBlock(decompressedBlockData);

            // Decode traces if hasDebug
            let traces: LazyTraces | undefined = undefined;
            if (this.hasDebug) {
                if (!blockRow.traces) {
                    throw new Error(`hasDebug is true but no traces found for block ${blockRow.id}`);
                }
                const decompressedTracesData = lz4UncompressSync(blockRow.traces);
                traces = new LazyTraces(decompressedTracesData);
            }

            // Decode transactions
            const txs: LazyTx[] = [];
            for (const txRow of blockTxs) {
                const decompressedTxData = lz4UncompressSync(txRow.data);
                txs.push(new LazyTx(decompressedTxData));
            }

            result.push({ block, txs, traces });
            totalTxs += blockTxs.length;

            if (totalTxs >= maxTransactions) {
                break;
            }
        }

        const totalTime = performance.now() - totalStart;
        console.log(`getBlocks(${start}, max ${maxTransactions} txs): got ${totalTxs} txs in ${result.length} blocks, total=${Math.round(totalTime)}ms`);

        return result;
    }

    getEvmChainId(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('evm_chain_id') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    setEvmChainId(chainId: number) {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)');
        upsert.run('evm_chain_id', chainId);
    }

    getLastStoredBlockNumber(): number {
        const selectMax = this.prepQuery('SELECT MAX(id) as max_id FROM blocks');
        const result = selectMax.get() as { max_id: number | null } | undefined;
        const result2 = result?.max_id ?? -1;
        return result2;
    }

    storeBlocks(batch: StoredBlock[]) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        if (batch.length === 0) return;

        let lastStoredBlockNum = this.getLastStoredBlockNumber();

        const insertMany = this.db.transaction((batch: StoredBlock[]) => {
            for (let i = 0; i < batch.length; i++) {
                const block = batch[i]!;
                if (Number(block.block.number) !== lastStoredBlockNum + 1) {
                    throw new Error(`Batch not sorted or has gaps: expected ${lastStoredBlockNum + 1}, got ${Number(block.block.number)}`);
                }
                this.storeBlock(block);
                lastStoredBlockNum++;
            }
        });
        insertMany(batch);
    }

    getBlock(n: number): LazyBlock {
        const selectBlock = this.prepQuery('SELECT data FROM blocks WHERE id = ?');
        const result = selectBlock.get(n) as { data: Buffer } | undefined;
        if (!result) throw new Error(`Block ${n} not found`);

        // Decompress the data
        const decompressedData = lz4UncompressSync(result.data);
        return new LazyBlock(decompressedData);
    }

    getTx(n: number, ix: number): LazyTx {
        const selectTx = this.prepQuery('SELECT data FROM txs WHERE block_id = ? AND tx_ix = ?');
        const result = selectTx.get(n, ix) as { data: Buffer } | undefined;
        if (!result) throw new Error(`Tx ${n}:${ix} not found`);

        // Decompress the data
        const decompressedData = lz4UncompressSync(result.data);
        return new LazyTx(decompressedData);
    }

    getBlockWithTransactions(blockNumber: number): { block: LazyBlock, txs: LazyTx[] } {
        // Get the block first
        const block = this.getBlock(blockNumber);

        // Query all transactions for this block at once, ordered by tx_ix
        const selectTxs = this.prepQuery('SELECT data FROM txs WHERE block_id = ? ORDER BY tx_ix');
        const results = selectTxs.all(blockNumber) as { data: Buffer }[];

        if (results.length !== block.transactionCount) {
            throw new Error(`Expected ${block.transactionCount} transactions for block ${blockNumber}, but found ${results.length}`);
        }

        // Decompress and create LazyTx objects
        const txs: LazyTx[] = [];
        for (const result of results) {
            const decompressedData = lz4UncompressSync(result.data);
            txs.push(new LazyTx(decompressedData));
        }

        return { block, txs };
    }

    getBlockTransactions(blockNumber: number): LazyTx[] {
        return this.getBlockWithTransactions(blockNumber).txs;
    }

    setBlockchainLatestBlockNum(blockNumber: number) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)');
        upsert.run('blockchain_latest_block', blockNumber);
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

    private storeBlock(b: StoredBlock) {
        // Validate hasDebug flag vs traces presence
        if (this.hasDebug && b.traces === undefined) {
            throw new Error('hasDebug is true but StoredBlock.traces is undefined');
        }
        if (!this.hasDebug && b.traces !== undefined) {
            throw new Error('hasDebug is false but StoredBlock.traces is defined');
        }

        const insertBlock = this.prepQuery('INSERT INTO blocks(id, data, traces) VALUES (?, ?, ?)');
        const insertTx = this.prepQuery('INSERT INTO txs(block_id, tx_ix, data) VALUES (?, ?, ?)');

        const blockNumber = Number(b.block.number);

        // Compress block data before storing
        const blockData = encodeLazyBlock(b.block);
        const compressedBlockData = lz4CompressSync(Buffer.from(blockData));

        // Handle traces if hasDebug is true
        let compressedTracesData: Buffer | null = null;
        if (this.hasDebug && b.traces) {
            // Encode all traces into a single buffer with one RLP call
            const tracesData = encodeLazyTraces(b.traces);
            compressedTracesData = lz4CompressSync(Buffer.from(tracesData));
        }

        insertBlock.run(blockNumber, compressedBlockData, compressedTracesData);

        for (let i = 0; i < b.block.transactions.length; ++i) {
            const tx = b.block.transactions[i]!;
            const receipt = b.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Compress transaction data before storing
            const txData = encodeLazyTx(tx, receipt);
            const compressedTxData = lz4CompressSync(Buffer.from(txData));
            insertTx.run(blockNumber, i, compressedTxData);
        }
    }

    private initSchema() {
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS blocks (
        id     INTEGER PRIMARY KEY,
        data   BLOB NOT NULL,
        traces BLOB
      ) WITHOUT ROWID;

      CREATE TABLE IF NOT EXISTS txs (
        block_id INTEGER NOT NULL,
        tx_ix    INTEGER NOT NULL,
        data     BLOB NOT NULL,
        PRIMARY KEY (block_id, tx_ix)
      ) WITHOUT ROWID;

      CREATE TABLE IF NOT EXISTS kv_int (
        key   TEXT PRIMARY KEY,
        value INTEGER NOT NULL
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
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('hasDebug') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    setHasDebug(hasDebug: boolean) {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value) VALUES (?, ?)');
        upsert.run('hasDebug', hasDebug ? 1 : 0);
    }
}
