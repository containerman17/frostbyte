import Database from 'better-sqlite3';
import { RpcBlock, RpcBlockTransaction, RpcTxReceipt, RpcTraceResult, StoredTx } from './evmTypes.js';
import { compress as zstdCompress, decompress as zstdDecompress, Compressor as ZstdCompressor, Decompressor as ZstdDecompressor } from 'zstd-napi';
import { StoredBlock } from './BatchRpc.js';
import { tmpdir } from 'os';
import { join } from 'path';
import { rm, mkdir, writeFile } from 'fs/promises';

export class BlockDB {
    private db: InstanceType<typeof Database>;
    private prepped: Map<string, any>;
    private isReadonly: boolean;
    private hasDebug: boolean;
    private blockDict: Buffer | null = null;
    private txDict: Buffer | null = null;
    private blockCompressor: ZstdCompressor;
    private blockDecompressor: ZstdDecompressor;
    private txCompressor: ZstdCompressor;
    private txDecompressor: ZstdDecompressor;
    private blocksDictTrainingLock: boolean = false;
    private txsDictTrainingLock: boolean = false;

    constructor({ path, isReadonly, hasDebug }: { path: string, isReadonly: boolean, hasDebug: boolean }) {
        this.db = new Database(path, {
            readonly: isReadonly,
        });
        this.isReadonly = isReadonly;
        this.prepped = new Map();
        if (!isReadonly) {
            this.initSchema();
        }
        this.initPragmas(isReadonly);

        this.blockCompressor = new ZstdCompressor();
        this.blockDecompressor = new ZstdDecompressor();
        this.txCompressor = new ZstdCompressor();
        this.txDecompressor = new ZstdDecompressor();

        // Load any stored dictionaries
        try {
            const selectDicts = this.db.prepare('SELECT name, dict FROM dictionaries');
            const rows = selectDicts.all() as Array<{ name: string; dict: Buffer }>;
            for (const row of rows) {
                if (row.name === 'blocks') this.blockDict = row.dict;
                else if (row.name === 'txs') this.txDict = row.dict;
            }
        } catch {
            // Table may not exist on older databases
        }
        if (this.blockDict) {
            this.blockCompressor.loadDictionary(this.blockDict);
            this.blockDecompressor.loadDictionary(this.blockDict);
        }
        if (this.txDict) {
            this.txCompressor.loadDictionary(this.txDict);
            this.txDecompressor.loadDictionary(this.txDict);
        }

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

    getIsCaughtUp(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('is_caught_up') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    setIsCaughtUp(isCaughtUp: boolean) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('is_caught_up', isCaughtUp ? 1 : 0, 0);
    }

    /**
     * Checks if the chain has caught up by comparing stored blocks with blockchain latest block
     */
    checkAndUpdateCatchUpStatus(): boolean {
        if (this.isReadonly) return false;

        const lastStoredBlock = this.getLastStoredBlockNumber();
        const latestBlockchain = this.getBlockchainLatestBlockNum();
        const isCaughtUp = lastStoredBlock >= latestBlockchain;

        if (isCaughtUp && this.getIsCaughtUp() !== 1) {
            // Chain just caught up - trigger post-catch-up maintenance
            console.log('BlockDB: Chain caught up! Triggering post-catch-up maintenance...');
            this.setIsCaughtUp(true);
            this.performPostCatchUpMaintenance();
            return true;
        }

        return false;
    }

    /**
     * Step 2: Post-catch-up maintenance - flush & zero the big WAL, set limits
     */
    performPostCatchUpMaintenance(): void {
        if (this.isReadonly) throw new Error('BlockDB is readonly');

        console.log('BlockDB: Starting post-catch-up maintenance...');
        const start = performance.now();

        // Flush & zero the big WAL
        this.db.pragma('wal_checkpoint(TRUNCATE)');

        // Set future WAL limits
        this.db.pragma('journal_size_limit = 67108864'); // 64 MB
        this.db.pragma('wal_autocheckpoint = 10000');    // merge every ~40 MB

        const end = performance.now();
        console.log(`BlockDB: Post-catch-up maintenance completed in ${Math.round(end - start)}ms`);
    }

    /**
     * Step 3: Periodic maintenance during idle gaps - reclaim space incrementally
     */
    performPeriodicMaintenance(): void {
        if (this.isReadonly) throw new Error('BlockDB is readonly');

        const isCaughtUp = this.getIsCaughtUp();
        if (isCaughtUp !== 1) return; // Only do this after catch-up

        const start = performance.now();

        // Reclaim ≤ 4 MB; finishes in ms
        this.db.pragma('incremental_vacuum(1000)');

        const end = performance.now();
        if (end - start > 10) { // Only log if it took more than 10ms
            console.log(`BlockDB: Periodic maintenance completed in ${Math.round(end - start)}ms`);
        }

        // Check if dictionary training or recompression is needed
        this.performDictionaryMaintenance();
    }

    /**
     * Dictionary maintenance - training and recompression
     */
    private performDictionaryMaintenance(): void {
        if (this.isReadonly) return;

        // Check if we need to train a dictionary
        if (this.shouldTrainDictionary()) {
            // Fire and forget - don't block maintenance
            this.selectAndStoreDictionaryTrainingSamples().catch(error => {
                console.error('BlockDB: Dictionary training failed:', error);
                process.exit(1);
            });
        }

        // Check if we need to train tx dictionary
        if (this.shouldTrainTxDictionary()) {
            this.selectAndStoreTxDictionaryTrainingSamples().catch(error => {
                console.error('BlockDB: Transaction dictionary training failed:', error);
                process.exit(1);
            });
        }

        // Check if we need to recompress blocks
        if (this.blockDict && !this.isRecompressionComplete()) {
            this.recompressBlocksWithDictionary();
        }

        // Check if we need to recompress transactions
        if (this.txDict && !this.isTxRecompressionComplete()) {
            this.recompressTxsWithDictionary();
        }
    }

    private shouldTrainDictionary(): boolean {
        return false; //TODO: debug and bring back
        // Check conditions for dictionary training
        // const blockCount = this.getLastStoredBlockNumber();
        // const hasDictionary = this.getDictionary('blocks') !== undefined;

        // // Don't train if we already have a dictionary or training is in progress
        // if (this.blocksDictTrainingLock || hasDictionary) {
        //     return false;
        // }

        // return blockCount > 10000;
    }

    private shouldTrainTxDictionary(): boolean {
        return false; //TODO: debug and bring back
        // const txCount = this.getTxCount();
        // const hasDictionary = this.getDictionary('txs') !== undefined;

        // // Don't train if we already have a dictionary or training is in progress
        // if (this.txsDictTrainingLock || hasDictionary) {
        //     return false;
        // }

        // return txCount > 10000;
    }

    private async selectAndStoreDictionaryTrainingSamples(): Promise<void> {
        console.log('BlockDB: Starting automatic dictionary training...');
        this.blocksDictTrainingLock = true;

        const { execSync } = await import('child_process');
        const { readFileSync, unlinkSync } = await import('fs');

        // Check if zstd is installed
        try {
            execSync('zstd --version', { stdio: 'ignore' });
        } catch {
            throw new Error('zstd is not installed. Install with: apt-get install zstd (Ubuntu) or brew install zstd (macOS)');
        }

        // Create a unique temp directory for this training session
        const tempDir = join(tmpdir(), `frostbyte-dict-blocks-${Date.now()}`);
        const dictPath = join(tmpdir(), `blocks-dict-${Date.now()}.zstd`);

        try {
            const totalBlocks = this.getLastStoredBlockNumber();
            console.log(`BlockDB: Training dictionary with ${totalBlocks} blocks...`);

            // Step 1: Export samples
            console.log(`BlockDB: Exporting samples to ${tempDir}...`);
            await this.exportDictionaryTrainingSamples(tempDir);

            // Step 2: Train dictionary using zstd
            console.log('BlockDB: Training dictionary...');
            const cmd = `zstd --train -r "${tempDir}" -o "${dictPath}" --maxdict=262144`;
            execSync(cmd, { stdio: 'inherit' });

            // Step 3: Load dictionary into database
            console.log('BlockDB: Loading dictionary...');
            const dictionary = readFileSync(dictPath);
            this.setDictionary('blocks', dictionary);

            // Clean up
            await rm(tempDir, { recursive: true, force: true });
            unlinkSync(dictPath);

            console.log('BlockDB: Dictionary training completed successfully!');
        } catch (error) {
            // Clean up on error
            await rm(tempDir, { recursive: true, force: true }).catch(() => { });
            if (dictPath) {
                try { unlinkSync(dictPath); } catch { }
            }

            console.error('BlockDB: Dictionary training failed:', error);
            throw error;
        } finally {
            this.blocksDictTrainingLock = false;
        }
    }

    async exportDictionaryTrainingSamples(outputPath: string): Promise<void> {
        const totalBlocks = this.getLastStoredBlockNumber();
        if (totalBlocks < 10000) {
            throw new Error(`Not enough blocks for dictionary training. Have ${totalBlocks}, need at least 10,000`);
        }

        // Clean directory and recreate
        await rm(outputPath, { recursive: true, force: true });
        await mkdir(outputPath, { recursive: true });

        // Calculate sample blocks
        const targetSamples = 10000;
        const interval = Math.max(1, Math.floor(totalBlocks / targetSamples));
        const samples: number[] = [];
        for (let i = 0; i <= totalBlocks && samples.length < targetSamples; i += interval) {
            samples.push(i);
        }

        // Select raw compressed blocks from database
        const selectBlock = this.prepQuery('SELECT data, codec FROM blocks WHERE number = ?');
        let exportedCount = 0;

        console.log(`BlockDB: Exporting ${samples.length} samples (every ${interval}th block) to ${outputPath}...`);

        for (const blockNum of samples) {
            const row = selectBlock.get(blockNum) as { data: Buffer; codec: number } | undefined;
            if (row) {
                // Decompress the raw block data (but don't parse as JSON)
                let decompressedData: Buffer;
                if (row.codec === 0) {
                    decompressedData = zstdDecompress(row.data);
                } else if (row.codec === 1 && this.blockDict) {
                    decompressedData = this.blockDecompressor.decompress(row.data);
                } else {
                    throw new Error(`Unsupported codec ${row.codec} for block ${blockNum}`);
                }

                // Save raw decompressed data for dictionary training
                await writeFile(join(outputPath, `block_${blockNum}.raw`), decompressedData);
                exportedCount++;
            }
        }

        console.log(`BlockDB: Exported ${exportedCount} raw block samples to ${outputPath}`);
        console.log(`BlockDB: Next step - train dictionary: zstd --train -r "${outputPath}" -o blocks.dict --maxdict=262144`);
    }

    private async selectAndStoreTxDictionaryTrainingSamples(): Promise<void> {
        console.log('BlockDB: Starting automatic transaction dictionary training...');
        this.txsDictTrainingLock = true;

        const { execSync } = await import('child_process');
        const { readFileSync, unlinkSync } = await import('fs');

        // Check if zstd is installed
        try {
            execSync('zstd --version', { stdio: 'ignore' });
        } catch {
            throw new Error('zstd is not installed. Install with: apt-get install zstd (Ubuntu) or brew install zstd (macOS)');
        }

        // Create a unique temp directory for this training session
        const tempDir = join(tmpdir(), `frostbyte-dict-txs-${Date.now()}`);
        const dictPath = join(tmpdir(), `txs-dict-${Date.now()}.zstd`);

        try {
            const totalTxs = this.getTxCount();
            console.log(`BlockDB: Training transaction dictionary with ${totalTxs} transactions...`);

            // Step 1: Export samples
            console.log(`BlockDB: Exporting transaction samples to ${tempDir}...`);
            await this.exportTxDictionaryTrainingSamples(tempDir);

            // Step 2: Train dictionary using zstd
            console.log('BlockDB: Training transaction dictionary...');
            const cmd = `zstd --train -r "${tempDir}" -o "${dictPath}" --maxdict=262144`;
            execSync(cmd, { stdio: 'inherit' });

            // Step 3: Load dictionary into database
            console.log('BlockDB: Loading transaction dictionary...');
            const dictionary = readFileSync(dictPath);
            this.setDictionary('txs', dictionary);

            // Clean up
            await rm(tempDir, { recursive: true, force: true });
            unlinkSync(dictPath);

            console.log('BlockDB: Transaction dictionary training completed successfully!');
        } catch (error) {
            // Clean up on error
            await rm(tempDir, { recursive: true, force: true }).catch(() => { });
            if (dictPath) {
                try { unlinkSync(dictPath); } catch { }
            }

            console.error('BlockDB: Transaction dictionary training failed:', error);
            throw error;
        } finally {
            this.txsDictTrainingLock = false;
        }
    }

    async exportTxDictionaryTrainingSamples(outputPath: string): Promise<void> {
        const totalTxs = this.getTxCount();
        if (totalTxs < 10000) {
            throw new Error(`Not enough transactions for dictionary training. Have ${totalTxs}, need at least 10,000`);
        }

        // Clean directory and recreate
        await rm(outputPath, { recursive: true, force: true });
        await mkdir(outputPath, { recursive: true });

        // Calculate sample transactions - we want 5000 tx samples (which will give us up to 10000 files with traces)
        const targetSamples = 5000;
        const interval = Math.max(1, Math.floor(totalTxs / targetSamples));

        // Get the max tx_num to know the range
        const getMaxTxNum = this.prepQuery('SELECT MAX(tx_num) as max_tx_num FROM txs');
        const maxTxNumRow = getMaxTxNum.get() as { max_tx_num: number } | undefined;
        if (!maxTxNumRow || maxTxNumRow.max_tx_num === null) {
            throw new Error('No transactions found in database');
        }
        const maxTxNum = maxTxNumRow.max_tx_num;

        // Select raw compressed transaction data from database
        const selectTx = this.prepQuery('SELECT data, traces, codec FROM txs WHERE tx_num = ?');
        let exportedCount = 0;

        console.log(`BlockDB: Exporting transaction samples (every ${interval}th transaction) to ${outputPath}...`);

        // Sample evenly across the tx_num range
        for (let i = 0; i <= maxTxNum && exportedCount < targetSamples; i += interval) {
            const row = selectTx.get(i) as { data: Buffer; traces: Buffer | null; codec: number } | undefined;
            if (row) {
                // Decompress the transaction data (but don't parse as JSON)
                let decompressedTxData: Buffer;
                if (row.codec === 0) {
                    decompressedTxData = zstdDecompress(row.data);
                } else if (row.codec === 1 && this.txDict) {
                    decompressedTxData = this.txDecompressor.decompress(row.data);
                } else {
                    throw new Error(`Unsupported codec ${row.codec} for tx_num ${i}`);
                }

                // Save raw decompressed tx data for dictionary training
                await writeFile(join(outputPath, `tx_${i}.raw`), decompressedTxData);

                // Also save trace data if available
                if (this.hasDebug && row.traces) {
                    let decompressedTraceData: Buffer;
                    if (row.codec === 0) {
                        decompressedTraceData = zstdDecompress(row.traces);
                    } else if (row.codec === 1 && this.txDict) {
                        decompressedTraceData = this.txDecompressor.decompress(row.traces);
                    } else {
                        throw new Error(`Unsupported codec ${row.codec} for trace at tx_num ${i}`);
                    }
                    await writeFile(join(outputPath, `trace_${i}.raw`), decompressedTraceData);
                }

                exportedCount++;
            }
        }

        console.log(`BlockDB: Exported ${exportedCount} transaction samples (and their traces if available) to ${outputPath}`);
    }

    private getLastRecompressedBlock(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('last_recompressed_block') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    private setLastRecompressedBlock(blockNumber: number): void {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('last_recompressed_block', blockNumber, 0);
    }

    private isRecompressionComplete(): boolean {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('recompression_complete') as { value: number } | undefined;
        return result?.value === 1;
    }

    private setRecompressionComplete(complete: boolean): void {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('recompression_complete', complete ? 1 : 0, 0);
    }

    private getLastRecompressedTx(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('last_recompressed_tx') as { value: number } | undefined;
        return result?.value ?? -1;
    }

    private setLastRecompressedTx(txNum: number): void {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('last_recompressed_tx', txNum, 0);
    }

    private isTxRecompressionComplete(): boolean {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('tx_recompression_complete') as { value: number } | undefined;
        return result?.value === 1;
    }

    private setTxRecompressionComplete(complete: boolean): void {
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('tx_recompression_complete', complete ? 1 : 0, 0);
    }

    private recompressBlocksWithDictionary(): void {
        const start = performance.now();
        const lastRecompressed = this.getLastRecompressedBlock();
        const batchSize = 10000;

        // Count total blocks that need recompression
        const countQuery = this.prepQuery('SELECT COUNT(*) as count FROM blocks WHERE codec = 0');
        const totalToRecompress = (countQuery.get() as { count: number }).count;

        // Count how many we've already done
        const doneQuery = this.prepQuery('SELECT COUNT(*) as count FROM blocks WHERE codec = 1');
        const alreadyRecompressed = (doneQuery.get() as { count: number }).count;

        // Select blocks with codec=0 starting after the last recompressed block
        const selectBlocks = this.prepQuery(`
            SELECT number, hash, data 
            FROM blocks 
            WHERE number > ? AND codec = 0 
            ORDER BY number ASC 
            LIMIT ?
        `);

        type BlockRow = {
            number: number;
            hash: Buffer;
            data: Buffer;
        };

        const blocks = selectBlocks.all(lastRecompressed, batchSize) as BlockRow[];

        if (blocks.length === 0) {
            // No more blocks to recompress
            this.setRecompressionComplete(true);
            console.log('BlockDB: Dictionary recompression complete');
            return;
        }

        const updateBlock = this.prepQuery('UPDATE blocks SET data = ?, codec = ? WHERE number = ?');

        const recompressMany = this.db.transaction((blocks: BlockRow[]) => {
            for (const block of blocks) {
                // Decompress with old method
                const decompressed = zstdDecompress(block.data);

                // Recompress with dictionary
                const recompressed = this.blockCompressor.compress(decompressed);

                // Update the block
                updateBlock.run(recompressed, 1, block.number);
            }

            // Update last recompressed block
            const lastBlock = blocks[blocks.length - 1];
            if (lastBlock) {
                this.setLastRecompressedBlock(lastBlock.number);
            }
        });

        recompressMany(blocks);

        const end = performance.now();
        const totalProcessed = alreadyRecompressed + blocks.length;
        const totalBlocks = totalToRecompress + alreadyRecompressed;
        console.log(`BlockDB: Recompressed ${blocks.length} blocks with dictionary in ${Math.round(end - start)}ms (${totalProcessed}/${totalBlocks} total)`);
    }

    private recompressTxsWithDictionary(): void {
        const start = performance.now();
        const lastRecompressed = this.getLastRecompressedTx();
        const batchSize = 10000;

        // Count total transactions that need recompression
        const countQuery = this.prepQuery('SELECT COUNT(*) as count FROM txs WHERE codec = 0');
        const totalToRecompress = (countQuery.get() as { count: number }).count;

        // Count how many we've already done
        const doneQuery = this.prepQuery('SELECT COUNT(*) as count FROM txs WHERE codec = 1');
        const alreadyRecompressed = (doneQuery.get() as { count: number }).count;

        // Select transactions with codec=0 starting after the last recompressed tx
        const selectTxs = this.prepQuery(`
            SELECT tx_num, data, traces 
            FROM txs 
            WHERE tx_num > ? AND codec = 0 
            ORDER BY tx_num ASC 
            LIMIT ?
        `);

        type TxRow = {
            tx_num: number;
            data: Buffer;
            traces: Buffer | null;
        };

        const txs = selectTxs.all(lastRecompressed, batchSize) as TxRow[];

        if (txs.length === 0) {
            // No more transactions to recompress
            this.setTxRecompressionComplete(true);
            console.log('BlockDB: Transaction dictionary recompression complete');
            return;
        }

        const updateTx = this.prepQuery('UPDATE txs SET data = ?, codec = ? WHERE tx_num = ?');

        const recompressMany = this.db.transaction((txs: TxRow[]) => {
            for (const tx of txs) {
                // Decompress with old method
                const decompressed = zstdDecompress(tx.data);

                // Recompress with dictionary
                const recompressed = this.txCompressor.compress(decompressed);

                // Update the transaction data
                updateTx.run(recompressed, 1, tx.tx_num);

                // If there are traces, recompress them too
                if (tx.traces) {
                    const updateTraces = this.prepQuery('UPDATE txs SET traces = ? WHERE tx_num = ?');
                    const decompressedTrace = zstdDecompress(tx.traces);
                    const recompressedTrace = this.txCompressor.compress(decompressedTrace);
                    updateTraces.run(recompressedTrace, tx.tx_num);
                }
            }

            // Update last recompressed tx
            const lastTx = txs[txs.length - 1];
            if (lastTx) {
                this.setLastRecompressedTx(lastTx.tx_num);
            }
        });

        recompressMany(txs);

        const end = performance.now();
        const totalProcessed = alreadyRecompressed + txs.length;
        const totalTxs = totalToRecompress + alreadyRecompressed;
        console.log(`BlockDB: Recompressed ${txs.length} transactions with dictionary in ${Math.round(end - start)}ms (${totalProcessed}/${totalTxs} total)`);
    }

    getLastStoredBlockNumber(): number {
        const selectMax = this.prepQuery('SELECT MAX(number) as max_number FROM blocks');
        const result = selectMax.get() as { max_number: number | null } | undefined;
        const result2 = result?.max_number ?? -1;
        return result2;
    }

    getTxCount(): number {
        const select = this.prepQuery('SELECT value FROM kv_int WHERE key = ?');
        const result = select.get('tx_count') as { value: number } | undefined;
        return result?.value ?? 0;
    }

    private setTxCount(count: number) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        const upsert = this.prepQuery('INSERT OR REPLACE INTO kv_int (key, value, codec) VALUES (?, ?, ?)');
        upsert.run('tx_count', count, 0);
    }

    storeBlocks(batch: StoredBlock[]) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        if (batch.length === 0) return;

        let lastStoredBlockNum = this.getLastStoredBlockNumber();
        let totalTxCount = 0;

        // Calculate total transactions in the batch
        for (const block of batch) {
            totalTxCount += block.block.transactions.length;
        }

        const insertMany = this.db.transaction((batch: StoredBlock[]) => {
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
        const compressedBlockData = this.blockDict
            ? this.blockCompressor.compress(Buffer.from(blockJsonStr))
            : zstdCompress(Buffer.from(blockJsonStr));
        const blockCodec = this.blockDict ? 1 : 0;

        // Extract block hash (remove '0x' prefix and convert to Buffer)
        const blockHash = Buffer.from(storedBlock.block.hash.slice(2), 'hex');

        insertBlock.run(blockNumber, blockHash, compressedBlockData, blockCodec);

        // Store transactions with their receipts and traces
        for (let i = 0; i < storedBlock.block.transactions.length; ++i) {
            const tx = storedBlock.block.transactions[i]!;
            const receipt = storedBlock.receipts[tx.hash];
            if (!receipt) throw new Error(`Receipt not found for tx ${tx.hash}`);

            // Calculate tx_num using the formula: (block_num << 16) | tx_idx
            // Use BigInt to avoid 32-bit overflow in JavaScript bitwise operations
            const tx_num = Number((BigInt(blockNumber) << 16n) | BigInt(i));

            // Prepare transaction data
            const txData: StoredTx = {
                txNum: tx_num,//FIXME: coule be fetched from the column. double-storage here
                tx: tx,
                receipt: receipt,
                blockTs: Number(storedBlock.block.timestamp)
            };

            // Compress transaction data
            const txJsonStr = JSON.stringify(txData);
            const compressedTxData = this.txDict
                ? this.txCompressor.compress(Buffer.from(txJsonStr))
                : zstdCompress(Buffer.from(txJsonStr));

            // Simple codec: 0 = no dict, 1 = dict
            const txCodec = this.txDict ? 1 : 0;

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
                // Use the same txDict for trace compression
                compressedTraceData = this.txDict
                    ? this.txCompressor.compress(Buffer.from(traceJsonStr))
                    : zstdCompress(Buffer.from(traceJsonStr));
            }

            insertTx.run(tx_num, txHash, compressedTxData, compressedTraceData, txCodec);
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
            tx_num    INTEGER PRIMARY KEY,                           -- (block_num<<16)|tx_idx (6 bytes: 4 for block, 2 for tx)
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

          CREATE TABLE IF NOT EXISTS kv_string (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            codec INTEGER NOT NULL DEFAULT 0
          ) WITHOUT ROWID;

          CREATE TABLE IF NOT EXISTS dictionaries (
            name TEXT PRIMARY KEY,
            dict BLOB NOT NULL
          ) WITHOUT ROWID;
        `);
    }

    private initPragmas(isReadonly: boolean) {
        // 8 KiB pages = good balance for sequential writes & mmap reads
        this.db.pragma('page_size = 8192');

        if (!isReadonly) {
            // Step 1: Initial setup with incremental vacuum enabled
            const isCaughtUp = this.getIsCaughtUp();
            if (isCaughtUp === -1) {
                // First time setup - enable incremental vacuum
                this.db.pragma('auto_vacuum = INCREMENTAL');
                this.db.pragma('VACUUM'); // builds the page map needed later
                console.log('BlockDB: Initial setup with incremental vacuum enabled');
            }

            // *** WRITER: fire-and-forget speed ***
            this.db.pragma('journal_mode      = WAL');         // enables concurrent readers
            this.db.pragma('synchronous       = OFF');         // lose at most one commit on crash

            // Set pragmas based on catch-up state
            if (isCaughtUp === 1) {
                // Step 2: Post-catch-up optimized settings
                this.db.pragma('journal_size_limit = 67108864'); // 64 MB WAL limit
                this.db.pragma('wal_autocheckpoint = 10000');    // merge every ~40 MB
                console.log('BlockDB: Using post-catch-up optimized settings');
            } else {
                // Pre-catch-up: larger WAL for bulk writes
                this.db.pragma('wal_autocheckpoint = 20000');    // ~80 MB before checkpoint pause
            }

            // Use default cache size (2000 pages = ~16 MB with 8 KiB pages)
            this.db.pragma('temp_store        = MEMORY');      // keep temp B-trees off disk
        } else {
            // *** READER: minimal memory footprint ***
            this.db.pragma('query_only         = TRUE');       // hard-lock to read-only
            this.db.pragma('read_uncommitted   = TRUE');       // skip commit window wait
            // Use default cache size (2000 pages = ~16 MB with 8 KiB pages)
            // this.db.pragma('busy_timeout       = 0');          // fail fast if writer stalls
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

    getDictionary(name: string): Buffer | undefined {
        const select = this.prepQuery('SELECT dict FROM dictionaries WHERE name = ?');
        const row = select.get(name) as { dict: Buffer } | undefined;
        return row?.dict;
    }

    setDictionary(name: string, data: Buffer) {
        if (this.isReadonly) throw new Error('BlockDB is readonly');
        const upsert = this.prepQuery('INSERT OR REPLACE INTO dictionaries(name, dict) VALUES (?, ?)');
        upsert.run(name, data);

        if (name === 'blocks') {
            this.blockDict = data;
            this.blockCompressor.loadDictionary(data);
            this.blockDecompressor.loadDictionary(data);
            console.log('BlockDB: Blocks dictionary loaded successfully');
        } else if (name === 'txs') {
            this.txDict = data;
            this.txCompressor.loadDictionary(data);
            this.txDecompressor.loadDictionary(data);
            console.log('BlockDB: Transactions dictionary loaded successfully');
        }
    }

    /**
     * Get direct access to the underlying database for custom queries.
     * USE WITH CAUTION: This bypasses all abstractions and safety checks.
     * @returns The better-sqlite3 database instance
     */
    getDatabase(): InstanceType<typeof Database> {
        return this.db;
    }

    getTxBatch(greaterThanTxNum: number, limit: number, includeTraces: boolean): { txs: StoredTx[], traces: RpcTraceResult[] | undefined } {
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
            let decompressedTxData: Buffer;
            let decompressedTraceData: Buffer | undefined;

            // Simple codec: 0 = no dict, 1 = dict (for both data and traces)
            if (row.codec === 0) {
                decompressedTxData = zstdDecompress(row.data);
                if (this.hasDebug && includeTraces && row.traces) {
                    decompressedTraceData = zstdDecompress(row.traces);
                }
            } else if (row.codec === 1) {
                if (!this.txDict) {
                    throw new Error(`Codec 1 requires tx dictionary but none loaded for tx_num ${row.tx_num}`);
                }
                decompressedTxData = this.txDecompressor.decompress(row.data);
                if (this.hasDebug && includeTraces && row.traces) {
                    // Use the same txDict for trace decompression
                    decompressedTraceData = this.txDecompressor.decompress(row.traces);
                }
            } else {
                // FATAL: codec should only be 0 or 1
                console.error(`FATAL: Invalid codec ${row.codec} for tx_num ${row.tx_num}. Database is corrupted!`);
                process.exit(1);
            }

            // Decompress and parse transaction data
            const storedTx = JSON.parse(decompressedTxData.toString()) as StoredTx;
            txs.push(storedTx);

            // Handle traces if both debug is enabled AND traces are requested
            if (this.hasDebug && includeTraces && decompressedTraceData) {
                const trace = JSON.parse(decompressedTraceData.toString()) as RpcTraceResult;
                traces.push(trace);
            }
        }

        return {
            txs,
            traces: this.hasDebug && includeTraces ? traces : undefined
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

        let decompressedBlockData: Buffer;
        if (blockRow.codec === 0) {
            decompressedBlockData = zstdDecompress(blockRow.data);
        } else if (blockRow.codec === 1 && this.blockDict) {
            decompressedBlockData = this.blockDecompressor.decompress(blockRow.data);
        } else {
            // FATAL: codec should only be 0 or 1
            console.error(`FATAL: Invalid codec ${blockRow.codec} for block ${blockNumber}. Database is corrupted!`);
            process.exit(1);
        }

        // Decompress and parse the block data (without transactions)
        const storedBlock = JSON.parse(decompressedBlockData.toString()) as Omit<RpcBlock, 'transactions'>;

        // Now fetch all transactions for this block
        // tx_num encoding: (block_num << 16) | tx_idx
        // For block N, tx_num range is [N << 16, (N << 16) | 0xFFFF]
        // This range covers all possible transaction indices (0 to 65535) for the block
        // Use BigInt to avoid 32-bit overflow in JavaScript bitwise operations
        const minTxNum = Number(BigInt(blockNumber) << 16n);
        const maxTxNum = Number((BigInt(blockNumber) << 16n) | 0xFFFFn);

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
            let decompressedTxData: Buffer;

            // Simple codec: 0 = no dict, 1 = dict
            if (txRow.codec === 0) {
                decompressedTxData = zstdDecompress(txRow.data);
            } else if (txRow.codec === 1) {
                if (!this.txDict) {
                    throw new Error(`Codec 1 requires tx dictionary but none loaded for tx_num ${txRow.tx_num}`);
                }
                decompressedTxData = this.txDecompressor.decompress(txRow.data);
            } else {
                // FATAL: codec should only be 0 or 1
                console.error(`FATAL: Invalid codec ${txRow.codec} for tx_num ${txRow.tx_num}. Database is corrupted!`);
                process.exit(1);
            }

            // Decompress and parse transaction data
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

    /**
     * Fetches the receipt for a transaction by hash.
     */
    getTxReceipt(txHash: string): RpcTxReceipt | null {
        const hashBuf = Buffer.from(txHash.replace(/^0x/, ''), 'hex');
        const select = this.prepQuery('SELECT data, codec FROM txs WHERE hash = ?');
        const row = select.get(hashBuf) as { data: Buffer; codec: number } | undefined;
        if (!row) return null;

        let decompressedTxData: Buffer;
        if (row.codec === 0) {
            decompressedTxData = zstdDecompress(row.data);
        } else if (row.codec === 1) {
            if (!this.txDict) {
                throw new Error(`Codec 1 requires tx dictionary but none loaded for tx ${txHash}`);
            }
            decompressedTxData = this.txDecompressor.decompress(row.data);
        } else {
            // FATAL: codec should only be 0 or 1
            console.error(`FATAL: Invalid codec ${row.codec} for tx ${txHash}. Database is corrupted!`);
            process.exit(1);
        }

        const storedTx = JSON.parse(decompressedTxData.toString()) as StoredTx;
        return storedTx.receipt;
    }

    /**
     * Fetches call traces for a block if available.
     */
    slow_getBlockTraces(blockNumber: number): RpcTraceResult[] {
        const select = this.prepQuery('SELECT traces, codec FROM txs WHERE block_num = ? ORDER BY tx_idx ASC');
        const rows = select.all(blockNumber) as Array<{ traces: Buffer | null; codec: number }>;
        const traces: RpcTraceResult[] = [];
        for (const row of rows) {
            if (!row.traces) continue;

            let decompressed: Buffer;
            if (row.codec === 0) {
                decompressed = zstdDecompress(row.traces);
            } else if (row.codec === 1) {
                if (!this.txDict) {
                    throw new Error(`Codec 1 requires tx dictionary but none loaded for block ${blockNumber}`);
                }
                // Use txDict for trace decompression
                decompressed = this.txDecompressor.decompress(row.traces);
            } else {
                // FATAL: codec should only be 0 or 1
                console.error(`FATAL: Invalid codec ${row.codec} for block ${blockNumber}. Database is corrupted!`);
                process.exit(1);
            }

            const trace = JSON.parse(decompressed.toString()) as RpcTraceResult;
            traces.push(trace);
        }
        return traces;
    }
}
