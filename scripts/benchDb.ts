import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import sqlite3 from 'better-sqlite3';
import { BlocksDBHelper } from '../blockFetcher/BlocksDBHelper.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Read chain config
const chainsData = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/chains.json'), 'utf-8'));
const { blockchainId } = chainsData[0];

const dbPath = path.join(__dirname, `../data/${blockchainId}/b_ndbg.db`);

const db = new sqlite3(dbPath, { readonly: true });

db.pragma('journal_mode = WAL');

db.pragma('query_only = ON');
db.pragma('cache_size = -32768'); // 32MB cache per database
db.pragma('mmap_size = 2199023255552'); // 2TB mmap
db.pragma('temp_store = MEMORY');

const blockHelper = new BlocksDBHelper(db, true, false);

console.log('Chain id: ', blockchainId);
console.log('Txs: ', blockHelper.getTxCount());

let batchesCount = 0;
const start = performance.now();
let lastIndexedTx = -1;
for (let i = 0; i < 100000; i++) {
    const { txs } = blockHelper.getTxBatch(lastIndexedTx, 10000, false, undefined);
    if (txs.length === 0) {
        break;
    }
    // Use the txNum from the last transaction in the batch
    lastIndexedTx = txs[txs.length - 1]!.txNum;
    batchesCount++;
}
const finish = performance.now();
console.log(`Batches count: ${batchesCount}`);
console.log(`Time taken: ${finish - start}ms`);

// Chain id:  HUwWdyoExrb1HgVp5X5sh3AWqhYFnKkfXBfGmGL3qjDsnMoR4
// Txs:  1004620
// Batches count: 101
// Time taken: 8901.959251999999ms
