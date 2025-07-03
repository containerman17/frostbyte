import { BlockDB } from "./blockFetcher/BlockDB"

const dbPath = "./database/C-Chain/blocks_no_dbg.db"
const db = new BlockDB({ path: dbPath, isReadonly: true, hasDebug: false })
const lastIndexedBlock = db.getLastStoredBlockNumber();
let lastBlock = 0;

const SCAN_BLOCKS = 1000;
const maxBlocks = Math.min(lastIndexedBlock, SCAN_BLOCKS);

while (lastBlock < maxBlocks) {
    const blocks = db.getBlocks(lastBlock, maxBlocks);
    if (blocks.length === 0) {
        break;
    }
    lastBlock = blocks[blocks.length - 1]!.block.number;
    console.log(lastBlock, '/', lastIndexedBlock);
}
