
import { BlocksDBHelper } from '../blockFetcher/BlocksDBHelper'
import sqlite3 from 'better-sqlite3'
import { StoredTx } from '../blockFetcher/evmTypes'
import fs from 'fs'
import chains from '../data/chains.json'

async function main() {
    const db = new BlocksDBHelper(
        new sqlite3(`data/${chains[0]!.blockchainId}/b_ndbg.db`),
        true,
        false)

    // Create directory if it doesn't exist
    if (!fs.existsSync('./data/dict_test/')) {
        fs.mkdirSync('./data/dict_test/', { recursive: true })
    }

    // Remove existing file if it exists
    if (fs.existsSync('./data/dict_test/txs.jsonl')) {
        fs.unlinkSync('./data/dict_test/txs.jsonl')
    }

    // Open output stream
    const txsFile = fs.createWriteStream('./data/dict_test/txs.jsonl')

    let txCount = 0
    let lastTxNum = -1

    const LIMIT_TXS = 200000
    const totalTxs = db.getTxCount()

    while (true) {
        const txsLeft = Math.min(totalTxs, LIMIT_TXS) - txCount
        const txs = db.getTxBatch(lastTxNum, Math.min(txsLeft, 100000), false, undefined)

        if (txs.txs.length === 0 || txCount >= LIMIT_TXS) {
            break
        }

        lastTxNum = txs.txs[txs.txs.length - 1]!.txNum

        for (const tx of txs.txs) {
            txsFile.write(JSON.stringify(tx) + '\n')
            txCount++
        }
        console.log(`Processed ${lastTxNum} of ${totalTxs} txs - written: ${txCount} / ${Math.min(LIMIT_TXS, totalTxs)}`)
    }

    txsFile.end()

    // Wait for stream to finish
    await new Promise<void>(resolve => txsFile.on('finish', () => resolve()))

    console.log(`Done writing JSONL file - total txs: ${txCount}`)
    process.exit(0)
}

main().catch(console.error)
