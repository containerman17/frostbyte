import SQLite from "better-sqlite3";
import { BlockDB } from "../blockFetcher/BlockDB";
import { CreateIndexerFunction, Indexer } from "./types";
import { LazyTx } from "../blockFetcher/lazy/LazyTx";
import { LazyBlock } from "../blockFetcher/lazy/LazyBlock";
import { OpenAPIHono } from "@hono/zod-openapi";
import { LazyTraces } from "../blockFetcher/lazy/LazyTrace";
import { DEBUG_RPC_AVAILABLE, IS_DEVELOPMENT } from "../config";

let lastBlockIndexed = -1

class SanityChecker implements Indexer {
    initialize(): void {
    }

    //TODO: unite with indexBlock
    indexBlocks(blocks: { block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined }[]): void {

        for (const block of blocks) {
            this.indexBlock(block.block, block.txs, block.traces);
        }

    }

    private indexBlock(block: LazyBlock, txs: LazyTx[], traces: LazyTraces | undefined): void {
        if (!IS_DEVELOPMENT) {
            return;
        }

        if (DEBUG_RPC_AVAILABLE && traces === undefined) {
            throw new Error(`Sanity checker failed: Traces are undefined but DEBUG_RPC_AVAILABLE is true`);
        } else if (!DEBUG_RPC_AVAILABLE && traces !== undefined) {
            throw new Error(`Sanity checker failed: Traces are defined but DEBUG_RPC_AVAILABLE is false`);
        }

        if (lastBlockIndexed !== -1 && (lastBlockIndexed + 1) !== (block.number)) {
            console.error(`[SanityChecker] ERROR: Expected block ${lastBlockIndexed + 1} but got block ${block.number}`);
            console.error(`[SanityChecker] Worker ID: ${process.pid}`);
            throw new Error(`Sanity checker failed: Block ${block.number} is not the next block after ${lastBlockIndexed}`);
        }
        lastBlockIndexed = Number(block.number);
        if (block.transactionCount !== txs.length) {
            throw new Error(`Sanity checker failed: Block ${block.number} has ${block.transactionCount} transactions, but ${txs.length} were provided`);
        }
        for (const tx of txs) {
            if (tx.blockNumber !== block.number) {
                throw new Error(`Sanity checker failed: Tx ${tx.hash} has block number ${tx.blockNumber}, but block ${block.number} was provided`);
            }
        }
    }

    registerRoutes(app: OpenAPIHono): void { }
}
export const createSanityChecker: CreateIndexerFunction = (blocksDb: BlockDB, indexingDb: SQLite.Database) => {
    return new SanityChecker();
}
