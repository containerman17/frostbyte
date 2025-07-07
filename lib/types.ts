import { BlockDB } from "../blockFetcher/BlockDB";
import { FastifyInstance } from "fastify";
import SQLite from "better-sqlite3";
import {
    RpcTraceResult,
    StoredTx
} from "../blockFetcher/evmTypes";

/** All plugin files must export these symbols. */
export interface IndexerModule {
    name: string;          // unique slug, no whitespaces only a-z0-9-_
    version: number;       // bump → wipe() runs
    usesTraces: boolean;   // if true, traces are included in the batch, that's 3x slower

    /** Called **once** if storedVersion ≠ version. */
    wipe: (db: SQLite.Database) => void | Promise<void>;

    /** Called once after wipe (if any). Create tables here. */
    initialize: (db: SQLite.Database) => void | Promise<void>;

    handleTxBatch: (
        db: SQLite.Database,
        blocksDb: BlockDB,
        batch: { txs: StoredTx[]; traces: RpcTraceResult[] | undefined },
    ) => void | Promise<void>;

    registerRoutes: (app: FastifyInstance, db: SQLite.Database, blocksDb: BlockDB) => void;
}
