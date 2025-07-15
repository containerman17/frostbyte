import { BlockDB } from "../blockFetcher/BlockDB";
import { FastifyInstance } from "fastify";
import SQLite from "better-sqlite3";
import {
    RpcTraceResult,
    StoredTx
} from "../blockFetcher/evmTypes";
import { ChainConfig } from "../config";

export interface RegisterRoutesContext {
    blocksDbFactory: (evmChainId: number) => BlockDB;
    indexerDbFactory: (evmChainId: number, indexerName: string) => SQLite.Database;
    getChainConfig: (evmChainId: number) => ChainConfig;
    getAllChainConfigs: () => ChainConfig[];
}

export type TxBatch = {
    txs: StoredTx[];
    traces: RpcTraceResult[] | undefined;
}

/** Indexing plugin - processes blockchain data and stores in its own database */
export interface IndexingPlugin {
    name: string;          // unique slug, no whitespaces only a-z0-9-_
    version: number;       // bump wipes the database
    usesTraces: boolean;   // if true, traces are included in the batch, that's 3x slower

    /** Called once. Create tables here. */
    initialize: (db: SQLite.Database) => void | Promise<void>;

    handleTxBatch: (
        db: SQLite.Database,
        blocksDb: BlockDB,
        batch: TxBatch,
    ) => void | Promise<void>;
}

/** API plugin - provides REST endpoints using data from specified indexer databases */
export interface ApiPlugin {
    name: string;          // unique slug for the API plugin
    /** List of indexer names whose databases this API plugin needs access to */
    requiredIndexers: string[];

    registerRoutes: (app: FastifyInstance, dbCtx: RegisterRoutesContext) => void;
}
