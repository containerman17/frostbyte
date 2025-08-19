import { BlocksDBHelper } from "../blockFetcher/BlocksDBHelper";
import { FastifyInstance } from "fastify";
import {
    RpcTraceResult,
    StoredTx
} from "../blockFetcher/evmTypes";
import { ChainConfig } from "../config";
import sqlite3 from "better-sqlite3";

export interface RegisterRoutesContext {
    getBlocksDbHelper: (evmChainId: number) => BlocksDBHelper;
    getIndexerDbConnection: (evmChainId: number, indexerName: string) => sqlite3.Database;
    getChainConfig: (evmChainIdOrBlockchainId: number | string) => ChainConfig | undefined;
    getAllChainConfigs: () => ChainConfig[];
}

export type TxBatch = {
    txs: StoredTx[];
    traces: RpcTraceResult[] | undefined;
}

/** Indexing plugin - processes blockchain data and stores in its own database */
export interface IndexingPlugin<ExtractedDataType> {
    name: string;          // unique slug, no whitespaces only a-z0-9-_
    version: number;       // bump wipes the database
    usesTraces: boolean;   // if true, traces are included in the batch, that's 3x slower
    filterEvents?: string[]; // if provided, only transactions with these topics will be processed

    /** Called once. Create tables here. */
    initialize: (db: sqlite3.Database) => void;

    extractData: (batch: TxBatch) => ExtractedDataType;

    saveExtractedData: (
        db: sqlite3.Database,
        blocksDb: BlocksDBHelper,
        data: ExtractedDataType,
    ) => void;
}

/** API plugin - provides REST endpoints using data from specified indexer databases */
export interface ApiPlugin {
    name: string;          // unique slug for the API plugin
    /** List of indexer names whose databases this API plugin needs access to */
    requiredIndexers: string[];

    registerRoutes: (app: FastifyInstance, dbCtx: RegisterRoutesContext) => void;
}
