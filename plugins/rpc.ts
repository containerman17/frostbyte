import type { IndexerModule } from "../lib/types";
import { createRoute, z } from "@hono/zod-openapi";
import { BlockDB } from "../blockFetcher/BlockDB";
import { RpcBlock } from "../blockFetcher/evmTypes";
import { utils } from "@avalabs/avalanchejs";
import { CHAIN_ID } from "../config";

// JSON-RPC request/response schemas for OpenAPI
const RPCRequestSchema = z.object({
    method: z.string(),
    params: z.array(z.any()).default([]),
    id: z.union([z.string(), z.number()]).optional(),
    jsonrpc: z.string().optional()
}).openapi('RPCRequest');

const RPCResponseSchema = z.object({
    result: z.any().optional(),
    error: z.object({
        code: z.number(),
        message: z.string()
    }).optional(),
    id: z.union([z.string(), z.number()]).optional(),
    jsonrpc: z.string().optional()
}).openapi('RPCResponse');

const RPCBatchRequestSchema = z.union([
    RPCRequestSchema,
    z.array(RPCRequestSchema)
]).openapi('RPCBatchRequest');

const RPCBatchResponseSchema = z.union([
    RPCResponseSchema,
    z.array(RPCResponseSchema)
]).openapi('RPCBatchResponse');


function parseBlockNumber(param: string | number | undefined, blocksDb: BlockDB): number {
    if (param === undefined) return 0;
    if (typeof param === 'number') return param;
    if (param === 'latest') return blocksDb.getLastStoredBlockNumber();
    if (param.startsWith('0x')) return parseInt(param, 16);
    return parseInt(param, 10);
}

function getBlockByNumber(blocksDb: BlockDB, blockNumber: number): RpcBlock | null {
    return blocksDb.slow_getBlockWithTransactions(blockNumber);
}

function getTxReceipt(blocksDb: BlockDB, txHash: string) {
    return blocksDb.getTxReceipt(txHash);
}

function getBlockTraces(blocksDb: BlockDB, blockNumber: number) {
    return blocksDb.slow_getBlockTraces(blockNumber);
}

function handleRpcRequest(blocksDb: BlockDB, request: z.infer<typeof RPCRequestSchema>): z.infer<typeof RPCResponseSchema> {
    const response: any = { id: request.id, jsonrpc: request.jsonrpc || '2.0' };

    try {
        switch (request.method) {
            case 'eth_chainId':
                response.result = '0x' + blocksDb.getEvmChainId().toString(16);
                break;
            case 'eth_blockNumber':
                response.result = '0x' + blocksDb.getLastStoredBlockNumber().toString(16);
                break;
            case 'eth_getBlockByNumber': {
                const blockNumber = parseBlockNumber(request.params[0], blocksDb);
                const block = getBlockByNumber(blocksDb, blockNumber);
                response.result = block ?? null;
                break;
            }
            case 'eth_getTransactionReceipt': {
                const txHash = request.params[0] as string;
                const receipt = getTxReceipt(blocksDb, txHash);
                response.result = receipt ?? null;
                break;
            }
            case 'debug_traceBlockByNumber': {
                const blockNumber = parseBlockNumber(request.params[0], blocksDb);
                const traces = getBlockTraces(blocksDb, blockNumber);
                response.result = traces;
                break;
            }
            case 'eth_call': {
                const callObj = request.params[0] as { to?: string; data?: string };
                const tag = request.params[1];
                const warpAddr = '0x0200000000000000000000000000000000000005';
                const getBlockchainIDSig = '0x4213cf78';
                if (tag === 'latest' && callObj && callObj.to?.toLowerCase() === warpAddr && callObj.data === getBlockchainIDSig) {
                    const bytes = utils.base58check.decode(CHAIN_ID);
                    response.result = '0x' + Buffer.from(bytes).toString('hex');
                } else {
                    response.error = { code: -32601, message: 'Unsupported eth_call' };
                }
                break;
            }
            default:
                response.error = { code: -32601, message: `Method ${request.method} not found` };
        }
    } catch (err: any) {
        response.error = { code: -32000, message: err?.message || 'Internal error' };
    }
    return response;
}

const registerRoutes: IndexerModule['registerRoutes'] = (app, _db, blocksDb) => {
    const rpcRoute = createRoute({
        method: 'post',
        path: '/rpc',
        request: {
            body: {
                content: {
                    'application/json': { schema: RPCBatchRequestSchema }
                }
            }
        },
        responses: {
            200: {
                content: {
                    'application/json': { schema: RPCBatchResponseSchema }
                },
                description: 'RPC response'
            }
        },
        tags: ['RPC'],
        summary: 'JSON-RPC endpoint',
        description: 'Handles JSON-RPC requests'
    });

    app.openapi(rpcRoute, async (c) => {
        const requests = c.req.valid('json') as any;
        if (Array.isArray(requests)) {
            const res = requests.map(req => handleRpcRequest(blocksDb, req));
            return c.json(res);
        } else {
            const res = handleRpcRequest(blocksDb, requests);
            return c.json(res);
        }
    });
};

const module: IndexerModule = {
    name: 'rpc',
    version: 0,
    usesTraces: false,
    wipe: () => {},
    initialize: () => {},
    handleTxBatch: () => {},
    registerRoutes
};

export default module;
