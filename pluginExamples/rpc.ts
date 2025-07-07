import type { IndexerModule } from "../lib/types";
import { BlockDB } from "../blockFetcher/BlockDB";
import { RpcBlock } from "../blockFetcher/evmTypes";
import { utils } from "@avalabs/avalanchejs";
import { CHAIN_ID } from "../config";

// JSON-RPC types
interface RPCRequest {
    method: string;
    params?: any[];
    id?: string | number;
    jsonrpc?: string;
}

interface RPCResponse {
    result?: any;
    error?: {
        code: number;
        message: string;
    };
    id?: string | number;
    jsonrpc?: string;
}

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

function handleRpcRequest(blocksDb: BlockDB, request: RPCRequest): RPCResponse {
    const response: RPCResponse = { jsonrpc: request.jsonrpc || '2.0' };
    if (request.id !== undefined) {
        response.id = request.id;
    }

    try {
        switch (request.method) {
            case 'eth_chainId':
                response.result = '0x' + blocksDb.getEvmChainId().toString(16);
                break;
            case 'eth_blockNumber':
                response.result = '0x' + blocksDb.getLastStoredBlockNumber().toString(16);
                break;
            case 'eth_getBlockByNumber': {
                const blockNumber = parseBlockNumber(request.params?.[0], blocksDb);
                const block = getBlockByNumber(blocksDb, blockNumber);
                response.result = block ?? null;
                break;
            }
            case 'eth_getTransactionReceipt': {
                const txHash = request.params?.[0] as string;
                const receipt = getTxReceipt(blocksDb, txHash);
                response.result = receipt ?? null;
                break;
            }
            case 'debug_traceBlockByNumber': {
                const blockNumber = parseBlockNumber(request.params?.[0], blocksDb);
                const traces = getBlockTraces(blocksDb, blockNumber);
                response.result = traces;
                break;
            }
            case 'eth_call': {
                const callObj = request.params?.[0] as { to?: string; data?: string };
                const tag = request.params?.[1];
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
    // JSON Schemas
    const rpcRequestSchema = {
        type: 'object',
        properties: {
            method: { type: 'string' },
            params: { 
                type: 'array',
                items: {}
            },
            id: { 
                oneOf: [
                    { type: 'string' },
                    { type: 'number' }
                ]
            },
            jsonrpc: { type: 'string' }
        },
        required: ['method']
    };

    const rpcResponseSchema = {
        type: 'object',
        properties: {
            result: {},
            error: {
                type: 'object',
                properties: {
                    code: { type: 'number' },
                    message: { type: 'string' }
                },
                required: ['code', 'message']
            },
            id: {
                oneOf: [
                    { type: 'string' },
                    { type: 'number' }
                ]
            },
            jsonrpc: { type: 'string' }
        }
    };

    const batchRequestSchema = {
        oneOf: [
            rpcRequestSchema,
            {
                type: 'array',
                items: rpcRequestSchema
            }
        ]
    };

    const batchResponseSchema = {
        oneOf: [
            rpcResponseSchema,
            {
                type: 'array',
                items: rpcResponseSchema
            }
        ]
    };

    app.post('/rpc', {
        schema: {
            description: 'JSON-RPC endpoint',
            tags: ['RPC'],
            summary: 'Handles JSON-RPC requests',
            body: batchRequestSchema,
            response: {
                200: batchResponseSchema
            }
        }
    }, async (request, reply) => {
        const requests = request.body as RPCRequest | RPCRequest[];
        
        if (Array.isArray(requests)) {
            const responses = requests.map(req => handleRpcRequest(blocksDb, req));
            return responses;
        } else {
            const response = handleRpcRequest(blocksDb, requests);
            return response;
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
