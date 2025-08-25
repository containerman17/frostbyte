import type { ApiPlugin, BlocksDBHelper, RegisterRoutesContext, evmTypes } from "../index.ts";
import { logsBloom } from "../index.ts";

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

function parseBlockNumber(param: string | number | undefined, blocksDb: BlocksDBHelper): number {
    if (param === undefined) return 0;
    if (typeof param === 'number') return param;
    if (param === 'latest') return blocksDb.getLastStoredBlockNumber();
    if (param.startsWith('0x')) return parseInt(param, 16);
    return parseInt(param, 10);
}

function getBlockByNumber(blocksDb: BlocksDBHelper, blockNumber: number): evmTypes.RpcBlock | null {
    const blockWithoutLogsBloom = blocksDb.slow_getBlockWithTransactions(blockNumber);
    if (!blockWithoutLogsBloom) return null;

    // Fetch all receipts for this block to compute logs bloom
    const receipts: evmTypes.RpcTxReceipt[] = [];
    if (blockWithoutLogsBloom.transactions && Array.isArray(blockWithoutLogsBloom.transactions)) {
        for (const tx of blockWithoutLogsBloom.transactions) {
            const receipt = blocksDb.getTxReceipt(tx.hash);
            if (receipt) {
                // Add logs bloom to receipt
                const receiptWithBloom = logsBloom.addLogsBloomToReceipt(receipt as any);
                receipts.push(receiptWithBloom);
            }
        }
    }

    // Add logs bloom to block
    return logsBloom.addLogsBloomToBlock(blockWithoutLogsBloom as any, receipts);
}

function getTxReceipt(blocksDb: BlocksDBHelper, txHash: string) {
    const receiptWithoutBloom = blocksDb.getTxReceipt(txHash);
    if (!receiptWithoutBloom) return null;

    // Add logs bloom to receipt
    return logsBloom.addLogsBloomToReceipt(receiptWithoutBloom as any);
}

function getBlockTraces(blocksDb: BlocksDBHelper, blockNumber: number) {
    return blocksDb.slow_getBlockTraces(blockNumber);
}

async function handleRpcRequest(blocksDb: BlocksDBHelper, request: RPCRequest, dbCtx: RegisterRoutesContext): Promise<RPCResponse> {
    const response: RPCResponse = { jsonrpc: request.jsonrpc || '2.0' };
    if (request.id !== undefined) {
        // Parse numeric strings to actual numbers
        if (typeof request.id === 'string' && /^\d+$/.test(request.id)) {
            response.id = parseInt(request.id, 10);
        } else {
            response.id = request.id;
        }
    }

    try {
        switch (request.method) {
            case 'eth_chainId':
                response.result = '0x' + (blocksDb.getEvmChainId()).toString(16);
                break;
            case 'eth_blockNumber':
                response.result = '0x' + (blocksDb.getLastStoredBlockNumber()).toString(16);
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
                const blockTag = request.params?.[1];

                // We only support 'latest' block tag for now
                if (blockTag !== 'latest' && blockTag !== undefined) {
                    response.error = { code: -32602, message: 'Only "latest" block tag is supported' };
                    break;
                }

                if (!callObj || !callObj.to || !callObj.data) {
                    response.error = { code: -32602, message: 'Invalid parameters: to and data are required' };
                    break;
                }

                try {
                    // Validate hex strings
                    if (!callObj.to.match(/^0x[0-9a-fA-F]{40}$/) || !callObj.data.match(/^0x[0-9a-fA-F]*$/)) {
                        response.error = { code: -32602, message: 'Invalid hex format for to or data' };
                        break;
                    }

                    // Use the cached ethCall function
                    response.result = await dbCtx.ethCall(
                        blocksDb.getEvmChainId(),
                        callObj.to as `0x${string}`,
                        callObj.data as `0x${string}`
                    );
                } catch (error: any) {
                    response.error = { code: -32000, message: `eth_call failed: ${error.message}` };
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

const registerRoutes: ApiPlugin['registerRoutes'] = (app, dbCtx) => {
    // JSON Schemas
    const paramsSchema = {
        type: 'object',
        properties: {
            evmChainId: { type: 'number' }
        },
        required: ['evmChainId']
    };

    const rpcRequestSchema = {
        type: 'object',
        properties: {
            method: { type: 'string' },
            params: {
                type: 'array',
                items: {}
            },
            id: {
                anyOf: [
                    { type: 'string' },
                    { type: 'number' },
                    { type: 'null' }
                ]
            },
            jsonrpc: { type: 'string' }
        },
        required: ['method'],
        examples: [
            {
                jsonrpc: '2.0',
                method: 'eth_chainId',
                id: 1
            },
            {
                jsonrpc: '2.0',
                method: 'eth_blockNumber',
                id: 2
            },
            {
                jsonrpc: '2.0',
                method: 'eth_getBlockByNumber',
                params: ['0x5BAD55', true],
                id: 3
            },
            {
                jsonrpc: '2.0',
                method: 'eth_getTransactionReceipt',
                params: ['0x85d995eba9763907fdf35cd2034144dd9d53ce32cbec21349d4b12823c6860c5'],
                id: 4
            },
            {
                jsonrpc: '2.0',
                method: 'eth_call',
                params: [
                    {
                        to: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
                        data: '0x06fdde03'  // name() function selector
                    },
                    'latest'
                ],
                id: 5
            }
        ]
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
                anyOf: [
                    { type: 'string' },
                    { type: 'number' },
                    { type: 'null' }
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
        ],
        examples: [
            // Single request example
            {
                jsonrpc: '2.0',
                method: 'eth_chainId',
                id: 1
            },
            // Batch request example
            [
                {
                    jsonrpc: '2.0',
                    method: 'eth_chainId',
                    id: 1
                },
                {
                    jsonrpc: '2.0',
                    method: 'eth_blockNumber',
                    id: 2
                },
                {
                    jsonrpc: '2.0',
                    method: 'eth_getBlockByNumber',
                    params: ['latest', false],
                    id: 3
                }
            ]
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

    app.post('/api/:evmChainId/rpc', {
        schema: {
            description: 'JSON-RPC endpoint',
            params: paramsSchema,
            body: {
                oneOf: [
                    {
                        type: 'object',
                        properties: {
                            method: { type: 'string' },
                            params: {
                                type: 'array',
                                items: {}
                            },
                            id: {
                                anyOf: [
                                    { type: 'string' },
                                    { type: 'number' },
                                    { type: 'null' }
                                ]
                            },
                            jsonrpc: { type: 'string' }
                        },
                        required: ['method']
                    },
                    {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                method: { type: 'string' },
                                params: {
                                    type: 'array',
                                    items: {}
                                },
                                id: {
                                    anyOf: [
                                        { type: 'string' },
                                        { type: 'number' },
                                        { type: 'null' }
                                    ]
                                },
                                jsonrpc: { type: 'string' }
                            },
                            required: ['method']
                        }
                    }
                ],
                examples: [
                    // Single request example
                    {
                        jsonrpc: '2.0',
                        method: 'eth_chainId',
                        id: 1
                    },
                    // Batch request example
                    [
                        {
                            jsonrpc: '2.0',
                            method: 'eth_chainId',
                            id: 1
                        },
                        {
                            jsonrpc: '2.0',
                            method: 'eth_blockNumber',
                            id: 2
                        },
                        {
                            jsonrpc: '2.0',
                            method: 'eth_call',
                            params: [
                                {
                                    to: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
                                    data: '0x06fdde03'  // name() function
                                },
                                'latest'
                            ],
                            id: 3
                        }
                    ]
                ]
            },
            response: {
                200: batchResponseSchema
            }
        }
    }, async (request, reply) => {
        const { evmChainId } = request.params as { evmChainId: number };
        const blocksDb = dbCtx.getBlocksDbHelper(evmChainId);

        const requests = request.body as RPCRequest | RPCRequest[];

        if (Array.isArray(requests)) {
            const responses = await Promise.all(requests.map(req => handleRpcRequest(blocksDb, req, dbCtx)));
            return responses;
        } else {
            const response = await handleRpcRequest(blocksDb, requests, dbCtx);
            return response;
        }
    });
};

const module: ApiPlugin = {
    name: 'rpc',
    version: 1,
    requiredIndexers: [], // This API doesn't need any indexer databases
    registerRoutes
};

export default module;
