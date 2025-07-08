import type { IndexerModule, RpcBlock } from "../index";

// Schema definition for RpcBlock
const rpcBlockSchema = {
    type: 'object',
    properties: {
        hash: { type: 'string' },
        number: { type: 'string' },
        parentHash: { type: 'string' },
        timestamp: { type: 'string' },
        gasLimit: { type: 'string' },
        gasUsed: { type: 'string' },
        baseFeePerGas: { type: 'string' },
        miner: { type: 'string' },
        difficulty: { type: 'string' },
        totalDifficulty: { type: 'string' },
        size: { type: 'string' },
        stateRoot: { type: 'string' },
        transactionsRoot: { type: 'string' },
        receiptsRoot: { type: 'string' },
        logsBloom: { type: 'string' },
        extraData: { type: 'string' },
        mixHash: { type: 'string' },
        nonce: { type: 'string' },
        sha3Uncles: { type: 'string' },
        uncles: { type: 'array', items: { type: 'string' } },
        transactions: {
            type: 'array',
            items: {
                type: 'object',
                properties: {
                    hash: { type: 'string' },
                    blockHash: { type: 'string' },
                    blockNumber: { type: 'string' },
                    transactionIndex: { type: 'string' },
                    from: { type: 'string' },
                    to: { type: ['string', 'null'] },
                    value: { type: 'string' },
                    gas: { type: 'string' },
                    gasPrice: { type: 'string' },
                    input: { type: 'string' },
                    nonce: { type: 'string' },
                    type: { type: 'string' },
                    chainId: { type: 'string' },
                    v: { type: 'string' },
                    r: { type: 'string' },
                    s: { type: 'string' },
                    maxFeePerGas: { type: 'string' },
                    maxPriorityFeePerGas: { type: 'string' },
                    accessList: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                address: { type: 'string' },
                                storageKeys: { type: 'array', items: { type: 'string' } }
                            }
                        }
                    },
                    yParity: { type: 'string' }
                },
                required: ['hash', 'blockHash', 'blockNumber', 'transactionIndex', 'from', 'value', 'gas', 'gasPrice', 'input', 'nonce', 'type', 'chainId', 'v', 'r', 's']
            }
        },
        blobGasUsed: { type: 'string' },
        excessBlobGas: { type: 'string' },
        parentBeaconBlockRoot: { type: 'string' },
        blockGasCost: { type: 'string' },
        blockExtraData: { type: 'string' },
        extDataHash: { type: 'string' }
    },
    required: ['hash', 'number', 'parentHash', 'timestamp', 'gasLimit', 'gasUsed', 'miner', 'difficulty', 'totalDifficulty', 'size', 'stateRoot', 'transactionsRoot', 'receiptsRoot', 'logsBloom', 'extraData', 'mixHash', 'nonce', 'sha3Uncles', 'uncles', 'transactions']
};

const module: IndexerModule = {
    name: "blocks",
    version: 1,
    usesTraces: false,

    wipe: (db) => { },
    initialize: (db) => { },
    handleTxBatch: (db, blocksDb, batch) => { },

    registerRoutes: (app, dbCtx) => {
        // GET /{chainid}/blocks - list blocks with pagination
        app.get('/:evmChainId/blocks', {
            schema: {
                description: 'List blocks with pagination, starting from the latest or specified block',
                tags: ['Blocks'],
                summary: 'Get paginated list of blocks',
                params: {
                    type: 'object',
                    properties: {
                        evmChainId: { type: 'number' }
                    },
                    required: ['evmChainId']
                },
                querystring: {
                    type: 'object',
                    properties: {
                        limit: {
                            type: 'integer',
                            minimum: 1,
                            maximum: 100,
                            default: 10,
                            description: 'Number of blocks to return (1-100)'
                        },
                        fromBlock: {
                            type: 'integer',
                            minimum: 0,
                            description: 'Starting block number (defaults to latest)'
                        }
                    }
                },
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            blocks: {
                                type: 'array',
                                items: rpcBlockSchema
                            },
                            hasMore: { type: 'boolean' }
                        },
                        required: ['blocks', 'hasMore']
                    }
                }
            }
        }, async (request, reply) => {
            const { evmChainId } = request.params as { evmChainId: number };
            const { limit = 10, fromBlock } = request.query as { limit?: number; fromBlock?: number };

            const blocksDb = dbCtx.blocksDbFactory(evmChainId);
            const latestBlock = blocksDb.getLastStoredBlockNumber();

            // Determine starting block
            let startBlock = fromBlock !== undefined ? fromBlock : latestBlock;

            // Validate startBlock
            if (startBlock > latestBlock) {
                return reply.code(400).send({
                    error: 'fromBlock is greater than latest block'
                });
            }

            const blocks: RpcBlock[] = [];
            let currentBlock = startBlock;

            // Fetch blocks in descending order
            while (blocks.length < limit && currentBlock >= 0) {
                const block = blocksDb.slow_getBlockWithTransactions(currentBlock);
                if (block) {
                    blocks.push(block);
                }
                currentBlock--;
            }

            return {
                blocks,
                hasMore: currentBlock >= 0
            };
        });

        // GET /{chainid}/blocks/{blockIdOrNumber} - get specific block
        app.get('/:evmChainId/blocks/:blockIdOrNumber', {
            schema: {
                description: 'Get a specific block by number or hash',
                tags: ['Blocks'],
                summary: 'Get block by number or hash',
                params: {
                    type: 'object',
                    properties: {
                        evmChainId: { type: 'number' },
                        blockIdOrNumber: { type: 'string' }
                    },
                    required: ['evmChainId', 'blockIdOrNumber']
                },
                response: {
                    200: rpcBlockSchema,
                    404: {
                        type: 'object',
                        properties: {
                            error: { type: 'string' }
                        },
                        required: ['error']
                    }
                }
            }
        }, async (request, reply) => {
            const { evmChainId, blockIdOrNumber } = request.params as {
                evmChainId: number;
                blockIdOrNumber: string;
            };

            const blocksDb = dbCtx.blocksDbFactory(evmChainId);

            // Parse blockIdOrNumber - could be block number or hash
            let blockIdentifier: number | string;

            // Check if it's a number
            if (/^\d+$/.test(blockIdOrNumber)) {
                blockIdentifier = parseInt(blockIdOrNumber, 10);
            } else {
                // Treat as hash
                blockIdentifier = blockIdOrNumber;
            }

            const block = blocksDb.slow_getBlockWithTransactions(blockIdentifier);

            if (!block) {
                return reply.code(404).send({
                    error: 'Block not found'
                });
            }

            return block;
        });
    }
};

export default module; 
