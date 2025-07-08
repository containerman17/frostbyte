import type { IndexerModule, StoredTx, RpcTxReceipt, RpcTraceResult } from "../index";

// Schema definitions for response types
const rpcBlockTransactionSchema = {
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
};

const rpcTxReceiptSchema = {
    type: 'object',
    properties: {
        blockHash: { type: 'string' },
        blockNumber: { type: 'string' },
        contractAddress: { type: ['string', 'null'] },
        cumulativeGasUsed: { type: 'string' },
        effectiveGasPrice: { type: 'string' },
        from: { type: 'string' },
        gasUsed: { type: 'string' },
        logs: {
            type: 'array',
            items: {
                type: 'object',
                properties: {
                    address: { type: 'string' },
                    topics: { type: 'array', items: { type: 'string' } },
                    data: { type: 'string' },
                    blockNumber: { type: 'string' },
                    transactionHash: { type: 'string' },
                    transactionIndex: { type: 'string' },
                    blockHash: { type: 'string' },
                    logIndex: { type: 'string' },
                    removed: { type: 'boolean' }
                },
                required: ['address', 'topics', 'data', 'blockNumber', 'transactionHash', 'transactionIndex', 'blockHash', 'logIndex', 'removed']
            }
        },
        logsBloom: { type: 'string' },
        status: { type: 'string' },
        to: { type: 'string' },
        transactionHash: { type: 'string' },
        transactionIndex: { type: 'string' },
        type: { type: 'string' }
    },
    required: ['blockHash', 'blockNumber', 'cumulativeGasUsed', 'effectiveGasPrice', 'from', 'gasUsed', 'logs', 'logsBloom', 'status', 'to', 'transactionHash', 'transactionIndex', 'type']
};

const rpcTraceResultSchema = {
    type: 'object',
    properties: {
        txHash: { type: 'string' },
        result: {
            type: 'object',
            properties: {
                from: { type: 'string' },
                gas: { type: 'string' },
                gasUsed: { type: 'string' },
                to: { type: 'string' },
                input: { type: 'string' },
                value: { type: 'string' },
                type: { type: 'string', enum: ['CALL', 'DELEGATECALL', 'STATICCALL', 'CALLCODE', 'CREATE', 'CREATE2', 'CREATE3', 'SELFDESTRUCT', 'SUICIDE', 'REWARD'] },
                calls: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            from: { type: 'string' },
                            gas: { type: 'string' },
                            gasUsed: { type: 'string' },
                            to: { type: 'string' },
                            input: { type: 'string' },
                            value: { type: 'string' },
                            type: { type: 'string', enum: ['CALL', 'DELEGATECALL', 'STATICCALL', 'CALLCODE', 'CREATE', 'CREATE2', 'CREATE3', 'SELFDESTRUCT', 'SUICIDE', 'REWARD'] },
                            calls: { type: 'array' } // Simplified recursive reference
                        },
                        required: ['from', 'gas', 'gasUsed', 'to', 'input', 'value', 'type']
                    }
                }
            },
            required: ['from', 'gas', 'gasUsed', 'to', 'input', 'value', 'type']
        }
    },
    required: ['txHash', 'result']
};

const module: IndexerModule = {
    name: "transactions",
    version: 1,
    usesTraces: false,

    wipe: (db) => { },
    initialize: (db) => { },
    handleTxBatch: (db, blocksDb, batch) => { },

    registerRoutes: (app, dbCtx) => {
        // GET /{chainid}/transactions - list transactions with pagination
        app.get('/:evmChainId/transactions', {
            schema: {
                description: 'List transactions with pagination, starting from the latest or specified block',
                tags: ['Transactions'],
                summary: 'Get paginated list of transactions',
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
                            description: 'Number of transactions to return (1-100)'
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
                            transactions: {
                                type: 'array',
                                items: {
                                    type: 'object',
                                    properties: {
                                        transaction: rpcBlockTransactionSchema,
                                        receipt: rpcTxReceiptSchema,
                                        blockTimestamp: { type: 'string' }
                                    },
                                    required: ['transaction', 'receipt', 'blockTimestamp']
                                }
                            },
                            hasMore: { type: 'boolean' }
                        },
                        required: ['transactions', 'hasMore']
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
            if (startBlock > latestBlock || startBlock < 0) {
                return reply.code(400).send({
                    error: 'Invalid fromBlock parameter'
                });
            }

            // Collect transactions from blocks starting from startBlock and going backwards
            const transactions: any[] = [];
            let currentBlock = startBlock;

            while (transactions.length < limit && currentBlock >= 0) {
                const block = blocksDb.slow_getBlockWithTransactions(currentBlock);
                if (block && block.transactions.length > 0) {
                    // Process transactions in reverse order (newest first)
                    for (let i = block.transactions.length - 1; i >= 0 && transactions.length < limit; i--) {
                        const tx = block.transactions[i];
                        if (tx) {
                            // Get the receipt for additional data
                            const receipt = blocksDb.getTxReceipt(tx.hash);

                            transactions.push({
                                transaction: tx,
                                receipt: receipt,
                                blockTimestamp: block.timestamp
                            });
                        }
                    }
                }
                currentBlock--;
            }

            return {
                transactions,
                hasMore: currentBlock >= 0
            };
        });

        // GET /{chainid}/transactions/{txHash} - get specific transaction
        app.get('/:evmChainId/transactions/:txHash', {
            schema: {
                description: 'Get a specific transaction by hash, including transaction data, receipt, and trace (if available)',
                tags: ['Transactions'],
                summary: 'Get transaction by hash',
                params: {
                    type: 'object',
                    properties: {
                        evmChainId: { type: 'number' },
                        txHash: { type: 'string' }
                    },
                    required: ['evmChainId', 'txHash']
                },
                response: {
                    200: {
                        type: 'object',
                        properties: {
                            transaction: rpcBlockTransactionSchema,
                            receipt: rpcTxReceiptSchema,
                            trace: {
                                ...rpcTraceResultSchema,
                                nullable: true
                            }
                        },
                        required: ['transaction', 'receipt']
                    },
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
            const { evmChainId, txHash } = request.params as {
                evmChainId: number;
                txHash: string;
            };

            const blocksDb = dbCtx.blocksDbFactory(evmChainId);

            // First check if we can get the receipt (quick way to check if tx exists)
            const receipt = blocksDb.getTxReceipt(txHash);

            if (!receipt) {
                return reply.code(404).send({
                    error: 'Transaction not found'
                });
            }

            // Now we need to get the full transaction data
            // Since BlockDB doesn't provide a direct method, we'll need to search by block
            const blockNumber = Number(receipt.blockNumber);
            const block = blocksDb.slow_getBlockWithTransactions(blockNumber);

            if (!block) {
                return reply.code(404).send({
                    error: 'Block not found for transaction'
                });
            }

            // Find the transaction in the block
            const tx = block.transactions.find(t => t.hash === txHash);

            if (!tx) {
                return reply.code(404).send({
                    error: 'Transaction not found in block'
                });
            }

            // Check if traces are available for this chain
            const hasDebug = blocksDb.getHasDebug() === 1;
            let trace = undefined;

            if (hasDebug) {
                // Get traces for this block
                const traces = blocksDb.slow_getBlockTraces(blockNumber);
                // Find the trace for this specific transaction
                trace = traces.find(t => t.txHash === txHash);
            }

            // Return transaction, receipt, and optionally trace
            return {
                transaction: tx,
                receipt: receipt,
                trace: trace
            };
        });
    }
};

export default module; 
