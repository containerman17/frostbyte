import type { ApiPlugin } from "../index";
import type { ContractHome, ContractHomeData, ContractHomeRemote } from './types/ictt.types';

const module: ApiPlugin = {
    name: "ictt_api",
    requiredIndexers: ['ictt'],

    registerRoutes: (app, dbCtx) => {
        app.get('/api/ictt/contract-homes', {
            schema: {
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                chainName: { type: 'string' },
                                blockchainId: { type: 'string' },
                                evmChainId: { type: 'number' },
                                address: { type: 'string' },
                                callFailed: { type: 'string' },
                                callSucceeded: { type: 'string' },
                                tokensWithdrawn: { type: 'string' },
                                remotes: {
                                    type: 'array',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            remoteBlockchainID: { type: 'string' },
                                            remoteTokenTransferrerAddress: { type: 'string' },
                                            initialCollateralNeeded: { type: 'boolean' },
                                            tokenDecimals: { type: 'number' },
                                            collateralAddedCnt: { type: 'number' },
                                            collateralAddedSum: { type: 'string' },
                                            tokensAndCallRoutedCnt: { type: 'number' },
                                            tokensAndCallRoutedSum: { type: 'string' },
                                            tokensAndCallSentCnt: { type: 'number' },
                                            tokensAndCallSentSum: { type: 'string' },
                                            tokensRoutedCnt: { type: 'number' },
                                            tokensRoutedSum: { type: 'string' },
                                            tokensSentCnt: { type: 'number' },
                                            tokensSentSum: { type: 'string' }
                                        },
                                        required: ['remoteBlockchainID', 'remoteTokenTransferrerAddress', 'initialCollateralNeeded', 'tokenDecimals',
                                            'collateralAddedCnt', 'collateralAddedSum', 'tokensAndCallRoutedCnt', 'tokensAndCallRoutedSum',
                                            'tokensAndCallSentCnt', 'tokensAndCallSentSum', 'tokensRoutedCnt', 'tokensRoutedSum',
                                            'tokensSentCnt', 'tokensSentSum']
                                    }
                                }
                            },
                            required: ['chainName', 'blockchainId', 'evmChainId', 'address', 'remotes',
                                'callFailedCnt', 'callFailedSum', 'callSucceededCnt', 'callSucceededSum',
                                'tokensWithdrawnCnt', 'tokensWithdrawnSum']
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const configs = dbCtx.getAllChainConfigs();
            const results: (ContractHome & { chainName: string; blockchainId: string; evmChainId: number; })[] = [];

            for (const config of configs) {
                const db = dbCtx.indexerDbFactory(config.evmChainId, 'ictt');

                // Query all contract homes from this chain
                const stmt = db.prepare(`
                    SELECT address, data
                    FROM contract_homes
                `);

                const rows = stmt.all() as Array<{
                    address: string;
                    data: string;
                }>;

                // Convert from nested structure to flat structure for API
                const contractHomes: ContractHome[] = rows.map(row => {
                    const data: ContractHomeData = JSON.parse(row.data);
                    const remotes: ContractHomeRemote[] = [];

                    // Flatten the nested structure
                    for (const [blockchainId, tokens] of Object.entries(data.remotes)) {
                        for (const [tokenAddress, remoteData] of Object.entries(tokens)) {
                            remotes.push({
                                remoteBlockchainID: blockchainId,
                                remoteTokenTransferrerAddress: tokenAddress,
                                initialCollateralNeeded: remoteData.initialCollateralNeeded,
                                tokenDecimals: remoteData.tokenDecimals,
                                collateralAddedCnt: remoteData.collateralAddedCnt,
                                collateralAddedSum: remoteData.collateralAddedSum,
                                tokensAndCallRoutedCnt: remoteData.tokensAndCallRoutedCnt,
                                tokensAndCallRoutedSum: remoteData.tokensAndCallRoutedSum,
                                tokensAndCallSentCnt: remoteData.tokensAndCallSentCnt,
                                tokensAndCallSentSum: remoteData.tokensAndCallSentSum,
                                tokensRoutedCnt: remoteData.tokensRoutedCnt,
                                tokensRoutedSum: remoteData.tokensRoutedSum,
                                tokensSentCnt: remoteData.tokensSentCnt,
                                tokensSentSum: remoteData.tokensSentSum
                            });
                        }
                    }

                    return {
                        address: data.address,
                        remotes,
                        callFailedCnt: data.callFailedCnt,
                        callFailedSum: data.callFailedSum,
                        callSucceededCnt: data.callSucceededCnt,
                        callSucceededSum: data.callSucceededSum,
                        tokensWithdrawnCnt: data.tokensWithdrawnCnt,
                        tokensWithdrawnSum: data.tokensWithdrawnSum
                    };
                });

                for (const home of contractHomes) {
                    results.push({
                        ...home,
                        chainName: config.chainName,
                        blockchainId: config.blockchainId,
                        evmChainId: config.evmChainId,
                    });
                }
            }

            // Sort by chain name for consistent output
            results.sort((a, b) => a.chainName.localeCompare(b.chainName));

            return reply.send(results);
        });
    }
};

export default module; 
