import type { ApiPlugin } from "../../index.ts";

const module: ApiPlugin = {
    name: "erc20_tokens_api",
    version: 1,
    requiredIndexers: ['erc20_tokens_registry'],

    registerRoutes: (app, dbCtx) => {
        app.get('/api/global/stats/top-erc20-tokens', {
            schema: {
                description: 'Get top 20 ERC20 tokens by transfer count across all chains',
                response: {
                    200: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                address: { type: 'string', description: 'Token contract address' },
                                transferCount: { type: 'number', description: 'Total number of transfer events' },
                                blockchainId: { type: 'string', description: 'Blockchain identifier' }
                            },
                            required: ['address', 'transferCount', 'blockchainId']
                        }
                    }
                }
            }
        }, async (request, reply) => {
            console.time('top-erc20-tokens');

            const chains = dbCtx.getAllChainConfigs();
            const allTokens: Array<{ address: string, transferCount: number, blockchainId: string }> = [];

            // Query each chain for ERC20 token data
            for (const chain of chains) {
                const indexerDb = dbCtx.getIndexerDbConnection(chain.evmChainId, 'erc20_tokens_registry');

                const tokens = indexerDb.prepare(`
                    SELECT contractAddress as address, transferEventCount as transferCount
                    FROM erc20_tokens_registry
                    ORDER BY transferEventCount DESC
                `).all() as Array<{ address: string, transferCount: number }>;

                // Add blockchain identifier to each token
                for (const token of tokens) {
                    allTokens.push({
                        address: token.address,
                        transferCount: token.transferCount,
                        blockchainId: chain.blockchainId || `chain-${chain.evmChainId}`
                    });
                }
            }

            // Sort all tokens by transfer count and get top 20
            const topTokens = allTokens
                .sort((a, b) => b.transferCount - a.transferCount)
                .slice(0, 20);

            console.timeEnd('top-erc20-tokens');
            reply.status(200).send(topTokens);
        });
    }
};

export default module;
