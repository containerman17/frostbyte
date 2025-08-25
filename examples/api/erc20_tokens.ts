import type { ApiPlugin } from "../../index.ts";
import { decodeFunctionResult, encodeFunctionData } from 'viem';

const module: ApiPlugin = {
    name: "erc20_tokens_api",
    version: 2,  // Bumped: added tokenName field
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
                                blockchainId: { type: 'string', description: 'Blockchain identifier' },
                                tokenName: { type: 'string', description: 'Token name from contract' }
                            },
                            required: ['address', 'transferCount', 'blockchainId', 'tokenName']
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

            // Fetch token names for top tokens
            const nameAbi = [{
                name: 'name',
                type: 'function',
                inputs: [],
                outputs: [{ name: '', type: 'string' }],
                stateMutability: 'view'
            }] as const;

            const tokensWithNames = await Promise.all(
                topTokens.map(async (token) => {
                    try {
                        // Find the chain config for this token
                        const chain = chains.find(c => c.blockchainId === token.blockchainId);
                        if (!chain) {
                            return { ...token, tokenName: 'Unknown' };
                        }

                        // Encode the function call
                        const data = encodeFunctionData({
                            abi: nameAbi,
                            functionName: 'name'
                        });

                        // Make the eth call
                        const result = await dbCtx.ethCall(chain.evmChainId, token.address as `0x${string}`, data);

                        // Decode the result
                        const decodedName = decodeFunctionResult({
                            abi: nameAbi,
                            functionName: 'name',
                            data: result as `0x${string}`
                        });

                        return { ...token, tokenName: decodedName || 'Unknown' };
                    } catch (error) {
                        // If the call fails, the token might not implement name() or might be a proxy
                        console.log(`Failed to get name for token ${token.address} on ${token.blockchainId}:`, error);
                        return { ...token, tokenName: 'Unknown' };
                    }
                })
            );

            console.timeEnd('top-erc20-tokens');
            reply.status(200).send(tokensWithNames);
        });
    }
};

export default module;
