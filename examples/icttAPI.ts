import type { ApiPlugin } from "../index";

type ContractHomeRemote = {
    remoteBlockchainID: string;
    remoteTokenTransferrerAddress: string;
    initialCollateralNeeded: boolean;
    tokenDecimals: number;
}

type ContractHome = {
    address: string;
    remotes: ContractHomeRemote[];
}

type ChainContractHomes = {
    chainName: string;
    blockchainId: string;
    evmChainId: number;
    contractHomes: ContractHome[];
}

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
                                contractHomes: {
                                    type: 'array',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            address: { type: 'string' },
                                            remotes: {
                                                type: 'array',
                                                items: {
                                                    type: 'object',
                                                    properties: {
                                                        remoteBlockchainID: { type: 'string' },
                                                        remoteTokenTransferrerAddress: { type: 'string' },
                                                        initialCollateralNeeded: { type: 'boolean' },
                                                        tokenDecimals: { type: 'number' }
                                                    },
                                                    required: ['remoteBlockchainID', 'remoteTokenTransferrerAddress', 'initialCollateralNeeded', 'tokenDecimals']
                                                }
                                            }
                                        },
                                        required: ['address', 'remotes']
                                    }
                                }
                            },
                            required: ['chainName', 'blockchainId', 'evmChainId', 'contractHomes']
                        }
                    }
                }
            }
        }, async (request, reply) => {
            const configs = dbCtx.getAllChainConfigs();
            const results: ChainContractHomes[] = [];

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

                const contractHomes: ContractHome[] = rows.map(row => {
                    const data = JSON.parse(row.data);
                    return {
                        address: data.address,
                        remotes: data.remotes
                    };
                });

                // Only add chains that have contract homes
                if (contractHomes.length > 0) {
                    results.push({
                        chainName: config.chainName,
                        blockchainId: config.blockchainId,
                        evmChainId: config.evmChainId,
                        contractHomes
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
