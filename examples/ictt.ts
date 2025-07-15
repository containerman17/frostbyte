import { type IndexingPlugin, abiUtils, prepQueryCached, encodingUtils, viem } from "../index";
import ERC20TokenHome from './abi/ERC20TokenHome.abi.json';
import type { ContractHomeData, RemoteData } from './types/ictt.types';

const events = abiUtils.getEventHashesMap(ERC20TokenHome as abiUtils.AbiItem[]);

const decodeRemoteRegistered = (log: viem.Log) => {
    const args = viem.decodeEventLog({
        abi: ERC20TokenHome as abiUtils.AbiItem[],
        data: log.data,
        topics: log.topics,
    }).args as {
        remoteBlockchainID: string;
        remoteTokenTransferrerAddress: `0x${string}`;
        initialCollateralNeeded: bigint;
        tokenDecimals: number;
    };
    return {
        ...args,
        initialCollateralNeeded: args.initialCollateralNeeded !== 0n,
    };
}

const decodeEventWithAmount = (log: viem.Log) => {
    return viem.decodeEventLog({
        abi: ERC20TokenHome as abiUtils.AbiItem[],
        data: log.data,
        topics: log.topics,
    }).args as any;
}

// Helper to add amounts safely
const addAmount = (existing: string | undefined, toAdd: bigint): string => {
    const current = existing ? BigInt(existing) : 0n;
    return (current + toAdd).toString();
}

const module: IndexingPlugin = {
    name: "ictt",
    version: 8, // Bumped for new fields
    usesTraces: false,

    initialize: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS contract_homes (
                address TEXT PRIMARY KEY,
                data JSON NOT NULL
            );
        `);
    },

    handleTxBatch: (db, blocksDb, batch) => {
        // First, get all existing contract homes
        const existingRows = db.prepare(`
            SELECT address, data FROM contract_homes
        `).all() as Array<{ address: string; data: string }>;

        const existingHomes = new Map<string, ContractHomeData>();
        for (const row of existingRows) {
            existingHomes.set(row.address.toLowerCase(), JSON.parse(row.data));
        }

        // Process events
        const updatedHomes = new Map<string, ContractHomeData>();
        const debugEventCounter: Record<string, number> = {};

        for (const tx of batch.txs) {
            for (const log of tx.receipt.logs) {
                const eventName = events.get(log.topics[0] || "");
                if (!eventName) continue;

                debugEventCounter[eventName] = (debugEventCounter[eventName] || 0) + 1;

                const contractAddress = log.address.toLowerCase();

                if (eventName === "RemoteRegistered") {
                    const event = decodeRemoteRegistered(log as unknown as viem.Log);
                    const remoteBlockchainId = encodingUtils.hexToCB58(event.remoteBlockchainID);
                    const remoteTokenAddress = event.remoteTokenTransferrerAddress.toLowerCase();

                    // Get or create contract home data
                    let homeData = updatedHomes.get(contractAddress) || existingHomes.get(contractAddress);
                    if (!homeData) {
                        homeData = {
                            address: contractAddress as `0x${string}`,
                            remotes: {},
                            callFailedCnt: 0,
                            callFailedSum: "0",
                            callSucceededCnt: 0,
                            callSucceededSum: "0",
                            tokensWithdrawnCnt: 0,
                            tokensWithdrawnSum: "0"
                        };
                    }

                    // Ensure nested structure exists
                    if (!homeData.remotes[remoteBlockchainId]) {
                        homeData.remotes[remoteBlockchainId] = {};
                    }

                    // Add the remote registration
                    homeData.remotes[remoteBlockchainId][remoteTokenAddress] = {
                        initialCollateralNeeded: event.initialCollateralNeeded,
                        tokenDecimals: event.tokenDecimals,
                        collateralAddedCnt: 0,
                        collateralAddedSum: "0",
                        tokensAndCallRoutedCnt: 0,
                        tokensAndCallRoutedSum: "0",
                        tokensAndCallSentCnt: 0,
                        tokensAndCallSentSum: "0",
                        tokensRoutedCnt: 0,
                        tokensRoutedSum: "0",
                        tokensSentCnt: 0,
                        tokensSentSum: "0"
                    };

                    updatedHomes.set(contractAddress, homeData);
                }
                // Process amount events for both existing homes and newly registered ones
                else if (existingHomes.has(contractAddress) || updatedHomes.has(contractAddress)) {
                    // Get the home data, making a deep copy to avoid mutation issues
                    const existingData = updatedHomes.get(contractAddress) || existingHomes.get(contractAddress)!;
                    const homeData = JSON.parse(JSON.stringify(existingData)) as ContractHomeData;

                    if (eventName === "CallFailed" || eventName === "CallSucceeded" || eventName === "TokensWithdrawn") {
                        // Local events
                        const args = decodeEventWithAmount(log as unknown as viem.Log);
                        const amount = args.amount as bigint;

                        if (eventName === "CallFailed") {
                            homeData.callFailedCnt += 1;
                            homeData.callFailedSum = addAmount(homeData.callFailedSum, amount);
                        } else if (eventName === "CallSucceeded") {
                            homeData.callSucceededCnt += 1;
                            homeData.callSucceededSum = addAmount(homeData.callSucceededSum, amount);
                        } else if (eventName === "TokensWithdrawn") {
                            homeData.tokensWithdrawnCnt += 1;
                            homeData.tokensWithdrawnSum = addAmount(homeData.tokensWithdrawnSum, amount);
                        }

                        updatedHomes.set(contractAddress, homeData);
                    }
                    else if (eventName === "CollateralAdded") {
                        // Remote event with indexed fields
                        const args = decodeEventWithAmount(log as unknown as viem.Log) as {
                            remoteBlockchainID: string;
                            remoteTokenTransferrerAddress: `0x${string}`;
                            amount: bigint;
                        };

                        const remoteBlockchainId = encodingUtils.hexToCB58(args.remoteBlockchainID);
                        const remoteTokenAddress = args.remoteTokenTransferrerAddress.toLowerCase();

                        const remote = homeData.remotes[remoteBlockchainId]?.[remoteTokenAddress];
                        if (remote) {
                            remote.collateralAddedCnt += 1;
                            remote.collateralAddedSum = addAmount(remote.collateralAddedSum, args.amount);
                            updatedHomes.set(contractAddress, homeData);
                        }
                    }
                    else if (eventName === "TokensAndCallRouted" || eventName === "TokensAndCallSent" ||
                        eventName === "TokensRouted" || eventName === "TokensSent") {
                        // Remote events with input struct
                        const args = decodeEventWithAmount(log as unknown as viem.Log) as {
                            input: {
                                destinationBlockchainID: string;
                                destinationTokenTransferrerAddress: `0x${string}`;
                            };
                            amount: bigint;
                        };

                        const remoteBlockchainId = encodingUtils.hexToCB58(args.input.destinationBlockchainID);
                        const remoteTokenAddress = args.input.destinationTokenTransferrerAddress.toLowerCase();

                        const remote = homeData.remotes[remoteBlockchainId]?.[remoteTokenAddress];
                        if (remote) {
                            if (eventName === "TokensAndCallRouted") {
                                remote.tokensAndCallRoutedCnt += 1;
                                remote.tokensAndCallRoutedSum = addAmount(remote.tokensAndCallRoutedSum, args.amount);
                            } else if (eventName === "TokensAndCallSent") {
                                remote.tokensAndCallSentCnt += 1;
                                remote.tokensAndCallSentSum = addAmount(remote.tokensAndCallSentSum, args.amount);
                            } else if (eventName === "TokensRouted") {
                                remote.tokensRoutedCnt += 1;
                                remote.tokensRoutedSum = addAmount(remote.tokensRoutedSum, args.amount);
                            } else if (eventName === "TokensSent") {
                                remote.tokensSentCnt += 1;
                                remote.tokensSentSum = addAmount(remote.tokensSentSum, args.amount);
                            }
                            updatedHomes.set(contractAddress, homeData);
                        }
                    }
                }
            }
        }

        // Save all updated homes
        if (updatedHomes.size > 0) {
            console.log('DEBUG: Event counter', debugEventCounter);

            for (const [address, data] of updatedHomes) {
                prepQueryCached(db, `
                    INSERT INTO contract_homes (address, data)
                    VALUES (?, ?)
                    ON CONFLICT(address) DO UPDATE SET
                        data = excluded.data;
                `).run(address, JSON.stringify(data));
            }
        }
    }
};

export default module;
