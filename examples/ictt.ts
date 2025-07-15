import { decodeEventLog, Log } from "viem";
import { type IndexingPlugin, abiUtils, prepQueryCached } from "../index";
import ERC20TokenHome from './abi/ERC20TokenHome.abi.json';
import { hexToCB58 } from "../lib/encodingUtils";

const events = abiUtils.getEventHashesMap(ERC20TokenHome as abiUtils.AbiItem[]);

const decodeRemoteRegistered = (log: Log) => {
    const args = decodeEventLog({
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

type ContractHomeData = {
    address: `0x${string}`;
    remotes: {
        remoteBlockchainID: string;
        remoteTokenTransferrerAddress: `0x${string}`;
        initialCollateralNeeded: boolean;
        tokenDecimals: number;
    }[]
}

const module: IndexingPlugin = {
    name: "ictt",
    version: 2,
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
        // Accumulate contract homes in memory
        const contractHomes = new Map<string, ContractHomeData>();

        const debugEventCounter: Record<string, number> = {};

        for (const tx of batch.txs) {
            for (const log of tx.receipt.logs) {
                const eventName = events.get(log.topics[0] || "");
                if (!eventName) continue;

                if (eventName === "RemoteRegistered") {
                    const event = decodeRemoteRegistered(log as unknown as Log);
                    const contractAddress = log.address.toLowerCase();

                    // Get or create contract home data
                    let homeData = contractHomes.get(contractAddress);
                    if (!homeData) {
                        homeData = {
                            address: contractAddress as `0x${string}`,
                            remotes: []
                        };
                        contractHomes.set(contractAddress, homeData);
                    }

                    // Add the remote registration
                    homeData.remotes.push({
                        remoteBlockchainID: hexToCB58(event.remoteBlockchainID),
                        remoteTokenTransferrerAddress: event.remoteTokenTransferrerAddress,
                        initialCollateralNeeded: event.initialCollateralNeeded,
                        tokenDecimals: event.tokenDecimals
                    });
                } else {
                    debugEventCounter[eventName] = (debugEventCounter[eventName] || 0) + 1;
                }
            }
        }

        console.log('DEBUG: Event counter', debugEventCounter);

        // Dump accumulated data to database
        if (contractHomes.size > 0) {
            // First, query all existing contract homes that we're about to update
            const addresses = Array.from(contractHomes.keys());
            const placeholders = addresses.map(() => '?').join(',');
            const existingQuery = prepQueryCached(db, `
                SELECT address, data 
                FROM contract_homes 
                WHERE address IN (${placeholders})
            `);

            const existingRows = existingQuery.all(...addresses) as Array<{
                address: string;
                data: string;
            }>;

            // Create a map of existing data
            const existingData = new Map<string, ContractHomeData>();
            for (const row of existingRows) {
                existingData.set(row.address, JSON.parse(row.data));
            }

            // Merge new remotes with existing ones
            for (const [address, newData] of contractHomes) {
                const existing = existingData.get(address);
                if (existing) {
                    // Merge remotes - add new ones to existing array
                    existing.remotes.push(...newData.remotes);
                    // Update the contractHomes map with merged data
                    contractHomes.set(address, existing);
                }
            }

            // Now insert/update all contract homes
            for (const [address, data] of contractHomes) {
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
