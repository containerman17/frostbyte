import { type IndexingPlugin } from "../../index.ts";

const TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

type ERC20TokensData = Record<string, number>;

const module: IndexingPlugin<ERC20TokensData> = {
    name: "erc20_tokens_registry",
    version: 1,
    usesTraces: false,

    // Initialize tables
    initialize: (db) => {
        db.exec(`
            CREATE TABLE IF NOT EXISTS erc20_tokens_registry (
                contractAddress TEXT PRIMARY KEY,
                transferEventCount INTEGER NOT NULL DEFAULT 0
            )
        `);
    },

    // Process transactions to find ERC20 Transfer events
    extractData: (batch): ERC20TokensData => {
        const tokenTransferCounts: ERC20TokensData = {};

        for (let tx of batch.txs) {
            for (let log of tx.receipt.logs) {
                // Check if this is a Transfer event (first topic matches the signature)
                if (log.topics.length > 0 && log.topics[0] === TRANSFER_EVENT_SIGNATURE) {
                    const contractAddress = log.address.toLowerCase();
                    tokenTransferCounts[contractAddress] = (tokenTransferCounts[contractAddress] || 0) + 1;
                }
            }
        }

        return tokenTransferCounts;
    },

    saveExtractedData: (db, blocksDb, data: ERC20TokensData) => {
        const stmt = db.prepare(`
            INSERT INTO erc20_tokens_registry (contractAddress, transferEventCount)
            VALUES (?, ?)
            ON CONFLICT(contractAddress) DO UPDATE SET 
                transferEventCount = transferEventCount + excluded.transferEventCount
        `);

        for (let [contractAddress, transferCount] of Object.entries(data)) {
            stmt.run(contractAddress, transferCount);
        }
    }
};

export default module;
