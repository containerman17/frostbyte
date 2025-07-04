import { StoredTx, RpcTraceResult } from "../../blockFetcher/evmTypes";
import { BlockDB } from "../../blockFetcher/BlockDB";
import SQLite from "better-sqlite3";
import { prepQueryCached } from "../../lib/prep";
import { 
    TRANSFER_EVENT_SIGNATURE, 
    TIME_INTERVALS, 
    METRIC_txCount, 
    METRIC_cumulativeContracts, 
    METRIC_cumulativeTxCount, 
    METRIC_activeSenders, 
    METRIC_activeAddresses 
} from "./constants";
import { normalizeTimestamp, countCreateCallsInTrace } from "./utils";

export interface BatchProcessorState {
    cumulativeContractCount: number;
    cumulativeTxCount: number;
}

export function initializeBatchProcessor(db: SQLite.Database): BatchProcessorState {
    // Initialize cumulative contract count from existing data
    const contractResult = prepQueryCached(db, `
        SELECT MAX(value) as maxValue 
        FROM metrics 
        WHERE metric = ? AND timeInterval = ?
    `).get(METRIC_cumulativeContracts, 1) as { maxValue: number | null }; // DAY interval

    // Initialize cumulative tx count from existing data
    const txResult = prepQueryCached(db, `
        SELECT MAX(value) as maxValue 
        FROM metrics 
        WHERE metric = ? AND timeInterval = ?
    `).get(METRIC_cumulativeTxCount, 1) as { maxValue: number | null }; // DAY interval

    return {
        cumulativeContractCount: contractResult?.maxValue || 0,
        cumulativeTxCount: txResult?.maxValue || 0
    };
}

export function processBatch(
    db: SQLite.Database,
    blocksDb: BlockDB,
    batch: { txs: StoredTx[]; traces: RpcTraceResult[] | undefined },
    state: BatchProcessorState
): void {
    // Group transactions by block timestamp
    const blockTxMap = new Map<number, StoredTx[]>();
    
    for (const tx of batch.txs) {
        const timestamp = tx.blockTs;
        if (!blockTxMap.has(timestamp)) {
            blockTxMap.set(timestamp, []);
        }
        blockTxMap.get(timestamp)!.push(tx);
    }

    // Maps to accumulate metric updates
    const incrementalUpdates = new Map<string, number>(); // key = "timeInterval,timestamp,metric"
    const cumulativeUpdates = new Map<string, number>(); // key = "timeInterval,timestamp,metric"

    // Maps to track unique senders and addresses per time period
    const activeSendersMap = new Map<string, Set<string>>(); // key = "timeInterval,periodTimestamp"
    const activeAddressesMap = new Map<string, Set<string>>(); // key = "timeInterval,periodTimestamp"

    // Process each block's transactions
    for (const [blockTimestamp, txs] of blockTxMap) {
        let txCount = 0;
        let contractCount = 0;
        const uniqueSenders = new Set<string>();
        const uniqueAddresses = new Set<string>();

        for (const tx of txs) {
            txCount++;

            // Extract senders and addresses
            uniqueSenders.add(tx.tx.from);
            uniqueAddresses.add(tx.tx.from);

            if (tx.tx.to) {
                uniqueAddresses.add(tx.tx.to);
            }

            // Count contract deployments (no traces fallback)
            if (!batch.traces && tx.receipt.contractAddress) {
                contractCount++;
            }

            // Process Transfer events for both senders and addresses
            for (const log of tx.receipt.logs) {
                if (log.topics.length >= 2 && log.topics[0] === TRANSFER_EVENT_SIGNATURE) {
                    // Extract 'from' address
                    if (log.topics[1]) {
                        const fromAddress = "0x" + log.topics[1].slice(-40);
                        uniqueSenders.add(fromAddress);
                        uniqueAddresses.add(fromAddress);
                    }

                    // Extract 'to' address for activeAddresses
                    if (log.topics.length >= 3 && log.topics[2]) {
                        const toAddress = "0x" + log.topics[2].slice(-40);
                        uniqueAddresses.add(toAddress);
                    }
                }
            }
        }

        // Count contract deployments from traces if available
        if (batch.traces) {
            for (const trace of batch.traces) {
                contractCount += countCreateCallsInTrace(trace.result);
            }
        }

        // Accumulate incremental metrics (txCount)
        for (const timeInterval of TIME_INTERVALS) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${normalizedTimestamp},${METRIC_txCount}`;
            incrementalUpdates.set(key, (incrementalUpdates.get(key) || 0) + txCount);
        }

        // Accumulate active senders
        for (const sender of uniqueSenders) {
            for (const timeInterval of TIME_INTERVALS) {
                const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                const key = `${timeInterval},${periodTimestamp}`;
                if (!activeSendersMap.has(key)) {
                    activeSendersMap.set(key, new Set<string>());
                }
                activeSendersMap.get(key)!.add(sender);
            }
        }

        // Accumulate active addresses
        for (const address of uniqueAddresses) {
            for (const timeInterval of TIME_INTERVALS) {
                const periodTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
                const key = `${timeInterval},${periodTimestamp}`;
                if (!activeAddressesMap.has(key)) {
                    activeAddressesMap.set(key, new Set<string>());
                }
                activeAddressesMap.get(key)!.add(address);
            }
        }

        // Update cumulative transaction count
        state.cumulativeTxCount += txCount;

        // Accumulate cumulative metrics (cumulativeTxCount)
        for (const timeInterval of TIME_INTERVALS) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${normalizedTimestamp},${METRIC_cumulativeTxCount}`;
            cumulativeUpdates.set(key, state.cumulativeTxCount);
        }

        // Update cumulative contract count
        state.cumulativeContractCount += contractCount;

        // Accumulate cumulative metrics (cumulativeContracts)
        for (const timeInterval of TIME_INTERVALS) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);
            const key = `${timeInterval},${normalizedTimestamp},${METRIC_cumulativeContracts}`;
            cumulativeUpdates.set(key, state.cumulativeContractCount);
        }
    }

    // Apply all updates in a single transaction
    const transaction = db.transaction(() => {
        // Prepare statements for batch operations
        const incrementalStmt = prepQueryCached(db, `
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = value + ?
        `);

        const cumulativeStmt = prepQueryCached(db, `
            INSERT INTO metrics (timeInterval, timestamp, metric, value) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timeInterval, timestamp, metric) 
            DO UPDATE SET value = ?
        `);

        const activeSenderStmt = prepQueryCached(db, `
            INSERT OR IGNORE INTO active_senders (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
        `);

        const activeAddressStmt = prepQueryCached(db, `
            INSERT OR IGNORE INTO active_addresses (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
        `);

        // Prepare count queries for active senders/addresses
        const countSendersStmt = prepQueryCached(db, `
            SELECT COUNT(DISTINCT address) as count 
            FROM active_senders 
            WHERE timeInterval = ? AND periodTimestamp = ?
        `);

        const countAddressesStmt = prepQueryCached(db, `
            SELECT COUNT(DISTINCT address) as count 
            FROM active_addresses 
            WHERE timeInterval = ? AND periodTimestamp = ?
        `);

        // Apply incremental updates
        for (const [key, value] of incrementalUpdates) {
            const [timeInterval, timestamp, metric] = key.split(',').map(Number);
            incrementalStmt.run(timeInterval, timestamp, metric, value, value);
        }

        // Apply cumulative updates
        for (const [key, value] of cumulativeUpdates) {
            const [timeInterval, timestamp, metric] = key.split(',').map(Number);
            cumulativeStmt.run(timeInterval, timestamp, metric, value, value);
        }

        // Insert active senders and update metrics
        for (const [key, senders] of activeSendersMap) {
            const [timeInterval, periodTimestamp] = key.split(',').map(Number);

            // Insert all unique senders for this period
            for (const address of senders) {
                activeSenderStmt.run(timeInterval, periodTimestamp, address);
            }

            // Count total unique senders (including existing ones)
            const countResult = countSendersStmt.get(timeInterval, periodTimestamp) as { count: number };

            // Update the metric with the total count
            cumulativeStmt.run(timeInterval, periodTimestamp, METRIC_activeSenders, countResult.count, countResult.count);
        }

        // Insert active addresses and update metrics
        for (const [key, addresses] of activeAddressesMap) {
            const [timeInterval, periodTimestamp] = key.split(',').map(Number);

            // Insert all unique addresses for this period
            for (const address of addresses) {
                activeAddressStmt.run(timeInterval, periodTimestamp, address);
            }

            // Count total unique addresses (including existing ones)
            const countResult = countAddressesStmt.get(timeInterval, periodTimestamp) as { count: number };

            // Update the metric with the total count
            cumulativeStmt.run(timeInterval, periodTimestamp, METRIC_activeAddresses, countResult.count, countResult.count);
        }
    });

    transaction();
}