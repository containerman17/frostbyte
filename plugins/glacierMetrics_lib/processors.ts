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
    // Pre-allocate maps with estimated capacity
    const incrementalUpdates = new Map<string, number>();
    const cumulativeUpdates = new Map<string, number>();
    const activeSendersMap = new Map<string, Set<string>>();
    const activeAddressesMap = new Map<string, Set<string>>();

    // Process traces once if available
    let traceContractCount = 0;
    if (batch.traces) {
        for (const trace of batch.traces) {
            traceContractCount += countCreateCallsInTrace(trace.result);
        }
    }

    // Process transactions in a single pass
    let totalTxCount = 0;
    let totalContractCount = traceContractCount;

    for (const tx of batch.txs) {
        totalTxCount++;
        const blockTimestamp = tx.blockTs;

        // Count contract deployments from receipts if no traces
        if (!batch.traces && tx.receipt.contractAddress) {
            totalContractCount++;
        }

        // Process addresses more efficiently
        const senders = new Set<string>();
        const addresses = new Set<string>();

        // Add transaction addresses
        senders.add(tx.tx.from);
        addresses.add(tx.tx.from);
        if (tx.tx.to) {
            addresses.add(tx.tx.to);
        }

        // Process Transfer events in a single loop
        for (const log of tx.receipt.logs) {
            // Quick filter: check topics length and first topic in one condition
            if (log.topics.length >= 3 && log.topics[0] === TRANSFER_EVENT_SIGNATURE) {
                // Extract addresses directly without intermediate variables
                const fromTopic = log.topics[1];
                const toTopic = log.topics[2];

                if (fromTopic && fromTopic.length >= 42) {
                    const fromAddress = "0x" + fromTopic.slice(-40);
                    senders.add(fromAddress);
                    addresses.add(fromAddress);
                }

                if (toTopic && toTopic.length >= 42) {
                    const toAddress = "0x" + toTopic.slice(-40);
                    addresses.add(toAddress);
                }
            }
        }

        // Update time interval maps efficiently
        for (const timeInterval of TIME_INTERVALS) {
            const normalizedTimestamp = normalizeTimestamp(blockTimestamp, timeInterval);

            // Update tx count
            const txKey = `${timeInterval},${normalizedTimestamp},${METRIC_txCount}`;
            incrementalUpdates.set(txKey, (incrementalUpdates.get(txKey) || 0) + 1);

            // Update active senders/addresses
            const periodKey = `${timeInterval},${normalizedTimestamp}`;

            if (!activeSendersMap.has(periodKey)) {
                activeSendersMap.set(periodKey, new Set<string>());
            }
            if (!activeAddressesMap.has(periodKey)) {
                activeAddressesMap.set(periodKey, new Set<string>());
            }

            const senderSet = activeSendersMap.get(periodKey)!;
            const addressSet = activeAddressesMap.get(periodKey)!;

            for (const sender of senders) {
                senderSet.add(sender);
            }
            for (const address of addresses) {
                addressSet.add(address);
            }
        }
    }

    // Update cumulative counts once
    state.cumulativeTxCount += totalTxCount;
    state.cumulativeContractCount += totalContractCount;

    // Build cumulative updates for all time intervals at once
    const uniqueTimestamps = new Set<string>();
    for (const tx of batch.txs) {
        for (const timeInterval of TIME_INTERVALS) {
            const normalizedTimestamp = normalizeTimestamp(tx.blockTs, timeInterval);
            uniqueTimestamps.add(`${timeInterval},${normalizedTimestamp}`);
        }
    }

    for (const timestampKey of uniqueTimestamps) {
        const [timeInterval, timestamp] = timestampKey.split(',').map(Number);

        const txKey = `${timeInterval},${timestamp},${METRIC_cumulativeTxCount}`;
        cumulativeUpdates.set(txKey, state.cumulativeTxCount);

        const contractKey = `${timeInterval},${timestamp},${METRIC_cumulativeContracts}`;
        cumulativeUpdates.set(contractKey, state.cumulativeContractCount);
    }

    // Apply all updates in a single transaction
    const transaction = db.transaction(() => {
        // Create temporary tables for batch operations
        db.exec(`
            CREATE TEMP TABLE temp_senders (
                timeInterval INTEGER,
                periodTimestamp INTEGER,
                address TEXT,
                PRIMARY KEY (timeInterval, periodTimestamp, address)
            ) WITHOUT ROWID
        `);

        db.exec(`
            CREATE TEMP TABLE temp_addresses (
                timeInterval INTEGER,
                periodTimestamp INTEGER,
                address TEXT,
                PRIMARY KEY (timeInterval, periodTimestamp, address)
            ) WITHOUT ROWID
        `);

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

        const tempSenderStmt = prepQueryCached(db, `
            INSERT OR IGNORE INTO temp_senders (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
        `);

        const tempAddressStmt = prepQueryCached(db, `
            INSERT OR IGNORE INTO temp_addresses (timeInterval, periodTimestamp, address) 
            VALUES (?, ?, ?)
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

        // Batch insert into temp tables
        for (const [key, senders] of activeSendersMap) {
            const [timeInterval, periodTimestamp] = key.split(',').map(Number);
            for (const address of senders) {
                tempSenderStmt.run(timeInterval, periodTimestamp, address);
            }
        }

        for (const [key, addresses] of activeAddressesMap) {
            const [timeInterval, periodTimestamp] = key.split(',').map(Number);
            for (const address of addresses) {
                tempAddressStmt.run(timeInterval, periodTimestamp, address);
            }
        }

        // Bulk insert from temp tables to main tables
        db.exec(`
            INSERT OR IGNORE INTO active_senders (timeInterval, periodTimestamp, address)
            SELECT timeInterval, periodTimestamp, address FROM temp_senders
        `);

        db.exec(`
            INSERT OR IGNORE INTO active_addresses (timeInterval, periodTimestamp, address)
            SELECT timeInterval, periodTimestamp, address FROM temp_addresses
        `);

        // Update metrics with counts in bulk
        db.exec(`
            INSERT OR REPLACE INTO metrics (timeInterval, timestamp, metric, value)
            SELECT 
                timeInterval,
                periodTimestamp,
                ${METRIC_activeSenders},
                COUNT(DISTINCT address)
            FROM active_senders
            WHERE (timeInterval, periodTimestamp) IN (
                SELECT DISTINCT timeInterval, periodTimestamp FROM temp_senders
            )
            GROUP BY timeInterval, periodTimestamp
        `);

        db.exec(`
            INSERT OR REPLACE INTO metrics (timeInterval, timestamp, metric, value)
            SELECT 
                timeInterval,
                periodTimestamp,
                ${METRIC_activeAddresses},
                COUNT(DISTINCT address)
            FROM active_addresses
            WHERE (timeInterval, periodTimestamp) IN (
                SELECT DISTINCT timeInterval, periodTimestamp FROM temp_addresses
            )
            GROUP BY timeInterval, periodTimestamp
        `);

        // Clean up temp tables
        db.exec(`DROP TABLE temp_senders`);
        db.exec(`DROP TABLE temp_addresses`);
    });

    transaction();
}
