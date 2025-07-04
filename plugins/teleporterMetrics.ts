import type { IndexerModule } from "../lib/types";
import { createRoute, z } from "@hono/zod-openapi";
import { prepQueryCached } from "../lib/prep";


// Constants
const TELEPORTER_ADDRESS = "0x253b2784c75e510dd0ff1da844684a1ac0aa5fcf";

// Topic IDs for the events we're tracking
const SEND_CROSS_CHAIN_MESSAGE_TOPIC = '0x2a211ad4a59ab9d003852404f9c57c690704ee755f3c79d2c2812ad32da99df8';
const RECEIVE_CROSS_CHAIN_MESSAGE_TOPIC = '0x292ee90bbaf70b5d4936025e09d56ba08f3e421156b6a568cf3c2840d9343e34';

// Response schema for teleporter metrics
const TeleporterMetricResponseSchema = z.object({
    result: z.object({
        value: z.number()
    })
}).openapi('TeleporterMetricResponse');

// Lifecycle hooks
const wipe: IndexerModule["wipe"] = (db) => {
    // Drop the metrics table to start fresh
    db.exec(`DROP TABLE IF EXISTS teleporter_metrics`);
};

const initialize: IndexerModule["initialize"] = (db) => {
    // Create a simple table to store the counters
    db.exec(`
        CREATE TABLE IF NOT EXISTS teleporter_metrics (
            metric_name TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        )
    `);

    // Initialize counters if they don't exist
    const insertStmt = prepQueryCached(db, `
        INSERT OR IGNORE INTO teleporter_metrics (metric_name, value) VALUES (?, ?)
    `);
    insertStmt.run('source_txn_count', 0);
    insertStmt.run('destination_txn_count', 0);
};

// Batch processor (required)
const handleTxBatch: IndexerModule["handleTxBatch"] = (db, blocksDb, batch) => {
    let sourceDelta = 0;
    let destinationDelta = 0;

    // Process all transactions in the batch
    for (const storedTx of batch.txs) {
        // Check logs for relevant events
        for (const log of storedTx.receipt.logs) {
            if (log.address.toLowerCase() !== TELEPORTER_ADDRESS.toLowerCase()) {
                continue;
            }

            // Check the first topic (event signature)
            const eventTopic = log.topics[0];

            if (eventTopic === SEND_CROSS_CHAIN_MESSAGE_TOPIC) {
                sourceDelta++;
            } else if (eventTopic === RECEIVE_CROSS_CHAIN_MESSAGE_TOPIC) {
                destinationDelta++;
            }
        }
    }

    // Update counters if there were any changes
    if (sourceDelta > 0) {
        prepQueryCached(db, `
            UPDATE teleporter_metrics 
            SET value = value + ? 
            WHERE metric_name = ?
        `).run(sourceDelta, 'source_txn_count');
    }

    if (destinationDelta > 0) {
        prepQueryCached(db, `
            UPDATE teleporter_metrics 
            SET value = value + ? 
            WHERE metric_name = ?
        `).run(destinationDelta, 'destination_txn_count');
    }
};

// Optional HTTP surface
const registerRoutes: IndexerModule["registerRoutes"] = (app, db) => {
    // Route for source transaction count
    const sourceRoute = createRoute({
        method: 'get',
        path: '/teleporterMetrics/teleporterSourceTxnCount',
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: TeleporterMetricResponseSchema
                    }
                },
                description: 'Teleporter source transaction count'
            }
        },
        tags: ['Teleporter Metrics'],
        summary: 'Get teleporter source transaction count',
        description: 'Returns the total count of SendCrossChainMessage events'
    });

    app.openapi(sourceRoute, (c) => {
        const result = prepQueryCached(db,
            `SELECT value FROM teleporter_metrics WHERE metric_name = ?`
        ).get('source_txn_count') as { value: number } | undefined;

        return c.json({
            result: {
                value: result?.value || 0
            }
        });
    });

    // Route for destination transaction count
    const destRoute = createRoute({
        method: 'get',
        path: '/teleporterMetrics/teleporterDestinationTxnCount',
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: TeleporterMetricResponseSchema
                    }
                },
                description: 'Teleporter destination transaction count'
            }
        },
        tags: ['Teleporter Metrics'],
        summary: 'Get teleporter destination transaction count',
        description: 'Returns the total count of ReceiveCrossChainMessage events'
    });

    app.openapi(destRoute, (c) => {
        const result = prepQueryCached(db,
            `SELECT value FROM teleporter_metrics WHERE metric_name = ?`
        ).get('destination_txn_count') as { value: number } | undefined;

        return c.json({
            result: {
                value: result?.value || 0
            }
        });
    });

    // Route for total transaction count
    const totalRoute = createRoute({
        method: 'get',
        path: '/teleporterMetrics/teleporterTotalTxnCount',
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: TeleporterMetricResponseSchema
                    }
                },
                description: 'Teleporter total transaction count'
            }
        },
        tags: ['Teleporter Metrics'],
        summary: 'Get teleporter total transaction count',
        description: 'Returns the total count of all teleporter transactions (source + destination)'
    });

    app.openapi(totalRoute, (c) => {
        const sourceResult = prepQueryCached(db,
            `SELECT value FROM teleporter_metrics WHERE metric_name = ?`
        ).get('source_txn_count') as { value: number } | undefined;

        const destResult = prepQueryCached(db,
            `SELECT value FROM teleporter_metrics WHERE metric_name = ?`
        ).get('destination_txn_count') as { value: number } | undefined;

        const sourceValue = sourceResult?.value || 0;
        const destValue = destResult?.value || 0;

        return c.json({
            result: {
                value: sourceValue + destValue
            }
        });
    });
};

const module: IndexerModule = {
    name: "teleporter-metrics",
    version: 0,
    usesTraces: false,
    wipe,
    initialize,
    handleTxBatch,
    registerRoutes,
};
export default module;
