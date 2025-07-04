import { OpenAPIHono, createRoute } from "@hono/zod-openapi";
import SQLite from "better-sqlite3";
import { BlockDB } from "../../blockFetcher/BlockDB";
import { MetricQuerySchema, MetricResponseSchema } from "./schemas";
import { METRICS } from "./constants";
import { getTimeIntervalId, isCumulativeMetric } from "./utils";
import { handleIncrementalMetricQuery, handleCumulativeMetricQuery } from "./query";

export function registerMetricsRoutes(app: OpenAPIHono, db: SQLite.Database, blocksDb: BlockDB): void {
    // Create a separate route for each metric
    for (const [metricName, metricId] of Object.entries(METRICS)) {
        createMetricRoute(app, db, metricName, metricId);
    }
}

function createMetricRoute(app: OpenAPIHono, db: SQLite.Database, metricName: string, metricId: number): void {
    const route = createRoute({
        method: 'get',
        path: `/metrics/${metricName}`,
        request: {
            query: MetricQuerySchema,
        },
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: MetricResponseSchema
                    }
                },
                description: `${metricName} metric data`
            },
            400: {
                description: 'Bad request (invalid parameters)'
            }
        },
        tags: ['Metrics'],
        summary: `Get ${metricName} data`,
        description: `Retrieve ${metricName} blockchain metric with optional filtering and pagination`
    });

    app.openapi(route, (c) => {
        const {
            startTimestamp,
            endTimestamp,
            timeInterval = 'hour',
            pageSize = 10,
            pageToken
        } = c.req.valid('query');

        // Map time interval to constant
        const timeIntervalId = getTimeIntervalId(timeInterval);
        if (timeIntervalId === -1) {
            return c.json({ error: `Invalid timeInterval: ${timeInterval}` }, 400);
        }

        // Validate pageSize
        const validPageSize = Math.min(Math.max(pageSize, 1), 2160);

        // Route to appropriate handler based on metric type
        if (isCumulativeMetric(metricId)) {
            const result = handleCumulativeMetricQuery(
                db, metricId, timeIntervalId, startTimestamp, endTimestamp,
                validPageSize, pageToken
            );
            return c.json(result);
        } else {
            const result = handleIncrementalMetricQuery(
                db, metricId, timeIntervalId, startTimestamp, endTimestamp,
                validPageSize, pageToken
            );
            return c.json(result);
        }
    });
}
