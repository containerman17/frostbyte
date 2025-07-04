import { createRoute, z } from "@hono/zod-openapi";

// Query schema for metrics API
export const MetricQuerySchema = z.object({
    startTimestamp: z.coerce.number().optional().openapi({
        example: 1640995200,
        description: 'Start timestamp for the query range'
    }),
    endTimestamp: z.coerce.number().optional().openapi({
        example: 1641081600,
        description: 'End timestamp for the query range'
    }),
    timeInterval: z.enum(['hour', 'day', 'week', 'month']).optional().default('hour').openapi({
        example: 'hour',
        description: 'Time interval for aggregation'
    }),
    pageSize: z.coerce.number().optional().default(10).openapi({
        example: 10,
        description: 'Number of results per page'
    }),
    pageToken: z.string().optional().openapi({
        example: '1641081600',
        description: 'Token for pagination'
    })
});

// Individual metric result schema
export const MetricResultSchema = z.object({
    timestamp: z.number().openapi({
        example: 1640995200,
        description: 'Timestamp of the metric data point'
    }),
    value: z.number().openapi({
        example: 1000,
        description: 'Metric value'
    })
}).openapi('MetricResult');

// Response schema for metrics API
export const MetricResponseSchema = z.object({
    results: z.array(MetricResultSchema).openapi({
        description: 'Array of metric results'
    }),
    nextPageToken: z.string().optional().openapi({
        description: 'Token for fetching the next page'
    })
}).openapi('MetricResponse');