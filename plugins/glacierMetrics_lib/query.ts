import SQLite from "better-sqlite3";
import { prepQueryCached } from "../../lib/prep";
import { MetricResult, backfillIncrementalMetric, backfillCumulativeMetric } from "./utils";

export function handleIncrementalMetricQuery(
    db: SQLite.Database,
    metricId: number,
    timeIntervalId: number,
    startTimestamp: number | undefined,
    endTimestamp: number | undefined,
    pageSize: number,
    pageToken: string | undefined
) {
    // Build query
    let query = `
        SELECT timestamp, value 
        FROM metrics 
        WHERE timeInterval = ? AND metric = ?
    `;
    const params: any[] = [timeIntervalId, metricId];

    if (startTimestamp) {
        query += ` AND timestamp >= ?`;
        params.push(startTimestamp);
    }

    if (endTimestamp) {
        query += ` AND timestamp <= ?`;
        params.push(endTimestamp);
    }

    if (pageToken) {
        query += ` AND timestamp < ?`;
        params.push(parseInt(pageToken));
    }

    query += ` ORDER BY timestamp DESC LIMIT ?`;
    params.push(pageSize + 1);

    const results = prepQueryCached(db, query).all(...params) as MetricResult[];

    // Check if there's a next page
    const hasNextPage = results.length > pageSize;
    if (hasNextPage) {
        results.pop();
    }

    // Backfill missing periods with zero values
    const backfilledResults = backfillIncrementalMetric(
        results, timeIntervalId, startTimestamp, endTimestamp
    );

    return {
        results: backfilledResults.slice(0, pageSize),
        nextPageToken: hasNextPage && backfilledResults.length > 0
            ? backfilledResults[Math.min(pageSize - 1, backfilledResults.length - 1)]!.timestamp.toString()
            : undefined
    };
}

export function handleCumulativeMetricQuery(
    db: SQLite.Database,
    metricId: number,
    timeIntervalId: number,
    startTimestamp: number | undefined,
    endTimestamp: number | undefined,
    pageSize: number,
    pageToken: string | undefined
) {
    // For cumulative metrics, we need to get the most recent value before the range
    // to properly backfill
    let baseQuery = `
        SELECT timestamp, value 
        FROM metrics 
        WHERE timeInterval = ? AND metric = ?
    `;
    const baseParams: any[] = [timeIntervalId, metricId];

    // Get the current timestamp to avoid future values
    const now = Math.floor(Date.now() / 1000);

    // First, get the most recent non-zero value that's not in the future
    const mostRecentResult = prepQueryCached(db,
        baseQuery + ` AND timestamp <= ? AND value > 0 ORDER BY timestamp DESC LIMIT 1`
    ).get(...baseParams, now) as MetricResult | undefined;

    const currentCumulativeValue = mostRecentResult?.value || 0;

    // Now build the main query
    let query = baseQuery;
    const params = [...baseParams];

    if (startTimestamp) {
        query += ` AND timestamp >= ?`;
        params.push(startTimestamp);
    }

    if (endTimestamp) {
        query += ` AND timestamp <= ?`;
        params.push(endTimestamp);
    }

    if (pageToken) {
        query += ` AND timestamp < ?`;
        params.push(parseInt(pageToken));
    }

    query += ` ORDER BY timestamp DESC LIMIT ?`;
    params.push(pageSize + 1);

    const results = prepQueryCached(db, query).all(...params) as MetricResult[];

    // Check if there's a next page
    const hasNextPage = results.length > pageSize;
    if (hasNextPage) {
        results.pop();
    }

    // Filter out any zero values for cumulative metrics and future timestamps
    const filteredResults = results.filter(r => r.value > 0 || r.timestamp <= now);

    // Backfill cumulative values
    const backfilledResults = backfillCumulativeMetric(
        filteredResults, timeIntervalId, currentCumulativeValue, startTimestamp, endTimestamp
    );

    return {
        results: backfilledResults.slice(0, pageSize),
        nextPageToken: hasNextPage && backfilledResults.length > 0
            ? backfilledResults[Math.min(pageSize - 1, backfilledResults.length - 1)]!.timestamp.toString()
            : undefined
    };
}
