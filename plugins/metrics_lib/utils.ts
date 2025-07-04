import { RpcTraceResult } from "../../blockFetcher/evmTypes";
import { 
    TIME_INTERVAL_HOUR, 
    TIME_INTERVAL_DAY, 
    TIME_INTERVAL_WEEK, 
    TIME_INTERVAL_MONTH,
    METRIC_cumulativeContracts,
    METRIC_cumulativeTxCount
} from "./constants";

export interface MetricResult {
    timestamp: number;
    value: number;
}

export function isCumulativeMetric(metricId: number): boolean {
    return metricId === METRIC_cumulativeContracts || metricId === METRIC_cumulativeTxCount;
}

export function getTimeIntervalId(timeInterval: string): number {
    switch (timeInterval) {
        case 'hour': return TIME_INTERVAL_HOUR;
        case 'day': return TIME_INTERVAL_DAY;
        case 'week': return TIME_INTERVAL_WEEK;
        case 'month': return TIME_INTERVAL_MONTH;
        default: return -1;
    }
}

export function normalizeTimestamp(timestamp: number, timeInterval: number): number {
    const date = new Date(timestamp * 1000); // Convert to milliseconds for Date constructor

    switch (timeInterval) {
        case TIME_INTERVAL_HOUR:
            return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCHours()) / 1000);

        case TIME_INTERVAL_DAY:
            return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()) / 1000);

        case TIME_INTERVAL_WEEK:
            const dayOfWeek = date.getUTCDay();
            const daysToMonday = (dayOfWeek === 0) ? 6 : dayOfWeek - 1;
            const monday = new Date(date);
            monday.setUTCDate(date.getUTCDate() - daysToMonday);
            return Math.floor(Date.UTC(monday.getUTCFullYear(), monday.getUTCMonth(), monday.getUTCDate()) / 1000);

        case TIME_INTERVAL_MONTH:
            return Math.floor(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1) / 1000);

        default:
            throw new Error(`Unknown time interval: ${timeInterval}`);
    }
}

export function getPreviousTimestamp(timestamp: number, timeInterval: number): number {
    const date = new Date(timestamp * 1000);

    switch (timeInterval) {
        case TIME_INTERVAL_HOUR:
            return timestamp - 3600;
        case TIME_INTERVAL_DAY:
            return timestamp - 86400;
        case TIME_INTERVAL_WEEK:
            return timestamp - 604800;
        case TIME_INTERVAL_MONTH:
            date.setUTCMonth(date.getUTCMonth() - 1);
            return Math.floor(date.getTime() / 1000);
        default:
            return timestamp;
    }
}

export function backfillIncrementalMetric(
    results: MetricResult[],
    timeInterval: number,
    startTimestamp?: number,
    endTimestamp?: number
): MetricResult[] {
    if (results.length === 0) return results;

    const backfilled: MetricResult[] = [];
    const resultMap = new Map(results.map(r => [r.timestamp, r.value]));

    const oldest = results[results.length - 1]!.timestamp;
    const newest = results[0]!.timestamp;

    const start = startTimestamp ? Math.max(startTimestamp, oldest) : oldest;
    const end = endTimestamp ? Math.min(endTimestamp, newest) : newest;

    let current = newest;
    while (current >= start) {
        const value = resultMap.get(current);
        backfilled.push({
            timestamp: current,
            value: value !== undefined ? value : 0
        });
        current = getPreviousTimestamp(current, timeInterval);
    }

    return backfilled;
}

export function backfillCumulativeMetric(
    results: MetricResult[],
    timeInterval: number,
    currentCumulativeValue: number,
    startTimestamp?: number,
    endTimestamp?: number
): MetricResult[] {
    // If no results, create synthetic results based on current value
    if (results.length === 0) {
        // If there's no cumulative value either, return empty array
        if (currentCumulativeValue <= 0) {
            return [];
        }

        // Generate timestamps for the requested range
        const now = Math.floor(Date.now() / 1000);
        const end = endTimestamp || now;
        const start = startTimestamp || end - (24 * 60 * 60); // Default to 1 day range

        const syntheticResults: MetricResult[] = [];
        let current = normalizeTimestamp(end, timeInterval);
        const normalizedStart = normalizeTimestamp(start, timeInterval);

        while (current >= normalizedStart) {
            syntheticResults.push({
                timestamp: current,
                value: currentCumulativeValue
            });
            current = getPreviousTimestamp(current, timeInterval);
        }

        return syntheticResults;
    }

    const backfilled: MetricResult[] = [];
    const resultMap = new Map(results.map(r => [r.timestamp, r.value]));

    const oldest = results[results.length - 1]!.timestamp;
    const newest = results[0]!.timestamp;

    const start = startTimestamp ? Math.max(startTimestamp, oldest) : oldest;
    const end = endTimestamp ? Math.min(endTimestamp, newest) : newest;

    // Find the first non-zero value in results, or use currentCumulativeValue
    let lastKnownValue = currentCumulativeValue;
    for (const result of results) {
        if (result.value > 0) {
            lastKnownValue = result.value;
            break;
        }
    }

    let current = newest;
    while (current >= start) {
        const value = resultMap.get(current);
        if (value !== undefined && value > 0) {
            // Found a real non-zero value
            lastKnownValue = value;
            backfilled.push({
                timestamp: current,
                value: value
            });
        } else {
            // For gaps or zero values, use the last known value
            backfilled.push({
                timestamp: current,
                value: lastKnownValue
            });
        }
        current = getPreviousTimestamp(current, timeInterval);
    }

    return backfilled;
}

// Helper function to count CREATE calls in traces
export function countCreateCallsInTrace(trace: any): number {
    let count = 0;

    // Check if this trace is a contract creation (CREATE, CREATE2, CREATE3)
    if (trace.type === 'CREATE' || trace.type === 'CREATE2' || trace.type === 'CREATE3') {
        count = 1;
    }

    // Recursively check nested calls
    if (trace.calls && Array.isArray(trace.calls)) {
        for (const nestedCall of trace.calls) {
            count += countCreateCallsInTrace(nestedCall);
        }
    }

    return count;
}