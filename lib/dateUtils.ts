// Date utility functions for normalizing timestamps to different time intervals

export const TIME_INTERVAL_HOUR = 0;
export const TIME_INTERVAL_DAY = 1;
export const TIME_INTERVAL_WEEK = 2;
export const TIME_INTERVAL_MONTH = 3;

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

export function getTimeIntervalFromString(timeInterval: string): number {
    switch (timeInterval) {
        case 'hour': return TIME_INTERVAL_HOUR;
        case 'day': return TIME_INTERVAL_DAY;
        case 'week': return TIME_INTERVAL_WEEK;
        case 'month': return TIME_INTERVAL_MONTH;
        default: return -1;
    }
}