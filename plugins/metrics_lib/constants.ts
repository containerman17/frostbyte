// Time interval constants
export const TIME_INTERVAL_HOUR = 0;
export const TIME_INTERVAL_DAY = 1;
export const TIME_INTERVAL_WEEK = 2;
export const TIME_INTERVAL_MONTH = 3;

// Metric constants
export const METRIC_txCount = 0;
export const METRIC_cumulativeContracts = 1;
export const METRIC_cumulativeTxCount = 2;
export const METRIC_activeSenders = 3;
export const METRIC_activeAddresses = 4;

// Event signature for Transfer events
export const TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

// Available metrics mapping
export const METRICS = {
    txCount: METRIC_txCount,
    cumulativeContracts: METRIC_cumulativeContracts,
    cumulativeTxCount: METRIC_cumulativeTxCount,
    activeSenders: METRIC_activeSenders,
    activeAddresses: METRIC_activeAddresses,
} as const;

// Time interval mapping
export const TIME_INTERVALS = [TIME_INTERVAL_HOUR, TIME_INTERVAL_DAY, TIME_INTERVAL_WEEK, TIME_INTERVAL_MONTH];