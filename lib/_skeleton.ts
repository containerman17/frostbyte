import type { IndexerModule } from "./types";

// Static identity
export const name = "skeleton-plugin";
export const version = "0.0.1";

// Lifecycle hooks
export const wipe: IndexerModule["wipe"] = () => {
    /* no-op – nothing to clean up */
};

export const initialize: IndexerModule["initialize"] = () => {
    /* no-op – no schema to create */
};

// Batch processor (required)
export const handleTxBatch: IndexerModule["handleTxBatch"] = () => {
    /* no-op – does absolutely nothing */
};

// Optional HTTP surface
export const registerRoutes: IndexerModule["registerRoutes"] = () => {
    /* no routes to expose */
};
