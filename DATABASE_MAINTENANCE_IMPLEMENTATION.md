# SQLite Database Maintenance Implementation

This document describes the implementation of the absolutely-minimal SQLite maintenance playbook for the blockchain indexer.

## Overview

The maintenance strategy follows a 3-step playbook to optimize SQLite database performance:
1. **Initial Setup**: Enable incremental vacuum once during database creation
2. **Post-Catch-Up Cleanup**: Optimize settings after the chain catches up
3. **Periodic Maintenance**: Small cleanup operations during idle periods

## Implementation Details

### 1. Initial Setup (Step 1)
**Location**: `BlockDB.ts:initPragmas()` and `lib/dbHelper.ts:initializeIndexingDB()`

```sql
PRAGMA auto_vacuum = INCREMENTAL;
VACUUM;                -- builds the page map needed later
```

**When**: 
- Executed once when the database is first created
- Tracked via `is_caught_up` flag (-1 = never initialized, 0 = initialized but not caught up, 1 = caught up)

### 2. Post-Catch-Up Cleanup (Step 2)
**Location**: `BlockDB.ts:performPostCatchUpMaintenance()` and `lib/dbHelper.ts:performIndexingPostCatchUpMaintenance()`

```sql
PRAGMA wal_checkpoint(TRUNCATE);   -- flush & zero the big WAL
PRAGMA journal_size_limit = 64*1024*1024;  -- cap future WAL at 64 MB
PRAGMA wal_autocheckpoint = 10000;         -- merge every ~40 MB
```

**When**:
- Triggered automatically when chain catches up (stored blocks == latest blockchain block)
- Detected in `startFetchingLoop.ts` via `BlockDB.checkAndUpdateCatchUpStatus()`
- Also triggered for indexing databases when blocks database catches up

### 3. Periodic Maintenance (Step 3)
**Location**: `BlockDB.ts:performPeriodicMaintenance()` and `lib/dbHelper.ts:performIndexingPeriodicMaintenance()`

```sql
PRAGMA incremental_vacuum(1000);   -- reclaims ≤ 4 MB; finishes in ms
```

**When**:
- During idle gaps when no new blocks are available
- Only runs if the database has caught up (`is_caught_up` flag = 1)
- Runs every 3 seconds during idle periods

## Key Implementation Components

### BlockDB Changes
- **`getIsCaughtUp()`/`setIsCaughtUp()`**: Track catch-up state
- **`checkAndUpdateCatchUpStatus()`**: Detect when chain catches up
- **`performPostCatchUpMaintenance()`**: Step 2 maintenance
- **`performPeriodicMaintenance()`**: Step 3 maintenance
- **Modified `initPragmas()`**: Dynamic pragma settings based on catch-up state

### Indexing Database Changes
- **Modified `initializeIndexingDB()`**: Initial setup and dynamic pragmas
- **`performIndexingPostCatchUpMaintenance()`**: Step 2 for indexing DBs
- **`performIndexingPeriodicMaintenance()`**: Step 3 for indexing DBs

### Integration Points
- **`startFetchingLoop.ts`**: Detect catch-up after storing blocks, periodic maintenance during idle
- **`indexer.ts`**: Check blocks DB catch-up status and trigger indexing DB maintenance
- **`tsconfig.json`**: Added DOM types for console/performance APIs

## Database State Tracking

The implementation uses a `is_caught_up` flag stored in the `kv_int` table:
- **-1**: Never initialized (first time setup needed)
- **0**: Initialized but not caught up (bulk write mode)
- **1**: Caught up (optimized mode with maintenance)

## Performance Characteristics

### Before Catch-Up (Bulk Write Mode)
- Larger WAL autocheckpoint (20000 pages / ~80MB)
- No size limits on WAL
- Optimized for sequential writes

### After Catch-Up (Maintenance Mode)
- Smaller WAL autocheckpoint (10000 pages / ~40MB)
- WAL size limit (64MB)
- Incremental vacuum during idle periods
- Optimized for steady-state operations

## Monitoring

The implementation includes logging for:
- Initial setup completion
- Post-catch-up maintenance execution and timing
- Periodic maintenance execution (only if > 10ms)
- Catch-up detection events

## Benefits

1. **Minimal Overhead**: Only 3 simple maintenance operations
2. **Automatic**: No manual intervention required
3. **Adaptive**: Changes behavior based on catch-up state
4. **Efficient**: Maintenance runs only when needed
5. **Fast**: Periodic maintenance completes in milliseconds

This implementation ensures optimal SQLite performance throughout the blockchain indexing lifecycle while maintaining database integrity and minimizing maintenance overhead.