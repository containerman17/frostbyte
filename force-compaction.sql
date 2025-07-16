-- Force compaction on all column families
SET GLOBAL rocksdb_force_flush_memtable_and_lzero_now = 1;
SET GLOBAL rocksdb_compact_cf = 'all';

-- Or compact a specific column family (e.g., 'default')
-- SET GLOBAL rocksdb_compact_cf = 'default';

-- Force flush all memtables first (optional but recommended)
-- This ensures all data in memory is written to SST files
SET GLOBAL rocksdb_force_flush_memtable_now = 1;

-- For a specific table, you can use:
-- ALTER TABLE your_table_name ENGINE=ROCKSDB;

-- Check compaction progress
SHOW ENGINE ROCKSDB STATUS;

-- Monitor compaction stats
SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_COMPACTION_STATS;

-- See current CF statistics
SELECT * FROM INFORMATION_SCHEMA.ROCKSDB_CFSTATS; 
