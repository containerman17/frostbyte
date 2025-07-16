#!/bin/bash

echo "Forcing RocksDB compaction..."

# Trigger manual flush and compaction
docker exec myrocks mysql -uroot -proot -e "
  SET GLOBAL rocksdb_force_flush_memtable_now = 1;
  SELECT SLEEP(2);
  SET GLOBAL rocksdb_force_flush_memtable_and_lzero_now = 1;
  SET GLOBAL rocksdb_compact_cf = 'all';
"

echo "Compaction triggered. Monitoring progress..."

# Define the monitoring query
MONITOR_SQL="
  SELECT
    CF_NAME          AS ColumnFamily,
    STAT_TYPE,
    VALUE
  FROM INFORMATION_SCHEMA.ROCKSDB_CFSTATS
  WHERE STAT_TYPE IN (
    'NUM_FILES_L0',
    'NUM_FILES_L1',
    'NUM_FILES_L2',
    'COMPACTED_FILES_SIZE',
    'CUR_SIZE_ALL_MEM_TABLES'
  )
  ORDER BY CF_NAME, STAT_TYPE;
"

# Use watch to re-run the above every 5 seconds
watch -n 5 "docker exec myrocks mysql -uroot -proot -e \"$MONITOR_SQL\""
