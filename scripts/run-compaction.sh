#!/bin/bash

echo "Forcing RocksDB compaction..."

# Trigger manual flush and compaction
docker exec myrocks mysql -uroot -proot -e "
  SET GLOBAL rocksdb_force_flush_memtable_now = 1;
  SELECT SLEEP(2);
  SET GLOBAL rocksdb_force_flush_memtable_and_lzero_now = 1;
  SET GLOBAL rocksdb_compact_cf = 'all';
"

echo "Compaction triggered."
