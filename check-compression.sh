#!/bin/bash

echo "Checking RocksDB compression effectiveness..."

docker exec -it myrocks mysql -uroot -proot -e "
-- Force a checkpoint to ensure all data is on disk
SET GLOBAL rocksdb_force_flush_memtable_now = 1;

-- Show compression stats from SST files
SELECT 
    'SST Files Summary' as Info,
    COUNT(*) as Total_Files,
    ROUND(SUM(FILE_SIZE)/1024/1024, 2) as Total_Size_MB,
    ROUND(SUM(NUM_DATA_BLOCKS * 4096)/1024/1024, 2) as Uncompressed_Estimate_MB,
    ROUND(100 - (SUM(FILE_SIZE) / (SUM(NUM_DATA_BLOCKS * 4096)) * 100), 2) as Compression_Saved_Pct
FROM INFORMATION_SCHEMA.ROCKSDB_SST_PROPS
WHERE COMPRESSION_NAME = 'Zstandard';

-- Show per-column family stats
SELECT 
    COLUMN_FAMILY as CF,
    COUNT(*) as Files,
    ROUND(SUM(FILE_SIZE)/1024/1024, 2) as Size_MB,
    COMPRESSION_NAME
FROM INFORMATION_SCHEMA.ROCKSDB_SST_PROPS
GROUP BY COLUMN_FAMILY, COMPRESSION_NAME
ORDER BY Size_MB DESC;

-- Global stats
SHOW STATUS LIKE 'rocksdb%compress%';
"

echo -e "\nDisk usage:"
docker exec -it myrocks du -sh /var/lib/mysql/ 
