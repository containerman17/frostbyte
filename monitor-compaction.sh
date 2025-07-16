#!/bin/bash

echo "Monitoring RocksDB compaction and compression..."

# Check disk usage before starting
echo "Current disk usage:"
docker exec -it myrocks du -sh /var/lib/mysql/

watch -n 3 "
echo '=== RocksDB Compaction Stats ==='
docker exec -it myrocks mysql -uroot -proot -s -N -e \"
SELECT CONCAT('Active compactions: ', COUNT(*)) 
FROM INFORMATION_SCHEMA.ROCKSDB_TRX 
WHERE TYPE = 'COMPACTION';

SELECT CONCAT('Column Family: ', CF, ' | ', STAT_TYPE, ': ', VALUE) as Status
FROM INFORMATION_SCHEMA.ROCKSDB_CFSTATS
WHERE STAT_TYPE IN ('NUM_IMMUTABLE_MEM_TABLE', 'CUR_SIZE_ALL_MEM_TABLES', 'ESTIMATE_TABLE_READERS_MEM')
ORDER BY CF;
\"

echo -e '\n=== SST File Stats ==='
docker exec -it myrocks mysql -uroot -proot -s -N -e \"
SELECT CONCAT('Total SST files: ', COUNT(*), ' | Total size: ', ROUND(SUM(FILE_SIZE)/1024/1024, 2), ' MB')
FROM INFORMATION_SCHEMA.ROCKSDB_SST_PROPS;
\"

echo -e '\n=== Disk Usage ==='
docker exec -it myrocks du -sh /var/lib/mysql/mydb/ 2>/dev/null | grep -v 'Warning'
" 
