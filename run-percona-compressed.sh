#!/bin/bash

# Clean up any existing container and volume
docker rm -f myrocks 2>/dev/null || true
docker volume rm -f mysql 2>/dev/null || true
docker volume create mysql

# Run Percona Server with RocksDB and custom compression config
docker run -d --name myrocks \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e INIT_ROCKSDB=1 \
  -v mysql:/var/lib/mysql \
  -v $(pwd)/rocksdb.cnf:/etc/mysql/conf.d/rocksdb.cnf \
  percona/percona-server:8.0

echo "Waiting for MySQL to start..."
sleep 10

# Set RocksDB as default storage engine
docker exec -it myrocks \
  mysql -uroot -proot \
  -e "SET PERSIST default_storage_engine = 'rocksdb';"

echo "RocksDB with zstd compression is now configured!"
echo "Your database should now use much less disk space." 
