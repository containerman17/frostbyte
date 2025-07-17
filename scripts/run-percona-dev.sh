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
  -v $(pwd)/assets/rocksdb_configs/dev/rocksdb.cnf:/etc/mysql/conf.d/rocksdb.cnf \
  percona/percona-server:8.0
