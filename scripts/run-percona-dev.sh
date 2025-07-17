#!/bin/bash

# Clean up any existing container and volume
docker rm -f myrocks 2>/dev/null || true
docker volume rm -f mysql 2>/dev/null || true
docker volume create mysql


docker run -d --name myrocks \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -v mysql:/var/lib/mysql \
  -e INIT_ROCKSDB=1 \
  percona/percona-server:8.4

sleep 10;
docker stop myrocks;
docker rm myrocks;

docker run -d --name myrocks \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -v mysql:/var/lib/mysql \
  -e INIT_ROCKSDB=1 \
  -v $(pwd)/assets/rocksdb_configs/dev/rocksdb.cnf:/etc/my.cnf.d/rocksdb.cnf \
  percona/percona-server:8.4

# Wait for MySQL to start completely
echo "Waiting for MySQL to start..."
sleep 10

# Check if container is running
if ! docker ps | grep -q myrocks; then
  echo "Container failed to start. Checking logs:"
  docker logs myrocks 2>&1 | tail -20
  exit 1
fi

echo "Container started successfully!"
echo "Verifying RocksDB is the default engine..."
docker exec myrocks mysql -uroot -proot -e "SHOW ENGINES;" | grep "ROCKSDB"
