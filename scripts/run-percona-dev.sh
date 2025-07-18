#!/bin/bash

set -exu

# Clean up any existing container and volume
docker rm -f mysql 2>/dev/null || true
docker volume rm -f mysql 2>/dev/null || true
sudo rm -rf /var/lib/docker/volumes/mysql/
docker volume create mysql


docker stop mysql || true
docker rm mysql || true
docker run -d --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -v mysql:/var/lib/mysql \
  -e INIT_ROCKSDB=1 \
  percona/percona-server:8.4

# Wait for MySQL to be ready AND for root password to be set
echo "Waiting for MySQL to be ready..."
for i in {1..60}; do
  if docker exec mysql mysql -uroot -proot -e "SELECT 1" 2>/dev/null; then
    echo "MySQL is ready!"
    break
  else
    echo "Attempt $i: MySQL not ready yet..."
    sleep 2
  fi
  if [ $i -eq 60 ]; then
    echo "MySQL failed to become ready after 2 minutes"
    docker logs mysql
    exit 1
  fi
done

# Wait for RocksDB plugins to be automatically installed
echo "Waiting for RocksDB plugins to be installed..."
for i in {1..30}; do
  if docker exec mysql mysql -uroot -proot -e "SHOW ENGINES;" 2>/dev/null | grep -q "ROCKSDB"; then
    echo "RocksDB plugins are installed!"
    break
  else
    echo "Attempt $i: RocksDB plugins not ready yet..."
    sleep 2
  fi
  if [ $i -eq 30 ]; then
    echo "RocksDB plugins failed to install automatically after 1 minute"
    echo "Available engines:"
    docker exec mysql mysql -uroot -proot -e "SHOW ENGINES;"
    exit 1
  fi
done

echo "RocksDB initialization complete!"

docker stop mysql;
docker rm mysql;

docker run -d --name mysql \
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
if ! docker ps | grep -q mysql; then
  echo "Container failed to start. Checking logs:"
  docker logs mysql 2>&1 | tail -20
  exit 1
fi

echo "Container started successfully!"
echo "Verifying RocksDB is the default engine..."
docker exec mysql mysql -uroot -proot -e "SHOW ENGINES;" | grep "ROCKSDB"
