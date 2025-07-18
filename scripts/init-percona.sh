#!/bin/bash

docker rm -f mysql 2>/dev/null || true
docker volume create mysql

docker run -d --name mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -v mysql:/var/lib/mysql \
  -e INIT_ROCKSDB=1 \
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

echo "Container started successfully! Exiting..."
docker stop mysql;
docker rm mysql;
