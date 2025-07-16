#!/bin/bash

echo "Current disk usage:"
docker exec myrocks du -sh /var/lib/mysql/

echo -e "\nPurging old binary logs..."
docker exec myrocks mysql -uroot -proot -e "PURGE BINARY LOGS BEFORE NOW();"

echo -e "\nDisabling binary logging for this session..."
docker exec myrocks mysql -uroot -proot -e "SET SQL_LOG_BIN = 0;"

echo -e "\nTo permanently disable binary logging, add this to my.cnf:"
echo "skip-log-bin"

echo -e "\nNew disk usage:"
docker exec myrocks du -sh /var/lib/mysql/ 
