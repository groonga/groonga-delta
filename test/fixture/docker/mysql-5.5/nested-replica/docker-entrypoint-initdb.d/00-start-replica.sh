#!/bin/bash

set -eux

while ! mysqladmin ping -u root -h mysql-5.5-replica; do
  sleep 1;
done

mysql -u root -h mysql-5.5-replica -e "FLUSH TABLES WITH READ LOCK; SHOW MASTER STATUS\\G" > \
      /tmp/master-status.txt
log_file=$(grep 'File:' /tmp/master-status.txt | sed -e 's/^ *File: //')
log_position=$(grep 'Position:' /tmp/master-status.txt | sed -e 's/^ *Position: //')
mysql -u root -e " \
    CHANGE MASTER TO \
        MASTER_HOST='mysql-5.5-replica', \
        MASTER_USER='replicator', \
        MASTER_PASSWORD='replicator-password', \
        MASTER_LOG_FILE='${log_file}', \
        MASTER_LOG_POS=${log_position};"
mysql -u root -e "START SLAVE;"

set +eux
