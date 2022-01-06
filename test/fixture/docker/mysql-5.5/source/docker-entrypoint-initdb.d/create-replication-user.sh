#!/bin/bash

set -eux

mysql -u root -e "CREATE USER 'replicator'@'%' IDENTIFIED BY 'replicator-password'"
mysql -u root -e "GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%'"

set +eux
