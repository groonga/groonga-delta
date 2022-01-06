#!/bin/bash

set -eux

mysql -u root -e "CREATE USER 'replicator'@'%' IDENTIFIED BY 'replicator-password'"
mysql -u root -e "GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%'"

mysql -u root -e "CREATE USER 'c-replicator'@'%' IDENTIFIED BY 'client-replicator-password'"
mysql -u root -e "GRANT REPLICATION CLIENT, RELOAD ON *.* TO 'c-replicator'@'%'"

mysql -u root -e "CREATE USER 'selector'@'%' IDENTIFIED BY 'selector-password'"
mysql -u root -e "GRANT SELECT ON *.* TO 'selector'@'%'"

set +eux
