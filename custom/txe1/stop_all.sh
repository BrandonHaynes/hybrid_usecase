#!/usr/bin/env bash

source scidb/scidb.conf
source myria/myria.conf

cd scidb
./scidb_cluster.sh stop "$@"
cd ../myria
./myria_cluster.sh stop "$@"

ssh $1 "pkill -f server_txe1.py"