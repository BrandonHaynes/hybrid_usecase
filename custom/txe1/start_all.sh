source scidb/scidb.conf
source myria/myria.conf

cd scidb
. scidb_cluster.sh start "$@"
#. myria/myria_cluster.sh start "$@" --config scidb/myria.conf

ssh $1 "$SCIDB_BASE/bin/iquery -p $SCIDB_BASE_PORT -anq 'store(build(<value: double>[id=0:599,1,0, time=0:255,256,0], (double(id+1) / (time+1)) / 600 - 1), SciDB__Demo__Vectors)'"
ssh $1 "$SCIDB_BASE/bin/iquery -p $SCIDB_BASE_PORT -anq \"load_library('bin')\""
#ssh node-038 "/state/partition1/scidb-bhaynes/bin/iquery -p $SCIDB_BASE_PORT -anq 'store(build(<value: double>[id=0:599,1,0, time=0:255,256,0], (double(id+1) / (time+1)) / 600 - 1), SciDB__Demo__Vectors)'"

ssh $1 nohup python ../../website/server_txe1.py --scidb-path $SCIDB_BASE

ssh $1 nohup /state/partition1/myria_bhaynes/stack/google_appengine/dev_appserver.py --host node-038 --port 8090 --admin_port 8091 --datastore_path /state/partition1/myria_bhaynes/stack/myria-web/database --logs_path /state/partition1/myria_bhaynes/stack/myria-web/logs --skip_sdk_update_check true /state/partition1/myria_bhaynes/stack/myria-web/appengine

echo ssh txe1-login.mit.edu -L 8080:172.16.4.60:8080 -N -f
echo ssh txe1-login.mit.edu -L 8751:$1:8751 -N -f