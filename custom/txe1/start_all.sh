#!/usr/bin/env bash

source scidb/scidb.conf
source myria/myria.conf

COORDINATOR=$1
WORKERS=$@
BASEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd scidb
./scidb_cluster.sh start "$@"
cd ../myria
./myria_cluster.sh start "$@"
cd ..

echo ------------------------------------------------------
echo Initializing SciDB with arrays and custom aggregates
echo ------------------------------------------------------
ssh $COORDINATOR "$SCIDB_BASE/bin/iquery -p $SCIDB_BASE_PORT -anq 'store(build(<value: double>[id=0:599,1,0, time=0:255,256,0], (double(id+1) / (time+1)) / 600 - 1), SciDB__Demo__Vectors)'"
ssh $COORDINATOR "$SCIDB_BASE/bin/iquery -p $SCIDB_BASE_PORT -anq \"load_library('bin')\""

echo ------------------------------------------------------
echo Creating SciDB symlinks
echo ------------------------------------------------------
NODE_ID=0

for node in $WORKERS
do
    echo "*** $node"
    for i in $(seq 0 $SCIDB_WORKERS)
    do
    	echo Worker $i
    	WORKER_DIR=$SCIDB_BASE/data/00$NODE_ID/$i
    	ssh $node "[ -d $WORKER_DIR ]"
		if [ $? -eq 0 ]; then
			echo ssh $node "mkdir -p $SCIDB_BASE/data/00$NODE_ID/$i/out"
			ssh $node "mkdir -p $SCIDB_BASE/data/00$NODE_ID/$i/out"
		fi
    done
    ((NODE_ID++))
done

echo ------------------------------------------------------
echo Starting demo screen webserver
echo ------------------------------------------------------
ssh $COORDINATOR "nohup python $BASEDIR/../../website/server_txe1.py --scidb-path $SCIDB_BASE &"

echo ------------------------------------------------------
echo Ensuring Myria-Web webserver
echo ------------------------------------------------------
ssh $COORDINATOR "nohup $MYRIA_BASE/stack/google_appengine/dev_appserver.py \
                     --host node-038
                     --port 8090
                     --admin_port 8091
                     --datastore_path /state/partition1/myria_bhaynes/stack/myria-web/database
                     --logs_path /state/partition1/myria_bhaynes/stack/myria-web/logs
                     --skip_sdk_update_check true /state/partition1/myria_bhaynes/stack/myria-web/appengine &"

echo ------------------------------------------------------
echo Ensuring Myria Relations
echo ------------------------------------------------------
echo SciDB:Demo:Vectors
curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST \
     -d 'query=singleton_symbols+%3D+%5B1+as+id%2C+0+as+index%2C+0.5+as+value%5D%3B+shuffled_symbols+%3D+%5Bfrom+singleton_symbols+emit+id%2C+index%2C+value%2C+count%28%2A%29%5D%3B+symbols+%3D+%5Bfrom+shuffled_symbols+emit+id%2C+index%2C+value%5D%3B+store%28symbols%2C+public%3Aadhoc%3Asymbols%29%3B&language=myrial' \
     http://node-038:8090/execute

MYRIA_REST_PORT=${MYRIA_REST_PORT=8753}
MYRIA_HTTP_PORT=${MYRIA_HTTP_PORT=8090}
ssh $COORDINATOR "export PYTHONPATH=$PYTHONPATH:$MYRIA_BASE/stack/myria-python && \
                  cd $BASEDIR/myria &&
                  python create_vectors.py 600 256 --url http://$COORDINATOR:$MYRIA_REST_PORT \
                                                   --execution-url http://$COORDINATOR:$MYRIA_HTTP_PORT \
                                                   --relation-name vectors"

echo ------------------------------------------------------
echo Required SSH tunnels:
echo ------------------------------------------------------
echo ssh txe1-login.mit.edu -L 8080:172.16.4.60:8080 -N -f
echo ssh txe1-login.mit.edu -L 8751:$COORDINATOR:8751 -N -f


