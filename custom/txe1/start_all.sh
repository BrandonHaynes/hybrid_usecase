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
ssh $COORDINATOR -f "(cd $BASEDIR/../../website && nohup python server_txe1.py --scidb-path $SCIDB_BASE)"

echo ------------------------------------------------------
echo Ensuring Myria-Web webserver
echo ------------------------------------------------------
ssh $COORDINATOR "nohup $MYRIA_BASE/stack/google_appengine/dev_appserver.py \
                     --host $COORDINATOR \
                     --port 8090 \
                     --admin_port 8091 \
                     --datastore_path /state/partition1/myria_bhaynes/stack/myria-web/database \
                     --logs_path /state/partition1/myria_bhaynes/stack/myria-web/logs \
                     --skip_sdk_update_check true /state/partition1/myria_bhaynes/stack/myria-web/appengine &"

echo ------------------------------------------------------
echo Ensuring Myria Relations
echo ------------------------------------------------------
echo SciDB:Demo:Vectors
#curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST -d 'query=T1+%3D+empty%28id%3Aint%2Ctime%3Aint%2Cvalue%3Afloat%29%3Bstore%28T1%2C+SciDB%3ADemo%3AVectors%2C+%5Bid%5D%29%3B&language=myrial' \
#http://node-037:8090/execute
curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST \
     -d 'query=T1+%3D+empty%28id%3Aint%2Ctime%3Aint%2Cvalue%3Afloat%29%3Bstore%28T1%2C+SciDB%3ADemo%3AVectors%2C+%5Bid%5D%29%3B&language=myrial' \
     http://$COORDINATOR:8090/execute
echo public:adhoc:symbols
curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST \
     -d 'query=singleton_symbols+%3D+%5B1+as+id%2C+0+as+index%2C+0.5+as+value%5D%3B+shuffled_symbols+%3D+%5Bfrom+singleton_symbols+emit+id%2C+index%2C+value%2C+count%28%2A%29%5D%3B+symbols+%3D+%5Bfrom+shuffled_symbols+emit+id%2C+index%2C+value%5D%3B+store%28symbols%2C+public%3Aadhoc%3Asymbols%29%3B&language=myrial' \
     http://$COORDINATOR:8090/execute

MYRIA_REST_PORT=${MYRIA_REST_PORT=8753}
MYRIA_HTTP_PORT=${MYRIA_HTTP_PORT=8090}
ssh $COORDINATOR "export PYTHONPATH=$PYTHONPATH:$MYRIA_BASE/stack/myria-python && \
                  cd $BASEDIR/myria &&
                  python create_vectors.py 600 256 --url http://$COORDINATOR:$MYRIA_REST_PORT \
                                                   --execution-url http://$COORDINATOR:$MYRIA_HTTP_PORT \
                                                   --input-mode random \
                                                   --relation-name vectors"

echo public:adhoc:vectors
#curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST -d 'query=t+%3D+load%28%27file%3A%2F%2F%2Fhome%2Fgridsan%2Fbhaynes%2Fwaveform_relation.csv%27%2C+csv%28schema%28id%3Aint%2Ctime%3Aint%2Cvalue%3Afloat%29%29%29%3B+store%28t%2C+public%3Aadhoc%3Avectors%2C+%5Bid%5D%29%3B&language=myrial' http://$COORDINATOR:8090/execute
curl -i -H "Content-Type: application/x-www-form-urlencoded" -X POST \
     -d 'query=t+%3D+load%28%27file%3A%2F%2F%2Fhome%2Fgridsan%2Fbhaynes%2Fwaveform_relation.csv%27%2C+csv%28schema%28id%3Aint%2Ctime%3Aint%2Cvalue%3Afloat%29%29%29%3B+store%28t%2C+public%3Aadhoc%3Avectors%2C+%5Bid%5D%29%3B&language=myrial' \
     http://$COORDINATOR:8090/execute

echo ------------------------------------------------------
echo Required SSH tunnels:
echo ------------------------------------------------------
API_ENDPOINT=172.16.4.60
echo ssh txe1-login.mit.edu -L 8080:$API_ENDPOINT:8080 -L 8751:$COORDINATOR:8751 -N -f


