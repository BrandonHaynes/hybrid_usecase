#!/bin/sh

start ()
{
MYRIA_CONFIG=./myria.conf
ARGS=$(getopt -n `basename "$0"` -u -o c: --long config: -- "$@") || exit 1
set -- $ARGS

while [ $# -gt 0 ]
do
    case "$1" in
    (-c | --config) MYRIA_CONFIG="$2"; shift 2;;
    (--) shift; break;;
    (-*) echo "$0: error - unrecognized option $1" 1>&2; exit 1;;
    (*)  break;;
    esac
    shift
done

echo "======================================================"
echo COORDINATOR=$1
echo WORKERS=$@
grep "^\w" $MYRIA_CONFIG
echo "======================================================"

. $MYRIA_CONFIG
COORDINATOR=$1
WORKERS=$@
WORKER_FILE="${TMPDIR-/tmp}/myria-nodes"

#set -e
rm -f $WORKER_FILE

for node in "$@"
do
    echo "$node" >> $WORKER_FILE
    { echo "MYRIA_MODE=START_WORKER" ; \
      cat $MYRIA_CONFIG `basename "$0"` ; } | \
        ssh $node 'bash -es' || exit 1
done

scp -q $WORKER_FILE $COORDINATOR:$WORKER_FILE
{ echo "MYRIA_MODE=START_COORDINATOR" ; \
  cat $MYRIA_CONFIG `basename "$0"` ; } | \
    ssh $COORDINATOR 'bash -es'
}


function start_coordinator()
{
echo "------------------------------------------------------"
echo "Launching Myria on coodinator `hostname`"
echo "------------------------------------------------------"

MYRIA_NAME=${MYRIA_NAME=`whoami`}
MYRIA_BASE=${MYRIA_BASE=/state/partition1/myria}
MYRIA_STACK=$MYRIA_BASE/stack
MYRIA_DEPLOY=$MYRIA_BASE/deploy
MYRIA_NODES="${TMPDIR-/tmp}/myria-nodes"
MYRIA_BRANCH=${MYRIA_BRANCH=create-deployment}
MYRIA_HEAP_SIZE=${MYRIA_HEAP_SIZE=2g}

POSTGRES_PORT=${POSTGRES_PORT=5433}
GRADLE_CACHE_ARCHIVE=${GRADLE_CACHE_ARCHIVE=https://drive.google.com/uc?export=download&confirm=HtgA&id=0B8yKPRGRXCo9Z19MMlFhU1hjVms}
GOOGLE_APPENGINE_URL=${GOOGLE_APPENGINE_URL=url_not_specified}
GATEWAY_NODE=${GATEWAY_NODE=login-1}
MYRIA_STACK_URL=${MYRIA_STACK_URL=https://github.com/uwescience/myria-stack}

if [ ! -d $MYRIA_STACK ]; then
    MYRIA_STAGING=`mktemp -d`
    echo "git clone $MYRIA_STACK_URL $MYRIA_STAGING && \
          cd $MYRIA_STAGING && \
            git submodule update --init --recursive && \
            git submodule foreach --recursive git checkout master && \
            git --git-dir=$MYRIA_STAGING/myria/.git checkout $MYRIA_BRANCH && \
          cd $MYRIA_STAGING/myria-web && \
            git submodule update --init --recursive &&
            git submodule foreach --recursive git checkout master" \
      | ssh $GATEWAY_NODE 'bash -es'

    echo "wget  -q  $GOOGLE_APPENGINE_URL -O $MYRIA_STAGING/gae.zip && \
          unzip -qd $MYRIA_STAGING $MYRIA_STAGING/gae.zip && \
          rm $MYRIA_STAGING/gae.zip" \
      | ssh $GATEWAY_NODE 'bash -es'

    rsync -al $GATEWAY_NODE:$MYRIA_STAGING/. $MYRIA_STACK
fi

if [ ! -d $MYRIA_STACK/myria/build ]; then
    GRADLE_CACHE=`tar tf $GRADLE_CACHE_ARCHIVE --absolute-names | head -n 1`
    tar xf $GRADLE_CACHE_ARCHIVE --absolute-names

    $MYRIA_STACK/myria/gradlew --project-dir=$MYRIA_STACK/myria --gradle-user-home=$GRADLE_CACHE --offline jar
fi

export PATH=/usr/java/jdk1.7.0_51/bin:$PATH
export PYTHONPATH=$MYRIA_STACK/myria-python:$PYTHONPATH

mkdir -p $MYRIA_DEPLOY
cd $MYRIA_STACK/myria/myriadeploy && \
    ./create_deployment.py \
        --name $MYRIA_NAME        \
        --database-password FIXME \
        --database-port $POSTGRES_PORT \
        --heap "$MYRIA_HEAP_SIZE -Djava.net.preferIPv4Stack=true" \
        $MYRIA_DEPLOY             \
        `head -n 1 $MYRIA_NODES`  \
        `cat $MYRIA_NODES | tr '\n' ' '` > $MYRIA_DEPLOY/deployment.config && \
    ./kill_all_java_processes.py $MYRIA_DEPLOY/deployment.config ; \
    ./setup_cluster.py $MYRIA_DEPLOY/deployment.config && \
    ./launch_cluster.sh $MYRIA_DEPLOY/deployment.config
}


function start_web()
{
echo "------------------------------------------------------"
echo "Launching Myria webserver on `hostname`"
echo "------------------------------------------------------"

MYRIA_BASE=${MYRIA_BASE=/state/partition1/myria}
MYRIA_STACK=$MYRIA_BASE/stack
MYRIA_HTTP_PORT=${MYRIA_HTTP_PORT=8088}
GAE_ADMIN_PORT=${GAE_ADMIN_PORT=8089}

nohup \
  $MYRIA_STACK/google_appengine/dev_appserver.py \
    --host `hostname` \
    --port $MYRIA_HTTP_PORT \
    --admin_port $GAE_ADMIN_PORT \
    --datastore_path $MYRIA_STACK/myria-web/database \
    --logs_path $MYRIA_STACK/myria-web/logs \
    --skip_sdk_update_check true \
   $MYRIA_STACK/myria-web/appengine \
     < /dev/null \
     > $MYRIA_STACK/myria-web/stdout &
}


function start_worker()
{
echo "------------------------------------------------------"
echo "Installing Myria prerequisites on `hostname`"
echo "------------------------------------------------------"

MYRIA_NAME=${MYRIA_NAME=myria}
MYRIA_BASE=${MYRIA_BASE=/state/partition1/myria}
MYRIA_DATA=$MYRIA_BASE/data
POSTGRES_PORT=${POSTGRES_PORT=5433}

set +e

if [ ! -d $MYRIA_DATA ]; then
    mkdir -p $MYRIA_DATA
    initdb -D $MYRIA_DATA
    export MYRIA_NAME
    export HOSTNAME
    awk '{ print "host " ENVIRON["MYRIA_NAME"] " uwdb " ENVIRON["HOSTNAME"] " md5" }' $MYRIA_NODES \
         >> $MYRIA_DATA/pg_hba.conf
fi

echo "Launch Postgres"
echo "pg_ctl -D $MYRIA_DATA -o \"-h '*' -p $POSTGRES_PORT\" start"
pg_ctl -p $POSTGRES_PORT -D $MYRIA_DATA status || \
    (pg_ctl -D $MYRIA_DATA -o "-h '*' -p $POSTGRES_PORT" start & \
     sleep 5 && \
     pg_ctl -p $POSTGRES_PORT -D $MYRIA_DATA status)
if [ $? -ne 0 ]; then
    echo "Postgres not started after 5 seconds; exiting."
    exit 1
fi

echo "Add roles"
psql -p $POSTGRES_PORT -d postgres -tAc \
  "SELECT 1 FROM pg_roles WHERE rolname='uwdb'" | grep -q 1
if [ $? -ne 0 ]; then
    psql -p $POSTGRES_PORT -d postgres -c \
      "CREATE USER uwdb WITH PASSWORD 'FIXME'"
fi

echo "Create database"
psql -p $POSTGRES_PORT -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '$MYRIA_NAME'" | grep -q 1
if [ $? -ne 0 ]; then
    psql -p $POSTGRES_PORT -d postgres -c "CREATE DATABASE $MYRIA_NAME"
fi

echo "Grant Myria access to database"
psql -p $POSTGRES_PORT -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE $MYRIA_NAME TO uwdb"
}


function terminate()
{
MYRIA_CONFIG=./myria.conf
ARGS=$(getopt -n `basename "$0"` -u -o c: --long config: -- "$@") || exit 1
set -- $ARGS

while [ $# -gt 0 ]
do
    case "$1" in
    (-c | --config) MYRIA_CONFIG="$2"; shift 2;;
    (--) shift; break;;
    (-*) echo "$0: error - unrecognized option $1" 1>&2; exit 1;;
    (*)  break;;
    esac
    shift
done

echo "======================================================"
echo COORDINATOR=$1
echo WORKERS=$@
grep "^\w" $MYRIA_CONFIG
echo "======================================================"

. $MYRIA_CONFIG
COORDINATOR=$1
WORKERS=$@

MYRIA_BASE=${MYRIA_BASE=/state/partition1/myria}
MYRIA_STACK=$MYRIA_BASE/stack
MYRIA_DATA=$MYRIA_BASE/data
MYRIA_DEPLOY=$MYRIA_BASE/deploy
POSTGRES_PORT=${POSTGRES_PORT=5433}

ssh $COORDINATOR "pkill -f \
                    'python $MYRIA_STACK/google_appengine/dev_appserver.py '"

ssh $COORDINATOR "$MYRIA_STACK/myria/myriadeploy/kill_all_java_processes.py \
                    $MYRIA_DEPLOY/deployment.config"

for node in "$@"
do
    echo "Terminating Postgres on $node" && \
    ssh $node "pg_ctl -p $POSTGRES_PORT -D $MYRIA_DATA stop" || exit 1
done
}

case $MYRIA_MODE in
  ("START_WORKER") start_worker ;;
  ("START_COORDINATOR") start_coordinator && start_web ;;
  (*) case $1 in
        ("start") shift && start $@ ;;
        ("stop") shift && terminate $@ ;;
        (*) echo "Usage: `basename $0` [start,stop] [-c configuration] coordinator worker1 worker2 ..." ; exit 1
      esac ;;
esac
