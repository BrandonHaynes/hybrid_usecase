#!/bin/sh

start ()
{
SCIDB_CONFIG=./scidb.conf
ARGS=$(getopt -n `basename "$0"` -u -o c: --long config: -- "$@") || exit 1
set -- $ARGS

while [ $# -gt 0 ]
do
    case "$1" in
    (-c | --config) SCIDB_CONFIG="$2"; shift 2;;
    (--) shift; break;;
    (-*) echo "$0: error - unrecognized option $1" 1>&2; exit 1;;
    (*)  break;;
    esac
    shift
done

echo "======================================================"
echo COORDINATOR=$1
echo WORKERS=$@
grep "^\w" $SCIDB_CONFIG
echo "======================================================"

. $SCIDB_CONFIG
COORDINATOR=$1
WORKERS=$@
WORKER_FILE=`mktemp`

SCIDB_CONFIG_FILE=`mktemp`
SCIDB_BASE=${SCIDB_BASE=/state/partition1/scidb-`whoami`}
SCIDB_NAME=${SCIDB_NAME=`whoami`}
SCIDB_WORKERS=${SCIDB_WORKERS=2}
SCIDB_BASE_PORT=${SCIDB_BASE_PORT=1250}
POSTGRES_PORT=${POSTGRES_PORT=5432}

$SCIDB_SOURCE/bin/create_configuration.py \
  --path $SCIDB_BASE \
  --database-name $SCIDB_NAME \
  --database-password $SCIDB_PASSWORD \
  --workers $SCIDB_WORKERS \
  --database-port $POSTGRES_PORT \
  --base-port $SCIDB_BASE_PORT \
  $@ > $SCIDB_CONFIG_FILE

echo "------------------------------------------------------"
echo "Copying deployment to workers"
echo "------------------------------------------------------"
for node in $WORKERS
do
    echo "*** $node"
    echo "$node" >> $WORKER_FILE
    scp -qr $SCIDB_SOURCE $node:$SCIDB_BASE
    scp -q  $SCIDB_CONFIG_FILE $node:$SCIDB_BASE/etc/config.ini
done

rm $SCIDB_CONFIG_FILE
scp -q $WORKER_FILE $COORDINATOR:${TMPDIR=/tmp}/scidb-nodes

{ echo "SCIDB_MODE=START_COORDINATOR" ; \
  cat $SCIDB_CONFIG `basename "$0"` ; } | \
    ssh $COORDINATOR 'bash -es'
}


start_coordinator ()
{
echo "------------------------------------------------------"
echo "Launching SciDB on coodinator `hostname`"
echo "------------------------------------------------------"

SCIDB_BASE=${SCIDB_BASE=/state/partition1/scidb-`whoami`}
SCIDB_NAME=${SCIDB_NAME=`whoami`}
SCIDB_BASE_PORT=${SCIDB_BASE_PORT=1250}
SCIDB_SHIM_PORT=${SCIDB_SHIM_PORT=8088}

set +e

cd $SCIDB_BASE/bin && \
  ./scidb.py stop_all \
      $SCIDB_NAME $SCIDB_BASE/etc/config.ini ; \
  ./scidb.py init_all -f \
      $SCIDB_NAME $SCIDB_BASE/etc/config.ini ; \
  ./scidb.py start_all \
      $SCIDB_NAME $SCIDB_BASE/etc/config.ini && \
  ./iquery -ap $SCIDB_BASE_PORT -q "load_library('bin')"

nohup $SCIDB_BASE/bin/shim \
  -f -p $SCIDB_SHIM_PORT -s $SCIDB_BASE_PORT \
    < /dev/null \
    > $SCIDB_BASE/data/shim.out &

}

start_catalog ()
{
echo "------------------------------------------------------"
echo "Installing catalog on `hostname`"
echo "------------------------------------------------------"

SCIDB_NAME=${SCIDB_NAME=`whoami`}
SCIDB_BASE=${SCIDB_BASE=/state/partition1/scidb-`whoami`}
SCIDB_NODES="${TMPDIR-/tmp}/scidb-nodes"
POSTGRES_DATA=$SCIDB_BASE/data/postgres
POSTGRES_PORT=${POSTGRES_PORT=5433}

set +e

echo 'Initialize catalog'
if [ ! -d $POSTGRES_DATA ]; then
    mkdir -p $POSTGRES_DATA
    initdb -D $POSTGRES_DATA 

    for node in `cat $SCIDB_NODES`
    do
	printf "host $SCIDB_NAME $(whoami) $node md5\n" \
	    >> $POSTGRES_DATA/pg_hba.conf
    done
fi

echo "Launch Postgres"
echo "pg_ctl -D $POSTGRES_DATA -o \"-h '*' -p $POSTGRES_PORT\" start"
pg_ctl -p $POSTGRES_PORT -D $POSTGRES_DATA status || \
    ((pg_ctl -D $POSTGRES_DATA -o "-h '*' -p $POSTGRES_PORT" start \
          2>&1 > $POSTGRES_DATA/init-postgres.log) & \
     sleep 5 && \
     pg_ctl -p $POSTGRES_PORT -D $POSTGRES_DATA status)
if [ $? -ne 0 ]; then
    echo "Postgres not started after 5 seconds; exiting."
    exit 1
fi

echo "Add roles"
psql -p $POSTGRES_PORT -d postgres -tAc \
  "SELECT 1 FROM pg_roles WHERE rolname='$SCIDB_NAME'" | grep -q 1
if [ $? -ne 0 ]; then
    psql -p $POSTGRES_PORT -d postgres -c \
      "CREATE USER $SCIDB_NAME WITH PASSWORD '$SCIDB_PASSWORD'"
fi

echo "Create database"
psql -p $POSTGRES_PORT -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '$SCIDB_NAME'" | grep -q 1
if [ $? -ne 0 ]; then
    psql -p $POSTGRES_PORT -d postgres -c "CREATE DATABASE $SCIDB_NAME"
fi

echo "Set catalog account password"
psql -p $POSTGRES_PORT -d postgres -c \
  "ALTER USER $SCIDB_NAME WITH PASSWORD '$SCIDB_PASSWORD'"

echo "Grant access to database"
psql -p $POSTGRES_PORT -d postgres -c \
  "GRANT ALL PRIVILEGES ON DATABASE $SCIDB_NAME TO $SCIDB_NAME"
}


function terminate()
{
SCIDB_CONFIG=./scidb.conf
ARGS=$(getopt -n `basename "$0"` -u -o c: --long config: -- "$@") || exit 1
set -- $ARGS

while [ $# -gt 0 ]
do
    case "$1" in
    (-c | --config) SCIDB_CONFIG="$2"; shift 2;;
    (--) shift; break;;
    (-*) echo "$0: error - unrecognized option $1" 1>&2; exit 1;;
    (*)  break;;
    esac
    shift
done

echo "======================================================"
echo COORDINATOR=$1
echo WORKERS=$@
grep "^\w" $SCIDB_CONFIG
echo "======================================================"

. $SCIDB_CONFIG
COORDINATOR=$1

SCIDB_BASE=${SCIDB_BASE=/state/partition1/scidb-`whoami`}
SCIDB_NAME=${SCIDB_NAME=`whoami`}
SCIDB_WORKERS=${SCIDB_WORKERS=2}
SCIDB_BASE_PORT=${SCIDB_BASE_PORT=1250}
POSTGRES_DATA=$SCIDB_BASE/data/postgres
POSTGRES_PORT=${POSTGRES_PORT=5432}

echo "Terminating SciDB on coordinator $COORDINATOR"
echo "  * Stopping shim"
ssh $COORDINATOR "pkill -f shim" 
echo "  * Stopping SciDB" 
ssh $COORDINATOR "$SCIDB_BASE/bin/scidb.py stop_all \
                    $SCIDB_NAME $SCIDB_BASE/etc/config.ini" 
echo "  * Stopping Postgres" 
ssh $COORDINATOR "pg_ctl -p $POSTGRES_PORT -D $POSTGRES_DATA stop"
}

case $SCIDB_MODE in
  ("START_WORKER") start_worker ;;
  ("START_COORDINATOR") start_catalog && start_coordinator ;;
  (*) case $1 in
        ("start") shift && start $@ ;;
        ("stop") shift && terminate $@ ;;
        (*) echo "Usage: `basename $0` [start,stop] [-c configuration] host1 host2 ..." ; exit 1
      esac ;;
esac
