#!/bin/sh

if [ $# -lt 3 ]
  then
    echo "Usage: demo.sh cluster_name patients vector_size [{myria, scidb, hybrid}]"
    exit 1
fi

CLUSTERNAME=$1
PATIENTS=$2
VECTOR_SIZE=$3
TEST_NAME=$4

CLUSTERCONFIGNAME=hybridbenchmarkcluster
SCIDB_PATH=/opt/scidb/14.12
SCIDB_HOME=/home/scidb
SCIDB_BASE=/mnt/scidb/db
SCIDB_WORKERS=4

if [ "$TEST_NAME" == '' ]
then

echo "*** Launching cluster"
starcluster start -c $CLUSTERCONFIGNAME $CLUSTERNAME
EC2_NAME=$(starcluster listclusters | grep $CLUSTERNAME-master | tr -s ' ' | cut -d ' ' -f 5)

echo "*** Updating SciDB configuration"
echo "Ensure a two-node cluster, if necessary"
starcluster sshmaster $CLUSTERNAME "sed -ri \"s/server-2=(.*),[[:digit:]]*/server-2=\1,0/g\" $SCIDB_PATH/etc/config.ini"

echo "* Rebuild with 2 workers per node and 0 redundancy"
starcluster sshmaster $CLUSTERNAME 'cd /mnt/scidb && ./rebuild.sh 2 0'

echo "* Ensure that binning plugin is distributed"
starcluster sshmaster $CLUSTERNAME "cp /mnt/scidb/stage/build/bin/plugins/libbin.so $SCIDB_PATH/lib/scidb/plugins"
starcluster sshmaster $CLUSTERNAME "scp /mnt/scidb/stage/build/bin/plugins/libbin.so $CLUSTERNAME-node001:$SCIDB_PATH/lib/scidb/plugins"

echo "* Create links for file-system SciDB<->Myria transport"
starcluster sshmaster $CLUSTERNAME "mkdir $SCIDB_HOME/0"
starcluster sshmaster $CLUSTERNAME "mkdir $SCIDB_HOME/1"
starcluster sshmaster $CLUSTERNAME "mkdir $SCIDB_HOME/2"
starcluster sshmaster $CLUSTERNAME "mkdir $SCIDB_HOME/3"
sleep 5
starcluster sshmaster $CLUSTERNAME "ln -s $SCIDB_HOME/0 $SCIDB_BASE/000/0/out"
starcluster sshmaster $CLUSTERNAME "ln -s $SCIDB_HOME/1 $SCIDB_BASE/000/1/out"
starcluster sshnode   $CLUSTERNAME $CLUSTERNAME-node001 "ln -s $SCIDB_HOME/2 $SCIDB_BASE/001/1/out"
starcluster sshnode   $CLUSTERNAME $CLUSTERNAME-node001 "ln -s $SCIDB_HOME/3 $SCIDB_BASE/001/2/out"

echo "* Restart SciDB"
starcluster sshmaster $CLUSTERNAME "$SCIDB_PATH/bin/scidb.py stopall mydb"
starcluster sshmaster $CLUSTERNAME "$SCIDB_PATH/bin/scidb.py startall mydb"

echo "*** Updating Myria configuration"

echo "* Add master node to list of workers to mirror SciDB configuration"
starcluster sshmaster $CLUSTERNAME '\
  MASTER=`grep "0 =" /root/deployment.cfg.ec2 | cut -f 3 -d " " | cut -f 1 -d ":"` &&\
  printf "\n2 = $MASTER:9001::myria\n" >> /root/deployment.cfg.ec2 && \
  cd /root/myria/myriadeploy && \
  ./kill_all_java_processes.py /root/deployment.cfg.ec2 && \
  ./setup_cluster.py /root/deployment.cfg.ec2 && \
  ./launch_cluster.sh /root/deployment.cfg.ec2'
starcluster sshmaster $CLUSTERNAME \
	"cd /mnt/myria_web/submodules/raco && \
	 git checkout master && \
	 git pull && \
	 service myria-web restart"

echo "* Ensure that 'symbols' relation exists on server"
starcluster sshmaster $CLUSTERNAME 'python -c "from myria import *; MyriaQuery.submit(\"x = empty(id:int, index:int, value:float); store(x, symbols);\", connection=MyriaConnection(rest_url=\"http://'"$EC2_NAME"':8753\"))"'

echo "*** Stage benchmarking scripts"
starcluster put $CLUSTERNAME --node $CLUSTERNAME-master hybrid.py /root
starcluster put $CLUSTERNAME --node $CLUSTERNAME-master myria_only.py /root
starcluster put $CLUSTERNAME --node $CLUSTERNAME-master scidb_only.py /root

echo "***********************************************************************************"
echo "*** NOTE: cluster is still running!"
echo "*** Terminate with 'starcluster terminate $CLUSTERNAME'"
echo "***********************************************************************************"

fi

if [ "$TEST_NAME" == 'myria' ] || [ "$TEST_NAME" == '' ]
then

echo "*** Benchmarking Myria"
EC2_NAME=$(starcluster listclusters | grep $CLUSTERNAME-master | tr -s ' ' | cut -d ' ' -f 5)
starcluster sshmaster $CLUSTERNAME \
  "python myria_only.py $PATIENTS $VECTOR_SIZE \
           --url http://$EC2_NAME:8753"

fi

if [ "$TEST_NAME" == 'scidb' ] || [ "$TEST_NAME" == '' ]
then

echo "*** Benchmarking SciDB"
starcluster sshmaster $CLUSTERNAME \
  "python scidb_only.py $PATIENTS $VECTOR_SIZE --url http://localhost:8080"

fi

if [ "$TEST_NAME" == 'hybrid' ] || [ "$TEST_NAME" == '' ]
then

echo "*** Benchmarking Hybrid"
EC2_NAME=$(starcluster listclusters | grep $CLUSTERNAME-master | tr -s ' ' | cut -d ' ' -f 5)
starcluster sshmaster $CLUSTERNAME \
  "python hybrid.py $PATIENTS $VECTOR_SIZE $SCIDB_WORKERS \
  --myria-url http://$EC2_NAME:8753"

fi
