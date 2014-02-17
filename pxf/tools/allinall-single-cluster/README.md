allinall SingleCluster
======================

allinall is a self contained, easy to deploy distribution of PHD & Hawq

Prerequisites
-------------

1.	$JAVA_HOME points to a JDK7 install
2.	[OSX] Set the following settings, otherwise Hawq will fail to start
	-	sudo sysctl -w kern.sysv.shmall=524288
	-	sudo sysctl -w kern.sysv.shmseg=16
	-	sudo sysctl -w kern.sysv.shmmax=4294967296
3.	Current user is NOT root
4.	Make sure ssh to your hostname is passwordless
5.	Make sure both forward and reverse DNS work

Initialization
--------------

1. Initialize an instance
	-	./init-cluster.sh
2. Add the following to your environment
	-	export GPHOME=$HAWQ_ROOT
	-	export GPDATA=$GPDATA
	-	export MASTER_DATA_DIRECTORY=$GPDATA/master/gpseg-1
	-	source \$GPHOME/greenplum_path.sh
	-	export PHD_ROOT=$PHD_ROOT
	-	export PATH=\$PATH:$PHD_ROOT/bin

Usage
-----

-	Start all PHD services
	-	$PHD_ROOT/bin/start-gphd.sh
-	Start HDFS only
	-	$PHD_ROOT/bin/start-hdfs.sh
- Start Hawq
	-	$GPHOME/bin/gpstart -a
- Login to Hawq
	-	$GPHOME/bin/psql

Notes
-----

1.	See phd/README.txt for PHD help
2.	Should init-cluster.sh fail, clean environment first before executing again
	1.	killall postgres (beware not to kill other postgres(s) running)
	2.	rm -rf /tmp/.s.*
	3.	phd/bin/stop-hdfs.sh

Changes
-------

-	Fixed GPSQL-1815
-	Fixed GPSQL-1374
-	Fixed GPSQL-1145
-	Fixed HD-6970

