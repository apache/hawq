AllinAll PHD & HAWQ single-cluster

Usage:
1. update JAVA_HOME in phd/conf/gphd-conf.sh 
2. ./init-cluster.sh
3. Add the following to your environment
	export GPHOME=$HAWQ_ROOT
	export GPDATA=$GPDATA
	export MASTER_DATA_DIRECTORY=$GPDATA/master/gpseg-1
	source \$GPHOME/greenplum_path.sh
	export PATH=\$PATH:$PHD_ROOT/bin
4. see phd/README.txt for PHD help (no need to initialize)

Changes
=======

- Fixed GPSQL-1374
- Fixed GPSQL-1145
- Fixed HD-6970

