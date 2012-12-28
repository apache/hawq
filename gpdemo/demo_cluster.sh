#!/bin/bash

# ======================================================================
# Data Directories
# ======================================================================

DATADIRS=`pwd`/datadirs
QDDIR=$DATADIRS/qddir
HDFSDIR=$DATADIRS/qddir/hdfs_gpsql

SEG_PREFIX=demoDataDir

declare -a DIRVEC=($DATADIRS/dbfast1 \
                   $DATADIRS/dbfast2 \
                   $DATADIRS/dbfast3 )

declare -a DIRVEC_MIRROR=($DATADIRS/dbfast_mirror1 \
                          $DATADIRS/dbfast_mirror2 \
                          $DATADIRS/dbfast_mirror3 )

# ======================================================================
# DATABASE PORTS
# ======================================================================

declare -a DEMO_SEG_PORTS=(`expr $DEMO_PORT_BASE`      \
                           `expr $DEMO_PORT_BASE + 1`  \
                           `expr $DEMO_PORT_BASE + 2`  \
                           `expr $DEMO_PORT_BASE + 3`  \
                           `expr $DEMO_PORT_BASE + 4`  \
                           `expr $DEMO_PORT_BASE + 5`  \
                           `expr $DEMO_PORT_BASE + 6`  \
                           `expr $DEMO_PORT_BASE + 7`  \
                           `expr $DEMO_PORT_BASE + 8`  \
                           `expr $DEMO_PORT_BASE + 9`  \
                           `expr $DEMO_PORT_BASE + 10` \
                           `expr $DEMO_PORT_BASE + 11` )

#******************************************************************************
# Functions
#******************************************************************************

checkDemoConfig(){
    echo "Checking for port availability... "
    # Check if Master_DEMO_Port is free
    echo "Checking if port ${MASTER_DEMO_PORT} is available"
    PORT_FILE="/tmp/.s.PGSQL.${MASTER_DEMO_PORT}"
    if [ -f ${PORT_FILE} -o  -S ${PORT_FILE} ] ; then 
        echo ""
        echo -n " Port ${MASTER_DEMO_PORT} appears to be in use. " 
        echo " This port is needed by the Master Database instance. "
        echo ">>> Edit Makefile to correct the port number (MASTER_PORT). <<<" 
        echo -n " Check to see if the port is free by using : "
        echo " 'netstat -an | grep ${MASTER_DEMO_PORT}"
        echo ""
        return 1
    fi

    for PORT_NUM in ${DEMO_SEG_PORTS[@]}; do
        echo "Checking if port ${PORT_NUM} is available"
        PORT_FILE="/tmp/.s.PGSQL.${PORT_NUM}"
        if [ -f ${PORT_FILE} -o -S ${PORT_FILE} ] ; then 
            echo ""
            echo -n "Port ${PORT_NUM} appears to be in use."
            echo " This port is needed for segment database instance."
            echo ">>> Edit Makefile to correct the base ports (PORT_BASE). <<<"
            echo -n " Check to see if the port is free by using : "
            echo " 'netstat -an | grep ${PORT_NUM}"
            echo ""
            return 1
        fi
    done
    return 0
}

USAGE(){
    echo ""
    echo " `basename $0` -c -d -u"
    echo " -c : check if demo is possible."
    echo " -d : Delete the demo."
    echo " -u : Usage, prints this message."
    echo ""
}

#
# Clean up the demo
#

cleanDemo(){

    ##
    ## Attempt to bring down using GPDB cluster instance using gpstop
    ##

    (export MASTER_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1;
     source ${GPHOME}/greenplum_path.sh;
     if [ "${ENABLE_HDFS}" = "true" ]; then
         [ -f cluster_env.sh ] && source cluster_env.sh;
         psql postgres -c "drop database hdfs";
         psql postgres -c "drop tablespace hdfs_tablespace";
     fi;
     gpstop -a)

    ##
    ## Remove the files and directories created; allow test harnesses
    ## to disable this
    ##

    if [ "${GPDEMO_DESTRUCTIVE_CLEAN}" != "false" ]; then
        if [ -f hostfile ];  then
            echo "Deleting hostfile"
            rm -f hostfile
        fi
        if [ -f clusterConfigFile ];  then
            echo "Deleting clusterConfigFile"
            rm -f clusterConfigFile
        fi
        if [ -d ${DATADIRS} ];  then
            echo "Deleting ${DATADIRS}"
            rm -rf ${DATADIRS}
        fi
        if [ -d logs ];  then
            rm -rf logs
        fi
    fi
}

#*****************************************************************************
# Main Section
#*****************************************************************************

while getopts ":cd'?'" opt
		do
		case $opt in 
				'?' ) USAGE ;;
                c) checkDemoConfig
                    RETVAL=$?
                    if [ $RETVAL -ne 0 ]; then
                        echo "Checking failed "
                        exit 1
                    fi
                    exit 0
                ;;
                d) cleanDemo
                   exit 0
                ;;
                *) USAGE
                   exit 0
                   ;;
		esac
done

if [ x"$GPHOME" = x ]; then
        echo ""
        echo "GPHOME is not set.  Point to the location"
        echo "of the Greenplum installation directory."
        echo ""
        exit 1
else
    GPSEARCH=$GPHOME
fi

cat <<-EOF

	**********************************************************************
	This is a demo of the Greenplum Database module.  We'll perform a
	local cluster installation with 1 master instance and 6 segment
	instances (3 primary & 3 mirror) all on this machine.

	  GPHOME=${GPHOME}
	  MASTER_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1

	In order to run the Greenplum Database module, you must have the
	GPHOME environment variable set to the location of the Greenplum
	install.  You must also have the following port numbers free:

	  ${MASTER_DEMO_PORT}
	  ${DEMO_SEG_PORTS[@]}

	**********************************************************************

EOF

GPPATH=`find $GPSEARCH -name gp_dump| tail -1`
RETVAL=$?

if [ "$RETVAL" -ne 0 ]; then
    echo "Error attempting to find Greenplum executables in $GPSEARCH"
    exit 1
fi

if [ ! -x "$GPPATH" ]; then
    echo "No executables found for Greenplum installation in $GPSEARCH"
    exit 1
fi
GPPATH=`dirname $GPPATH`
if [ ! -x $GPPATH/gpinitsystem ]; then
    echo "No mgmt executables found for Greenplum installation in $GPPATH"
    exit 1
fi

if [ -d $DATADIRS ]; then
  rm -rf $DATADIRS
fi
mkdir $DATADIRS
mkdir $QDDIR
if [ "${ENABLE_HDFS}" = "true" ]; then
    mkdir $HDFSDIR
fi

for dir in ${DIRVEC[@]} ${DIRVEC_MIRROR[@]}
do
  if [ ! -d $dir ]; then
    mkdir $dir
  fi
done

#*****************************************************************************************
# Host configuration
#*****************************************************************************************

LOCALHOST=`hostname`
echo $LOCALHOST > hostfile

#*****************************************************************************************
# Name of the system configuration file.
#*****************************************************************************************

CLUSTER_CONFIG=clusterConfigFile
CLUSTER_CONFIG_POSTGRES_ADDONS=clusterConfigPostgresAddonsFile

rm -f ${CLUSTER_CONFIG}
rm -f ${CLUSTER_CONFIG_POSTGRES_ADDONS}

#*****************************************************************************************
# Create the system configuration file
#*****************************************************************************************

cat >> $CLUSTER_CONFIG <<-EOF
	# Set this to anything you like
	ARRAY_NAME="Demo $HOSTNAME Cluster"
	CLUSTER_NAME="Demo $HOSTNAME Cluster"
	
	# This file must exist in the same directory that you execute gpinitsystem in
	MACHINE_LIST_FILE=hostfile
	
	# This names the data directories for the Segment Instances and the Entry Postmaster
	SEG_PREFIX=$SEG_PREFIX
	
	# This is the port at which to contact the resulting Greenplum database, e.g.
	#   psql -p \$PORT_BASE -d template1
	PORT_BASE=${DEMO_PORT_BASE}
	
	# Prefix for script created database
	DATABASE_PREFIX=demoDatabase
	
	# Array of data locations for each hosts Segment Instances, the number of directories in this array will
	# set the number of segment instances per host
	declare -a DATA_DIRECTORY=(${DIRVEC[@]})
	
	# Name of host on which to setup the QD
	MASTER_HOSTNAME=$LOCALHOST
	
	# Name of directory on that host in which to setup the QD
	MASTER_DIRECTORY=$QDDIR
	
	MASTER_PORT=${MASTER_DEMO_PORT}
	
	# Hosts to allow to connect to the QD (and Segment Instances)
	# By default, allow everyone to connect (0.0.0.0/0)
	IP_ALLOW=0.0.0.0/0
	
	# Shell to use to execute commands on all hosts
	TRUSTED_SHELL="`pwd`/lalshell"
	
	CHECK_POINT_SEGMENTS=8
	
	ENCODING=UNICODE
EOF

ENABLE_MIRRORS=${ENABLE_MIRRORS:=true}

if [ `uname -p` != "sparc" ] && [ `uname -s` != "Darwin" ] && [ "${ENABLE_MIRRORS}" = "true" ]; then
	cat >> $CLUSTER_CONFIG <<-EOF
	
		# Array of mirror data locations for each hosts Segment Instances, the number of directories in this array will
		# set the number of segment instances per host
		declare -a MIRROR_DATA_DIRECTORY=(${DIRVEC_MIRROR[@]})
		
		MIRROR_PORT_BASE=`expr $DEMO_PORT_BASE + 3`
	
		REPLICATION_PORT_BASE=`expr $DEMO_PORT_BASE + 6`
		MIRROR_REPLICATION_PORT_BASE=`expr $DEMO_PORT_BASE + 9`
	EOF
fi

if [ "${ENABLE_HDFS}" = "true" ]; then

    DATE=$( date +%Y%m%dt%H%M%S )

    HDFS_HOST=${HDFS_HOST:=localhost:8020}

	cat >> $CLUSTER_CONFIG <<-EOF

		# HDFS Support
		DFS_NAME=hdfs
		DFS_URL=${HDFS_HOST}/gpsql/gpdb${DATE}
		DATABASE_NAME=hdfs
	EOF

fi

cat >> $CLUSTER_CONFIG <<-EOF

	# Path for Greenplum mgmt utils and Greenplum binaries
	PATH=$GPHOME/bin:$PATH
	LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH
	export PATH
	export LD_LIBRARY_PATH
	export MASTER_DATA_DIRECTORY
	export TRUSTED_SHELL
EOF

cat >> $CLUSTER_CONFIG <<-EOF

	# Keep max_connection settings to reasonable values for
	# installcheck good execution.

	DEFAULT_QD_MAX_CONNECT=25
	QE_CONNECT_FACTOR=5

EOF

#*****************************************************************************************
# Create environment file
#*****************************************************************************************

cat > cluster_env.sh <<-EOF
	export MASTER_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1
	export PGPORT=${MASTER_DEMO_PORT}
EOF

#*****************************************************************************************
# Create cluster
#*****************************************************************************************

if [ "${BUILD_TYPE}" = "gcov" ] && [ -f "${CLUSTER_CONFIG_POSTGRES_ADDONS}" ]; then
    echo "executing: $GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG -p ${CLUSTER_CONFIG_POSTGRES_ADDONS} \"$@\""
    $GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG -p ${CLUSTER_CONFIG_POSTGRES_ADDONS} "$@"
else
    echo "executing: $GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG \"$@\""
    $GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG "$@"
fi

RETURN=$?
echo "========================================"
echo "gpinitsystem returned: ${RETURN}"
echo "========================================"

exit ${RETURN}
