#!/bin/sh
#
# Copyright (c) Greenplum Inc 2013. All Rights Reserved.
#
USAGE="$0 -f <hosts file>"

if [[ $(uname) =~ "Linux*" ]]; then
    BLD_ARCH=rhel5_x86_64
fi
if [ ! $BLD_ARCH ]; then
    echo "PL/Java is currently supported only on RHEL platform."
    exit 1
fi

if [ ! -d $GPHOME ]; then
    echo "GPHOME is either not set or is not a directory."
    exit 1
fi

if [ ! -d $JAVA_HOME ]; then
    echo "JAVA_HOME is not either not set or is invalid."
    exit 1
fi

jvmso=$(find $JAVA_HOME -name libjvm.so)
if [ ! $jvmso ]; then
    echo "JAVA_HOME is invalid, cannot find libjvm.so."
    exit 1
fi
jvmsodir=$(dirname $jvmso)
# Being paranoid...
if [ ! -d $jvmsodir ]; then
    echo "Cannot get directory name for libjvm.so."
    exit 1
fi

pljsql="$GPHOME/share/postgresql/pljava/install.sql"
if [ ! -f $pljsql ]; then
    echo "Cannot find $pljsql."
    exit 1
fi

which gpssh > /dev/null
if [ 0 -ne $? ]; then
    echo "gpssh not found in PATH."
    exit 1
fi

which gpscp > /dev/null
if [ 0 -ne $? ]; then
    echo "gpscp not found in PATH."
    exit 1
fi

hosts=""
while getopts f:d: opt; do
  case $opt in
  f)
      hosts=$OPTARG
      ;;
  esac
done

if [ "$hosts" = "" ]; then
    echo "<hosts file> not specified."
    echo $USAGE
    exit 1
fi

# Validate GPHOME exists on all segments.
cmd="gpssh -f $hosts test -d $GPHOME"
output=$($cmd)
if [ 0 -ne $? ]; then
    echo "Directory $GPHOME not found on one or more segments."
    exit 1
fi
if [[ $output == *ERROR* ]]; then
    echo "Error running gpssh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi

# Patch greenplum_path.sh to include JNI library in LD_LIBRARY_PATH.
# Remove existing JAVA_HOME related lines, if any, so that we don't
# end up appending the same lines again.
grep -v "$jvmsodir" $GPHOME/greenplum_path.sh > ./greenplum_path.sh.tmp
if [ ! -f ./greenplum_path.sh.tmp ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi

echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$jvmsodir" >> ./greenplum_path.sh.tmp
cmd="mv ./greenplum_path.sh.tmp $GPHOME/greenplum_path.sh"
output=$($cmd)
if [ 0 -ne $? ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi

# Verify if we patched greenplum_path.sh correctly.
output=$(grep "LD_LIBRARY_PATH.*$jvmsodir" $GPHOME/greenplum_path.sh)
if [ 0 -ne $? ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh"
    exit 1
fi

# nodes will be > 1 for multi-node clusters.
nodes=$(psql -d template1 -t -c "select count(distinct hostname) from gp_segment_configuration;")
if [ 0 -ne $? ]; then
    echo "Cannot connect to HAWQ using psql".
    exit 1
fi
# Following steps are needed only for multi-node clusters.
if [ $nodes -gt 1 ]; then
    # Ensure that JAVA_HOME is the same on segments.
    cmd="gpssh -f $hosts -e test -x $jvmso"
    output=$($cmd)
    if [ 0 -ne $? ]; then
	echo "$jvmso not found on one or more segments."
	exit 1
    fi
    if [[ $output == *ERROR* ]]; then
	echo "Error running gpssh."
	echo "Command: $cmd"
	exit 1
    fi
    # Copy modified greenplum_path.sh to segments.
    cmd="gpscp -f $hosts $GPHOME/greenplum_path.sh =:$GPHOME"
    output=$($cmd)
    if [ 0 -ne $? ]; then
	echo "Failed to copy greenplum_path.sh to one or more segments."
	exit 1
    fi
    if [[ $output == *ERROR* ]]; then
	echo "Error running gpscp."
	echo "Command: $cmd"
	exit 1
    fi
fi

echo "pljava installation was successful."
echo "The following steps are needed before pljava language may be used:"
echo "(1) source \$GPHOME/greenplum_path.sh."
echo "(2) Restart HAWQ using 'gpstop -ar'."
echo "(3) Create pljava language by executing 'CREATE LANGUAGE pljava;' from psql."
