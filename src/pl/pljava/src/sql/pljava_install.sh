#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Disclaimer: There is a lot of common code among pgcrypto_install.sh,
# plr_install.sh and pljava_install.sh.  These scripts must be
# refactored at an opportune time.  In fact, HAWQ segments should be
# intelligent enough so as to pick up a list of packages from master
# and install them.

USAGE="$0 -f <hosts file> [-x]"

if [ "$BLD_ARCH" == "" ]; then
    if [[ $(uname) =~ Darwin* ]]; then
	BLD_ARCH=osx106_x86
    elif [[ $(uname) =~ Linux* ]]; then
	BLD_ARCH=rhel5_x86_64
    else
	echo "PL/Java is not supported on your platform."
	exit 1
    fi
fi

if [ ! -d $GPHOME ]; then
    echo "GPHOME is either not set or is not a directory."
    exit 1
fi

if [ "$JAVA_HOME" == "" ]; then
    echo "JAVA_HOME not set."
    exit 1
fi

jvmso=$(find $JAVA_HOME -name libjvm.*)
if [ ! $jvmso ]; then
    echo "JAVA_HOME is invalid, cannot find libjvm."
    exit 1
fi
jvmsodir=$(dirname $jvmso)
# Being paranoid...
if [ ! -d $jvmsodir ]; then
    echo "Cannot get directory name for libjvm."
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
expand_mode=0
while getopts f:x opt; do
  case $opt in
  f)
      hosts=$OPTARG
      ;;
  x)
      expand_mode=1
      ;;
  esac
done

if [ "$hosts" = "" ]; then
    echo "<hosts file> not specified."
    echo $USAGE
    exit 1
fi
if [ ! -f $hosts ]; then
    echo "Cannot read file: $hosts."
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

# Copy modified greenplum_path.sh on segments.
function install_pljava_segments() {
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
}

if [ $expand_mode -eq 1 ]; then
    install_pljava_segments
    echo "pljava installation succeeded on new segments."
    echo "Restart HAWQ using 'gpstop -ar' for the changes to take effect."
    exit 0
fi

# Patch greenplum_path.sh to include JNI library in
# (DY)LD_LIBRARY_PATH.  Remove existing JAVA_HOME related lines, if
# any, so that we don't end up appending the same lines again.
grep -v "$jvmsodir" $GPHOME/greenplum_path.sh > ./greenplum_path.sh.tmp
if [ ! -f ./greenplum_path.sh.tmp ]; then
    echo "Failed to patch $GPHOME/greenplum_path.sh."
    echo "Command: $cmd"
    echo "Output: $output"
    exit 1
fi

if [ $BLD_ARCH == "rhel5_x86_64" ]; then
    echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$jvmsodir" >> ./greenplum_path.sh.tmp
else
    echo "export DYLD_LIBRARY_PATH=\$DYLD_LIBRARY_PATH:$jvmsodir" >> ./greenplum_path.sh.tmp
fi

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
    install_pljava_segments
fi

echo "pljava installation was successful."
echo "The following steps are needed before pljava language may be used:"
echo "(1) source \$GPHOME/greenplum_path.sh."
echo "(2) Restart HAWQ using 'gpstop -ar'."
echo "(3) Create pljava language by executing 'CREATE LANGUAGE pljava;' from psql."
