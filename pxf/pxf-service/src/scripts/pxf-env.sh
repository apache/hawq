#!/bin/sh
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

PARENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Default PXF_HOME
export PXF_HOME=${PXF_HOME:=$PARENT_SCRIPT_DIR}

# Path to HDFS native libraries
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native:${LD_LIBRARY_PATH}

# Path to JAVA
export JAVA_HOME=${JAVA_HOME:=/usr/java/default}

# Path to Log directory
export PXF_LOGDIR=@pxfLogDir@

# Path to Run directory
export PXF_RUNDIR=@pxfRunDir@

# Configured user
if [ ! -z '@pxfDefaultUser@' ]; then
    export PXF_USER=${PXF_USER:-@pxfDefaultUser@}
fi

# Port
export PXF_PORT=${PXF_PORT:-51200}

# Memory
export PXF_JVM_OPTS="-Xmx2g -Xms1g"

# Hadoop Distribution Type (optional), supported values:
# <empty> - for auto discovery of HDP, CDH or tarball based client installation
# HDP     - for HDP Hadoop client installation
# CDH     - for CDH Hadoop client installation
# CUSTOM  - for custom Hadoop client installation
export HADOOP_DISTRO=${HADOOP_DISTRO}

# Parent directory of Hadoop client installation (optional)
# used in case of tarball-based installation when all clients are under a common parent directory
export HADOOP_ROOT=${HADOOP_ROOT}
