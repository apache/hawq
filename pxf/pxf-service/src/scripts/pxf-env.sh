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

# Hadoop Distribution Type for embedded installations only. Use the following values:
# AUTO - for auto discovery of RPM-based install of Hadoop clients
# HDP  - for RPM-based install of HDP Hadoop clients
# CDH  - for RPM-based install of CDH Hadoop clients
# TAR  - for tarball-based install of Hadoop clients
# leave empty for non-embedded (standalone) installations
export HADOOP_DISTRO=@hadoopDistro@

# Location of Hadoop client installation : optional, required for TAR distro only
export HADOOP_HOME=

# Location of Hive client installation : optional, required for TAR distro only
export HIVE_HOME=

# Location of HBase client installation : optional, required for TAR distro only
export HBASE_HOME=
