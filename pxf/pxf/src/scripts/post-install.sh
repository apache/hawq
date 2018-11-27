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

# Post installation script

env_script=/etc/pxf/conf/pxf-env.sh
user=pxf
group=pxf

# load pxf-env.sh script
if [ ! -e $env_script ]; then
  echo WARNING: failed to find $env_script
else
  source $env_script
fi

# create log directory with necessary permission
mkdir -p $PXF_LOGDIR
chown $user:$group -R $PXF_LOGDIR
chmod 755 $PXF_LOGDIR

# create run directory with necessary permission
mkdir -p $PXF_RUNDIR
chown $user:$group -R $PXF_RUNDIR
chmod 755 $PXF_RUNDIR
