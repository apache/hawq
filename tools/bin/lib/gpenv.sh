#!/bin/bash
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
# gpmigrator requires calling itself on individual hosts, but it is a python script
# so it needs certain environment variables to be setup before it can successfully
# execute itself.
#
# This is a bash script (since /bin/bash is guaranteed) that sets up the environment
# needed to execute a python script.  It uses the path to itself as the basis from
# which it determines GPHOME.

# Get the absolute path to this script
absPath=`cd $(dirname $0); pwd`

# absPath is $GPHOME/bin/lib, so backup two directories to find $GPHOME
GPHOME=$(dirname $(dirname $absPath))
export GPHOME

# get the PATH setup
PATH=$GPHOME/bin:$GPHOME/ext/python/bin:$PATH
export PATH

# Set PYTHONPATH and HOME
PYTHONPATH=$GPHOME/lib/python
export PYTHONPATH
PYTHONHOME=$GPHOME/ext/python
export PYTHONHOME

# Add in library paths apropriate for this system
case `uname` in
  Darwin) 
     if [ $DYLD_LIBRARY_PATH ]
     then DYLD_LIBRARY_PATH=$GPHOME/lib:$GPHOME/ext/python/lib:$DYLD_LIBRARY_PATH
     else DYLD_LIBRARY_PATH=$GPHOME/lib:$GPHOME/ext/python/lib
     fi
     export DYLD_LIBRARY_PATH
     ;;
  *) 
     if [ $LD_LIBRARY_PATH ]
     then LD_LIBRARY_PATH=$GPHOME/lib:$GPHOME/ext/python/lib:$LD_LIBRARY_PATH
     else LD_LIBRARY_PATH=$GPHOME/lib:$GPHOME/ext/python/lib
     fi
     export LD_LIBRARY_PATH
     ;;
esac


# execute whatever was given to us
if [ "$#" -gt 0 ]
then eval $*
else env
fi
