#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# check parameter
function heartbeat_show_help(){
  echo "Usage: autoswitch.sh start <heartbeat_sender|heartbeat_monitor> <log_dir>"
  echo "or autoswitch.sh stop"
}

if [ $# -ne 1 -a $# -ne 3 ]; then
  heartbeat_show_help
  exit 1
fi

# stop
if [ $# -eq 1 ]; then
  if [ $1 = "stop" ]; then
    pids=`ps -ef | grep hawq_heartbeat | grep -v grep | awk '{print $2}'`
    if [ "$pids" ]; then
      kill -9 $pids
    fi
    exit 0
  else
    heartbeat_show_help
    exit 1
  fi
fi

heartbeat_logdir=$3
if [ ! -d ${heartbeat_logdir} ]; then
  mkdir -p ${heartbeat_logdir}
fi

# args number should only be 3 that checks before.
if [ $1 = "start" ]; then
  if [ $2 = "heartbeat_sender" ]; then
    hawq_heartbeat_sender --logdir=$3 >> /dev/null 2>$3/HEARTBEAT_ERROR_LOG &
    bkpid=$!
  elif [ $2 = "heartbeat_monitor" ]; then
    hawq_heartbeat_monitor --logdir=$3 >> /dev/null 2>$3/HEARTBEAT_ERROR_LOG &
    bkpid=$!
  else
    heartbeat_show_help
    exit 1
  fi
  result=`ps aux | grep -v grep | grep heartbeat | grep $bkpid`
  if [ "$result" == "" ]; then
    echo "Failed to start autoswitch service."
    exit 1
  fi
else
  heartbeat_show_help
  exit 1
fi

