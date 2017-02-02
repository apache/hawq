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

if [ $# -le 0 ]; then
  echo "Usage: rps.sh (start|stop)"
  exit 1
fi

action=$1
shift

CWDIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd -P)
BASEDIR=$( dirname ${CWDIR} )
# read properties from the file
source ${BASEDIR}/etc/rps.properties

export CATALINA_HOME=/usr/lib/bigtop-tomcat
export CATALINA_BASE=${BASEDIR}/plugin-service
export CATALINA_PID=${CATALINA_BASE}/work/rps.pid

# options used to start the RPS process
export CATALINA_OPTS="-server -Xms512m -Xmx512m -XX:MaxPermSize=128m -Dproc_rps -Dversion=${RPS_VERSION} -Dranger.hawq.instance=${RANGER_HAWQ_INSTANCE} -Drps.http.port=${RPS_HTTP_PORT} -Drps.https.port=${RPS_HTTPS_PORT}"

# options used to stop the RPS process
export JAVA_OPTS="-Drps.shutdown.port=${RPS_SHUTDOWN_PORT}"

RPS_URL="http://localhost:${RPS_HTTP_PORT}/rps"
RPS_LOG="${CATALINA_BASE}/logs/catalina.out"

function fail() {
    echo "FATAL: Failed to ${1} HAWQ Ranger Plugin Service. Check ${RPS_LOG} for details."
    exit 2
}

function tomcat_command() {
    ${CATALINA_HOME}/bin/catalina.sh ${1}
    if [ $? -ne 0 ]; then
      fail ${1}
    fi
}

function wait_until_server_started() {
    echo "Waiting for Hawq Ranger Plugin Service to start ."
    local pid="20"
    local n=0
    until $(curl -s --output /dev/null --fail ${RPS_URL}/version); do
      n=$[${n}+1]
      if [ ${n} -ge ${retries} ]; then
        fail "start"
      fi
      printf '.'
      sleep 3
    done
    echo "Hawq Ranger Plugin Service is available at ${RPS_URL}"
}

function wait_until_server_stopped() {
    if [ ${RPS_PID} -ne 0 ]; then
      local retries="20"
      local n=0
      while $(ps -p ${1} > /dev/null); do
        n=$[${n}+1]
        if [ ${n} -ge ${retries} ]; then
          fail "stop"
        fi
        printf '.'
        sleep 3
      done
    fi
}

function get_pid() {
    local pid=0
    if [ -f "${CATALINA_PID}" ]; then
        pid=$(cat "$CATALINA_PID")
    fi
    echo ${pid}
}

case ${action} in
  (start)
    tomcat_command "start"
    wait_until_server_started
    ;;
  (stop)
    RPS_PID=$(get_pid)
    tomcat_command "stop"
    wait_until_server_stopped
    ;;
esac
