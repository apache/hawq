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

export JAVA_HOME
export CATALINA_HOME
export CATALINA_BASE=${BASEDIR}/plugin-service
export CATALINA_PID=${CATALINA_BASE}/work/rps.pid

# options used to start the RPS process
export CATALINA_OPTS="-server -Xms${RPS_HEAP_SIZE} -Xmx${RPS_HEAP_SIZE}
                     -Dproc_rps -Dversion=${RPS_VERSION}
                     -Dranger.hawq.instance=${RANGER_HAWQ_INSTANCE}
                     -Drps.http.port=${RPS_HTTP_PORT} -Drps.https.port=${RPS_HTTPS_PORT}
                     -Dpolicy.manager.url=${POLICY_MGR_URL}"

# options used to stop the RPS process
export JAVA_OPTS="-Drps.shutdown.port=${RPS_SHUTDOWN_PORT}"

RPS_URL="http://localhost:${RPS_HTTP_PORT}/rps"
RPS_LOG="${CATALINA_BASE}/logs/catalina.out"

function fail() {
    echo "FATAL: Failed to ${1} HAWQ Ranger Plugin Service. Check ${RPS_LOG} for details."
    exit 2
}

function tomcat_command() {
    ${CWDIR}/catalina.sh ${1} ${2}
    if [ $? -ne 0 ]; then
      fail ${1}
    fi
}

function wait_until_server_started() {
    echo -n "Waiting for Hawq Ranger Plugin Service to start ."
    local retries="20"
    local n=0
    until $(curl -s --output /dev/null --fail ${RPS_URL}/version); do
      n=$[${n}+1]
      if [ ${n} -ge ${retries} ]; then
        echo
        fail "start"
      fi
      printf '.'
      sleep 3
    done
    echo -e "\nHawq Ranger Plugin Service is available at ${RPS_URL}"
}

case ${action} in
  (start)
    tomcat_command "start"
    wait_until_server_started
    ;;
  (stop)
    # allow the server 10 seconds after shutdown command before force killing it
    tomcat_command "stop" "10 -force"
    echo "Hawq Ranger Plugin Service is stopped."
    ;;
esac
