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
  echo "Usage: rps (start|stop|init) [<catalina-args...>]"
  exit 1
fi

actionCmd=$1
shift

CWDIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
source $CWDIR/rps-env.sh

setup_rps() {
  echo "Initializing Hawq Ranger Plugin Service..."
  cp $CATALINA_HOME/conf.template/* $CATALINA_BASE/conf
  cp $CATALINA_BASE/conf/tomcat-server.xml $CATALINA_BASE/conf/server.xml
  pushd $CATALINA_BASE/webapps >/dev/null
  unzip -d rps rps.war >/dev/null
  find . -name ranger-hawq-security.xml | xargs sed -i \
    "s/localhost:6080/$RANGER_ADMIN_HOST:$RANGER_ADMIN_PORT/g"
  popd >/dev/null
  echo "Hawq Ranger Plugin Service installed on http://$RPS_HOST:$RPS_PORT/rps"
  echo "Please use 'rps.sh start' to start the service"
}

case $actionCmd in
  (init)
    setup_rps
    ;;
  (start)
    $CATALINA_HOME/bin/catalina.sh start "$@"
    echo "Waiting for RPS service to start..."
    sleep 15
    ;;
  (stop)
    $CATALINA_HOME/bin/catalina.sh stop "$@"
    echo "Waiting for RPS service to stop..."
    sleep 10
    ;;
esac
