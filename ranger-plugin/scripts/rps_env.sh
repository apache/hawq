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

export CATALINA_HOME=/usr/lib/bigtop-tomcat
export CATALINA_BASE=/usr/local/hawq/ranger/plugin-service

export RANGER_ADMIN_HOST=${RANGER_ADMIN_HOST:-localhost}
export RANGER_ADMIN_PORT=${RANGER_ADMIN_PORT:-6080}

export RPS_HOST=${RPS_HOST:-localhost}
export RPS_PORT=${RPS_PORT:-8432}
export CATALINA_OPTS="-Dhttp.host=$RPS_HOST -Dhttp.port=$RPS_PORT"
