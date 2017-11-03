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
# Edit this file to set custom options
# Tomcat accepts two parameters JAVA_OPTS and CATALINA_OPTS
# JAVA_OPTS are used during START/STOP/RUN
# CATALINA_OPTS are used during START/RUN

AGENT_PATHS=""
JAVA_AGENTS=""
JAVA_LIBRARY_PATH=""
JVM_OPTS=""
JAVA_OPTS="$JVM_OPTS $AGENT_PATHS $JAVA_AGENTS $JAVA_LIBRARY_PATH -Dpxf.log.dir=$CATALINA_BASE/logs"
CATALINA_PID="$CATALINA_BASE/logs/catalina.pid"
CATALINA_OUT="$CATALINA_BASE/logs/catalina.out"
