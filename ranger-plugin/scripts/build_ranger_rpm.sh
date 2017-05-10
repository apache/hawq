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

set -eox pipefail

MVN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B -e"

# Set HAWQ ranger-plugin rpm build number to 777 as default
BUILD_NUMBER=${BUILD_NUMBER:-777}

# Get current HAWQ releave version number.
if [ -f ../getversion ] ; then
  HAWQ_RELEASE_VERSION=$(cat ../getversion| grep ^GP_VERSION | cut -d '=' -f2 | sed 's|"||g' | cut -d '-' -f1)
else
  HAWQ_RELEASE_VERSION=$(mvn ${MVN_OPTS} org.apache.maven.plugins:maven-help-plugin:2.2:evaluate \
                        -Dexpression=project.version | grep -Ev "^\[INFO\]")
fi
HAWQ_VERSION=${HAWQ_VERSION:-${HAWQ_RELEASE_VERSION}}

# Set build options
HAWQ_DEP_NAME=${HAWQ_DEP_NAME:-apache-hawq}
BUILD_SUFFIX=${BUILD_SUFFIX:-}
BUILD_OPTS="-Drelease.version=${BUILD_NUMBER} -Dbuild.suffix=${BUILD_SUFFIX} -Dhawq.dep.name=${HAWQ_DEP_NAME}"

# set plugin version
mvn ${MVN_OPTS} versions:set -DnewVersion=${HAWQ_VERSION}

# generate jar and war files.
mvn ${MVN_OPTS} clean package

# build rpm
mvn ${MVN_OPTS} -N ${BUILD_OPTS} rpm:rpm

# verify the size of plugin rpm
find target/rpm -name *.rpm | xargs ls -l
