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

MVN_OPTS="-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B -e"

# Set HAWQ ranger-plugin rpm build number to 1 as default
BUILD_NUMBER=1
BUILD_OPTS="-Drelease.version=${BUILD_NUMBER}"
BUILD_OPTS="${BUILD_OPTS} -Dbuild.suffix= -Dhawq.dep.name=apache-hawq"
BUILD_OPTS="${BUILD_OPTS} -Ddestination.dir=/usr/local/apache-hawq/ranger"

# Get current HAWQ releave version number.
if [ -z "${HAWQ_RELEASE_VERSION}" ]; then
    HAWQ_RELEASE_VERSION=$(cat ../getversion| grep ^GP_VERSION | cut -d '=' -f2 | sed 's|"||g' | cut -d '-' -f1)
fi

# Set HAWQ ranger-plugin.
mvn ${MVN_OPTS} versions:set -DnewVersion=${HAWQ_RELEASE_VERSION}
if [ $? != 0 ]; then
    echo "Set HAWQ ranger-plugin failed."
    exit $?
fi

# generate jar and war files.
mvn ${MVN_OPTS} clean package
if [ $? != 0 ]; then
    echo "Generate HAWQ ranger-plugin jar and war files failed."
    exit $?
fi

# build rpm
mvn ${MVN_OPTS} -N ${BUILD_OPTS} install
if [ $? != 0 ]; then
    echo "Build HAWQ ranger-plugin rpm package failed."
    exit $?
fi

exit 0
