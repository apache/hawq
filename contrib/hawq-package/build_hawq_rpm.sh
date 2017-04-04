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

CUR_DIR=$(pwd)

if [ -f /etc/redhat-release ] ; then
    DISTRO_VERSION=$(cat /etc/redhat-release | sed s/.*release\ // | sed s/\ .*//)
    DISTRO_MAJOR_VERSION=$(echo ${DISTRO_VERSION} | awk -F '.' '{print $1}')
    OS_TYPE="el${DISTRO_MAJOR_VERSION}"
elif [ -f /etc/SuSE-release ] ; then
    DISTRO_VERSION=$(cat /etc/SuSE-release | tr "\n" ' ' | sed s/.*=\ //)
    DISTRO_MAJOR_VERSION=$(echo ${DISTRO_VERSION} |awk -F '.' '{print $1}')
    OS_TYPE="suse${DISTRO_MAJOR_VERSION}"
fi

if [ -z "${HAWQ_RELEASE_VERSION}" ]; then
    HAWQ_RELEASE_VERSION=$(cat ../../getversion| grep ^GP_VERSION | cut -d '=' -f2 | sed 's|"||g' | cut -d '-' -f1)
fi

echo "HAWQ_RELEASE_VERSION is $HAWQ_RELEASE_VERSION"

if [ -d  rpmbuild ]; then
    echo "Clean rpmbuild directory."
    rm -rf rpmbuild
fi

echo "Make directories: rpmbuild/BUILD rpmbuild/RPMS rpmbuild/SOURCES rpmbuild/SPECS rpmbuild/SRPMS"
mkdir -p rpmbuild/BUILD rpmbuild/RPMS rpmbuild/SOURCES rpmbuild/SPECS rpmbuild/SRPMS

# Copy HAWQ RPM configuration file for the build
cp hawq.spec rpmbuild/SPECS/

HAWQ_SOURCE_TARBALL_FILE=apache-hawq-src-${HAWQ_RELEASE_VERSION}-incubating.tar.gz

# Get PATH where to find HAWQ source code tarball
if [ -z ${HAWQ_SOURCE_TARBALL_PATH} ]; then
    HAWQ_SOURCE_TARBALL_PATH=${CUR_DIR}
fi

# Copy HAWQ source code tarball for rpm build
if [ -f "${HAWQ_SOURCE_TARBALL_PATH}/${HAWQ_SOURCE_TARBALL_FILE}" ]; then
    echo "Using HAWQ source code tarball: ${HAWQ_SOURCE_TARBALL_PATH}/${HAWQ_SOURCE_TARBALL_FILE}"
    cp ${HAWQ_SOURCE_TARBALL_PATH}/${HAWQ_SOURCE_TARBALL_FILE} rpmbuild/SOURCES/
else
    echo "========================================================================="
    echo "Can not find ${HAWQ_SOURCE_TARBALL_PATH}/${HAWQ_SOURCE_TARBALL_FILE}"
    echo "Please copy the source code tarball in place."
    echo "Or use environment variable 'HAWQ_SOURCE_TARBALL_PATH' to specify the find path of HAWQ source tarball."
    echo "========================================================================="
    exit 1
fi

pushd rpmbuild > /dev/null
RPM_TOP_DIR=$(pwd)

set -x

rpmbuild --define "_topdir ${RPM_TOP_DIR}" \
         --define "_hawq_version ${HAWQ_RELEASE_VERSION}" \
         --define "_rpm_os_version ${OS_TYPE}" \
         -bb SPECS/hawq.spec
if [ $? != 0 ]; then
    echo "Build HAWQ rpm package failed, exit..."
    exit $?
fi

set +x
popd > /dev/null

echo "========================================================================="
echo "Build HAWQ rpm package successfully."
echo "========================================================================="
echo ""
exit 0
