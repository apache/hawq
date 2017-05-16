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
SRC_TOP_DIR=../..

if [ -z  "${HAWQ_RELEASE_VERSION}" ]; then
    HAWQ_RELEASE_VERSION=$(cat ../../getversion| grep ^GP_VERSION | cut -d '=' -f2 | sed 's|"||g' | cut -d '-' -f1)
fi

RPM_PKG_DIR=${CUR_DIR}/hawq_rpm_packages
if [ -d ${RPM_PKG_DIR} ]; then
    rm -rf ${RPM_PKG_DIR}
fi

mkdir -p ${RPM_PKG_DIR}
if [ $? != 0 ]; then
    echo "Create HAWQ rpm package directory: ${RPM_PKG_DIR} failed."
    exit $?
fi

echo "Copying HAWQ rpm packages into directory: ${RPM_PKG_DIR}"

# Copy hawq rpm packages
cp ${SRC_TOP_DIR}/contrib/hawq-package/rpmbuild/RPMS/x86_64/apache-hawq-${HAWQ_RELEASE_VERSION}*.rpm ${RPM_PKG_DIR}/
if [ $? != 0 ]; then
    echo "Copy HAWQ rpm package failed."
    exit $?
fi

# Copy apache tomcat rpm package for PXF
cp ${SRC_TOP_DIR}/pxf/distributions/apache-tomcat*.rpm ${RPM_PKG_DIR}/
if [ $? != 0 ]; then
    echo "Copy Tomcat rpm package failed."
    exit $?
fi

# Copy PXF rpm packages
cp ${SRC_TOP_DIR}/pxf/build/distributions/pxf*.rpm ${RPM_PKG_DIR}/
if [ $? != 0 ]; then
    echo "Copy PXF rpm packages failed."
    exit $?
fi

# Copy HAWQ Ranger rpm package
cp ${SRC_TOP_DIR}/ranger-plugin/target/rpm/hawq-ranger-plugin*/RPMS/noarch/hawq-ranger-plugin*.rpm ${RPM_PKG_DIR}/
if [ $? != 0 ]; then
    echo "Copy HAWQ Ranger plugin rpm package failed."
    exit $?
fi

echo "Copied all the HAWQ/PXF/Range-plugin rpm packages."

ls ${RPM_PKG_DIR}/

# Make tarball for all the HAWQ/PXF/RANGER rpms
tar czvf apache-hawq-rpm-${HAWQ_RELEASE_VERSION}-incubating.tar.gz  hawq_rpm_packages
if [ $? != 0 ]; then
    echo "Make HAWQ/PXF/Ranger-plugin rpm tarball failed."
    exit $?
else
    echo "Make HAWQ/PXF/Ranger-plugin rpm tarball successfully."
    echo "You can find the rpm binary tarball at:"
    echo "${CUR_DIR}/apache-hawq-rpm-${HAWQ_RELEASE_VERSION}-incubating.tar.gz"
    ls -l apache-hawq-rpm-${HAWQ_RELEASE_VERSION}-incubating.tar.gz
fi

exit 0
