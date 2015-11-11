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
## ======================================================================
## Post download untar trigger
## ======================================================================

FILE=$1
REVISION=$2

ARTIFACT_DIR=$( dirname ${FILE} )

##
## Expand tarball
##

if [ ! -f "${FILE}" ]; then
    echo "WARNING: tarball does not exist (${FILE})"
    exit 2
else
    pushd ${ARTIFACT_DIR}/..
    gunzip -qc ${FILE} | tar xf -
    popd

    if [ $? != 0 ]; then
        echo "FATAL: Problem exapanding tarball (${FILE})"
        exit 1
    fi
fi

## ----------------------------------------------------------------------
## Devel package processing
##   Restore object files into loation used for build
## ----------------------------------------------------------------------

echo "${FILE}" | grep devel
if [ $? = 0 ]; then
    BASE_PULSE_PROJECT=`tar tvf ${FILE} | head -1 | sed -e 's|\(.* \./\)\(.*\)|\2|' | cut -d '/' -f1`
    BASE_BUILD_STAGE=`tar tvf ${FILE}  | head -1 | sed -e 's|\(.* \./\)\(.*\)|\2|' | cut -d '/' -f2`
    rm -rf /opt/releng/build/${BASE_PULSE_PROJECT}/${BASE_BUILD_STAGE}
    cd ..
    [ ! -d /opt/releng/build/${BASE_PULSE_PROJECT} ] && mkdir -p /opt/releng/build/${BASE_PULSE_PROJECT}
    ln -s ${ARTIFACT_DIR}/../${BASE_PULSE_PROJECT}/${BASE_BUILD_STAGE} /opt/releng/build/${BASE_PULSE_PROJECT}/${BASE_BUILD_STAGE}
fi

exit 0
