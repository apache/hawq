#!/bin/bash
# ----------------------------------------------------------------------
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
# ----------------------------------------------------------------------
#                        HAWQ-build-pullrequest
#
#          This file captures the Apache Jenkins build script
# ----------------------------------------------------------------------

work_dir=$(pwd)

echo "working dir is ${work_dir}"

## ----------------------------------------------------------------------
## Diagnostic scriptlet to fix cloning permission denied issue for HAWQ
## PR Project (HAWQ-build-pullrequest).
## 
## 0) Open two browser "configure" tab's for project:
##
##    https://builds.apache.org/job/HAWQ-build-pullrequest/configure
##
##    One will be used to make the updates to the project's
##    configuration that will be reverted by the second unmodified tab
##    when the slave has been fixed.
##
## 1) Change "Source Code Management" to None
## 2) Uncomment to following section
## 3) Ensure  the "Restrict where this project can be run" --> "Label
##    Expression" specifies the slave with the build issue.
## 4) Manually trigger project "HAWQ-build-pullrequest"
## 5) Restore project in 2nd "configure" browser tab.
##
## ----------------------------------------------------------------------

## ls -al
##
## # Remove previous source artifacts from slave. The operation is
## # performed in the container to have sufficient privileges.
##
## docker run -v $(pwd):/data/hawq-src \
##            -u root rlei/mydocker:centos7-build \
##            /bin/sh -c "rm -rf /data/hawq-src/*"
## 
## ls -al
## exit 1

## ----------------------------------------------------------------------
## End Diagnositc scriptlet
## ----------------------------------------------------------------------

git diff
git log -n 5
git status

#
# Create compile build script to be executed in Docker container.
#

cat > compile_hawq.sh <<EOF
#!/bin/sh
set -ex

cd /data/hawq-src

time ./configure --prefix=/data/hawq-install 
time make -j8
time make install 

EOF

chmod a+x compile_hawq.sh

#
# Run compile build script.
#

docker run -v $(pwd):/data/hawq-src \
           -u root rlei/mydocker:centos7-build \
           /bin/sh -c "/data/hawq-src/compile_hawq.sh"

# Return status from docker build run.

if [ $? = 0 ]; then
    echo "Apache Jenkins job ($JOB_NAME) PASSED"
    exit 0
else
    echo "Apache Jenkins job ($JOB_NAME) FAILED"
    exit 1
fi
