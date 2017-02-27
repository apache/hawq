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
