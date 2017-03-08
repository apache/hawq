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
#                             HAWQ-rat
#
#          This file captures the Apache Jenkins build script
# ----------------------------------------------------------------------

set -exu

# Check if NOTICE file year is current (HAWQ-1267)

grep "Copyright $(date +"%Y") The Apache Software Foundation." NOTICE

# Check if pom.xml file version is current (HAWQ-1268)

grep "<version>$(./getversion | cut -c1-3)</version>" pom.xml

set +x

badfile_extentions="class jar tar tgz zip"
badfiles_found=false

for extension in ${badfile_extentions}; do
    echo "Searching for ${extension} files:"
    badfile_count=$(find . -name "*.${extension}" | wc -l)
    if [ ${badfile_count} != 0 ]; then
        echo "----------------------------------------------------------------------"
        echo "FATAL: ${extension} files should not exist"
        echo "For ASF compatibility: the source tree should not contain"
        echo "binary (jar) files as users have a hard time verifying their"
        echo "contents."

        find . -name "*.${extension}"
        echo "----------------------------------------------------------------------"
        badfiles_found=true
    else
        echo "PASSED: No ${extension} files found."
    fi
done

if [ ${badfiles_found} = "true" ]; then
    exit 1
fi

set -x
