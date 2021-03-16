#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e



# Check
if [[ -z $GPHOME ]]; then
  echo "Please source HAWQ's greenplum_path.sh"
  exit 1
fi

# Configure
tee $GPHOME/etc/hawq-site.xml << EOF_hawq_site
<configuration>
    <property>
        <name>hawq_dfs_url</name>
        <value>localhost:8020/hawq_default</value>
        <description>URL for accessing HDFS.</description>
    </property>
    <property>
        <name>hawq_master_address_host</name>
        <value>localhost</value>
    </property>
    <property>
        <name>hawq_master_address_port</name>
        <value>5432</value>
    </property>
    <property>
        <name>hawq_segment_address_port</name>
        <value>40000</value>
    </property>
    <property>
        <name>hawq_master_directory</name>
        <value>/tmp/db_data/hawq-data-directory/masterdd</value>
    </property>
    <property>
        <name>hawq_segment_directory</name>
        <value>/tmp/db_data/hawq-data-directory/segmentdd</value>
    </property>
    <property>
        <name>hawq_master_temp_directory</name>
        <value>/tmp</value>
    </property>
    <property>
        <name>hawq_segment_temp_directory</name>
        <value>/tmp</value>
    </property>
</configuration>
EOF_hawq_site

# Clean
pkill -9 postgres || true
hdfs dfs -rm -f -r hdfs://localhost:8020/hawq_default
rm -rf /tmp/db_data/hawq-data-directory

# Initialize
install -d /tmp/db_data/hawq-data-directory/masterdd
install -d /tmp/db_data/hawq-data-directory/segmentdd
hawq init cluster -a
