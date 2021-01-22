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



# Configure
tee $HADOOP_HOME/etc/hadoop/core-site.xml << EOF_core_site
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:8020</value>
    </property>
</configuration>
EOF_core_site
tee $HADOOP_HOME/etc/hadoop/hdfs-site.xml << EOF_hdfs_site
<configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///tmp/db_data/hdfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///tmp/db_data/hdfs/data</value>
    </property>
</configuration>
EOF_hdfs_site

# Initialize
install -d /tmp/db_data/hdfs/name
install -d /tmp/db_data/hdfs/data
hdfs namenode -format

# Start
$HADOOP_HOME/sbin/start-dfs.sh

# Connect
hdfs dfsadmin -report
hdfs dfs -ls /
