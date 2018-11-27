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

FROM hawq/hawq-dev:centos6

MAINTAINER Richard Guo <riguo@pivotal.io>

USER root

# install HDP 2.5.0
RUN curl -L "http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.5.0.0/hdp.repo" -o /etc/yum.repos.d/hdp.repo && \
 yum install -y hadoop hadoop-hdfs hadoop-libhdfs hadoop-yarn hadoop-mapreduce hadoop-client hdp-select && \
 yum clean all

RUN ln -s /usr/hdp/current/hadoop-hdfs-namenode/../hadoop/sbin/hadoop-daemon.sh /usr/bin/hadoop-daemon.sh

COPY conf/* /etc/hadoop/conf/

COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY start-hdfs.sh /usr/bin/start-hdfs.sh

USER gpadmin

ENTRYPOINT ["entrypoint.sh"]
CMD ["bash"]

