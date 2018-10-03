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

FROM hawq/hawq-dev:centos7

MAINTAINER Richard Guo <riguo@pivotal.io>

USER root

ARG PXF_CLASSPATH_TEMPLATE
ARG PXF_LOG4J_PROPERTIES

## install HDP 2.5.0
RUN curl -L "http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.5.0.0/hdp.repo" -o /etc/yum.repos.d/hdp.repo && \
 yum install -y hadoop hadoop-hdfs hadoop-libhdfs hadoop-yarn hadoop-mapreduce hadoop-client hdp-select && \
 yum clean all

RUN ln -s /usr/hdp/current/hadoop-hdfs-namenode/../hadoop/sbin/hadoop-daemon.sh /usr/bin/hadoop-daemon.sh

RUN touch /tmp/pxf-private.classpath && \
 touch /tmp/log4j.properties && \
 echo "$PXF_CLASSPATH_TEMPLATE" > /tmp/pxf-private.classpath && \
 echo "$PXF_LOG4J_PROPERTIES" > /tmp/pxf-log4j.properties

COPY conf/* /etc/hadoop/conf/

COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY service-hawq.sh /usr/bin/service-hawq.sh
COPY service-pxf.sh /usr/bin/service-pxf.sh
COPY start-hdfs.sh /usr/bin/start-hdfs.sh

USER gpadmin

ENTRYPOINT ["entrypoint.sh"]
CMD ["bash"]

