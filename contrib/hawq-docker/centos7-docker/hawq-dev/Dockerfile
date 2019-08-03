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

FROM centos:7

MAINTAINER Richard Guo <riguo@pivotal.io>

# install all software we need
RUN yum install -y epel-release && \
 yum makecache && \
 yum install -y man glog-devel jsoncpp-devel json gflags-devel lz4-devel passwd sudo tar which git mlocate links make bzip2 net-tools \
 autoconf automake libtool m4 gcc gcc-c++ gdb flex gperf maven indent \
 libuuid-devel krb5-devel libgsasl-devel expat-devel libxml2-devel \
 perl-ExtUtils-Embed pam-devel python-devel libcurl-devel snappy-devel \
 thrift-devel libyaml-devel libevent-devel bzip2-devel openssl-devel \
 openldap-devel readline-devel net-snmp-devel apr-devel \
 libesmtp-devel python-pip json-c-devel \
 java-1.7.0-openjdk-devel lcov cmake3 \
 wget rpm-build \
 https://forensics.cert.org/centos/cert/7/x86_64//libiconv-1.15-1.el7.x86_64.rpm \
 https://forensics.cert.org/centos/cert/7/x86_64//libiconv-devel-1.15-1.el7.x86_64.rpm \
 openssh-clients openssh-server perl-JSON unzip && \
 yum clean all

RUN ln -s /usr/bin/cmake3 /usr/bin/cmake

RUN rpm -ivh --nodeps   https://rpmfind.net/linux/centos/6.10/os/x86_64/Packages/bison-2.4.1-5.el6.x86_64.rpm

RUN cd ~ && git clone https://github.com/protocolbuffers/protobuf.git && \
	cd protobuf && git submodule update --init --recursive && \
	./autogen.sh && ./configure && make && \
	make check && make install && ldconfig && cd -

RUN pip --retries=50 --timeout=300 install pycrypto
RUN pip --retries=50 --timeout=300 install cogapp

# OS requirement
RUN echo "kernel.sem = 250 512000 100 2048" >> /etc/sysctl.conf

# setup ssh server and keys for root
RUN sshd-keygen && \
 ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa && \
 cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
 chmod 0600 ~/.ssh/authorized_keys

# create user gpadmin since HAWQ cannot run under root
RUN groupadd -g 1000 gpadmin && \
 useradd -u 1000 -g 1000 gpadmin && \
 echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin

# sudo should not require tty
RUN sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers

# setup JAVA_HOME for all users
RUN echo "#!/bin/sh" > /etc/profile.d/java.sh && \
 echo "export JAVA_HOME=/etc/alternatives/java_sdk" >> /etc/profile.d/java.sh && \
 chmod a+x /etc/profile.d/java.sh && \
 ln -s /usr/lib/jvm/java-1.7.0-openjdk-1.7.0.221-2.6.18.0.el7_6.x86_64/jre/lib/amd64/server/libjvm.so /usr/local/lib/libjvm.so

# set USER env
RUN echo "#!/bin/bash" > /etc/profile.d/user.sh && \
 echo "export USER=\`whoami\`" >> /etc/profile.d/user.sh && \
 chmod a+x /etc/profile.d/user.sh

ENV BASEDIR /data
RUN mkdir -p /data && chmod 777 /data

USER gpadmin

# setup ssh client keys for gpadmin
RUN ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa && \
 cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
 chmod 0600 ~/.ssh/authorized_keys

WORKDIR /data

ENTRYPOINT ["bash", "/data/hawq-docker/apache-hawq/entrypoint.sh"]

