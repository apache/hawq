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

FROM centos:6

MAINTAINER Richard Guo <riguo@pivotal.io>

# install all software we need
RUN yum install -y epel-release && \
 yum makecache && \
 yum install -y man passwd sudo tar which git mlocate links make bzip2 \
 autoconf automake libtool m4 gcc gcc-c++ gdb flex cmake gperf indent \
 libuuid-devel krb5-devel libgsasl-devel expat-devel libxml2-devel \
 perl-ExtUtils-Embed pam-devel python-devel snappy-devel \
 libyaml-devel libevent-devel bzip2-devel openssl-devel \
 openldap-devel readline-devel net-snmp-devel apr-devel \
 libesmtp-devel xerces-c-devel python-pip json-c-devel \
 apache-ivy java-1.7.0-openjdk-devel wget \
 openssh-clients openssh-server perl-JSON && \
 yum clean all

# update gcc
RUN wget -O /etc/yum.repos.d/slc6-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo && \
 rpm --import http://linuxsoft.cern.ch/cern/slc68/x86_64/RPM-GPG-KEY-cern && \
 yum install -y devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ && \
 echo "source /opt/rh/devtoolset-2/enable" >> ~/.bashrc && \
 source ~/.bashrc

# install libcurl 7.45.0
RUN mkdir -p /tmp/build/ && \
 cd /tmp/build && curl -L "http://curl.haxx.se/download/curl-7.45.0.tar.bz2" -o curl-7.45.0.tar.bz2 && \
 tar -xjf curl-7.45.0.tar.bz2 && cd curl-7.45.0 && \
 ./configure --prefix=/usr && make && make install && \
 rm -rf /tmp/build && ldconfig

# install maven
RUN curl -L "http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo" -o /etc/yum.repos.d/epel-apache-maven.repo && \
 yum install -y apache-maven && \
 yum clean all

# OS requirements
RUN echo "kernel.sem = 250 512000 100 2048" >> /etc/sysctl.conf

# setup ssh server and keys for root
RUN ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa && \
 cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
 chmod 0600 ~/.ssh/authorized_keys

# setup JAVA_HOME for all users
RUN echo "#!/bin/sh" > /etc/profile.d/java.sh && \
 echo "export JAVA_HOME=/etc/alternatives/java_sdk" >> /etc/profile.d/java.sh && \
 chmod a+x /etc/profile.d/java.sh

# install boost 1.59
 RUN mkdir -p /tmp/build && \
  cd /tmp/build && curl -L "http://downloads.sourceforge.net/project/boost/boost/1.59.0/boost_1_59_0.tar.bz2" -o boost_1_59_0.tar.bz2 && \
  tar -xjf boost_1_59_0.tar.bz2 && cd boost_1_59_0 && \
  ./bootstrap.sh && ./b2 --prefix=/usr -q && ./b2 --prefix=/usr -q install && \
  rm -rf /tmp/build

# install bison 2.5.1
RUN mkdir -p /tmp/build/ && \
 cd /tmp/build && curl -L "ftp://ftp.gnu.org/gnu/bison/bison-2.5.1.tar.gz" -o bison-2.5.1.tar.gz && \
 tar -xzf bison-2.5.1.tar.gz && cd bison-2.5.1 && \
 ./configure --prefix=/usr && make && make install && \
 rm -rf /tmp/build

# install thrift 0.9.1
RUN mkdir -p /tmp/build && \
 cd /tmp/build && curl -L "https://archive.apache.org/dist/thrift/0.9.1/thrift-0.9.1.tar.gz" -o thrift-0.9.1.tar.gz && \
 tar -xf thrift-0.9.1.tar.gz && cd thrift-0.9.1 && \
 ./configure --prefix=/usr --without-tests && \
 make && make install && \
 rm -rf /tmp/build

# install protobuf 2.5.0
RUN mkdir -p /tmp/build/ && \
 cd /tmp/build && curl -L "https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2" -o protobuf-2.5.0.tar.bz2 && \
 tar -xjf protobuf-2.5.0.tar.bz2 && cd protobuf-2.5.0 && \
 ./configure --prefix=/usr && make && make install && ldconfig && \
 rm -rf /tmp/build

# install python module 
RUN pip --retries=50 --timeout=300 install pycrypto

# create user gpadmin since HAWQ cannot run under root
RUN groupadd -g 1000 gpadmin && \
 useradd -u 1000 -g 1000 gpadmin && \
 echo "gpadmin  ALL=(ALL)       NOPASSWD: ALL" > /etc/sudoers.d/gpadmin

# sudo should not require tty
RUN sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers

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

