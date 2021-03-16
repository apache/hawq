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



# Setup passphraseless ssh
test -f ~/.ssh/id_rsa || ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod go-w ~
chmod 0700 ~/.ssh
chmod 0600 ~/.ssh/authorized_keys

tee -a ~/.ssh/config <<EOF_ssh_config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF_ssh_config
chmod 600 ~/.ssh/config

ssh -v localhost whoami

# Configure system kernel state
sudo tee /etc/sysctl.conf << EOF_sysctl
kernel.shmmax = 1000000000
kernel.shmmni = 4096
kernel.shmall = 4000000000
kernel.sem = 250 512000 100 2048
kernel.sysrq = 1
kernel.core_uses_pid = 1
kernel.msgmnb = 65536
kernel.msgmax = 65536
kernel.msgmni = 2048
net.ipv4.tcp_syncookies = 0
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.tcp_max_syn_backlog = 200000
net.ipv4.conf.all.arp_filter = 1
net.ipv4.ip_local_port_range = 10000 65535
net.core.netdev_max_backlog = 200000
net.netfilter.nf_conntrack_max = 524288
fs.nr_open = 3000000
kernel.threads-max = 798720
kernel.pid_max = 798720

net.core.rmem_max=2097152
net.core.wmem_max=2097152
net.core.somaxconn=4096
EOF_sysctl
sudo sysctl -p

# Add data folder
sudo install -o $USER -d /tmp/db_data/
