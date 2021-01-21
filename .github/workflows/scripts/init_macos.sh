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
sudo systemsetup -setremotelogin on
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0700 ~/.ssh
chmod 0600 ~/.ssh/authorized_keys

tee -a ~/.ssh/config <<EOF_ssh_config
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
EOF_ssh_config

ssh -v localhost whoami

# Configure system kernel state
sudo tee /etc/sysctl.conf << EOF_sysctl
kern.sysv.shmmax=2147483648
kern.sysv.shmmin=1
kern.sysv.shmmni=64
kern.sysv.shmseg=16
kern.sysv.shmall=524288
kern.maxfiles=65535
kern.maxfilesperproc=65536
kern.corefile=/cores/core.%N.%P
EOF_sysctl
</etc/sysctl.conf xargs sudo sysctl

# Add data folder
sudo install -o $USER -d /tmp/db_data/
