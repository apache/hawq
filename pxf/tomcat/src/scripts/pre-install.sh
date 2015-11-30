#!/bin/sh
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

# Pre installation script

user=tomcat
group=tomcat

groupadd=/usr/sbin/groupadd
useradd=/usr/sbin/useradd
usermod=/usr/sbin/usermod

# Create system group tomcat if doesn't exist
getent group $group > /dev/null || $groupadd -r $group

# Create system user tomcat if doens't exist
getent passwd $user > /dev/null || $useradd --comment "tomcat server user" -M -r -g $group $user
