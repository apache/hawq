#!/bin/ksh
#
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
#

disks=`format </dev/null | grep c.t.d | nawk '{print $2}'`

getspeed1()
{
   ptime dd if=/dev/rdsk/${1}s0 of=/dev/null bs=64k count=1024 2>&1 |
       nawk '$1 == "real" { printf("%.0f\n", 67.108864 / $2) }'
}

getspeed()
{
   for iter in 1 2 3
   do
       getspeed1 $1
   done | sort -n | tail -2 | head -1
}

for disk in $disks
do
   echo $disk `getspeed $disk` MB/sec
done

