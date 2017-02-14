#!/bin/sh
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
# $PostgreSQL: pgsql/src/test/bench/runwisc.sh,v 1.10 2007/08/19 01:41:25 adunstan Exp $

if [ ! -d $1 ]; then
        echo " you must specify a valid data directory " >&2
        exit
fi

if [ -d ./obj ]; then
	cd ./obj
fi

echo =============== vacuuming benchmark database... ================= >&2
echo "vacuum" | postgres -D"$1" bench > /dev/null

echo =============== running benchmark... ================= >&2
time postgres -D"$1" -texecutor -tplanner -c log_min_messages=log -c log_destination=stderr -c start_log_collector=off bench < bench.sql 2>&1
