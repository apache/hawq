#!/bin/bash
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
if [ -z "$1" ]; then
	echo "master directory required"
	exit 1
fi
if [ -z "$2" ]; then
	echo "segment directory required"
	exit 1
fi
if [ -z "$3" ]; then
	echo "dbname required"
	exit 1
fi

MASTER_DIRECTORY=$1 
SEGMENT_DIRECTORY=$2
DBNAME=$3

echo "shared_preload_libraries = 'vexecutor' "  >> $MASTER_DIRECTORY/postgresql.conf
echo "shared_preload_libraries = 'vexecutor' "  >> $SEGMENT_DIRECTORY/postgresql.conf

hawq restart cluster -a

psql -d $DBNAME -f ./create_type.sql

exit 0
