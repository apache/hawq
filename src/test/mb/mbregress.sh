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
# $PostgreSQL: pgsql/src/test/mb/mbregress.sh,v 1.10 2009/05/06 16:15:21 tgl Exp $

if echo '\c' | grep -s c >/dev/null 2>&1
then
	ECHO_N="echo -n"
	ECHO_C=""
else
	ECHO_N="echo"
	ECHO_C='\c'
fi

if [ ! -d results ];then
    mkdir results
fi

dropdb utf8
createdb -T template0 -l C -E UTF8 utf8

PSQL="psql -n -e -q"
tests="euc_jp sjis euc_kr euc_cn euc_tw big5 utf8 mule_internal"
unset PGCLIENTENCODING
for i in $tests
do
	$ECHO_N "${i} .. " $ECHO_C

	if [ $i = sjis ];then
		PGCLIENTENCODING=SJIS
		export PGCLIENTENCODING
		$PSQL euc_jp < sql/sjis.sql > results/sjis.out 2>&1
		unset PGCLIENTENCODING
        elif [ $i = big5 ];then
		PGCLIENTENCODING=BIG5
		export PGCLIENTENCODING
		$PSQL euc_tw < sql/big5.sql > results/big5.out 2>&1
		unset PGCLIENTENCODING
	else
		dropdb $i >/dev/null 2>&1
		createdb -T template0 -l C -E `echo $i | tr 'abcdefghijklmnopqrstuvwxyz' 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'` $i >/dev/null
		$PSQL $i < sql/${i}.sql > results/${i}.out 2>&1
	fi

	if [ -f expected/${i}-${SYSTEM}.out ]
	then
		EXPECTED="expected/${i}-${SYSTEM}.out"
	else
		EXPECTED="expected/${i}.out"
	fi
  
	if [ `diff ${EXPECTED} results/${i}.out | wc -l` -ne 0 ]
	then
		( diff -wC3 ${EXPECTED} results/${i}.out; \
		echo "";  \
		echo "----------------------"; \
		echo "" ) >> regression.diffs
		echo failed
	else
		echo ok
	fi
done
