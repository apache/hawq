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
subdir=src/backend/nodes
top_builddir=../../../..

TARGETS=list

COMMON_REAL_OBJS = \
	$(top_srcdir)/src/backend/access/hash/hashfunc.o \
	$(top_srcdir)/src/backend/bootstrap/bootparse.o \
	$(top_srcdir)/src/backend/catalog/caql/gram.o \
	$(top_srcdir)/src/backend/lib/stringinfo.o \
	$(top_srcdir)/src/backend/nodes/bitmapset.o \
	$(top_srcdir)/src/backend/nodes/equalfuncs.o \
	$(top_srcdir)/src/backend/nodes/copyfuncs.o \
	$(top_srcdir)/src/backend/nodes/value.o \
	$(top_srcdir)/src/backend/parser/gram.o \
	$(top_srcdir)/src/backend/parser/kwlookup.o \
	$(top_srcdir)/src/backend/parser/scansup.o \
	$(top_srcdir)/src/backend/regex/regcomp.o \
	$(top_srcdir)/src/backend/regex/regerror.o \
	$(top_srcdir)/src/backend/regex/regexec.o \
	$(top_srcdir)/src/backend/regex/regfree.o \
	$(top_srcdir)/src/backend/storage/page/itemptr.o \
	$(top_srcdir)/src/backend/utils/adt/datum.o \
	$(top_srcdir)/src/backend/utils/adt/like.o \
	$(top_srcdir)/src/backend/utils/error/elog.o \
	$(top_srcdir)/src/backend/utils/hash/dynahash.o \
	$(top_srcdir)/src/backend/utils/hash/hashfn.o \
	$(top_srcdir)/src/backend/utils/mb/mbutils.o \
	$(top_srcdir)/src/backend/utils/mb/wchar.o \
	$(top_srcdir)/src/backend/utils/misc/guc.o \
	$(top_srcdir)/src/backend/utils/init/globals.o \
	$(top_srcdir)/src/port/strlcpy.o \
	$(top_srcdir)/src/port/pgsleep.o \
	$(top_srcdir)/src/port/path.o \
	$(top_srcdir)/src/port/pgstrcasecmp.o \
	$(top_srcdir)/src/port/qsort.o \
	$(top_srcdir)/src/port/thread.o \
	$(top_srcdir)/src/timezone/localtime.o \
	$(top_srcdir)/src/timezone/strftime.o \
	$(top_srcdir)/src/timezone/pgtz.o
	
# Objects from backend, which don't need to be mocked but need to be linked.
list_REAL_OBJS=$(COMMON_REAL_OBJS) \


include ../../../Makefile.mock
