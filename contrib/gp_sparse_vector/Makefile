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
#Intel compiler
ifeq "$(CC)" "icc"
	USE_ICC = 1
endif

override CFLAGS+=-std=gnu99

MODULE_big = gp_svec
OBJS = sparse_vector.o operators.o SparseData.o
ifdef USE_PGXS
	PGXS := $(shell pg_config --pgxs)
	include $(PGXS)
else
	subdir = contrib/gp_sparse_vector
	top_builddir = ../..
	include $(top_builddir)/src/Makefile.global
	include $(top_srcdir)/contrib/contrib-global.mk
endif
ifdef USE_ICC
	override CFLAGS=-O3 -Werror -std=c99 -vec-report2 -vec-threshold0
endif
