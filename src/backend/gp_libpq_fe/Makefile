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
#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gp_libpq_fe subsystem (internal frontend half of libpq to talk from MasterSeg to SegDBs)
#
# IDENTIFICATION
#
#-------------------------------------------------------------------------

subdir = src/backend/gp_libpq_fe
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS :=  -DUNSAFE_STAT_OK -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/port

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS =  fe-exec.o pqexpbuffer.o \
    fe-auth.o   \
    fe-connect.o fe-misc.o fe-protocol3.o

ifeq ($(PORTNAME), win32)
OBJS += win32.o 
endif


include $(top_srcdir)/src/backend/common.mk