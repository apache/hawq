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
#    Makefile for src/backend/resourcemanager/communication
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager/communication
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I ../include $(CPPFLAGS)

OBJS = rmcomm_AsyncComm.o \
       rmcomm_MessageServer.o \
       rmcomm_Message.o \
       rmcomm_Connect.o \
       rmcomm_MessageHandler.o \
       rmcomm_QD2RM.o \
       rmcomm_RM2RMSEG.o \
       rmcomm_RMSEG2RM.o \
       rmcomm_QE2RMSEG.o \
       rmcomm_SyncComm.o
      
include $(top_srcdir)/src/backend/common.mk

