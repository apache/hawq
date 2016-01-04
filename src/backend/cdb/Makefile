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
#    Makefile for cdb (GPDB specific routines)
#
#-------------------------------------------------------------------------


subdir = src/backend/cdb
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

SUBDIRS := motion


OBJS = cdbanalyze.o \
       cdbappendonlystorage.o cdbappendonlystorageformat.o \
       cdbappendonlystorageread.o cdbappendonlystoragewrite.o \
	   cdbbackup.o cdbbufferedappend.o cdbbufferedread.o \
	   cdbcat.o cdbcellbuf.o cdbchunkpool.o cdbconn.o cdbcopy.o \
	   cdbdatabaseinfo.o cdbdirectopen.o \
	   cdbdisp.o cdbdispatchresult.o \
	   cdbdistributedxid.o cdbdistributedxacts.o \
	   cdbdoublylinked.o \
	   cdbexplain.o \
	   cdbfilerep.o \
	   cdbfts.o \
	   cdbglobalsequence.o \
	   cdbgang.o cdbgroup.o \
	   cdbhash.o cdbheap.o \
	   cdblink.o cdbllize.o cdblogsync.o \
	   cdbmaxdistributedxid.o \
	   cdbmirroredbufferpool.o \
	   cdbmirroredfilesysobj.o cdbmirroredflatfile.o \
	   cdbmirroredappendonly.o \
	   cdbmutate.o \
	   cdboidsync.o \
	   cdbparquetstorageread.o cdbparquetstoragewrite.o cdbparquetrleencoder.o \
	   cdbparquetbytepacker.o cdbparquetbitstreamutil.o cdbparquetrowgroup.o \
	   cdbparquetcolumn.o cdbparquetfooterprocessor.o cdbparquetfooterbuffer.o	\
	   cdbparquetfooterserializer.o cdbparquetfooterserializer_protocol.o \
	   cdbpartindex.o \
	   cdbpartition.o \
	   cdbpath.o cdbpathlocus.o cdbpathtoplan.o \
	   cdbpersistentbuild.o cdbpersistentfilespace.o \
	   cdbpersistentrelfile.o cdbpersistentrelation.o \
	   cdbpersistentdatabase.o cdbpersistenttablespace.o \
	   cdbpersistentfilesysobj.o cdbpersistentrecovery.o cdbpersistentstore.o \
	   cdbmetadatacache.o cdbmetadatacache_process.o cdbmetadatacache_test.o\
	   cdbtmpdir.o \
	   cdbpgdatabase.o \
	   cdbplan.o cdbpullup.o \
	   cdbrelsize.o cdbresynchronizechangetracking.o \
	   cdbshareddoublylinked.o cdbsharedoidsearch.o \
	   cdbsetop.o cdbsreh.o cdbsrlz.o cdbsubplan.o cdbsubselect.o \
	   cdbtargeteddispatch.o cdbthreadwork.o \
	   cdbtimer.o \
	   cdbutil.o \
	   cdbvars.o cdbvarblock.o \
	   cdbinmemheapam.o \
	   cdbdispatchedtablespaceinfo.o \
	   cdbquerycontextdispatching.o \
	   cdbsharedstorageop.o \
	   cdbfilesystemcredential.o \
	   cdbfilesplit.o \
	   cdbdatalocality.o \
	   dispatcher.o \
	   dispatcher_mgt.o \
	   workermgr.o \
	   executormgr.o \
	   poolmgr.o \
	   partitionselection.o

ifeq ($(PORTNAME),cygwin)
.LIBPATTERNS := $(filter-out %.so,$(.LIBPATTERNS))
endif



ALLOBJS = $(OBJS) $(SUBDIROBJS)

include $(top_srcdir)/src/backend/common.mk
