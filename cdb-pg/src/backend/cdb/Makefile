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
	   cdbdistributedsnapshot.o \
	   cdbdistributedxid.o cdbdistributedxacts.o \
	   cdbdoublylinked.o \
	   cdbdtxcontextinfo.o \
	   cdbexplain.o \
	   cdbfilerep.o cdbfilerepservice.o cdbfilerepprimaryrecovery.o \
	   cdbfilerepprimary.o cdbfilerepmirror.o \
	   cdbfilerepprimaryack.o cdbfilerepmirrorack.o \
	   cdbfilerepconnclient.o cdbfilerepconnserver.o \
	   cdbfilerepresyncmanager.o cdbfilerepresyncworker.o \
	   cdbfilerepresetpeerprocess.o \
	   cdbfilerepverify.o \
	   cdbfts.o \
	   cdbglobalsequence.o \
	   cdbgang.o cdbgroup.o \
	   cdbhash.o cdbheap.o \
	   cdblink.o cdbllize.o cdblocaldistribxact.o cdblogsync.o \
	   cdbmaxdistributedxid.o \
	   cdbmirroredbufferpool.o \
	   cdbmirroredfilesysobj.o cdbmirroredflatfile.o \
	   cdbmirroredappendonly.o \
	   cdbmutate.o \
	   cdboidsync.o \
	   cdbparquetstorageread.o cdbparquetstoragewrite.o cdbparquetrleencoder.o \
	   cdbparquetbytepacker.o cdbparquetbitstreamutil.o cdbparquetrowgroup.o\
	   cdbparquetfooterprocessor.o cdbparquetcolumn.o\
	   cdbpartindex.o \
	   cdbpartition.o \
	   cdbpath.o cdbpathlocus.o cdbpathtoplan.o \
	   cdbpersistentbuild.o cdbpersistentfilespace.o \
	   cdbpersistentrelation.o cdbpersistentdatabase.o cdbpersistenttablespace.o \
	   cdbpersistentfilesysobj.o cdbpersistentrecovery.o cdbpersistentstore.o \
	   cdbpgdatabase.o \
	   cdbplan.o cdbpullup.o \
	   cdbrelsize.o cdbresynchronizechangetracking.o \
	   cdbshareddoublylinked.o cdbsharedoidsearch.o \
	   cdbsetop.o cdbsreh.o cdbsrlz.o cdbsubplan.o cdbsubselect.o \
	   cdbtargeteddispatch.o cdbthreadwork.o \
	   cdbtimer.o \
	   cdbtm.o cdbtmutils.o \
	   cdbutil.o \
	   cdbvars.o cdbvarblock.o \
	   cdbinmemheapam.o \
	   cdbdispatchedtablespaceinfo.o \
	   cdbquerycontextdispatching.o \
	   cdbsharedstorageop.o \
	   cdbfilesystemcredential.o

ifeq ($(PORTNAME),cygwin)
.LIBPATTERNS := $(filter-out %.so,$(.LIBPATTERNS))
endif



ALLOBJS = $(OBJS) $(SUBDIROBJS)

include $(top_srcdir)/src/backend/common.mk
