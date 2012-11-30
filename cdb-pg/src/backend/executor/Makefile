#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for executor
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/executor/Makefile,v 1.29 2008/12/28 18:53:55 tgl Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/executor
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)


OBJS = execAmi.o execGrouping.o execHHashagg.o execJunk.o execMain.o \
       execProcnode.o execQual.o execScan.o execTuples.o \
       execUtils.o execWorkfile.o \
       functions.o \
       instrument.o \
       nodeAppend.o nodeAppendOnlyscan.o nodeAgg.o nodeAOCSScan.o \
       nodeBitmapAnd.o nodeBitmapOr.o \
       nodeBitmapAppendOnlyscan.o nodeBitmapHeapscan.o nodeBitmapIndexscan.o \
       nodeExternalscan.o \
       nodeFunctionscan.o \
       nodeHash.o nodeHashjoin.o \
       nodeIndexscan.o \
       nodeLimit.o \
       nodeMaterial.o nodeMergejoin.o nodeMotion.o \
       nodeNestloop.o \
       nodeResult.o \
       nodeRepeat.o \
       nodeSeqscan.o nodeSetOp.o nodeShareInputScan.o nodeSort.o \
       nodeSubplan.o nodeSubqueryscan.o \
       nodeTableFunction.o nodeTidscan.o \
       nodeUnique.o \
       nodeValuesscan.o \
       nodeWindow.o \
       tstoreReceiver.o \
       spi.o

include $(top_srcdir)/src/backend/common.mk
