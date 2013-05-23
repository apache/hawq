#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/gpopt/utils/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt/relcache
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

BLD_TOP = $(top_builddir)/..
include $(top_builddir)/../releng/releng.mk

include $(top_builddir)/src/backend/gpopt/gpopt.mk

CFLAGS_DL = -fPIC

ifeq (Linux, $(UNAME))
CFLAGS_DL := $(CFLAGS_DL) -rdynamic
endif

override CPPFLAGS := $(CFLAGS_DL) $(CPPFLAGS)

OBJS = COptTasks.o COptServer.o COptClient.o CCatalogUtils.o nodeutils.o funcs.o

include $(top_srcdir)/src/backend/common.mk

	