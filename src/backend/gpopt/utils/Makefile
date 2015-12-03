#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/gpopt/utils/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt/utils
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

include $(top_builddir)/src/backend/gpopt/gpopt.mk

CFLAGS_DL = -fPIC

ifeq (Linux, $(UNAME))
CFLAGS_DL := $(CFLAGS_DL) -rdynamic
endif

override CPPFLAGS := $(CFLAGS_DL) $(CPPFLAGS)

OBJS = COptTasks.o CCatalogUtils.o CConstExprEvaluatorProxy.o nodeutils.o funcs.o

include $(top_srcdir)/src/backend/common.mk

	
