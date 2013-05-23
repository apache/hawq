#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for optimizer/plan
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/optimizer/plan/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/optimizer/plan
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

BLD_TOP = $(top_builddir)/..
include $(top_builddir)/../releng/releng.mk

override CPPFLAGS := -I $(LIBGPOS)/include $(CPPFLAGS)
override CPPFLAGS := -I $(OPTIMIZER)/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I $(OPTIMIZER)/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I $(OPTIMIZER)/libgpoptudf/include $(CPPFLAGS)

OBJS = createplan.o \
	   initsplan.o \
	   planagg.o \
	   plangroupext.o \
	   planmain.o \
	   planner.o \
	   planshare.o \
	   planwindow.o \
	   setrefs.o \
	   subselect.o \
	   planpartition.o \
	   transform.o

include $(top_srcdir)/src/backend/common.mk
