#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/appendonly
#
#-------------------------------------------------------------------------

subdir = src/backend/access/appendonly
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = appendonlyam.o aosegfiles.o aomd.o appendonlywriter.o appendonlytid.o appendonlyblockdirectory.o

include $(top_srcdir)/src/backend/common.mk

