#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/resourcemanager
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)
override CPPFLAGS := -I include $(CPPFLAGS)

OBJS = resourcepool.o requesthandler_ddl.o \
	   resqueuemanager.o resqueuedeadlock.o resqueuecommand.o \
       conntrack.o \
       hawqsite.o \
       requesthandler_RMSEG.o requesthandler.o \
       resourcemanager_RMSEG.o \
       resourcemanager.o

SUBDIRS = communication utils resourcebroker resourceenforcer

include $(top_srcdir)/src/backend/common.mk

