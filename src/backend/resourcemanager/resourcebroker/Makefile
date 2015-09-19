#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/resourcemanager/utils
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager/resourcebroker
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I ../include $(CPPFLAGS)
override CPPFLAGS := -I ../libyarn/include $(CPPFLAGS)

OBJS = resourcebroker_API.o \
       resourcebroker_NONE.o \
       resourcebroker_LIBYARN.o \
       resourcebroker_LIBYARN_proc.o

include $(top_srcdir)/src/backend/common.mk

