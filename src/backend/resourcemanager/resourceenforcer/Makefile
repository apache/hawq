#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/resourcemanager/resourceenforcer
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager/resourceenforcer
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I ../include $(CPPFLAGS)
override CPPFLAGS := -I ../libyarn/include $(CPPFLAGS)

OBJS = resourceenforcer_queue.o resourceenforcer_list.o resourceenforcer_pair.o \
       resourceenforcer_simpstring.o resourceenforcer_hash.o \
       resourceenforcer.o

include $(top_srcdir)/src/backend/common.mk

