#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/resourcemanager/utils
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager/utils
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I ../include $(CPPFLAGS)
#override CPPFLAGS := -I ../libyarn/include $(CPPFLAGS)
#override CPPFLAGS := -I ../protobuf/include $(CPPFLAGS)


OBJS = balancedbst.o \
	   hashtable.o \
	   linkedlist.o \
	   pair.o \
	   simplestring.o \
	   network_utils.o \
	   memutilities.o \
	   kvproperties.o \
	   filesystem_utils.o

include $(top_srcdir)/src/backend/common.mk

