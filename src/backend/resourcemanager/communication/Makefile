#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/resourcemanager/communication
#
#-------------------------------------------------------------------------

subdir = src/backend/resourcemanager/communication
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I ../include $(CPPFLAGS)

OBJS = rmcomm_AsyncComm.o \
       rmcomm_MessageServer.o \
       rmcomm_Message.o \
       rmcomm_Connect.o \
       rmcomm_MessageHandler.o \
       rmcomm_QD2RM.o \
       rmcomm_RM2RMSEG.o \
       rmcomm_RMSEG2RM.o \
       rmcomm_QE2RMSEG.o \
       rmcomm_SyncComm.o
      
include $(top_srcdir)/src/backend/common.mk

