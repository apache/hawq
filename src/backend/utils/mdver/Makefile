#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/mdver
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/utils/mdver/Makefile,v 1.15 2008/02/19 10:30:09 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/mdver
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = mdver_global_mdvsn.o mdver_dep_translator.o mdver_utils.o \
		mdver_global_handler.o mdver_local_mdvsn.o mdver_local_handler.o

include $(top_srcdir)/src/backend/common.mk
