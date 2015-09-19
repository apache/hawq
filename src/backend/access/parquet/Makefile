#
# Makefile for the access methods module
#
# $PostgreSQL: pgsql/src/backend/access/Makefile,v 1.14 2008/02/19 10:30:06 petere Exp $
#

subdir = src/backend/access/parquet
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

CXXFLAGS += -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H

OBJS = parquetam.o parquetsegfiles.o

SUBDIRS	    = metadatautil_c++

include $(top_srcdir)/src/backend/common.mk
