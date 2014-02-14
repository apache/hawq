#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/parquet
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/access/parquet/Makefile,v 1.16 2008/02/19 10:30:06 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/access/parquet/metadatautil_c++
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

CXXFLAGS += -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H $(subst -Wmissing-prototypes,,$(subst -std=gnu99,,$(CFLAGS)))
#override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)


MetadataUtil.o: MetadataUtil.cpp thrift-util.h

OBJS = Metadatainterface.o MetadataUtil.o parquet_types.o
# testlib.o



#libTest.dylib: $(OBJS)
#	$(CXX) $(LDFLAGS) -dynamiclib -o "libTest.dylib" $(OBJS) $(LIBS)
	
#TARGET =	parquet

#$(TARGET):	libTest.dylib testlib.o
#	$(CC) -o $(TARGET) testlib.o $(LIBS) -L./ -lTest $(LDFLAGS)

include $(top_srcdir)/src/backend/common.mk
