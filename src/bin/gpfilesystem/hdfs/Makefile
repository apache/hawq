top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

MODULE_big = gpfshdfs
OBJS       = gpfshdfs.o


PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)
SHLIB_LINK = -lprotobuf -lboost_thread -lboost_system -lboost_date_time -lstdc++ -lhdfs3

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = src/bin/gpfilesystem/hdfs
include $(top_srcdir)/contrib/contrib-global.mk
endif

