MODULE_big = vexecutor
OBJS    = vexecutor.o

PG_CXXFLAGS = -Wall -O0 -g -std=c++11
PG_LIBS = $(libpq_pgport) 

PG_CONFIG = pg_config

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/vexecutor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

SHLIB_LINK += $(filter -lz -lsnappy, $(LIBS))
