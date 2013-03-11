top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

MODULE_big = gpxf
OBJS       = gpdbwritableformatter.o gpbridgeapi.o gphdfilters.o gpxf.o 

CFLAGS += -lcurl

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = src/bin/gpfusion
include $(top_srcdir)/contrib/contrib-global.mk
endif

