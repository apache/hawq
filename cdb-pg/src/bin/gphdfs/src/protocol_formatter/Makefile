top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

MODULE_big = gphdfs
OBJS       = gphdfsformatter.o gphdfsprotocol.o


PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = src/bin/gphdfs/src/protocol_formatter
include $(top_srcdir)/contrib/contrib-global.mk
endif

