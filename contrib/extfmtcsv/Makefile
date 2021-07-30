MODULE_big = extfmtcsv
OBJS       = extfmtcsv.o

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/extfmtcsv
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

override CFLAGS += -ljson-c -luuid

endif
