MODULE_big = magma
OBJS       = magma.o

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/magma
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CFLAGS += -lmagma-client -lstorage -lstorage-magma-format -lunivplan -ljson-c

override CPPFLAGS := $(CPPFLAGS) $(LIBASAN)
override CFLAGS := $(CFLAGS) $(LIBASAN)

endif
