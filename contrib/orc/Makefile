MODULE_big = orc
OBJS       = orc.o

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/orc
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
override CFLAGS += -lstorage -ljson-c -luuid -I${top_builddir}/src/backend/utils \
				   -L${top_builddir}/depends/storage/build/install/usr/local/hawq/lib \
				   -I${top_builddir}/depends/storage/build/install/usr/local/hawq/include 
endif
