PROGRAM = gp_filedump
OBJS	= gp_filedump.o pg_crc.o crc32c.o numutils.o heaptuple.o

EXTRA_CLEAN = pg_crc.c crc32c.c
#PG_CPPFLAGS = -msse4.2

DOCS = README.gp_filedump

ifdef USE_PGXS
PGXS := $(shell gp_config --gpxs)
include $(PGXS)
else
subdir = contrib/gp_filedump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

pg_crc.c: $(top_srcdir)/src/backend/utils/hash/pg_crc.c
	rm -f $@ && $(LN_S) $< .
	
crc32c.c: $(top_srcdir)/src/backend/utils/hash/crc32c.c
	rm -f $@ && $(LN_S) $< .

assert.c: $(top_srcdir)/src/backend/utils/error/assert.c
	rm -f $@ && $(LN_S) $< .

numutils.c: $(top_srcdir)/src/backend/utils/adt/numutils.c
	rm -f $@ && $(LN_S) $< .

heaptuple.c: $(top_srcdir)/src/backend/access/common/heaptuple.c
	rm -f $@ && $(LN_S) $< .
