
subdir = src/backend/catalog/core
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = catcore.o catcoretable.o

include $(top_srcdir)/src/backend/common.mk

catcoretable.c: catcoregen.py catheader.py $(catdump_json)
	PYTHONPATH=$(python_libdir) $(PYTHON) $(srcdir)/catcoregen.py $@ $(catdump_json)

