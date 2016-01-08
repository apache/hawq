MODULE_big = gp_mdver_utils
OBJS       = gp_mdver_contents.o

DATA_built = gp_mdver_utils.sql
DATA = uninstall_gp_mdver_utils.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/gp_mdver
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
