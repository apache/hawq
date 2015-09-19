#-------------------------------------------------------------------------
#
# Makefile for src/backend/libgppc test
#
# This directory is for simple regression test for one GPDB release, plus
# one-compile multi-backend test.  Set USE_PGXS=1 to build against
# installed GPDB with pg_config, otherwise it's built with source tree.
#
# Since we keep the source identical between different GPDB releases,
# this Makefile finds GP_VERSION_NUM and runs appropriate test cases only.
# We assume user libraries use PGXS, so this Makefile is a bit too verbose
# as a simple developer example.  Essentially, user libraries can follow
# normal PGXS convention except PG_CPPFLAGS and SHLIB_LINK should be set
# pointing to gppc headers and library.  Or, they can even use only pg_config
# without PGXS.
#-------------------------------------------------------------------------

MODULE_big = gppc_test
OBJS = gppc_test.o
REGRESS = gppc_basic

ifdef USE_PGXS

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK = -L$(shell $(PG_CONFIG) --libdir) -lgppc
gppc_config_path = $(shell $(PG_CONFIG) --includedir)/gppc_config.h
include $(PGXS)

else # !USE_PGXS

subdir = src/backend/libgppc/test
top_builddir = ../../../..

PG_CPPFLAGS = -I../lib
SHLIB_LINK = -L../lib -lgppc
gppc_config_path = ../lib/gppc_config.h
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

endif # USE_PGXS

# Version check. Some tests cannot run against newer backends.
# We intentionally leave possibility (capability) to run tests
# against different GPDB version from building time (say, build
# agains 4.1 and run test on 4.2 without recompile), so that
# we can test compatibility which is the main purpose of GPPC.
#
# For now, no additional tests are defined.
GP_VERSION_NUM := $(shell test -f $(gppc_config_path) && \
	(grep 'define  *GP_VERSION_NUM' $(gppc_config_path) \
		| sed 's/^[^0-9]*\([0-9]*\)/\1/') || echo 0)

ifeq ($(shell test $(GP_VERSION_NUM) -ge 40200 && echo yes),yes)
REGRESS += tablefunc
endif

installcheck: install
