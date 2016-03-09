#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/gpopt/Makefile,v 1.15 2008/02/19 10:30:07 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

include $(top_builddir)/src/backend/gpopt/gpopt.mk

SUBDIRS = config translate relcache utils

OBJS = CGPOptimizer.o gpdbwrappers.o

OTHER_TASK = libdxltranslators.$(LDSFX) libgpoptudf.$(LDSFX)

ifeq (Darwin, $(UNAME))
	LDLIBFLAGS = -dynamiclib -flat_namespace -undefined dynamic_lookup -Wl
endif

ifeq (Linux, $(UNAME))
	LDLIBFLAGS = -shared -Wl,--version-script=library.ver
endif

ifeq (SunOS, $(UNAME))
	LDLIBFLAGS = -shared -ldl -lrt
endif

# define build flags for GP Optimizer UDFs library
ifeq (Darwin, $(UNAME))
	GPOPT_UDF_FLAGS := -dynamiclib -flat_namespace -undefined dynamic_lookup
else
	GPOPT_UDF_FLAGS := -shared
endif

include $(top_srcdir)/src/backend/common.mk

libdxltranslators.$(LDSFX): $(SUBDIROBJS) $(OBJS)
	$(CXX) $(CXXFLAGS) $(LDLIBFLAGS) \
	  -lgpos \
	  -lxerces-c \
	  -lnaucrates \
	  -lgpdbcost \
	  -lgpopt -lpthread -o $@ \
	  $(SUBDIROBJS) $(OBJS)

libgpoptudf.$(LDSFX): $(SUBDIROBJS)
	$(CXX) $(CXXFLAGS) $(GPOPT_UDF_FLAGS) \
	  -lxerces-c \
	  -lgpos \
	  -lnaucrates \
	  -lgpdbcost \
	  -lgpopt \
	  -o $@ ./utils/funcs.o

install:
	$(INSTALL_DATA) libdxltranslators.$(LDSFX) '$(DESTDIR)$(libdir)/libdxltranslators.$(LDSFX)'
	$(INSTALL_DATA) libgpoptudf.$(LDSFX) '$(DESTDIR)$(libdir)/libgpoptudf.$(LDSFX)'

.PHONY: install
