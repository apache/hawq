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

OBJS = Allocators.o CGPOptimizer.o gpdbwrappers.o

OTHER_TASK = libdxltranslators.$(LDSFX) libgpoptudf.$(LDSFX)

ifeq (Darwin, $(UNAME))
	LDLIBFLAGS = -dynamiclib -flat_namespace -undefined dynamic_lookup -Wl,-unexported_symbols_list -Wl,unexported_symbols_list.txt
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

LIBGPOS_DIR = $(ORCA_DEPENDS_DIR_INTER)/libgpos
LIBNAUCRATES_DIR = $(ORCA_DEPENDS_DIR_INTER)/libnaucrates
LIBGPDBCOST_DIR = $(ORCA_DEPENDS_DIR_INTER)/libgpdbcost
LIBGPOPT_DIR = $(ORCA_DEPENDS_DIR_INTER)/libgpopt

include $(top_srcdir)/src/backend/common.mk

libdxltranslators.$(LDSFX): $(SUBDIROBJS) $(OBJS)
	$(CXX) $(CXXFLAGS) $(LDLIBFLAGS) \
	  -L$(LIBGPOS_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpos \
	  -L$(ORCA_DEPENDS_DIR_INTER)/lib -lxerces-c \
	  -L$(LIBNAUCRATES_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lnaucrates \
	  -L$(LIBGPDBCOST_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpdbcost \
	  -L$(LIBGPOPT_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpopt -lpthread -o $@ \
	  $(SUBDIROBJS) $(OBJS)

libgpoptudf.$(LDSFX): $(SUBDIROBJS)
	$(CXX) $(CXXFLAGS) $(GPOPT_UDF_FLAGS) \
	  -L$(ORCA_DEPENDS_DIR_INTER)/lib -lxerces-c \
	  -L$(LIBGPOS_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpos \
	  -L$(LIBNAUCRATES_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lnaucrates \
	  -L$(LIBGPDBCOST_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpdbcost \
	  -L$(LIBGPOPT_DIR)/$(ORCA_DEPENDS_OBJDIR_DEFAULT) -lgpopt \
	  -o $@ ./utils/funcs.o

install:
	$(INSTALL_DATA) libdxltranslators.$(LDSFX) '$(DESTDIR)$(libdir)/libdxltranslators.$(LDSFX)'
	$(INSTALL_DATA) libgpoptudf.$(LDSFX) '$(DESTDIR)$(libdir)/libgpoptudf.$(LDSFX)'

.PHONY: install
