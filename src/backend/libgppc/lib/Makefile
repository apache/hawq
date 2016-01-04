# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------------------
#
# Makefile for src/backend/libgppc library
#
#
#-------------------------------------------------------------------------

# GPPC is ready to build against installed backend, so that we can try it
# against different backend releases without switching source tree.
# This means GPPC should build and work with older releases without
# source modified and we need to keep it identical between different
# backend release branches.
# We achieve to build with another backend release by PGXS. Everything
# GPPC needs (i.e. headers and makefiles) are installed along with pg_config.
ifdef USE_PGXS
ifndef PG_CONFIG
PG_CONFIG = pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
top_builddir := $(dir $(PGXS))../..
else
subdir = src/backend/libgppc/lib
top_builddir = ../../../..
endif

include $(top_builddir)/src/Makefile.global

# We require Doxygen to generate html/latex manual, and this is the part of
# dependency chain since gppkg should contain pdf manual. But we know these
# tools are not installed in the standard system, so check it and skip if
# not available.
HAS_DOXYGEN = $(shell which doxygen > /dev/null 2>&1 && echo yes)

# Doxygen generates latex directory which uses pdflatex to generate pdf manual.
# We suppress error if it's not available.
# For Mac OS, MacTex is the good distribution. For Linux/CentOS, install tetex
# and tetex-latex packages via yum for pdflatex.
HAS_PDFLATEX = $(shell which pdflatex > /dev/null 2>&1 && echo yes)

# shared library parameters
NAME = gppc
override CPPFLAGS := -I$(includedir_server) -I$(includedir_internal) $(CPPFLAGS)
OBJS = gppc.o
include ./Makefile.version


all: gppc_gp_version_num_check gppc_config.h latex/refman.pdf all-lib

# Fetch version number from pg_config.h, because
# GPPC should have its own config header so that
# user library can avoid to include any backend headers.
ifdef PGXS
pg_config_path := $(includedir_server)/pg_config.h
else
pg_config_path := $(top_srcdir)/src/include/pg_config.h
endif

GP_VERSION_NUM := $(shell grep 'define  *GP_VERSION_NUM' $(pg_config_path) \
		| sed 's/^[^0-9]*\([0-9]*\)/\1/')
GPPC_GP_VERSION_NUM := $(shell test -f gppc_config.h \
		&& (grep 'define  *GP_VERSION_NUM' gppc_config.h | sed 's/^[^0-9]*\([0-9]*\)/\1/'))

# Reset if we attempt to build against different version. This may happen
# when we build without PGXS after building with PGXS, vice versa.
gppc_gp_version_num_check:
ifneq ($(GP_VERSION_NUM),$(GPPC_GP_VERSION_NUM))
	rm -f gppc_config.h
endif
gppc_config.h: gppc_config.h.in
	cat $< | sed 's/$$GP_VERSION_NUM_IN/$(GP_VERSION_NUM)/' | \
		sed 's/$$GPPC_VERSION_NUM_IN/$(GPPC_VERSION_NUM)/' > $@

# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib

# Mac OS needs dynamic_lookup for undefined symbols when using -dynamiclib
ifeq ($(PORTNAME), darwin)
  override LINK.shared += -undefined dynamic_lookup
endif


# Depend on Makefile.global to force rebuild on re-run of configure.
gppc_config.h: $(top_builddir)/src/Makefile.global

# The target name is arbitrary, just make sure dependency works.
latex/Makefile: gppc_config.h
ifneq ($(HAS_DOXYGEN),yes)
	@echo "*** doc is not generated, for doxygen is not installed ***"
else
	doxygen > doxygen.log
endif

latex/refman.pdf: latex/Makefile
ifneq ($(HAS_PDFLATEX),yes)
	@echo "*** PDF manual s not generated, pdflatex is not installed ***"
else
	$(MAKE) -C latex > doxygen-pdf.log
endif

doc: latex/refman.pdf

clean-doc:
	rm -rf html latex
	rm -rf doxygen.log doxygen-pdf.log

# Top-level Makefile requires each package directory to be able to
# run intallcheck.  For GPPC, installcheck program is in ../test.
installcheck: install
	$(MAKE) -C ../test $@


install: all installdirs install-lib
	$(INSTALL_DATA) $(srcdir)/gppc.h '$(DESTDIR)$(includedir)'
	$(INSTALL_DATA) $(srcdir)/gppc_config.h '$(DESTDIR)$(includedir)'
# docdir is desiable, but it's not aware of prefix parameter, so we install
# pdf manual to datadir.
ifeq ($(HAS_PDFLATEX),yes)
	$(INSTALL_DATA) $(srcdir)/latex/refman.pdf '$(DESTDIR)$(datadir)/libgppc-refman.pdf'
endif

installdirs: installdirs-lib
	$(MKDIR_P) '$(DESTDIR)$(includedir)' '$(DESTDIR)$(includedir_internal)'

uninstall: uninstall-lib
	rm -f '$(DESTDIR)$(includedir)/gppc.h'
	rm -f '$(DESTDIR)$(includedir)/gppc_config.h'

clean distclean: clean-lib clean-doc
	rm -f $(OBJS)
	rm -f gppc_config.h

maintainer-clean: distclean
