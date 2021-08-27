#-------------------------------------------------------------------------
#
# Makefile for src
#
# Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL: pgsql/src/Makefile,v 1.40 2006/06/22 23:50:35 tgl Exp $
#
#-------------------------------------------------------------------------

subdir = src
top_builddir = ..

include Makefile.global

all install installdirs uninstall distprep:
	$(MAKE) -C port $@
	$(MAKE) -C timezone $@
	$(MAKE) -C backend $@
	$(MAKE) -C backend/utils/mb/conversion_procs $@
	$(MAKE) -C include $@
	$(MAKE) -C interfaces $@
	$(MAKE) -C bin $@
	$(MAKE) -C pl $@
	$(MAKE) -C makefiles $@
	$(MAKE) -C test/regress $@
	$(MAKE) -C tools/fsync $@

all: clear-duplicated-includes

clear-duplicated-includes:
	rm -rf $(top_builddir)/.github/workflows/scripts/download/dependency/package/include/magma

feature-test:
	$(MAKE) -C test feature-test

feature-test-clean:
	$(MAKE) -C test clean

install: install-local

install-local: installdirs-local
	$(INSTALL_DATA) Makefile.global '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.global'
	$(INSTALL_DATA) Makefile.port '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.port'
	$(INSTALL_DATA) $(srcdir)/Makefile.shlib '$(DESTDIR)$(pgxsdir)/$(subdir)/Makefile.shlib'
	$(INSTALL_DATA) $(srcdir)/nls-global.mk '$(DESTDIR)$(pgxsdir)/$(subdir)/nls-global.mk'

installdirs: installdirs-local

installdirs-local:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'
ifeq ($(with_oushudb_extension), yes)
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/magma_ctl '$(DESTDIR)${bindir}/magma_ctl'
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/magma_server '$(DESTDIR)${bindir}/magma_server'
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/hawq_heartbeat_monitor '$(DESTDIR)${bindir}/hawq_heartbeat_monitor'
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/hawq_heartbeat_sender '$(DESTDIR)${bindir}/hawq_heartbeat_sender'
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/hawq_magma_tool '$(DESTDIR)${bindir}/hawq_magma_tool'
	${INSTALL_DATA} $(DEPENDENCY_PATH)/include/magma/utils/template-magma-site.xml '$(DESTDIR)${sysconfdir}/template-magma-site.xml'
	${INSTALL_DATA} $(DEPENDENCY_PATH)/include/magma/utils/magma-site.xml '$(DESTDIR)${sysconfdir}/magma-site.xml'
endif

uninstall: uninstall-local

uninstall-local:
	rm -f $(addprefix '$(DESTDIR)$(pgxsdir)/$(subdir)'/, Makefile.global Makefile.port Makefile.shlib nls-global.mk)

clean:
	$(MAKE) -C port $@
	$(MAKE) -C timezone $@
	$(MAKE) -C backend $@
	$(MAKE) -C include $@
	$(MAKE) -C interfaces $@
	$(MAKE) -C bin $@
	$(MAKE) -C pl $@
	$(MAKE) -C makefiles $@
	$(MAKE) -C test $@
	$(MAKE) -C tutorial NO_PGXS=1 $@
	$(MAKE) -C test/thread $@
	$(MAKE) -C test/regress $@
	$(MAKE) -C tools/fsync $@

distclean maintainer-clean:
	$(MAKE) -C port $@
	$(MAKE) -C timezone $@
	$(MAKE) -C backend $@
	#$(MAKE) -C backend/snowball $@
	$(MAKE) -C include $@
	$(MAKE) -C interfaces $@
	$(MAKE) -C bin $@
	$(MAKE) -C pl $@
	$(MAKE) -C makefiles $@
	$(MAKE) -C test $@
	$(MAKE) -C tutorial NO_PGXS=1 $@
	$(MAKE) -C test/thread $@
	$(MAKE) -C test/regress $@
	$(MAKE) -C tools/fsync $@
	rm -f Makefile.port Makefile.global

.PHONY: install-local installdirs-local uninstall-local
