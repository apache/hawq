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

all install installdirs uninstall distprep: check-and-install-cwrapper
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

check-and-install-cwrapper:
ifeq ("$(wildcard $(DEPENDENCY_PATH)/include/cwrapper-installed)", "")
	rm -rf $(DEPENDENCY_PATH)/include/executor
	rm -rf $(DEPENDENCY_PATH)/include/scheduler
	rm -rf $(DEPENDENCY_PATH)/include/magma
	rm -rf $(DEPENDENCY_PATH)/include/dbcommon
	rm -rf $(DEPENDENCY_PATH)/include/univplan
	rm -rf $(DEPENDENCY_PATH)/include/storage
	mkdir -p $(DEPENDENCY_PATH)/include/executor/cwrapper
	mkdir -p $(DEPENDENCY_PATH)/include/scheduler/cwrapper
	mkdir -p $(DEPENDENCY_PATH)/include/magma/cwrapper
	mkdir -p $(DEPENDENCY_PATH)/include/dbcommon/function
	mkdir -p $(DEPENDENCY_PATH)/include/dbcommon/type
	mkdir -p $(DEPENDENCY_PATH)/include/dbcommon/utils
	mkdir -p $(DEPENDENCY_PATH)/include/univplan/cwrapper
	mkdir -p $(DEPENDENCY_PATH)/include/storage/cwrapper
	$(INSTALL_DATA) include/cwrapper/cached-result.h '$(DEPENDENCY_PATH)/include/executor/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/executor-c.h '$(DEPENDENCY_PATH)/include/executor/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/scheduler-c.h '$(DEPENDENCY_PATH)/include/scheduler/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/magma-client-c.h '$(DEPENDENCY_PATH)/include/magma/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/func-kind.cg.h '$(DEPENDENCY_PATH)/include/dbcommon/function/'
	$(INSTALL_DATA) include/cwrapper/type-kind.h '$(DEPENDENCY_PATH)/include/dbcommon/type/'
	$(INSTALL_DATA) include/cwrapper/instrument.h '$(DEPENDENCY_PATH)/include/dbcommon/utils/'
	$(INSTALL_DATA) include/cwrapper/univplan-c.h '$(DEPENDENCY_PATH)/include/univplan/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/hdfs-file-system-c.h '$(DEPENDENCY_PATH)/include/storage/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/hive-file-system-c.h '$(DEPENDENCY_PATH)/include/storage/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/orc-format-c.h '$(DEPENDENCY_PATH)/include/storage/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/text-format-c.h '$(DEPENDENCY_PATH)/include/storage/cwrapper/'
	$(INSTALL_DATA) include/cwrapper/magma-format-c.h '$(DEPENDENCY_PATH)/include/storage/cwrapper/'
	touch $(DEPENDENCY_PATH)/include/cwrapper-installed
endif

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
ifeq ($(with_oushudb_extension), yes)
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/magma_* $(DESTDIR)${bindir}
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/hawq_heartbeat* $(DESTDIR)${bindir}
	${INSTALL_PROGRAM} $(DEPENDENCY_PATH)/bin/hawq_magma_tool $(DESTDIR)${bindir}
	${INSTALL_DATA} $(DEPENDENCY_PATH)/include/magma/utils/template-magma-site.xml $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(DEPENDENCY_PATH)/include/magma/utils/magma-site.xml $(DESTDIR)${sysconfdir}
endif

installdirs-local:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall: uninstall-local

uninstall-local:
	rm -f $(addprefix '$(DESTDIR)$(pgxsdir)/$(subdir)'/, Makefile.global Makefile.port Makefile.shlib nls-global.mk)

clear-cwrapper:
	rm -rf $(DEPENDENCY_PATH)/include/executor
	rm -rf $(DEPENDENCY_PATH)/include/scheduler
	rm -rf $(DEPENDENCY_PATH)/include/magma
	rm -rf $(DEPENDENCY_PATH)/include/dbcommon
	rm -rf $(DEPENDENCY_PATH)/include/univplan
	rm -rf $(DEPENDENCY_PATH)/include/storage
	rm -f $(DEPENDENCY_PATH)/include/cwrapper-installed

clean: clear-cwrapper
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
	$(MAKE) -C test/feature $@
	$(MAKE) -C tools/fsync $@

distclean maintainer-clean: clear-cwrapper
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
	$(MAKE) -C test/feature clean
	$(MAKE) -C tools/fsync $@
	rm -f Makefile.port Makefile.global

.PHONY: install-local installdirs-local uninstall-local
