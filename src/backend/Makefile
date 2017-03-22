#-------------------------------------------------------------------------
#
# Makefile for the postgres backend
#
# Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL: pgsql/src/backend/Makefile,v 1.120 2006/10/08 17:15:33 tgl Exp $
#
#-------------------------------------------------------------------------

PGFILEDESC = "PostgreSQL Server"
subdir = src/backend
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = access bootstrap catalog parser commands executor foreign \
	lib libpq gp_libpq_fe main nodes optimizer port postmaster regex \
	rewrite storage tcop utils resourcemanager $(top_builddir)/src/timezone cdb

include $(srcdir)/common.mk

# As of 9/2009:
# * The probes.o file is necessary for dtrace support on Solaris.
# * OS X's dtrace doesn't use it and doesn't even recognize the -G option.
# * Systemtap's dtrace will take -G, but it produces a useless empty file.
# So, build probes.o only on Solaris.
# This will likely need adjustment as other platforms add dtrace support.
ifeq ($(PORTNAME), solaris)
ifeq ($(enable_dtrace), yes)
LOCALOBJS += utils/probes.o
endif
endif

OBJS = $(SUBDIROBJS) $(LOCALOBJS) $(top_builddir)/src/port/libpgport_srv.a

ifeq ($(PORTNAME), darwin)
LDOPTS = -Z
endif

ifeq ($(enable_connectemc), yes)
CONNECTEMCLIB += -lemcconnect
endif

# We put libpgport into OBJS, so remove it from LIBS; also add libldap
LIBS := $(filter-out -lpgport, $(LIBS)) $(LDAP_LIBS_BE) $(CONNECTEMCLIB)

# The backend doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))
LIBS := -lprotobuf -lboost_system -lboost_date_time -lstdc++ -lhdfs3 -lgsasl -lxml2 $(LIBS)

ORCA_BLD_PATH=$(abs_top_builddir)/depends/thirdparty

# adding orca libraries
ifeq ($(enable_orca),yes)
LIBGPOS_DIR = $(ORCA_BLD_PATH)/gpos/build/install/$(prefix)/lib
LIBXERCES_DIR = $(ORCA_BLD_PATH)/gp-xerces/build/install/$(prefix)/lib
LIBNAUCRATES_DIR = $(ORCA_BLD_PATH)/gporca/build/install/$(prefix)/lib
LIBGPDBCOST_DIR = $(ORCA_BLD_PATH)/gporca/build/install/$(prefix)/lib
LIBGPOPT_DIR = $(ORCA_BLD_PATH)/gporca/build/install/$(prefix)/lib

LIBS := $(LIBS) -L$(LIBGPOS_DIR) -lgpos
LIBS := $(LIBS) -L$(LIBXERCES_DIR) -lxerces-c
LIBS := $(LIBS) -L$(LIBNAUCRATES_DIR) -lnaucrates
LIBS := $(LIBS) -L$(LIBGPDBCOST_DIR) -lgpdbcost
LIBS := $(LIBS) -L$(LIBGPOPT_DIR) -lgpopt
LIBS := $(LIBS) -L$(abs_top_builddir)/$(subdir)/gpopt -ldxltranslators
endif

# HAWQ uses threads in the backend
LIBS := $(LIBS) -lpthread

THRIFTLIB = -lthrift
LIBS := $(LIBS) $(THRIFTLIB)

SNAPPYLIB = -lsnappy
LIBS := $(LIBS) $(SNAPPYLIB)
##########################################################################

ifeq ($(enable_orca),yes)

ifeq ($(PORTNAME), darwin)
XERCES_LIB_FILE=libxerces-c.dylib
GPOS_LIB_FILE=libgpos.dylib
else
XERCES_LIB_FILE=libxerces-c.so
GPOS_LIB_FILE=libgpos.so
endif

ORCA_SRC_PATH=$(abs_top_srcdir)/depends/thirdparty

GPORCA_SRC_PATH := $(ORCA_SRC_PATH)/gporca
GPOS_SRC_PATH := $(ORCA_SRC_PATH)/gpos
GP_XERCES_SRC_PATH := $(ORCA_SRC_PATH)/gp-xerces

submake-gporca: submake-gpos submake-gp-xerces $(ORCA_BLD_PATH)/gporca_prepare_timestamp
	if [ ! -f $(ORCA_BLD_PATH)/gporca_build_timestamp ]; then \
		rm -rf $(ORCA_BLD_PATH)/gporca/build; mkdir -p $(ORCA_BLD_PATH)/gporca/build; cd $(ORCA_BLD_PATH)/gporca/build; mkdir -p install; \
		cmake $(GPORCA_SRC_PATH) -DCMAKE_INSTALL_PREFIX=$(prefix) \
			-DXERCES_INCLUDE_DIR=$(ORCA_BLD_PATH)/gp-xerces/build/install/$(prefix)/include \
			-DXERCES_LIBRARY=$(ORCA_BLD_PATH)/gp-xerces/build/install/$(prefix)/lib/$(XERCES_LIB_FILE) \
			-DGPOS_INCLUDE_DIR=$(ORCA_BLD_PATH)/gpos/build/install/$(prefix)/include \
			-DGPOS_LIBRARY=$(ORCA_BLD_PATH)/gpos/build/install/$(prefix)/lib/$(GPOS_LIB_FILE) && \
		$(MAKE) DESTDIR=$(ORCA_BLD_PATH)/gporca/build/install install && \
		touch $(ORCA_BLD_PATH)/gporca_build_timestamp; \
	fi

submake-gpos: $(ORCA_BLD_PATH)/gpos_prepare_timestamp
	if [ ! -f $(ORCA_BLD_PATH)/gpos_build_timestamp ]; then \
		rm -rf $(ORCA_BLD_PATH)/gpos/build; mkdir -p $(ORCA_BLD_PATH)/gpos/build; cd $(ORCA_BLD_PATH)/gpos/build; mkdir -p install; \
		cmake $(GPOS_SRC_PATH) -DCMAKE_INSTALL_PREFIX=$(prefix) && \
		$(MAKE) DESTDIR=$(ORCA_BLD_PATH)/gpos/build/install install && \
		touch $(ORCA_BLD_PATH)/gpos_build_timestamp; \
	fi

submake-gp-xerces: $(ORCA_BLD_PATH)/gp-xerces_prepare_timestamp
	if [ ! -f $(ORCA_BLD_PATH)/gp-xerces_build_timestamp ]; then \
		rm -rf $(ORCA_BLD_PATH)/gp-xerces/build; mkdir -p $(ORCA_BLD_PATH)/gp-xerces/build; cd $(ORCA_BLD_PATH)/gp-xerces/build; mkdir -p install; \
		$(GP_XERCES_SRC_PATH)/configure --prefix=$(prefix); \
		$(MAKE) DESTDIR=$(ORCA_BLD_PATH)/gp-xerces/build/install install && \
		touch $(ORCA_BLD_PATH)/gp-xerces_build_timestamp; \
	fi

gporca_git := $(shell cat $(ORCA_SRC_PATH)/gporca.commit | awk '{print $$1}')
gpos_git := $(shell cat $(ORCA_SRC_PATH)/gpos.commit | awk '{print $$1}')
gp_xerces_git := $(shell cat $(ORCA_SRC_PATH)/gp-xerces.commit | awk '{print $$1}')

gporca_branch := $(shell cat $(ORCA_SRC_PATH)/gporca.commit | awk '{print $$2}')
gpos_branch := $(shell cat $(ORCA_SRC_PATH)/gpos.commit | awk '{print $$2}')
gp_xerces_branch := $(shell cat $(ORCA_SRC_PATH)/gp-xerces.commit | awk '{print $$2}')

gporca_commit := $(shell cat $(ORCA_SRC_PATH)/gporca.commit | awk '{print $$3}')
gpos_commit := $(shell cat $(ORCA_SRC_PATH)/gpos.commit | awk '{print $$3}')
gp_xerces_commit := $(shell cat $(ORCA_SRC_PATH)/gp-xerces.commit | awk '{print $$3}')

$(ORCA_BLD_PATH)/gporca_prepare_timestamp: $(ORCA_SRC_PATH)/gporca.commit $(ORCA_BLD_PATH)/gpos_prepare_timestamp $(ORCA_BLD_PATH)/gp-xerces_prepare_timestamp
	[ "x$(gporca_commit)" != "x" ] || exit 1; \
	cd $(ORCA_SRC_PATH); mkdir -p gporca; cd gporca; \
	[ ! -d .git ] && git clone $(gporca_git) . && git checkout $(gporca_branch); \
	git reset --hard $(gporca_commit) || (echo Now fetch from upstream and try again; git fetch origin $(gporca_branch); git reset --hard $(gporca_commit)) || exit 2; \
	touch $(ORCA_BLD_PATH)/gporca_prepare_timestamp; \
	rm -f $(ORCA_BLD_PATH)/gporca_build_timestamp

$(ORCA_BLD_PATH)/gpos_prepare_timestamp: $(ORCA_SRC_PATH)/gpos.commit
	[ "x$(gpos_commit)" != "x" ] || exit 1; \
	cd $(ORCA_SRC_PATH); mkdir -p gpos; cd gpos; \
	[ ! -d .git ] && git clone $(gpos_git) . && git checkout $(gpos_branch); \
	git reset --hard $(gpos_commit) || (echo Now fetch from upstream and try again; git fetch origin $(gpos_branch); git reset --hard $(gpos_commit)) || exit 2; \
	touch $(ORCA_BLD_PATH)/gpos_prepare_timestamp; \
	rm -f $(ORCA_BLD_PATH)/gpos_build_timestamp

$(ORCA_BLD_PATH)/gp-xerces_prepare_timestamp: $(ORCA_SRC_PATH)/gp-xerces.commit
	[ "x$(gp_xerces_commit)" != "x" ] || exit 1; \
	cd $(ORCA_SRC_PATH); mkdir -p gp-xerces; cd gp-xerces; \
	[ ! -d .git ] && git clone $(gp_xerces_git) . && git checkout $(gp_xerces_branch); \
	git reset --hard $(gp_xerces_commit) || (echo Now fetch from upstream and try again; git fetch origin $(gp_xerces_branch); git reset --hard $(gp_xerces_commit)) || exit 2; \
	touch $(ORCA_BLD_PATH)/gp-xerces_prepare_timestamp; \
	rm -f $(ORCA_BLD_PATH)/gp-xerces_build_timestamp

submake-libdxltranslators: submake-gporca
	$(MAKE) -C gpopt all

.PHONY: submake-gporca submake-gpos submake-gp-xerces

else
submake-libdxltranslators:
endif # enable_orca

all: submake-libpgport postgres $(POSTGRES_IMP)

ifneq ($(PORTNAME), cygwin)
ifneq ($(PORTNAME), win32)
ifneq ($(PORTNAME), aix)

postgres: $(OBJS) submake-libdxltranslators
	$(CXX) $(CFLAGS) $(LDFLAGS) $(export_dynamic) $(call expand_subsys, $(filter-out submake-libdxltranslators, $^)) $(LIBS) -o $@

endif
endif
endif

ifeq ($(PORTNAME), cygwin)

postgres: $(OBJS) postgres.def libpostgres.a
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def postgres.def
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def postgres.def
	$(CXX) $(CFLAGS) $(LDFLAGS) -Wl,--stack,$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(LIBS)
	rm -f $@.exp $@.base

postgres.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libpostgres.a: postgres.def
	$(DLLTOOL) --dllname postgres.exe --def postgres.def --output-lib $@

endif # cygwin

ifeq ($(PORTNAME), win32)
LIBS += -lsecur32

postgres: $(OBJS) postgres.def libpostgres.a $(WIN32RES)
	$(DLLTOOL) --dllname $@$(X) --output-exp $@.exp --def postgres.def
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@$(X) -Wl,--base-file,$@.base $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	$(DLLTOOL) --dllname $@$(X) --base-file $@.base --output-exp $@.exp --def postgres.def
	$(CXX) $(CFLAGS) $(LDFLAGS) -Wl,--stack=$(WIN32_STACK_RLIMIT) -o $@$(X) $@.exp $(call expand_subsys,$(OBJS)) $(WIN32RES) $(LIBS)
	rm -f $@.exp $@.base

postgres.def: $(OBJS)
	$(DLLTOOL) --export-all --output-def $@ $(call expand_subsys,$^)

libpostgres.a: postgres.def
	$(DLLTOOL) --dllname postgres.exe --def postgres.def --output-lib $@

endif # win32

ifeq ($(PORTNAME), aix)

postgres: $(POSTGRES_IMP)
	$(CXX) $(CFLAGS) $(LDFLAGS) $(call expand_subsys,$(OBJS)) -Wl,-bE:$(top_builddir)/src/backend/$(POSTGRES_IMP) $(LIBS) -o $@

$(POSTGRES_IMP): $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(call expand_subsys,$^)
ifeq ($(host_os), aix3.2.5)
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
ifneq (,$(findstring aix4.1, $(host_os)))
	$(MKLDEXPORT) SUBSYS.o $(bindir)/postgres > $@
else
	$(MKLDEXPORT) SUBSYS.o . > $@
endif
endif
	@rm -f SUBSYS.o

endif # aix

# Update the commonly used headers before building the subdirectories
$(SUBDIRS:%=%-recursive): $(top_builddir)/src/include/parser/gram.h $(top_builddir)/src/include/utils/fmgroids.h $(top_builddir)/src/include/utils/probes.h


# The postgres.o target is needed by the rule in Makefile.global that
# creates the exports file when MAKE_EXPORTS = true.
postgres.o: $(OBJS)
	$(CXX) $(LDREL) $(LDFLAGS) $(call expand_subsys,$^) $(LIBS) -o $@


# The following targets are specified in make commands that appear in
# the make files in our subdirectories. Note that it's important we
# match the dependencies shown in the subdirectory makefiles!

$(srcdir)/parser/gram.h: parser/gram.y
	$(MAKE) -C parser gram.h

utils/fmgroids.h: utils/Gen_fmgrtab.sh $(top_srcdir)/src/include/catalog/pg_proc.h
	$(MAKE) -C utils fmgroids.h

utils/probes.h: utils/probes.d
	$(MAKE) -C utils probes.h

# Make symlinks for these headers in the include directory. That way
# we can cut down on the -I options. Also, a symlink is automatically
# up to date when we update the base file.

$(top_builddir)/src/include/parser/gram.h: $(srcdir)/parser/gram.h
	prereqdir=`cd $(dir $<) >/dev/null && pwd` && \
	  cd $(dir $@) && rm -f $(notdir $@) && \
	  $(LN_S) "$$prereqdir/$(notdir $<)" .

$(top_builddir)/src/include/utils/fmgroids.h: utils/fmgroids.h
	cd $(dir $@) && rm -f $(notdir $@) && \
	    $(LN_S) ../../../$(subdir)/utils/fmgroids.h .

$(top_builddir)/src/include/utils/probes.h: utils/probes.h
	cd $(dir $@) && rm -f $(notdir $@) && \
	    $(LN_S) ../../../$(subdir)/utils/probes.h .


utils/probes.o: utils/probes.d $(SUBDIROBJS)
	$(DTRACE) $(DTRACEFLAGS) -C -G -s $(call expand_subsys,$^) -o $@


##########################################################################

distprep:
	$(MAKE) -C parser	gram.c gram.h scan.c
	$(MAKE) -C bootstrap	bootparse.c bootscanner.c
	$(MAKE) -C utils/misc	guc-file.c


##########################################################################

install: all installdirs install-bin
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libpostgres.a '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(INSTALL_DATA) libpostgres.a '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
	$(MAKE) -C catalog install-data
	$(INSTALL_DATA) $(srcdir)/libpq/pg_hba.conf.sample '$(DESTDIR)$(datadir)/pg_hba.conf.sample'
	$(INSTALL_DATA) $(srcdir)/libpq/pg_ident.conf.sample '$(DESTDIR)$(datadir)/pg_ident.conf.sample'
	$(INSTALL_DATA) $(srcdir)/utils/misc/postgresql.conf.sample '$(DESTDIR)$(datadir)/postgresql.conf.sample'
	$(INSTALL_DATA) $(srcdir)/access/transam/recovery.conf.sample '$(DESTDIR)$(datadir)/recovery.conf.sample'
	${INSTALL_SCRIPT} -d $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/hawq-site.xml $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/hdfs-client.xml $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/yarn-client.xml $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/slaves $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/template-hawq-site.xml $(DESTDIR)${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/gpcheck.cnf $(DESTDIR)${sysconfdir}
ifeq ($(enable_orca), yes)
	$(MAKE) -C $(ORCA_BLD_PATH)/gporca/build $@
	$(MAKE) -C $(ORCA_BLD_PATH)/gpos/build $@
	$(MAKE) -C $(ORCA_BLD_PATH)/gp-xerces/build $@
	$(MAKE) -C gpopt $@ INSTLOC=$(DESTDIR)$(libdir)
endif

install-bin: postgres $(POSTGRES_IMP) installdirs
	$(INSTALL_PROGRAM) postgres$(X) '$(DESTDIR)$(bindir)/postgres$(X)'
ifneq ($(PORTNAME), win32)
	@rm -f $(DESTDIR)$(bindir)/postmaster$(X) $(DESTDIR)$(bindir)/gpsyncmaster$(X) $(DESTDIR)$(bindir)/resmanager
	ln -s postgres$(X) $(DESTDIR)$(bindir)/postmaster$(X)
	ln -s postgres$(X) $(DESTDIR)$(bindir)/gpsyncmaster$(X)
	ln -s postgres$(X) $(DESTDIR)$(bindir)/resmanager$(X)
else
	$(INSTALL_PROGRAM) postgres$(X) '$(DESTDIR)$(bindir)/postmaster$(X)'
endif
ifeq ($(MAKE_EXPORTS), true)
	$(INSTALL_DATA) $(POSTGRES_IMP) '$(DESTDIR)$(pkglibdir)/$(POSTGRES_IMP)'
endif

.PHONY: install-bin

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)' '$(DESTDIR)$(datadir)'
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	$(MKDIR_P) '$(DESTDIR)$(libdir)'
endif
endif
ifeq ($(MAKE_EXPORTS), true)
	$(MKDIR_P) '$(DESTDIR)$(pkglibdir)'
endif


##########################################################################

uninstall:
	rm -f $(DESTDIR)$(bindir)/postgres$(X) $(DESTDIR)$(bindir)/postmaster $(DESTDIR)$(bindir)/cdbsyncmaster
ifeq ($(MAKE_EXPORTS), true)
	rm -f '$(DESTDIR)$(pkglibdir)/$(POSTGRES_IMP)'
endif
ifeq ($(PORTNAME), cygwin)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
ifeq ($(PORTNAME), win32)
ifeq ($(MAKE_DLL), true)
	rm -f '$(DESTDIR)$(libdir)/libpostgres.a'
endif
endif
	$(MAKE) -C catalog uninstall-data
	rm -f '$(DESTDIR)$(datadir)/pg_hba.conf.sample' \
	      '$(DESTDIR)$(datadir)/pg_ident.conf.sample' \
              '$(DESTDIR)$(datadir)/postgresql.conf.sample' \
	      '$(DESTDIR)$(datadir)/recovery.conf.sample'


##########################################################################

clean:
	rm -f $(LOCALOBJS) postgres$(X) $(POSTGRES_IMP) \
		$(top_srcdir)/src/include/parser/gram.h \
		$(top_srcdir)/src/include/parser/parse.h \
		$(top_builddir)/src/include/utils/fmgroids.h
ifeq ($(PORTNAME), cygwin)
	rm -f postgres.dll postgres.def libpostgres.a
endif
ifeq ($(PORTNAME), win32)
	rm -f postgres.dll postgres.def libpostgres.a $(WIN32RES)
endif
	find gpopt -name *.o | xargs rm -rf
	find gpopt -name *.so | xargs rm -rf
	find gpopt -name *.dylib | xargs rm -rf
	rm -rf $(ORCA_BLD_PATH)/gporca/build $(ORCA_BLD_PATH)/gporca_build_timestamp
	rm -rf $(ORCA_BLD_PATH)/gpos/build $(ORCA_BLD_PATH)/gpos_build_timestamp
	rm -rf $(ORCA_BLD_PATH)/gp-xerces/build $(ORCA_BLD_PATH)/gp-xerces_build_timestamp
	rm -f $(ORCA_BLD_PATH)/gporca_build_timestamp
	rm -f $(ORCA_BLD_PATH)/gpos_build_timestamp
	rm -f $(ORCA_BLD_PATH)/gp-xerces_build_timestamp

distclean: clean
	rm -f port/tas.s port/dynloader.c port/pg_sema.c port/pg_shmem.c

maintainer-clean: distclean
	rm -f $(srcdir)/bootstrap/bootparse.c \
	      $(srcdir)/bootstrap/bootscanner.c \
	      $(srcdir)/parser/gram.c \
	      $(srcdir)/parser/scan.c \
	      $(srcdir)/parser/gram.h \
	      $(srcdir)/parser/parse.h \
	      $(srcdir)/utils/misc/guc-file.c


##########################################################################
#
# Support for code development.
#
# Use target "quick" to build "postgres" when you know all the subsystems 
# are up to date.  It saves the time of doing all the submakes.
.PHONY: quick
quick: $(OBJS)
	$(CXX) $(CFLAGS) $(LDFLAGS) $(export_dynamic) $(call expand_subsys,$^) $(LIBS) -o postgres
