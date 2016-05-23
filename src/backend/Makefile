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

ifdef ADDON_DIR
include $(ADDON_DIR)/src/Makefile
endif

SUBDIRS = access bootstrap catalog parser commands executor foreign \
	lib libpq gp_libpq_fe main nodes optimizer port postmaster regex \
	rewrite storage tcop utils resourcemanager $(top_builddir)/src/timezone cdb \
	$(ADDON_SUBDIR)

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

# adding orca libraries
ifeq ($(enable_orca),yes)
LIBGPOS_DIR = $(abs_top_builddir)/$(subdir)/gpos/build/install/$(prefix)/lib
LIBXERCES_DIR = $(abs_top_builddir)/$(subdir)/gp-xerces/build/install/$(prefix)/lib
LIBNAUCRATES_DIR = $(abs_top_builddir)/$(subdir)/gporca/build/install/$(prefix)/lib
LIBGPDBCOST_DIR = $(abs_top_builddir)/$(subdir)/gporca/build/install/$(prefix)/lib
LIBGPOPT_DIR = $(abs_top_builddir)/$(subdir)/gporca/build/install/$(prefix)/lib

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

GPORCA_SRC_PATH := $(abs_top_srcdir)/$(subdir)/gporca
GPOS_SRC_PATH := $(abs_top_srcdir)/$(subdir)/gpos
GP_XERCES_SRC_PATH := $(abs_top_srcdir)/$(subdir)/gp-xerces

submake-gporca: submake-gpos submake-gp-xerces gporca_prepare
ifneq ($(prefix),)
	if [ ! -f gporca_build_timestamp ]; then \
		rm -rf gporca/build; mkdir -p gporca/build; cd gporca/build; \
		cmake $(GPORCA_SRC_PATH) -DCMAKE_INSTALL_PREFIX=$(prefix) \
			-DXERCES_INCLUDE_DIR=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install/$(prefix)/include \
			-DXERCES_LIBRARY=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install/$(prefix)/lib/$(XERCES_LIB_FILE) \
			-DGPOS_INCLUDE_DIR=$(abs_top_builddir)/$(subdir)/gpos/build/install/$(prefix)/include \
			-DGPOS_LIBRARY=$(abs_top_builddir)/$(subdir)/gpos/build/install/$(prefix)/lib/$(GPOS_LIB_FILE) && \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gporca/build/install install && \
		touch ../../gporca_build_timestamp; \
	fi
else
	if [ ! -f gporca_build_timestamp ]; then \
		rm -rf gporca/build; mkdir -p gporca/build; cd gporca/build; \
 		cmake $(ORCA_SRC_PATH) \
			-DXERCES_INCLUDE_DIR=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install/$(prefix)/include \
			-DXERCES_LIBRARY=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install/$(prefix)/lib/$(XERCES_LIB_FILE) \
			-DGPOS_INCLUDE_DIR=$(abs_top_builddir)/$(subdir)/gpos/build/install/$(prefix)/include \
			-DGPOS_LIBRARY=$(abs_top_builddir)/$(subdir)/gpos/build/install/$(prefix)/lib/$(GPOS_LIB_FILE) && \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gporca/build/install install && \
		touch ../../gporca_build_timestamp; \
	fi
endif

submake-gpos: gporca_prepare
ifneq ($(prefix),)
	if [ ! -f gpos_build_timestamp ]; then \
		rm -rf gpos/build; mkdir -p gpos/build; cd gpos/build; mkdir -p install; \
		cmake $(GPOS_SRC_PATH) -DCMAKE_INSTALL_PREFIX=$(prefix) && \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gpos/build/install install && \
		touch ../../gpos_build_timestamp; \
	fi
else
	if [ ! -f gpos_build_timestamp ]; then \
		rm -rf gpos/build; mkdir -p gpos/build; cd gpos/build; mkdir -p install; \
		cmake $(GPOS_SRC_PATH) && \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gpos/build/install install && \
		touch ../../gpos_build_timestamp; \
	fi
endif

submake-gp-xerces: submake-gpos gporca_prepare
ifneq ($(prefix),)
	if [ ! -f gp-xerces_build_timestamp ]; then \
		rm -rf gp-xerces/build; mkdir -p gp-xerces/build; cd gp-xerces/build; mkdir -p install; \
		$(GP_XERCES_SRC_PATH)/configure --prefix=$(prefix); \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install install && \
		touch ../../gp-xerces_build_timestamp; \
	fi
else
	if [ ! -f gp-xerces_build_timestamp ]; then \
		rm -rf gp-xerces/build; mkdir -p gp-xerces/build; cd gp-xerces/build; mkdir -p install; \
		$(GP_XERCES_SRC_PATH)/configure --prefix=$(prefix); \
		$(MAKE) DESTDIR=$(abs_top_builddir)/$(subdir)/gp-xerces/build/install install && \ 
		touch ../../gp-xerces_build_timestamp; \
	fi
endif

gporca_prepare:
	if [ ! -f $(abs_top_builddir)/$(subdir)/gporca_prepare_timestamp ]; then \
			cd $(abs_top_srcdir); \
			git submodule update --init $(subdir)/gporca && \
			git submodule update --init $(subdir)/gp-xerces && \
			git submodule update --init $(subdir)/gpos && \
			touch $(abs_top_builddir)/$(subdir)/gporca_prepare_timestamp; \
	fi

submake-libdxltranslators: submake-gporca
	$(MAKE) -C gpopt all

# No distclean for gpor & gpos by now.
distclean_gporca:
	rm -rf gporca/build gporca_*_timestamp
	rm -rf gpos/build gpos_*_timestamp
	rm -rf gp-xerces/build gp-xerces_*_timestamp

.PHONY: submake-gporca submake-gpos submake-gp-xerces distclean_gporca gporca_prepare

else
submake-libdxltranslators distclean_gporca:
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
	$(MAKE) -C bootstrap	bootparse.c bootstrap_tokens.h bootscanner.c
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
	${INSTALL_SCRIPT} -d ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/hawq-site.xml ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/hdfs-client.xml ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/yarn-client.xml ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/slaves ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/template-hawq-site.xml ${sysconfdir}
	${INSTALL_DATA} $(srcdir)/utils/misc/etc/gpcheck.cnf ${sysconfdir}
ifeq ($(enable_orca), yes)
	$(MAKE) -C gpopt $@ INSTLOC=$(DESTDIR)$(libdir)
	$(MAKE) -C gporca/build $@
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

distclean: clean distclean_gporca
	rm -f port/tas.s port/dynloader.c port/pg_sema.c port/pg_shmem.c

maintainer-clean: distclean
	rm -f $(srcdir)/bootstrap/bootparse.c \
	      $(srcdir)/bootstrap/bootscanner.c \
	      $(srcdir)/bootstrap/bootstrap_tokens.h \
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
