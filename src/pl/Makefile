#-------------------------------------------------------------------------
#
# Makefile for src/pl (procedural languages)
#
# Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL: pgsql/src/pl/Makefile,v 1.23 2005/05/14 17:55:20 tgl Exp $
#
#-------------------------------------------------------------------------

subdir = src/pl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

DIRS = plpgsql

ifeq ($(with_python), yes)
DIRS += plpython
endif

ifeq ($(with_tcl), yes)
DIRS += tcl
endif

ifeq ($(with_java), yes)
DIRS += pljava
endif

ifeq ($(with_perl), yes)
DIRS += plperl
endif

ifeq ($(with_r), yes)
DIRS += plr
endif

# fixme: depend on plr.patch also.
plr_prepare:
	if [ ! -e $(abs_top_builddir)/$(subdir)/plr_prepare_timestamp ]; then \
		cd $(abs_top_srcdir); \
		git submodule update --init $(subdir)/plr && \
		(cd $(abs_top_srcdir)/$(subdir)/plr; git apply ../plr.patch) && \
		touch $(abs_top_builddir)/$(subdir)/plr_prepare_timestamp; \
	fi

ABS_PLR_SRCDIR = $(abs_top_srcdir)/$(subdir)/plr

# rpm only so far.
plr_pkgbuild: plr_prepare
	if [ ! -f /etc/redhat-release ]; then echo "rpm build is not supported on the platform."; exit 1; fi
	rm -rf plr/build; mkdir plr/build;
	rpmbuild -bb plr.spec --define 'plr_srcdir $(ABS_PLR_SRCDIR)' --define '_topdir $(ABS_PLR_SRCDIR)/build' --define '__os_install_post \%{nil}'

ifeq ($(with_r), yes)
plr_prepare2: plr_prepare
else
plr_prepare2:
endif

.PHONY: plr_prepare plr_prepare2 plr_pkgbuild

all distprep install installdirs uninstall: plr_prepare2
	@for dir in $(DIRS); do $(MAKE) -C $$dir $@ || exit 1; done

clean distclean maintainer-clean:
	@for dir in $(DIRS); do $(MAKE) -C $$dir $@ || true; done

# We'd like check operations to run all the subtests before failing;
# also insert a sleep to ensure the previous test backend exited before
# we try to drop the regression database.
check installcheck:
	@CHECKERR=0; for dir in $(DIRS); do \
		sleep 1; \
		$(MAKE) -C $$dir $@ || CHECKERR=$$?; \
	done; \
	exit $$CHECKERR
