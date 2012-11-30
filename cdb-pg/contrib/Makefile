# $PostgreSQL: pgsql/contrib/Makefile,v 1.70 2006/09/09 04:07:51 tgl Exp $

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

WANTED_DIRS = \
		gp_filedump \
		xlogdump \
		pgbench \
		orafce \
		changetrackingdump \
		formatter \
		formatter_fixedwidth \
		extprotocol \
		gp_internal_tools
		gp_cancel_query

ifeq ($(with_openssl),yes)
WANTED_DIRS += sslinfo
endif

ifeq ($(with_ossp_uuid),yes)
WANTED_DIRS += uuid-ossp
endif

ifeq ($(with_libxml),yes)
WANTED_DIRS += xml2
endif

# Missing:
#		start-scripts	\ (does not have a makefile)
#		xml2		\ (requires libxml installed)


all install installdirs uninstall distprep clean distclean maintainer-clean:
	@for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || exit; \
	done

# We'd like check operations to run all the subtests before failing.
check installcheck:
	@CHECKERR=0; for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || CHECKERR=$$?; \
	done; \
	exit $$CHECKERR
