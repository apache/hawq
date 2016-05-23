# $PostgreSQL: pgsql/contrib/Makefile,v 1.70 2006/09/09 04:07:51 tgl Exp $

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

WANTED_DIRS = \
		orafce \
		extprotocol \
		gp_cancel_query \
		formatter_fixedwidth \
		hawq-hadoop

ifeq ($(with_pgcrypto), yes)
WANTED_DIRS += pgcrypto

# fixme: depend on pgcrypto.patch also.
pgcrypto_prepare: pg_prepare
	if [ ! -e $(abs_top_builddir)/$(subdir)/pgcrypto_prepare_timestamp ]; then \
		(cd $(abs_top_srcdir)/src/postgres/contrib/pgcrypto; git apply $(abs_top_srcdir)/$(subdir)/pgcrypto.patch) && \
		touch $(abs_top_builddir)/$(subdir)/pgcrypto_prepare_timestamp; \
	fi
else
pgcrypto_prepare:
endif

.PHONY: pgcrypto_prepare

# Missing:
#		start-scripts	\ (does not have a makefile)
#		xml2		\ (requires libxml installed)

all install installdirs uninstall distprep: pgcrypto_prepare
	@for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || exit; \
	done

clean distclean maintainer-clean:
	@for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || true; \
	done

# We'd like check operations to run all the subtests before failing.
check installcheck: pgcrypto_prepare
	@CHECKERR=0; for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || CHECKERR=$$?; \
	done; \
	exit $$CHECKERR
