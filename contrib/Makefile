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

pgcrypto_commit := $(shell cat $(abs_top_srcdir)/depends/thirdparty/postgres.commit)

pgcrypto_prepare_timestamp: pgcrypto.patch $(abs_top_srcdir)/depends/thirdparty/postgres.commit
	rm -f pgcrypto_prepare_timestamp

# This includes an ugly hack which remove two added files after "git-reset".
# I do this because "git apply" fails if the two files have existed.
pgcrypto_prepare: pgcrypto_prepare_timestamp
	if [ ! -f $(abs_top_builddir)/$(subdir)/pgcrypto_prepare_timestamp ]; then \
		[ "x$(pgcrypto_commit)" != "x" ] || exit 1; \
		cd $(abs_top_srcdir)/depends/thirdparty; mkdir -p postgres; cd postgres; \
		mv -f postgres.git_keep_empty_directory.readme ..; \
		[ ! -d .git ] && git clone https://github.com/postgres/postgres.git .; \
		mv -f ../postgres.git_keep_empty_directory.readme .; \
		git reset --hard $(pgcrypto_commit) || (echo Now fetch from upstream and try again; git fetch origin master; git reset --hard $(pgcrypto_commit)) || exit 2; \
		rm -f contrib/pgcrypto/pgcrypto.sql.in contrib/pgcrypto/uninstall_pgcrypto.sql; \
		git apply $(abs_top_srcdir)/$(subdir)/pgcrypto.patch || exit 3; \
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
