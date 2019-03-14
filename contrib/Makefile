# $PostgreSQL: pgsql/contrib/Makefile,v 1.70 2006/09/09 04:07:51 tgl Exp $

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

WANTED_DIRS = \
		orafce \
		extprotocol \
		gp_cancel_query \
		formatter_fixedwidth \
		exthdfs \
		hawq-hadoop

ifeq ($(with_pgcrypto), yes)
WANTED_DIRS += pgcrypto
endif

# ifeq ($(with_orc), yes)
# WANTED_DIRS += orc
# endif

# Missing:
#		start-scripts	\ (does not have a makefile)
#		xml2		\ (requires libxml installed)

all install installdirs uninstall distprep:
	@for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || exit; \
	done

clean distclean maintainer-clean:
	@for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || true; \
	done
	$(MAKE) -C hawq-package $@

# We'd like check operations to run all the subtests before failing.
check installcheck: pgcrypto_prepare
	@CHECKERR=0; for dir in $(WANTED_DIRS); do \
		$(MAKE) -C $$dir $@ || CHECKERR=$$?; \
	done; \
	exit $$CHECKERR
