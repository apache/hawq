# $PostgreSQL: pgsql/contrib/Makefile,v 1.70 2006/09/09 04:07:51 tgl Exp $

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

WANTED_DIRS = \
                pgbench \
                orafce \
                extprotocol \
                gp_cancel_query
                gp_mdver \
                hawq-hadoop

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
