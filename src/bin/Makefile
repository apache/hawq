#-------------------------------------------------------------------------
#
# Makefile for src/bin (client programs)
#
# Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL: pgsql/src/bin/Makefile,v 1.54 2009/01/01 17:23:53 momjian Exp $
#
#-------------------------------------------------------------------------

subdir = src/bin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

DIRS = initdb ipcclean pg_ctl pg_dump pgbench \
	psql scripts pg_config pg_controldata pg_resetxlog \
	gpfilesystem/hdfs gpmirrortransition gpupgrade \
	gpfusion gp_workfile_mgr gpcheckhdfs gpfdist gpoptutils

all install installdirs uninstall distprep clean distclean maintainer-clean:
	@for dir in $(DIRS); do echo "build $$dir"; $(MAKE) -C $$dir $@ || exit; done
