#-------------------------------------------------------------------------
#
# GNUmakefile--
#    Makefile for src/test/regress (the regression tests)
#
# Portions Copyright (c) 1996-2007, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL: pgsql/src/test/regress/GNUmakefile,v 1.68 2007/06/12 11:07:32 mha Exp $
#
#-------------------------------------------------------------------------

subdir = src/test/regress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

contribdir = $(top_builddir)/contrib

# where to find psql for testing an existing installation
PSQLDIR = $(bindir)

# default encoding
##MULTIBYTE = LATIN1

# maximum simultaneous connections for parallel tests
MAXCONNOPT =
ifdef MAX_CONNECTIONS
MAXCONNOPT += --max-connections=$(MAX_CONNECTIONS)
endif

# locale
NOLOCALE =
ifdef NO_LOCALE
NOLOCALE += --no-locale
endif

# stuff to pass into build of pg_regress
EXTRADEFS = '-DHOST_TUPLE="$(host_tuple)"' \
	'-DMAKEPROG="$(MAKE)"' \
	'-DSHELLPROG="$(SHELL)"' \
	'-DDLSUFFIX="$(DLSUFFIX)"'


# The frontend doesn't need everything that's in LIBS, some are backend only
LIBS := $(filter-out -llapack -lblas -lf2c -lresolv -lbz2, $(LIBS))
# This program isn't interactive, so doesn't need these
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses -lcurl -lssl -lz, $(LIBS))

##
## Prepare for tests
##

# Build regression test driver

all: submake-libpgport pg_regress$(X)

pg_regress$(X): pg_regress.o pg_regress_main.o
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

# dependencies ensure that path changes propagate
pg_regress.o: pg_regress.c $(top_builddir)/src/port/pg_config_paths.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -I$(top_builddir)/src/port $(EXTRADEFS) -c -o $@ $<

$(top_builddir)/src/port/pg_config_paths.h: $(top_builddir)/src/Makefile.global
	$(MAKE) -C $(top_builddir)/src/port pg_config_paths.h

install: all installdirs
	$(INSTALL_PROGRAM) pg_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'
	for file in *.pl ; do $(INSTALL_PROGRAM) $${file} ${bindir}; done

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_regress$(X)'


# Build dynamically-loaded object file for CREATE FUNCTION ... LANGUAGE C.

NAME = regress
OBJS = regress.o caqltest.o caqlinmem.o hcatalog_utils.o json_utils.o

include $(top_srcdir)/src/Makefile.shlib

all: all-lib

# Test input and expected files.  These are created by pg_regress itself, so we
# don't have a rule to create them.  We do need rules to clean them however.
ifile_list := $(subst .source,, $(notdir $(wildcard $(top_srcdir)/$(subdir)/input/*.source)))
input_files  := $(foreach file, $(ifile_list), sql/$(file).sql)
ofile_list := $(subst .source,, $(notdir $(wildcard $(top_srcdir)/$(subdir)/output/*.source)))
output_files := $(foreach file, $(ofile_list), expected/$(file).out)

ifneq ($(PORTNAME),win32)
abs_srcdir := $(shell cd $(srcdir) && pwd)
abs_builddir := $(shell pwd)
else
abs_srcdir := $(shell cd $(srcdir) && pwd -W)
abs_builddir := $(shell pwd -W)
endif

# When doing a VPATH build, copy over the remaining .sql and .out
# files so that the driver script can find them.  We have to use an
# absolute path for the targets, because otherwise make will try to
# locate the missing files using VPATH, and will find them in
# $(srcdir), but the point here is that we want to copy them from
# $(srcdir) to the build directory.

ifdef VPATH
remaining_files_src := $(wildcard $(srcdir)/sql/*.sql) $(wildcard $(srcdir)/expected/*.out) $(srcdir)/resultmap
remaining_files_build := $(patsubst $(srcdir)/%, $(abs_builddir)/%, $(remaining_files_src))

all: $(remaining_files_build)
$(remaining_files_build): $(abs_builddir)/%: $(srcdir)/%
	ln -s $< $@
endif


# And finally some extra C modules...

all: tablespace-setup includecheck

# Tablespace setup
.PHONY: tablespace-setup
tablespace-setup:
	-rm -rf ./testtablespace
	mkdir ./testtablespace

# Check for include files that are not being shipped
.PHONY: includecheck
includecheck:
	$(srcdir)/checkinc.py

##
## Run tests
##

## To load a procedural language use "--load-language=..."

pg_regress_call = ./pg_regress --inputdir=$(srcdir) --multibyte=$(MULTIBYTE) $(MAXCONNOPT) $(NOLOCALE)

installcheck: all upg2-setup ugpart-setup
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=$(srcdir)/serial_schedule --srcdir=$(abs_srcdir)

installcheck-parallel: all upg2-setup ugpart-setup
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=$(srcdir)/parallel_schedule --srcdir=$(abs_srcdir)

installcheck-good: all ./current_good_schedule upg2-setup ugpart-setup
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=./current_good_schedule --srcdir=$(abs_srcdir)

installcheck-goh: all ./goh_schedule upg2-setup ugpart-setup
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=./goh_schedule --srcdir=$(abs_srcdir) --tablespace=hdfs_ts

$(srcdir)/sql/orca_udfs.sql: $(srcdir)/sql/orca_udfs.sql.in
	sed -e 's,%%GPOPTUTILS_NAMESPACE%%,orcaudftest,g; s,%%EXT%%,$(LDSFX),g' $(srcdir)/sql/orca_udfs.sql.in > $(srcdir)/sql/orca_udfs.sql

.PHONY: ./current_good_schedule
./current_good_schedule: $(srcdir)/sql/orca_udfs.sql $(srcdir)/known_good_schedule
ifneq ($(origin TT), undefined)
	cd $(srcdir) && ./maketestschedule.py "$(TT)" > $(abs_builddir)/current_good_schedule
else
	cd $(srcdir) && ./makeschedule ./known_good_schedule $(abs_builddir)/current_good_schedule
endif

# Copy and transform all of the upgrade related data files
# For upgrade installcheck-good test, we need to get rid of
# 1. WITH clause (because create table doesn't like tidycat definition)
# 2. tablespace pg_global
# 3. (oid=...)
upg2-setup:
	rm -rf data/upg_catupgrade_12.sql
	perl -pi \
		-e 's/(, )?(indexid|relid|reltype_oid|toast_oid|toast_index|toast_relid|toast_reltype|shared|CamelCase)=[0-9a-zA-Z]+//gi;' \
		-e 's/oid=false//g;' \
		-e 's/with \([^a-zA-Z]*\)//g;' \
		-e 's/\(oid=[0-9]+\)//;' \
		-e 's/tablespace pg_global//g;' \
		-e 's/oids=true, /oids=true/;' \
		-e 's/\(,/(/;' \
 		< $(srcdir)/data/upgrade20/upg2_catupgrade_20.sql.in \
		> data/upg_catupgrade_20.sql
	@gpstringsubs.pl data/upg_catupgrade_20.sql

ugpart-setup:
	@cp -f $(srcdir)/data/upgrade_pg_partitions.sql.in data/upgrade_pg_partitions.sql
	@gpstringsubs.pl data/upgrade_pg_partitions.sql	


# old interfaces follow...

runcheck: check
runtest: installcheck
runtest-parallel: installcheck-parallel

bigtest: all
	$(pg_regress_call) --psqldir=$(PSQLDIR) --schedule=$(srcdir)/serial_schedule --srcdir=$(abs_srcdir) numeric_big 

bkuprestore: all
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=$(srcdir)/bkuprestore_schedule --srcdir=$(abs_srcdir)

installcheck-olaptest: all
	$(pg_regress_call)  --psqldir=$(PSQLDIR) --schedule=$(srcdir)/olap_schedule --srcdir=$(abs_srcdir)

##
## Clean up
##

clean distclean maintainer-clean: clean-lib
# things built by `all' target
	rm -f $(NAME)$(DLSUFFIX) $(OBJS)
	rm -f $(output_files) $(input_files) pg_regress_main.o pg_regress.o pg_regress$(X)
# things created by dynamic configs
	rm -f ./current_good_schedule
	find sql -type l | xargs rm -f
	find expected -type l | xargs rm -f
# things created by various check targets
	rm -rf testtablespace
	rm -rf results tmp_check log
	rm -f regression.diffs regression.out regress.out run_check.out
	rm -f data/pg_class32.data
	rm -f input/loadcat33.source
	rm -f input/loadcat34.source
	rm -f input/loadcat34_sh.source
	rm -f gmon.out
	rm -f $(upg2_temp_files)
	rm -f $(srcdir)/sql/cppudf.sql
ifeq ($(PORTNAME), cygwin)
	rm -f regress.def
endif
ifdef VPATH
	rm -f $(remaining_files_build)
endif
