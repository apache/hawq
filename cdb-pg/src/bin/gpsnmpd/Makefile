#-------------------------------------------------------------------------
#
# Makefile for src/bin/gpsnmpd
#
#
#-------------------------------------------------------------------------

PGFILEDESC = "gpsnmpd - SNMP daemon for GPDB"
subdir = src/bin/gpsnmpd
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/bin/gpsnmpd -I$(top_srcdir)/src/bin/gpsnmpd/RDBMS-MIB_src $(CPPFLAGS)
ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS) -pthread
endif

# The frontend doesn't need everything that's in LIBS, some are backend only
LIBS := $(filter-out -llapack -lblas -lf2c -lresolv -lbz2, $(LIBS))
# This program isn't interactive, so doesn't need these
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses -lcurl -lssl -lcrypto -lz, $(LIBS))

override CFLAGS += -Wno-error

# we pick up the wrong net-snmp-config here  with lots of references to /home/build/hridenour/dev
#SNMPFLAGS=-I. -I./RDBMS-MIB_src $(shell ../../../../ext/bin/net-snmp-config --cflags) 
#AGENTLIBS=$(shell net-snmp-config --agent-libs)
#
# instead try to "wing it"
SNMPFLAGS=-I. -I./RDBMS-MIB_src $(CFLAGS)
AGENTLIBS=-lnetsnmpagent -lnetsnmphelpers -lnetsnmpmibs -lnetsnmp 


override CPPFLAGS := -DFRONTEND -DDEF_PGPORT=$(DEF_PGPORT) -I$(libpq_srcdir) $(CPPFLAGS) $(SNMPFLAGS)
LIBS +=  $(AGENTLIBS) -lyaml


#PG_CPPFLAGS = -O0 -g -ggdb -I$(libpq_srcdir) $(SNMPFLAGS) 
#PG_LIBS = $(libpq_pgport) $(AGENTLIBS) -lyaml


PROGRAM = gpsnmpd
OBJS	= gpsnmpd.o RDBMS-MIB_src/rdbmsDbTable/rdbmsDbTable.o RDBMS-MIB_src/rdbmsDbTable/rdbmsDbTable_data_access.o  RDBMS-MIB_src/rdbmsDbTable/rdbmsDbTable_data_get.o \
          RDBMS-MIB_src/rdbmsDbTable/rdbmsDbTable_data_set.o  RDBMS-MIB_src/rdbmsDbTable/rdbmsDbTable_interface.o \
          RDBMS-MIB_src/rdbmsDbInfoTable/rdbmsDbInfoTable.o RDBMS-MIB_src/rdbmsDbInfoTable/rdbmsDbInfoTable_data_access.o  \
	  RDBMS-MIB_src/rdbmsDbInfoTable/rdbmsDbInfoTable_data_get.o  RDBMS-MIB_src/rdbmsDbInfoTable/rdbmsDbInfoTable_interface.o \
          RDBMS-MIB_src/rdbmsDbParamTable/rdbmsDbParamTable.o RDBMS-MIB_src/rdbmsDbParamTable/rdbmsDbParamTable_data_access.o \
	  RDBMS-MIB_src/rdbmsDbParamTable/rdbmsDbParamTable_data_get.o  RDBMS-MIB_src/rdbmsDbParamTable/rdbmsDbParamTable_data_set.o \
	  RDBMS-MIB_src/rdbmsDbParamTable/rdbmsDbParamTable_interface.o RDBMS-MIB_src/rdbmsSrvTable/rdbmsSrvTable_interface.o \
	  RDBMS-MIB_src/rdbmsSrvTable/rdbmsSrvTable.o RDBMS-MIB_src/rdbmsSrvTable/rdbmsSrvTable_data_access.o \
	  RDBMS-MIB_src/rdbmsSrvTable/rdbmsSrvTable_data_get.o RDBMS-MIB_src/rdbmsSrvTable/rdbmsSrvTable_data_set.o \
	  RDBMS-MIB_src/rdbmsSrvParamTable/rdbmsSrvParamTable.o RDBMS-MIB_src/rdbmsSrvParamTable/rdbmsSrvParamTable_data_access.o \
	  RDBMS-MIB_src/rdbmsSrvParamTable/rdbmsSrvParamTable_data_get.o RDBMS-MIB_src/rdbmsSrvParamTable/rdbmsSrvParamTable_data_set.o \
	  RDBMS-MIB_src/rdbmsSrvParamTable/rdbmsSrvParamTable_interface.o \
	  RDBMS-MIB_src/rdbmsSrvInfoTable/rdbmsSrvInfoTable.o RDBMS-MIB_src/rdbmsSrvInfoTable/rdbmsSrvInfoTable_data_access.o \
	  RDBMS-MIB_src/rdbmsSrvInfoTable/rdbmsSrvInfoTable_data_get.o RDBMS-MIB_src/rdbmsSrvInfoTable/rdbmsSrvInfoTable_data_set.o \
	  RDBMS-MIB_src/rdbmsSrvInfoTable/rdbmsSrvInfoTable_interface.o \
	  RDBMS-MIB_src/rdbmsRelTable/rdbmsRelTable.o RDBMS-MIB_src/rdbmsRelTable/rdbmsRelTable_data_access.o \
	  RDBMS-MIB_src/rdbmsRelTable/rdbmsRelTable_data_get.o RDBMS-MIB_src/rdbmsRelTable/rdbmsRelTable_data_set.o \
	  RDBMS-MIB_src/rdbmsRelTable/rdbmsRelTable_interface.o \
	  RDBMS-MIB_src/rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable.o RDBMS-MIB_src/rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable_data_set.o \
	  RDBMS-MIB_src/rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable_data_access.o RDBMS-MIB_src/rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable_interface.o \
	  RDBMS-MIB_src/rdbmsDbLimitedResourceTable/rdbmsDbLimitedResourceTable_data_get.o \
	  pg_array.o \
	  customquery.o \
	  query_reader.o

    	  #PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAmopTable/pgsqlPgAmopTable.o \
    	  #PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable.o PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable_data_access.o \
    	  #PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable_data_get.o PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable_data_set.o \
    	  #PGSQL-MIB_src/pgsqlCatalogTables/pgsqlPgAggregateTable/pgsqlPgAggregateTable_interface.o \


all: submake-libpq submake-libpgport gpsnmpd

DOCS = README.gpsnmpd README.greenplum

DATA = gpsnmpd.conf gpsnmpd.sql GPDB-MIB.txt RDBMS-MIB.txt NETWORK-SERVICES-MIB.txt

gpsnmpd: $(OBJS) $(libpq_builddir)/libpq.a
	$(CC) $(CFLAGS) $(OBJS) $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)
	
install: all installdirs
	$(INSTALL_PROGRAM) gpsnmpd$(X) '$(DESTDIR)$(bindir)/gpsnmpd$(X)'
	$(INSTALL_DATA) $(srcdir)/README.gpsnmpd '$(DESTDIR)$(datadir)/README.gpsnmpd'
	$(INSTALL_DATA) $(srcdir)/README.greenplum '$(DESTDIR)$(datadir)/README.greenplum'
	$(INSTALL_DATA) $(srcdir)/RDBMS-MIB.txt '$(DESTDIR)$(datadir)/RDBMS-MIB.txt'
	$(INSTALL_DATA) $(srcdir)/NETWORK-SERVICES-MIB.txt '$(DESTDIR)$(datadir)/NETWORK-SERVICES-MIB.txt'
	$(INSTALL_DATA) $(srcdir)/GPDB-MIB.txt '$(DESTDIR)$(datadir)/GPDB-MIB.txt'
	$(INSTALL_DATA) $(srcdir)/gpsnmpd.sql '$(DESTDIR)$(datadir)/gpsnmpd.sql'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gpsnmpd$(X)'

clean distclean maintainer-clean:
clean distclean maintainer-clean:
	rm -f gpsnmpd$(X) $(OBJS)
