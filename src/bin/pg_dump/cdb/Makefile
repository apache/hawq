#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_dump/cdb
#
# Portions Copyright (c) 1996-2002, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $Header: /var/cvsroot/cdb/src/bin/pg_dump/cdb/Makefile,v 1.2 2004/08/06 23:25:50 donniep Exp $
#
#-------------------------------------------------------------------------

subdir = src/bin/pg_dump/cdb
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS) -pthread
endif


# The frontend doesn't need everything that's in LIBS, some are backend only
LIBS := $(filter-out -lresolv, $(LIBS))
# This program isn't interactive, so doesn't need these
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses -lcurl -lssl -lcrypto, $(LIBS))


PGDUMP_SRCDIR= $(top_srcdir)/src/bin/pg_dump
PGDUMP_DIR= $(top_builddir)/src/bin/pg_dump

override CPPFLAGS := -I$(PGDUMP_SRCDIR) -I$(libpq_srcdir) $(CPPFLAGS) -DBINDIR=\"$(bindir)\"


OBJS= $(PGDUMP_DIR)/pg_backup_db.o $(PGDUMP_DIR)/pg_backup_custom.o \
	$(PGDUMP_DIR)/pg_backup_files.o $(PGDUMP_DIR)/pg_backup_null.o \
	$(PGDUMP_DIR)/pg_backup_tar.o $(PGDUMP_DIR)/dumputils.o


KEYWRDOBJS = ../keywords.o ../kwlookup.o

all: submake-libpq submake-libpgport cdb_dump cdb_dump_agent cdb_restore cdb_restore_agent

cdb_dump: cdb_dump.o cdb_backup_status.o cdb_seginst.o cdb_backup_state.o cdb_table.o cdb_backup_archiver.o cdb_dump_util.o cdb_dump_include.o $(PGDUMP_DIR)/common.o $(OBJS) $(KEYWRDOBJS) $(libpq_builddir)/libpq.a $(EXTRA_OBJS) 
	$(CC) $(CFLAGS) cdb_dump.o cdb_backup_status.o cdb_seginst.o cdb_backup_state.o cdb_table.o cdb_backup_archiver.o cdb_dump_util.o cdb_dump_include.o $(PGDUMP_DIR)/common.o $(OBJS) $(KEYWRDOBJS)  $(libpq) $(LDFLAGS) $(LIBS) -o $@

cdb_dump_agent: cdb_dump_agent.o $(PGDUMP_DIR)/common.o $(PGDUMP_DIR)/pg_dump_sort.o cdb_backup_archiver.o cdb_dump_util.o cdb_seginst.o cdb_table.o cdb_backup_status.o cdb_dump_include.o $(OBJS) $(KEYWRDOBJS) $(libpq_builddir)/libpq.a 
	$(CC) $(CFLAGS) cdb_dump_agent.o $(PGDUMP_DIR)/common.o $(PGDUMP_DIR)/pg_dump_sort.o cdb_dump_util.o cdb_seginst.o cdb_table.o cdb_backup_archiver.o cdb_backup_status.o cdb_dump_include.o $(OBJS) $(KEYWRDOBJS)  $(libpq) $(LDFLAGS) $(LIBS) -o $@

#cdb_dumpall_agent: cdb_dumpall_agent.o ../common.o cdb_backup_archiver.o cdb_dump_util.o cdb_seginst.o cdb_table.o cdb_backup_status.o $(OBJS) $(KEYWRDOBJS) $(libpq_builddir)/libpq.a 
#	$(CC) $(CFLAGS) cdb_dumpall_agent.o cdb_backup_archiver.o $(OBJS) $(KEYWRDOBJS)  $(libpq) $(LDFLAGS) $(LIBS) -o $@

cdb_restore: cdb_restore.o cdb_backup_status.o cdb_seginst.o cdb_backup_state.o cdb_table.o cdb_dump_util.o $(libpq_builddir)/libpq.a 
	$(CC) $(CFLAGS) cdb_restore.o cdb_backup_status.o cdb_seginst.o cdb_backup_state.o cdb_table.o cdb_dump_util.o $(libpq) $(LDFLAGS) $(LIBS) -o $@

cdb_restore_agent: cdb_restore_agent.o cdb_backup_archiver.o cdb_backup_status.o cdb_dump_util.o cdb_seginst.o cdb_table.o $(OBJS) $(libpq_builddir)/libpq.a
	$(CC) $(CFLAGS) cdb_restore_agent.o  cdb_backup_archiver.o  cdb_backup_status.o cdb_dump_util.o cdb_seginst.o cdb_table.o $(OBJS) $(KEYWRDOBJS)  $(libpq) $(LDFLAGS) $(LIBS) -o $@


.PHONY: submake-backend
submake-backend:
	$(MAKE) -C $(top_builddir)/src/backend/parser keywords.o

install: all installdirs
	$(INSTALL_PROGRAM) cdb_dump$(X) $(DESTDIR)$(bindir)/gp_dump$(X)
	$(INSTALL_PROGRAM) cdb_restore$(X) $(DESTDIR)$(bindir)/gp_restore$(X)
	$(INSTALL_PROGRAM) cdb_dump_agent$(X) $(DESTDIR)$(bindir)/gp_dump_agent$(X)
	$(INSTALL_PROGRAM) cdb_restore_agent$(X) $(DESTDIR)$(bindir)/gp_restore_agent$(X)

installdirs:
	$(MKDIR_P) $(DESTDIR)$(bindir)

uninstall:
	rm -f $(addprefix $(DESTDIR)$(bindir)/, cdb_dump$(X) cdb_dump_agent$(X) cdb_restore$(X) cdb_restore_agent$(X))  

clean distclean maintainer-clean:
	rm -f cdb_dump$(X) cdb_restore$(X) cdb_dump_agent$(X) cdb_restore_agent$(X) cdbheadsync$(X) cdbheadmakeactive$(X) $(OBJS) $(OBJS) cdb_dump.o cdb_restore.o cdb_dump_agent.o cdb_restore_agent.o $(PGDUMP_DIR)/common.o cdb_backup_status.o \
	cdb_dump_include.o cdb_seginst.o cdb_backup_state.o cdb_table.o cdb_dump_util.o cdb_backup_archiver.o cdbheadsync.o cdbheadmakeactive.o kwlookup.c $(KEYWRDOBJS)
