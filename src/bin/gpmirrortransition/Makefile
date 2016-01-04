#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------------------
#
# Makefile for src/bin/gpmirrortransition
#
# This Makefile was copied from the pg_dump makefile and modified accordingly
#
# $PostgreSQL: pgsql/src/bin/gpmirrortransition/Makefile,v 1.62 2006/03/05 15:58:50 momjian Exp $
#
#-------------------------------------------------------------------------

PGFILEDESC = "gp_primarymirror - inform a segment of a change in primary/mirror status"
subdir = src/bin/gpmirrortransition
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# The frontend doesn't need everything that's in LIBS, some are backend only
LIBS := $(filter-out -llapack -lblas -lf2c -lresolv, $(LIBS))
# This program isn't interactive, so doesn't need these
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses -lcurl -lssl -lcrypto, $(LIBS))

# the use of tempnam in pg_backup_tar.c causes a warning when using newer versions of GCC
override CPPFLAGS := -Wno-deprecated-declarations -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)

OBJS=gpmirrortransition.o $(WIN32RES)

EXTRA_OBJS = $(top_builddir)/src/backend/libpq/ip.o $(top_builddir)/src/backend/postmaster/primary_mirror_transition_client.o $(top_builddir)/src/timezone/gptime.o

all: submake-libpq submake-libpgport submake-backend gp_primarymirror

gp_primarymirror: gpmirrortransition.o $(OBJS) $(EXTRA_OBJS) $(libpq_builddir)/libpq.a 
	$(CC) $(CFLAGS) $(OBJS) $(EXTRA_OBJS) $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)

.PHONY: submake-backend
submake-backend:
	$(MAKE) -C $(top_builddir)/src/backend/libpq ip.o

install: all installdirs
	$(INSTALL_PROGRAM) gp_primarymirror$(X) '$(DESTDIR)$(bindir)'/gp_primarymirror$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, gp_primarymirror$(X))

clean distclean maintainer-clean:
	rm -f gp_primarymirror$(X) $(OBJS) gpmirrortransition.o
