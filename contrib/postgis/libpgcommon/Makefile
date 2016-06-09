# **********************************************************************
# * $Id: Makefile.in 
# *
# * PostGIS - Spatial Types for PostgreSQL
# * http://postgis.refractions.net
# * Copyright 2008 Mark Cave-Ayland
# *
# * This is free software; you can redistribute and/or modify it under
# * the terms of the GNU General Public Licence. See the COPYING file.
# *
# **********************************************************************

CC=clang-mp-3.7
CFLAGS=-msse4.2 -I../liblwgeom -I/opt/local/include/postgresql/server -I/opt/local/include/postgresql/server/../internal -I/opt/local/include  -fno-common -DPIC  -Wall -Wmissing-prototypes 
LDFLAGS= -L/opt/local/lib -lintl -Wl,-framework -Wl,CoreFoundation

NUMERICFLAGS=

YACC=bison -y
LEX=flex

# Standalone COMMON objects
SA_OBJS = \
	gserialized_gist.o \
	libsrid.o	   \
	lwgeom_transform.o \
	lwgeom_pg.o


SA_HEADERS = \
	lwgeom_pg.h \
	libsrid.h   \
	lwgeom_transform.h \
	gserialized_gist.h \
	pgsql_compat.h

all: libpgcommon.a

# nothing to install or uninstall
install uninstall:

libpgcommon.a: $(SA_OBJS) $(SA_HEADERS)
	ar rs libpgcommon.a $(SA_OBJS) 

maintainer-clean: clean

clean: 
	$(MAKE) -C cunit clean
	rm -f $(SA_OBJS) 
	rm -f $(NM_OBJS) 
	rm -f libpgcommon.a 

distclean: clean
	$(MAKE) -C cunit distclean
	rm -f Makefile

check: libpgcommon.a
	$(MAKE) -C cunit check

# Command to build each of the .o files
$(SA_OBJS): %.o: %.c 
	$(CC) $(CFLAGS) -c -o $@ $<

$(SA_OBJS): ../postgis_config.h

