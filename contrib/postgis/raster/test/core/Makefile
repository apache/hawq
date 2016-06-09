#############################################################################
# $Id$
#
# Copyright (c) 2012 Regents of the University of California
#   <bkpark@ucdavis.edu>
# Copyright (c) 2009 Sandro Santilli <strk@keybit.net>
#
# This is free software; you can redistribute and/or modify it under
# the terms of the GNU General Public Licence. See the COPYING file.
#
#############################################################################

# Filenames with extension as determined by the OS
TESTAPI=testapi
TESTWKB=testwkb

RT_CORE=../../rt_core

LIBLWGEOM_LDFLAGS=../../../liblwgeom/.libs/liblwgeom.a
LIBLWGEOM_CFLAGS=-I../../../liblwgeom
LIBGDAL_CFLAGS=-I/opt/local/include
LIBGDAL_LDFLAGS=-L/opt/local/lib -lgdal
PROJ_CFLAGS=-I/opt/local/include
GEOS_CFLAGS=-I/opt/local/include
GEOS_LDFLAGS=-L/opt/local/lib -lgeos_c

RTCORE_CFLAGS=-I$(RT_CORE)
RTCORE_LDFLAGS=$(RT_CORE)/librtcore.a

CC=clang-mp-3.7
CFLAGS = \
	-msse4.2  -fno-common -DPIC  -Wall -Wmissing-prototypes \
	$(RTCORE_CFLAGS) \
	$(LIBLWGEOM_CFLAGS) \
	$(PROJ_CFLAGS) \
	$(LIBGDAL_CFLAGS) \
	$(GEOS_CFLAGS)

LDFLAGS = \
	$(RTCORE_LDFLAGS) \
	$(LIBLWGEOM_LDFLAGS) \
	$(LIBGDAL_LDFLAGS) \
	$(GEOS_LDFLAGS) \
	-lm \
       
all: $(TESTAPI) $(TESTWKB)

check: $(RT_CORE)/librtcore.a $(TESTAPI) $(TESTWKB)
	./testapi
	./testwkb

testapi.o: testapi.c
	$(CC) $(CFLAGS) -c $<

$(TESTAPI): testapi.o $(RT_CORE)/librtcore.a
	$(LIBTOOL) --mode=link $(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

testwkb.o: testwkb.c
	$(CC) $(CFLAGS) -c $<

$(TESTWKB): testwkb.o $(RT_CORE)/librtcore.a
	$(LIBTOOL) --mode=link $(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

$(RT_CORE)/librtcore.a:
	$(MAKE) -C ../../rt_core

clean:
	rm -rf .libs
	rm -f *.o $(TESTAPI) $(TESTWKB)

distclean: clean
	rm -f Makefile
