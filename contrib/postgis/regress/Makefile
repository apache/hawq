# **********************************************************************
# *
# * PostGIS - Spatial Types for PostgreSQL
# * http://postgis.refractions.net
# * 
# * Copyright (C) 2011-2012 Sandro Santilli <strk@keybit.net>
# * Copyright (C) 2009-2011 Paul Ramsey <pramsey@cleverelephant.ca>
# * Copyright (C) 2008-2009 Mark Cave-Ayland
# *
# * This is free software; you can redistribute and/or modify it under
# * the terms of the GNU General Public Licence. See the COPYING file.
# *
# **********************************************************************

PERL=/opt/local/bin/perl
TMPDIR?=/tmp

POSTGIS_PGSQL_VERSION=82
POSTGIS_GEOS_VERSION=35
POSTGIS_PROJ_VERSION=49
HAVE_JSON=no
MINGWBUILD=0

# MingW hack: rather than use PGSQL_BINDIR directly, we change
# to the directory and then use "pwd" to return the path. This
# ensures that the returned path is in MSYS format, otherwise
# colons in drive letters will break PATH.
PGSQL_BINDIR=$(shell cd "/Users/kdunn/repos/incubator-hawq/src/bin/pg_config" && pwd)

# Where we put our regression installation 
ifeq ($(MINGWBUILD),1)
        srcdir=$(shell bash -c "pwd -W")
else
        srcdir=$(shell pwd)
endif
REGRESS_INSTALLDIR=$(srcdir)/00-regress-install

# 
# Put path from pg_config into front of search path
#
PATH := $(PGSQL_BINDIR):$(PATH)
export PATH


TESTS = \
	loader/Point \
	loader/PointM \
	loader/PointZ \
	loader/MultiPoint \
	loader/MultiPointM \
	loader/MultiPointZ \
	loader/Arc \
	loader/ArcM \
	loader/ArcZ \
	loader/Polygon \
	loader/PolygonM \
	loader/PolygonZ \
	loader/TSTPolygon \
	loader/TSIPolygon \
	loader/TSTIPolygon \
	loader/PointWithSchema \
	loader/NoTransPoint \
	loader/NotReallyMultiPoint \
	loader/MultiToSinglePoint \
	loader/ReprojectPts \
	loader/ReprojectPtsGeog \
	loader/Latin1 \
	binary \
	regress \
	regress_index \
	regress_index_nulls \
	lwgeom_regress \
	regress_lrs \
	removepoint \
	setpoint \
	simplify \
	snaptogrid \
	summary \
	affine \
	empty \
	measures \
	legacy \
	ctors \
	sql-mm-serialize \
	sql-mm-circularstring \
	sql-mm-compoundcurve \
	sql-mm-curvepoly \
	sql-mm-general \
	sql-mm-multicurve \
	sql-mm-multisurface \
	polyhedralsurface \
	polygonize \
	postgis_type_name \
	out_geometry \
	out_geography \
	in_gml \
	in_kml \
	iscollection \
	regress_ogc \
	regress_ogc_cover \
	regress_ogc_prep \
	regress_bdpoly \
	regress_proj \
	regress_management \
	dump \
	dumppoints \
	wmsservers \
	wkt \
	wkb \
	remove_repeated_points \
	split \
	relate \
	bestsrid \
	concave_hull
# TODO: MPP-15724 reenable tests
#	long_xact
#	typmod
#	tickets


ifeq ($(shell expr $(POSTGIS_GEOS_VERSION) ">=" 32),1)
	# GEOS-3.3 adds:
	# ST_HausdorffDistance, ST_Buffer(params)
	TESTS += \
		hausdorff \
		regress_buffer_params
endif

ifeq ($(shell expr $(POSTGIS_GEOS_VERSION) ">=" 33),1)
	# GEOS-3.3 adds:
	# ST_RelateMatch, ST_IsValidDetail, ST_SharedPaths ,
	# ST_Snap, ST_UnaryUnion, ST_MakeClean 
	TESTS += \
		relatematch  \
		isvaliddetail \
		sharedpaths \
		snap \
		node \
		unaryunion \
		clean \
		relate_bnr \
		offsetcurve
endif

ifeq ($(HAVE_JSON),yes)
	# JSON-C adds:
	# ST_GeomFromGeoJSON()
	TESTS += \
		in_geojson 
endif

all install uninstall:

distclean: clean
	rm Makefile

staged-install-topology:
	@if test x"topology" != "x"; then \
		$(MAKE) -C ../topology USE_PGXS=1 REGRESS=1 DESTDIR=$(REGRESS_INSTALLDIR) install; \
	fi

staged-install-raster:
	@if test x"raster" != "x"; then \
		$(MAKE) -C ../raster/rt_pg REGRESS=1 DESTDIR=$(REGRESS_INSTALLDIR) install; \
	fi

staged-install: staged-install-raster staged-install-topology
	$(MAKE) -C ../postgis USE_PGXS=1 REGRESS=1 DESTDIR=$(REGRESS_INSTALLDIR) install
	$(MAKE) -C ../ REGRESS=1 DESTDIR=$(REGRESS_INSTALLDIR) comments-install
	$(PERL) -pi.bak -e 's,\$$libdir,$(REGRESS_INSTALLDIR)/lib,g' $(REGRESS_INSTALLDIR)/share/contrib/postgis/*.sql
	#$(MAKE) -C ../loader REGRESS=1 DESTDIR=$(REGRESS_INSTALLDIR) install

test check: staged-install
	./run_test $(RUNTESTFLAGS) $(TESTS)
	./run_test --upgrade $(RUNTESTFLAGS) $(TESTS)

garden:
	createdb postgis_garden
	createlang plpgsql postgis_garden
	psql -d postgis_garden < ../postgis/postgis.sql
	psql -d postgis_garden < ../spatial_ref_sys.sql
	@echo '-------------------------------------------------'
	@echo 'Regression tests in progress (it will take time)'
	@echo 'Result output: ./regress/garden_result.txt'
	@echo '-------------------------------------------------'
	psql -d postgis_garden < ../doc/postgis_gardentest_${POSTGIS_MAJOR_VERSION}${POSTGIS_MINOR_VERSION}.sql > postgis_garden_result.txt 2>&1
	#dropdb postgis_garden

cleanup:
	@sleep 1
	@dropdb postgis_reg > /dev/null

clean:
	rm -rf $(REGRESS_INSTALLDIR)
