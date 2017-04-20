-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- $Id: rtpostgis_upgrade.sql.in.c 8448 2011-12-16 22:07:26Z dustymugs $
--
-- PostGIS Raster - Raster Type for PostGIS
-- http://trac.osgeo.org/postgis/wiki/WKTRaster
--
-- Copyright (c) 2011 Regina Obe <lr@pcorp.us>
-- Copyright (C) 2011 Regents of the University of California
--   <bkpark@ucdavis.edu>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- WARNING: Any change in this file must be evaluated for compatibility.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

-- This section is take add / drop things like CASTS, TYPES etc. that have changed
-- Since these are normally excluded from sed upgrade generator
-- they must be explicitly added
-- So that they can immediately be recreated. 
-- It is not run thru the sed processor to prevent it from being stripped
-- Note: We put these in separate file from drop since the extension module has
-- to add additional logic to drop them from the extension as well





































-- drop st_bytea
DROP CAST IF EXISTS (raster AS bytea);
DROP FUNCTION IF EXISTS st_bytea(raster);

CREATE OR REPLACE FUNCTION bytea(raster)
    RETURNS bytea
    AS '$libdir/rtpostgis-2.0', 'RASTER_to_bytea'
    LANGUAGE 'c' IMMUTABLE STRICT;
CREATE CAST (raster AS bytea)
    WITH FUNCTION bytea(raster) AS ASSIGNMENT;

-- drop box2d
DROP CAST IF EXISTS (raster AS box2d);
DROP FUNCTION IF EXISTS box2d(raster);

-- create box3d cast if it does not exist

-- if we are running 8.4 we need to use brute force
DROP CAST IF EXISTS (raster AS box3d);
CREATE OR REPLACE FUNCTION box3d(raster)
    RETURNS box3d
    AS 'SELECT box3d(st_convexhull($1))'
    LANGUAGE 'sql' IMMUTABLE STRICT;
CREATE CAST (raster AS box3d)
    WITH FUNCTION box3d(raster) AS ASSIGNMENT;


-- make geometry cast ASSIGNMENT
DROP CAST IF EXISTS (raster AS geometry);
CREATE CAST (raster AS geometry)
	WITH FUNCTION st_convexhull(raster) AS ASSIGNMENT;

