-- $Id: legacy.sql.in.c 9735 2012-05-16 08:29:14Z robe $
-- Legacy functions without chip functions --
-- This is the full list including the legacy_minimal.sql (minimal)
-- so no need to install both legacy and the minimal 

-- $Id: legacy_minimal.sql.in.c 10736 2012-11-25 22:17:48Z robe $
-- Bare minimum Legacy functions --
-- This file that contains what we have determined are
-- the most common functions used by older apps
-- You should be able to get by with just to support
-- older versions of mapserver, geoserver, qgis, gdal, openjump etc.



































-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsBinary(geometry)
	RETURNS bytea
	AS '$libdir/postgis-2.0','LWGEOM_asBinary'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsBinary(geometry,text)
	RETURNS bytea
	AS '$libdir/postgis-2.0','LWGEOM_asBinary'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsText(geometry)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asText'
	LANGUAGE 'c' IMMUTABLE STRICT;	

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Estimated_Extent(text,text,text) RETURNS box2d AS
	'$libdir/postgis-2.0', 'geometry_estimated_extent'
	LANGUAGE 'c' IMMUTABLE STRICT SECURITY DEFINER;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Estimated_Extent(text,text) RETURNS box2d AS
	'$libdir/postgis-2.0', 'geometry_estimated_extent'
	LANGUAGE 'c' IMMUTABLE STRICT SECURITY DEFINER;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomFromText(text, int4)
	RETURNS geometry AS 'SELECT ST_GeomFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomFromText(text)
	RETURNS geometry AS 'SELECT ST_GeomFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION ndims(geometry)
	RETURNS smallint
	AS '$libdir/postgis-2.0', 'LWGEOM_ndims'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SetSRID(geometry,int4)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_set_srid'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SRID(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0','LWGEOM_get_srid'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.5.0
-- hack to allow unknown to cast to geometry
-- so does not yield function is not unique
CREATE OR REPLACE FUNCTION ST_AsBinary(text)
	RETURNS bytea
	AS 
	$$ SELECT ST_AsBinary($1::geometry);$$
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.5.0
-- hack to allow unknown to cast to geometry
CREATE OR REPLACE FUNCTION ST_AsText(bytea)
	RETURNS text
	AS 
	$$ SELECT ST_AsText($1::geometry);$$
	LANGUAGE 'sql' IMMUTABLE STRICT;

--- start functions that in theory should never have been used or internal like stuff deprecated

-- these were superceded by PostGIS_AddBBOX , PostGIS_DropBBOX, PostGIS_HasBBOX in 1.5 --
CREATE OR REPLACE FUNCTION addbbox(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_addBBOX'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
CREATE OR REPLACE FUNCTION dropbbox(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_dropBBOX'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION hasbbox(geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'LWGEOM_hasBBOX'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Availability: 1.2.2 -- never deprecated but don't think anyone uses it
CREATE OR REPLACE FUNCTION getsrid(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0','LWGEOM_get_srid'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeometryFromText(text, int4)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_from_text'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeometryFromText(text)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_from_text'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomFromWKB(bytea)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_from_WKB'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomFromWKB(bytea, int)
	RETURNS geometry
	AS 'SELECT ST_SetSRID(ST_GeomFromWKB($1), $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION noop(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_noop'
	LANGUAGE 'c' VOLATILE STRICT;
	
-- ESRI ArcSDE compatibility functions --
-- We are remiving these because we don't
-- think ESRI relies on them 
-- so their existence is pointless
-- Availability: 1.5.0
-- PostGIS equivalent function: none
CREATE OR REPLACE FUNCTION SE_EnvelopesIntersect(geometry,geometry)
	RETURNS boolean
	AS $$ 
	SELECT $1 && $2
	$$	
	LANGUAGE 'sql' IMMUTABLE STRICT; 
	
CREATE OR REPLACE FUNCTION SE_Is3D(geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'LWGEOM_hasz'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Availability: 1.5.0
CREATE OR REPLACE FUNCTION SE_IsMeasured(geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'LWGEOM_hasm'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- PostGIS equivalent function: Z(geometry)
CREATE OR REPLACE FUNCTION SE_Z(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_z_point'
	LANGUAGE 'c' IMMUTABLE STRICT; 

-- PostGIS equivalent function: M(geometry)
CREATE OR REPLACE FUNCTION SE_M(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_m_point'
	LANGUAGE 'c' IMMUTABLE STRICT; 
	
-- PostGIS equivalent function: locate_between_measures(geometry, float8, float8)
CREATE OR REPLACE FUNCTION SE_LocateBetween(geometry, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_locate_between_m'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- PostGIS equivalent function: locate_along_measure(geometry, float8)
CREATE OR REPLACE FUNCTION SE_LocateAlong(geometry, float8)
	RETURNS geometry
	AS $$ SELECT SE_LocateBetween($1, $2, $2) $$
	LANGUAGE 'sql' IMMUTABLE STRICT;

	
--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box2d(geometry)
	RETURNS box2d
	AS '$libdir/postgis-2.0','LWGEOM_to_BOX2D'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box3d(geometry)
	RETURNS box3d
	AS '$libdir/postgis-2.0','LWGEOM_to_BOX3D'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box(geometry)
	RETURNS box
	AS '$libdir/postgis-2.0','LWGEOM_to_BOX'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box2d(box3d)
	RETURNS box2d
	AS '$libdir/postgis-2.0','BOX3D_to_BOX2D'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box3d(box2d)
	RETURNS box3d
	AS '$libdir/postgis-2.0','BOX2D_to_BOX3D'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box(box3d)
	RETURNS box
	AS '$libdir/postgis-2.0','BOX3D_to_BOX'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_text(geometry)
	RETURNS text
	AS '$libdir/postgis-2.0','LWGEOM_to_text'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry(box2d)
	RETURNS geometry
	AS '$libdir/postgis-2.0','BOX2D_to_LWGEOM'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry(box3d)
	RETURNS geometry
	AS '$libdir/postgis-2.0','BOX3D_to_LWGEOM'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry(text)
	RETURNS geometry
	AS '$libdir/postgis-2.0','parse_WKT_lwgeom'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry(bytea)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_from_bytea'
	LANGUAGE 'c' IMMUTABLE STRICT;

--- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_bytea(geometry)
	RETURNS bytea
	AS '$libdir/postgis-2.0','LWGEOM_to_bytea'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box3d_in(cstring)
	RETURNS box3d
	AS '$libdir/postgis-2.0', 'BOX3D_in'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_box3d_out(box3d)
	RETURNS cstring
	AS '$libdir/postgis-2.0', 'BOX3D_out'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- START MANAGEMENT FUNCTIONS
-- These are legacy management functions with no place in our 2.0 world
-----------------------------------------------------------------------
-- RENAME_GEOMETRY_TABLE_CONSTRAINTS()
-----------------------------------------------------------------------
-- This function has been obsoleted for the difficulty in
-- finding attribute on which the constraint is applied.
-- AddGeometryColumn will name the constraints in a meaningful
-- way, but nobody can rely on it since old postgis versions did
-- not do that.
-----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rename_geometry_table_constraints() RETURNS text
AS
$$
SELECT 'rename_geometry_table_constraint() is obsoleted'::text
$$
LANGUAGE 'sql' IMMUTABLE;

-----------------------------------------------------------------------
-- FIX_GEOMETRY_COLUMNS()
-----------------------------------------------------------------------
-- This function will:
--
--	o try to fix the schema of records with an integer one
--		(for PG>=73)
--
--	o link records to system tables through attrelid and varattnum
--		(for PG<75)
--
--	o delete all records for which no linking was possible
--		(for PG<75)
--
--
-----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fix_geometry_columns() RETURNS text
AS
$$
DECLARE
	mislinked record;
	result text;
	linked integer;
	deleted integer;
	foundschema integer;
BEGIN

	-- Since 7.3 schema support has been added.
	-- Previous postgis versions used to put the database name in
	-- the schema column. This needs to be fixed, so we try to
	-- set the correct schema for each geometry_colums record
	-- looking at table, column, type and srid.
	

	return 'This function is obsolete now that geometry_columns is a view';

END;
$$
LANGUAGE 'plpgsql' VOLATILE;

-----------------------------------------------------------------------
-- PROBE_GEOMETRY_COLUMNS()
-----------------------------------------------------------------------
-- Fill the geometry_columns table with values probed from the system
-- catalogues. This is done by simply looking up constraints previously
-- added to a geometry column. If geometry constraints are missing, no
-- attempt is made to add the necessary constraints to the geometry
-- column, nor is it recorded in the geometry_columns table.
-- 3d flag cannot be probed, it defaults to 2
--
-- Note that bogus records already in geometry_columns are not
-- overridden (a check for schema.table.column is performed), so
-- to have a fresh probe backup your geometry_columns, delete from
-- it and probe.
-----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION probe_geometry_columns() RETURNS text AS
$$
DECLARE
	inserted integer;
	oldcount integer;
	probed integer;
	stale integer;
BEGIN



	RETURN 'This function is obsolete now that geometry_columns is a view';
END

$$
LANGUAGE 'plpgsql' VOLATILE;



-- END MANAGEMENT FUNCTIONS --

-- Deprecation in 1.5.0
-- these remarked out functions cause problems and no one uses them directly
-- They should not be installed


-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_lt(geometry, geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'lwgeom_lt'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_le(geometry, geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'lwgeom_le'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_gt(geometry, geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'lwgeom_gt'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_ge(geometry, geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'lwgeom_ge'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_eq(geometry, geometry)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'lwgeom_eq'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION st_geometry_cmp(geometry, geometry)
	RETURNS integer
	AS '$libdir/postgis-2.0', 'lwgeom_cmp'
	LANGUAGE 'c' IMMUTABLE STRICT;
	

--- end functions that in theory should never have been used


-- begin old ogc (and non-ST) names that have been replaced with new SQL-MM and SQL ST_ Like names --
-- AFFINE Functions --
-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Affine(geometry,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_affine'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Affine(geometry,float8,float8,float8,float8,float8,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1,  $2, $3, 0,  $4, $5, 0,  0, 0, 1,  $6, $7, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION RotateZ(geometry,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1,  cos($2), -sin($2), 0,  sin($2), cos($2), 0,  0, 0, 1,  0, 0, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Rotate(geometry,float8)
	RETURNS geometry
	AS 'SELECT st_rotateZ($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION RotateX(geometry,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1, 1, 0, 0, 0, cos($2), -sin($2), 0, sin($2), cos($2), 0, 0, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.1.2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION RotateY(geometry,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1,  cos($2), 0, sin($2),  0, 1, 0,  -sin($2), 0, cos($2), 0,  0, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Availability: 1.1.0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Scale(geometry,float8,float8,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1,  $2, 0, 0,  0, $3, 0,  0, 0, $4,  0, 0, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.1.0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Scale(geometry,float8,float8)
	RETURNS geometry
	AS 'SELECT st_scale($1, $2, $3, 1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Translate(geometry,float8,float8,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1, 1, 0, 0, 0, 1, 0, 0, 0, 1, $2, $3, $4)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Translate(geometry,float8,float8)
	RETURNS geometry
	AS 'SELECT st_translate($1, $2, $3, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;


-- Availability: 1.1.0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION TransScale(geometry,float8,float8,float8,float8)
	RETURNS geometry
	AS 'SELECT st_affine($1,  $4, 0, 0,  0, $5, 0,
		0, 0, 1,  $2 * $4, $3 * $5, 0)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- END Affine functions
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AddPoint(geometry, geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_addpoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AddPoint(geometry, geometry, integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_addpoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Area(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_area_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- this is an alias for 'area(geometry)'
-- there is nothing such an 'area3d'...
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Area2D(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_area_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsEWKB(geometry)
	RETURNS BYTEA
	AS '$libdir/postgis-2.0','WKBFromLWGEOM'
	LANGUAGE 'c' IMMUTABLE STRICT;
	

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsEWKB(geometry,text)
	RETURNS bytea
	AS '$libdir/postgis-2.0','WKBFromLWGEOM'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsEWKT(geometry)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asEWKT'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- AsGML(geom) / precision=15 version=2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsGML(geometry)
	RETURNS TEXT
	AS 'SELECT _ST_AsGML(2, $1, 15, 0, null)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsGML(geometry, int4)
	RETURNS TEXT
	AS 'SELECT _ST_AsGML(2, $1, $2, 0, null)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- AsKML(geom, precision) / version=2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsKML(geometry, int4)
	RETURNS TEXT
	AS 'SELECT _ST_AsKML(2, ST_transform($1,4326), $2, null)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- AsKML(geom) / precision=15 version=2
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsKML(geometry)
	RETURNS TEXT
	AS 'SELECT _ST_AsKML(2, ST_Transform($1,4326), 15, null)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- AsKML(version, geom, precision)
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsKML(int4, geometry, int4)
	RETURNS TEXT
	AS 'SELECT _ST_AsKML($1, ST_Transform($2,4326), $3, null)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsHEXEWKB(geometry)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asHEXEWKB'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsHEXEWKB(geometry, text)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asHEXEWKB'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsSVG(geometry)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asSVG'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsSVG(geometry,int4)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asSVG'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION AsSVG(geometry,int4,int4)
	RETURNS TEXT
	AS '$libdir/postgis-2.0','LWGEOM_asSVG'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION azimuth(geometry,geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0', 'LWGEOM_azimuth'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION BdPolyFromText(text, integer)
RETURNS geometry
AS $$
DECLARE
	geomtext alias for $1;
	srid alias for $2;
	mline geometry;
	geom geometry;
BEGIN
	mline := ST_MultiLineStringFromText(geomtext, srid);

	IF mline IS NULL
	THEN
		RAISE EXCEPTION 'Input is not a MultiLinestring';
	END IF;

	geom := ST_BuildArea(mline);

	IF GeometryType(geom) != 'POLYGON'
	THEN
		RAISE EXCEPTION 'Input returns more then a single polygon, try using BdMPolyFromText instead';
	END IF;

	RETURN geom;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION BdMPolyFromText(text, integer)
RETURNS geometry
AS $$
DECLARE
	geomtext alias for $1;
	srid alias for $2;
	mline geometry;
	geom geometry;
BEGIN
	mline := ST_MultiLineStringFromText(geomtext, srid);

	IF mline IS NULL
	THEN
		RAISE EXCEPTION 'Input is not a MultiLinestring';
	END IF;

	geom := ST_Multi(ST_BuildArea(mline));

	RETURN geom;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION boundary(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','boundary'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION buffer(geometry,float8,integer)
	RETURNS geometry
	AS 'SELECT ST_Buffer($1, $2, $3)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION buffer(geometry,float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0','buffer'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION BuildArea(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'ST_BuildArea'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- This is also available w/out GEOS
CREATE OR REPLACE FUNCTION Centroid(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
		
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Contains(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION convexhull(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','convexhull'
	LANGUAGE 'c' IMMUTABLE STRICT
	;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION crosses(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION distance(geometry,geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0', 'LWGEOM_mindistance2d'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
		
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION difference(geometry,geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','difference'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Dimension(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_dimension'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION disjoint(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION distance_sphere(geometry,geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_distance_sphere'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION distance_spheroid(geometry,geometry,spheroid)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_distance_ellipsoid'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Dump(geometry)
	RETURNS SETOF geometry_dump
	AS '$libdir/postgis-2.0', 'LWGEOM_dump'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION DumpRings(geometry)
	RETURNS SETOF geometry_dump
	AS '$libdir/postgis-2.0', 'LWGEOM_dump_rings'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Envelope(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_envelope'
	LANGUAGE 'c' IMMUTABLE STRICT;
		
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Expand(box2d,float8)
	RETURNS box2d
	AS '$libdir/postgis-2.0', 'BOX2D_expand'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Expand(box3d,float8)
	RETURNS box3d
	AS '$libdir/postgis-2.0', 'BOX3D_expand'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Expand(geometry,float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_expand'
	LANGUAGE 'c' IMMUTABLE STRICT;
		
CREATE AGGREGATE Extent(
	sfunc = ST_combine_bbox,
	basetype = geometry,
	finalfunc = box2d,
	stype = box3d
	);
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Find_Extent(text,text) RETURNS box2d AS
$$
DECLARE
	tablename alias for $1;
	columnname alias for $2;
	myrec RECORD;

BEGIN
	FOR myrec IN EXECUTE 'SELECT ST_Extent("' || columnname || '") As extent FROM "' || tablename || '"' LOOP
		return myrec.extent;
	END LOOP;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Find_Extent(text,text,text) RETURNS box2d AS
$$
DECLARE
	schemaname alias for $1;
	tablename alias for $2;
	columnname alias for $3;
	myrec RECORD;

BEGIN
	FOR myrec IN EXECUTE 'SELECT ST_Extent("' || columnname || '") FROM "' || schemaname || '"."' || tablename || '" As extent ' LOOP
		return myrec.extent;
	END LOOP;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION EndPoint(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_endpoint_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION ExteriorRing(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_exteriorring_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_2d(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_2d'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- an alias for force_3dz
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_3d(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_3dz'
	LANGUAGE 'c' IMMUTABLE STRICT;
	

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_3dm(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_3dm'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_3dz(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_3dz'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_4d(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_4d'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Force_Collection(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_collection'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION ForceRHR(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_clockwise_poly'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomCollFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE
	WHEN geometrytype(GeomFromText($1, $2)) = ''GEOMETRYCOLLECTION''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomCollFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE
	WHEN geometrytype(GeomFromText($1)) = ''GEOMETRYCOLLECTION''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomCollFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE
	WHEN geometrytype(GeomFromWKB($1, $2)) = ''GEOMETRYCOLLECTION''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

	-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomCollFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE
	WHEN geometrytype(GeomFromWKB($1)) = ''GEOMETRYCOLLECTION''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeometryN(geometry,integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_geometryn_collection'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION GeomUnion(geometry,geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','geomunion'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Availability: 1.5.0  -- replaced with postgis_getbbox
CREATE OR REPLACE FUNCTION getbbox(geometry)
	RETURNS box2d
	AS '$libdir/postgis-2.0','LWGEOM_to_BOX2D'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION intersects(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION IsRing(geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION IsSimple(geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0', 'issimple'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION length_spheroid(geometry, spheroid)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_length_ellipsoid_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION length2d_spheroid(geometry, spheroid)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_length2d_ellipsoid'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION length3d_spheroid(geometry, spheroid)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_length_ellipsoid_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineMerge(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'linemerge'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION locate_along_measure(geometry, float8)
	RETURNS geometry
	AS $$ SELECT ST_locate_between_measures($1, $2, $2) $$
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakeBox2d(geometry, geometry)
	RETURNS box2d
	AS '$libdir/postgis-2.0', 'BOX2D_construct'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePolygon(geometry, geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoly'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePolygon(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoly'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPolyFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTIPOLYGON''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
		
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION multi(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_force_multi'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPolyFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''MULTIPOLYGON''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPolyFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTIPOLYGON''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION InteriorRingN(geometry,integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_interiorringn_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION intersection(geometry,geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','intersection'
	LANGUAGE 'c' IMMUTABLE STRICT;

	-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION IsClosed(geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0', 'LWGEOM_isclosed'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION IsEmpty(geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0', 'LWGEOM_isempty'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION IsValid(geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0', 'isvalid'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
-- this is a fake (for back-compatibility)
-- uses 3d if 3d is available, 2d otherwise
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION length3d(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_length_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION length2d(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_length2d_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION length(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_length_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION line_interpolate_point(geometry, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_line_interpolate_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION line_locate_point(geometry, geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0', 'LWGEOM_line_locate_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION line_substring(geometry, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_line_substring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''LINESTRING''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1, $2)) = ''LINESTRING''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineFromMultiPoint(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_line_from_mpoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''LINESTRING''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''LINESTRING''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineStringFromText(text)
	RETURNS geometry
	AS 'SELECT LineFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LineStringFromText(text, int4)
	RETURNS geometry
	AS 'SELECT LineFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LinestringFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''LINESTRING''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION LinestringFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''LINESTRING''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION locate_between_measures(geometry, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_locate_between_m'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION M(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_m_point'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakeBox3d(geometry, geometry)
	RETURNS box3d
	AS '$libdir/postgis-2.0', 'BOX3D_construct'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION makeline_garray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makeline_garray'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE AGGREGATE makeline (
	BASETYPE = geometry,
	SFUNC = pgis_geometry_accum_transfn,
	STYPE = pgis_abs,
	FINALFUNC = pgis_geometry_makeline_finalfn
	);
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakeLine(geometry, geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makeline'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePoint(float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePoint(float8, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoint'
	LANGUAGE 'c' IMMUTABLE STRICT;

	-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePoint(float8, float8, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MakePointM(float8, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makepoint3dm'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- This should really be deprecated -- 2011-01-04 robe
CREATE OR REPLACE FUNCTION max_distance(geometry,geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0', 'LWGEOM_maxdistance2d_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT; 
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION mem_size(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_mem_size'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MLineFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE
	WHEN geometrytype(GeomFromText($1, $2)) = ''MULTILINESTRING''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MLineFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''MULTILINESTRING''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MLineFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''MULTILINESTRING''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MLineFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTILINESTRING''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPointFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1,$2)) = ''MULTIPOINT''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPointFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''MULTIPOINT''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPointFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1,$2)) = ''MULTIPOINT''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPointFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTIPOINT''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPolyFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1, $2)) = ''MULTIPOLYGON''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPolyFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''MULTIPOLYGON''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MPolyFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''MULTIPOLYGON''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiLineFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''MULTILINESTRING''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Availability: 1.2.2
CREATE OR REPLACE FUNCTION MultiLineFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''MULTILINESTRING''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiLineFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTILINESTRING''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiLineStringFromText(text)
	RETURNS geometry
	AS 'SELECT ST_MLineFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiLineStringFromText(text, int4)
	RETURNS geometry
	AS 'SELECT MLineFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPointFromText(text)
	RETURNS geometry
	AS 'SELECT MPointFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPointFromText(text)
	RETURNS geometry
	AS 'SELECT MPointFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPointFromText(text, int4)
	RETURNS geometry
	AS 'SELECT MPointFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPointFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1,$2)) = ''MULTIPOINT''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPointFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''MULTIPOINT''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPolygonFromText(text, int4)
	RETURNS geometry
	AS 'SELECT MPolyFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION MultiPolygonFromText(text)
	RETURNS geometry
	AS 'SELECT MPolyFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION NumInteriorRing(geometry)
	RETURNS integer
	AS '$libdir/postgis-2.0','LWGEOM_numinteriorrings_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION NumInteriorRings(geometry)
	RETURNS integer
	AS '$libdir/postgis-2.0','LWGEOM_numinteriorrings_polygon'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION npoints(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_npoints'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION nrings(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_nrings'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION NumGeometries(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_numgeometries_collection'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION NumPoints(geometry)
	RETURNS int4
	AS '$libdir/postgis-2.0', 'LWGEOM_numpoints_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION overlaps(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- this is a fake (for back-compatibility)
-- uses 3d if 3d is available, 2d otherwise
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION perimeter3d(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_perimeter_poly'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION perimeter2d(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_perimeter2d_poly'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION point_inside_circle(geometry,float8,float8,float8)
	RETURNS bool
	AS '$libdir/postgis-2.0', 'LWGEOM_inside_circle_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''POINT''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1, $2)) = ''POINT''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''POINT''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(ST_GeomFromWKB($1, $2)) = ''POINT''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointN(geometry,integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0','LWGEOM_pointn_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PointOnSurface(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;

	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolyFromText(text)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1)) = ''POLYGON''
	THEN GeomFromText($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolyFromText(text, int4)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromText($1, $2)) = ''POLYGON''
	THEN GeomFromText($1,$2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolyFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1, $2)) = ''POLYGON''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolyFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''POLYGON''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolygonFromText(text, int4)
	RETURNS geometry
	AS 'SELECT PolyFromText($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolygonFromText(text)
	RETURNS geometry
	AS 'SELECT PolyFromText($1)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolygonFromWKB(bytea, int)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1,$2)) = ''POLYGON''
	THEN GeomFromWKB($1, $2)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;

	-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION PolygonFromWKB(bytea)
	RETURNS geometry
	AS '
	SELECT CASE WHEN geometrytype(GeomFromWKB($1)) = ''POLYGON''
	THEN GeomFromWKB($1)
	ELSE NULL END
	'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Polygonize_GArray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'polygonize_garray'
	LANGUAGE 'c' IMMUTABLE STRICT
	;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION relate(geometry,geometry)
	RETURNS text
	AS '$libdir/postgis-2.0','relate_full'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION relate(geometry,geometry,text)
	RETURNS boolean
	AS '$libdir/postgis-2.0','relate_pattern'
	LANGUAGE 'c' IMMUTABLE STRICT;	

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION RemovePoint(geometry, integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_removepoint'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION reverse(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_reverse'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Segmentize(geometry, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_segmentize2d'
	LANGUAGE 'c' IMMUTABLE STRICT;	
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SetPoint(geometry, integer, geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_setpoint_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Availability: 1.1.0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION shift_longitude(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_longitude_shift'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Simplify(geometry, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_simplify2d'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- SnapToGrid(input, size) # xsize=ysize=size, offsets=0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SnapToGrid(geometry, float8, float8, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_snaptogrid'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SnapToGrid(geometry, float8)
	RETURNS geometry
	AS 'SELECT ST_SnapToGrid($1, 0, 0, $2, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- SnapToGrid(input, point_offsets, xsize, ysize, zsize, msize)
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SnapToGrid(geometry, geometry, float8, float8, float8, float8)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_snaptogrid_pointoff'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- SnapToGrid(input, xsize, ysize) # offsets=0
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION SnapToGrid(geometry, float8, float8)
	RETURNS geometry
	AS 'SELECT ST_SnapToGrid($1, 0, 0, $2, $3)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Availability: 1.2.2 -- this should be deprecated (do not think anyone has ever used it)
CREATE OR REPLACE FUNCTION ST_MakeLine_GArray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_makeline_garray'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION StartPoint(geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_startpoint_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION symdifference(geometry,geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','symdifference'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION symmetricdifference(geometry,geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0','symdifference'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION summary(geometry)
	RETURNS text
	AS '$libdir/postgis-2.0', 'LWGEOM_summary'
	LANGUAGE 'c' IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION transform(geometry,integer)
	RETURNS geometry
	AS '$libdir/postgis-2.0','transform'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION touches(geometry,geometry)
	RETURNS boolean
	AS '$libdir/postgis-2.0'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION within(geometry,geometry)
	RETURNS boolean
	AS 'SELECT ST_Within($1, $2)'
	LANGUAGE 'sql' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION X(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_x_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION xmax(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_xmax'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION xmin(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_xmin'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Y(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_y_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION ymax(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_ymax'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION ymin(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_ymin'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION Z(geometry)
	RETURNS float8
	AS '$libdir/postgis-2.0','LWGEOM_z_point'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION zmax(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_zmax'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION zmin(box3d)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','BOX3D_zmin'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION zmflag(geometry)
	RETURNS smallint
	AS '$libdir/postgis-2.0', 'LWGEOM_zmflag'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- end old ogc names that have been replaced with new SQL-MM names --

--- Start Aggregates and supporting functions --
-- Deprecation in: 1.2.3
CREATE AGGREGATE accum (
	sfunc = pgis_geometry_accum_transfn,
	basetype = geometry,
	stype = pgis_abs,
	finalfunc = pgis_geometry_accum_finalfn
	);
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION collect(geometry, geometry)
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'LWGEOM_collect'
	LANGUAGE 'c' IMMUTABLE;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION combine_bbox(box2d,geometry)
	RETURNS box2d
	AS '$libdir/postgis-2.0', 'BOX2D_combine'
	LANGUAGE 'c' IMMUTABLE;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION combine_bbox(box3d,geometry)
	RETURNS box3d
	AS '$libdir/postgis-2.0', 'BOX3D_combine'
	LANGUAGE 'c' IMMUTABLE;

	-- Deprecation in 1.5.0
CREATE OR REPLACE FUNCTION ST_Polygonize_GArray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'polygonize_garray'
	LANGUAGE 'c' IMMUTABLE STRICT
	;

-- Deprecation in 1.4.0
CREATE OR REPLACE FUNCTION ST_unite_garray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0','pgis_union_geometry_array'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE OR REPLACE FUNCTION unite_garray (geometry[])
	RETURNS geometry
	AS '$libdir/postgis-2.0', 'pgis_union_geometry_array'
	LANGUAGE 'c' IMMUTABLE STRICT;
	
-- Deprecation in 1.2.3
CREATE AGGREGATE Extent3d(
	sfunc = combine_bbox,
	basetype = geometry,
	stype = box3d
	);
	
-- Deprecation in 1.2.3
CREATE AGGREGATE memcollect(
	sfunc = ST_collect,
	basetype = geometry,
	stype = geometry
	);

-- Deprecation in 1.2.3
CREATE AGGREGATE MemGeomUnion (
	basetype = geometry,
	sfunc = geomunion,
	stype = geometry
	);

-- End Aggregates and supporting functions --
------------------------------------------------
--Begin 3D functions --
------------------------------------------------

-- Renamed in 2.0.0 to ST_3DLength
CREATE OR REPLACE FUNCTION ST_Length3D(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_length_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Renamed in 2.0.0 to ST_3DLength_spheroid
CREATE OR REPLACE FUNCTION ST_Length_spheroid3D(geometry, spheroid)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0','LWGEOM_length_ellipsoid_linestring'
	LANGUAGE 'c' IMMUTABLE STRICT
	;
	
-- Renamed in 2.0.0 to ST_3DPerimeter
CREATE OR REPLACE FUNCTION ST_Perimeter3D(geometry)
	RETURNS FLOAT8
	AS '$libdir/postgis-2.0', 'LWGEOM_perimeter_poly'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Renamed in 2.0.0 to ST_3DMakeBox
CREATE OR REPLACE FUNCTION ST_MakeBox3D(geometry, geometry)
	RETURNS box3d
	AS '$libdir/postgis-2.0', 'BOX3D_construct'
	LANGUAGE 'c' IMMUTABLE STRICT;

-- Renamed in 2.0.0 to ST_3DExtent
CREATE AGGREGATE ST_Extent3D(
	sfunc = ST_combine_bbox,
	basetype = geometry,
	stype = box3d
	);
--END 3D functions--

