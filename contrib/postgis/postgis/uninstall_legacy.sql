-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- 
-- PostGIS - Spatial Types for PostgreSQL 
-- http://postgis.refractions.net 
-- 
-- This is free software; you can redistribute and/or modify it under 
-- the terms of the GNU General Public Licence. See the COPYING file. 
-- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- 
-- Generated on: Wed Jun  8 19:29:39 2016
--           by: ../utils/create_undef.pl
--         from: legacy.sql
-- 
-- Do not edit manually, your changes will be lost.
-- 
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

BEGIN;

-- Drop all views.
-- Drop all tables.
-- Drop all aggregates.
DROP AGGREGATE IF EXISTS Extent (geometry);
DROP AGGREGATE IF EXISTS makeline (geometry);
DROP AGGREGATE IF EXISTS accum (geometry);
DROP AGGREGATE IF EXISTS Extent3d (geometry);
DROP AGGREGATE IF EXISTS memcollect (geometry);
DROP AGGREGATE IF EXISTS MemGeomUnion (geometry);
DROP AGGREGATE IF EXISTS ST_Extent3D (geometry);
-- Drop all operators classes and families.
-- Drop all operators.
-- Drop all casts.
-- Drop all functions except 0 needed for type definition.
DROP FUNCTION IF EXISTS AsBinary (geometry);
DROP FUNCTION IF EXISTS AsBinary (geometry,text);
DROP FUNCTION IF EXISTS AsText (geometry);
DROP FUNCTION IF EXISTS Estimated_Extent (text,text,text);
DROP FUNCTION IF EXISTS Estimated_Extent (text,text);
DROP FUNCTION IF EXISTS GeomFromText (text, int4);
DROP FUNCTION IF EXISTS GeomFromText (text);
DROP FUNCTION IF EXISTS ndims (geometry);
DROP FUNCTION IF EXISTS SetSRID (geometry,int4);
DROP FUNCTION IF EXISTS SRID (geometry);
DROP FUNCTION IF EXISTS ST_AsBinary (text);
DROP FUNCTION IF EXISTS ST_AsText (bytea);
DROP FUNCTION IF EXISTS addbbox (geometry);
DROP FUNCTION IF EXISTS dropbbox (geometry);
DROP FUNCTION IF EXISTS hasbbox (geometry);
DROP FUNCTION IF EXISTS getsrid (geometry);
DROP FUNCTION IF EXISTS GeometryFromText (text, int4);
DROP FUNCTION IF EXISTS GeometryFromText (text);
DROP FUNCTION IF EXISTS GeomFromWKB (bytea);
DROP FUNCTION IF EXISTS GeomFromWKB (bytea, int);
DROP FUNCTION IF EXISTS noop (geometry);
DROP FUNCTION IF EXISTS SE_EnvelopesIntersect (geometry,geometry);
DROP FUNCTION IF EXISTS SE_Is3D (geometry);
DROP FUNCTION IF EXISTS SE_IsMeasured (geometry);
DROP FUNCTION IF EXISTS SE_Z (geometry);
DROP FUNCTION IF EXISTS SE_M (geometry);
DROP FUNCTION IF EXISTS SE_LocateBetween (geometry, float8, float8);
DROP FUNCTION IF EXISTS SE_LocateAlong (geometry, float8);
DROP FUNCTION IF EXISTS st_box2d (geometry);
DROP FUNCTION IF EXISTS st_box3d (geometry);
DROP FUNCTION IF EXISTS st_box (geometry);
DROP FUNCTION IF EXISTS st_box2d (box3d);
DROP FUNCTION IF EXISTS st_box3d (box2d);
DROP FUNCTION IF EXISTS st_box (box3d);
DROP FUNCTION IF EXISTS st_text (geometry);
DROP FUNCTION IF EXISTS st_geometry (box2d);
DROP FUNCTION IF EXISTS st_geometry (box3d);
DROP FUNCTION IF EXISTS st_geometry (text);
DROP FUNCTION IF EXISTS st_geometry (bytea);
DROP FUNCTION IF EXISTS st_bytea (geometry);
DROP FUNCTION IF EXISTS st_box3d_in (cstring);
DROP FUNCTION IF EXISTS st_box3d_out (box3d);
DROP FUNCTION IF EXISTS rename_geometry_table_constraints ();
DROP FUNCTION IF EXISTS fix_geometry_columns ();
DROP FUNCTION IF EXISTS probe_geometry_columns ();
DROP FUNCTION IF EXISTS st_geometry_lt (geometry, geometry);
DROP FUNCTION IF EXISTS st_geometry_le (geometry, geometry);
DROP FUNCTION IF EXISTS st_geometry_gt (geometry, geometry);
DROP FUNCTION IF EXISTS st_geometry_ge (geometry, geometry);
DROP FUNCTION IF EXISTS st_geometry_eq (geometry, geometry);
DROP FUNCTION IF EXISTS st_geometry_cmp (geometry, geometry);
DROP FUNCTION IF EXISTS Affine (geometry,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8,float8);
DROP FUNCTION IF EXISTS Affine (geometry,float8,float8,float8,float8,float8,float8);
DROP FUNCTION IF EXISTS RotateZ (geometry,float8);
DROP FUNCTION IF EXISTS Rotate (geometry,float8);
DROP FUNCTION IF EXISTS RotateX (geometry,float8);
DROP FUNCTION IF EXISTS RotateY (geometry,float8);
DROP FUNCTION IF EXISTS Scale (geometry,float8,float8,float8);
DROP FUNCTION IF EXISTS Scale (geometry,float8,float8);
DROP FUNCTION IF EXISTS Translate (geometry,float8,float8,float8);
DROP FUNCTION IF EXISTS Translate (geometry,float8,float8);
DROP FUNCTION IF EXISTS TransScale (geometry,float8,float8,float8,float8);
DROP FUNCTION IF EXISTS AddPoint (geometry, geometry);
DROP FUNCTION IF EXISTS AddPoint (geometry, geometry, integer);
DROP FUNCTION IF EXISTS Area (geometry);
DROP FUNCTION IF EXISTS Area2D (geometry);
DROP FUNCTION IF EXISTS AsEWKB (geometry);
DROP FUNCTION IF EXISTS AsEWKB (geometry,text);
DROP FUNCTION IF EXISTS AsEWKT (geometry);
DROP FUNCTION IF EXISTS AsGML (geometry);
DROP FUNCTION IF EXISTS AsGML (geometry, int4);
DROP FUNCTION IF EXISTS AsKML (geometry, int4);
DROP FUNCTION IF EXISTS AsKML (geometry);
DROP FUNCTION IF EXISTS AsKML (int4, geometry, int4);
DROP FUNCTION IF EXISTS AsHEXEWKB (geometry);
DROP FUNCTION IF EXISTS AsHEXEWKB (geometry, text);
DROP FUNCTION IF EXISTS AsSVG (geometry);
DROP FUNCTION IF EXISTS AsSVG (geometry,int4);
DROP FUNCTION IF EXISTS AsSVG (geometry,int4,int4);
DROP FUNCTION IF EXISTS azimuth (geometry,geometry);
DROP FUNCTION IF EXISTS BdPolyFromText (text, integer);
DROP FUNCTION IF EXISTS BdMPolyFromText (text, integer);
DROP FUNCTION IF EXISTS boundary (geometry);
DROP FUNCTION IF EXISTS buffer (geometry,float8,integer);
DROP FUNCTION IF EXISTS buffer (geometry,float8);
DROP FUNCTION IF EXISTS BuildArea (geometry);
DROP FUNCTION IF EXISTS Centroid (geometry);
DROP FUNCTION IF EXISTS Contains (geometry,geometry);
DROP FUNCTION IF EXISTS convexhull (geometry);
DROP FUNCTION IF EXISTS crosses (geometry,geometry);
DROP FUNCTION IF EXISTS distance (geometry,geometry);
DROP FUNCTION IF EXISTS difference (geometry,geometry);
DROP FUNCTION IF EXISTS Dimension (geometry);
DROP FUNCTION IF EXISTS disjoint (geometry,geometry);
DROP FUNCTION IF EXISTS distance_sphere (geometry,geometry);
DROP FUNCTION IF EXISTS distance_spheroid (geometry,geometry,spheroid);
DROP FUNCTION IF EXISTS Dump (geometry);
DROP FUNCTION IF EXISTS DumpRings (geometry);
DROP FUNCTION IF EXISTS Envelope (geometry);
DROP FUNCTION IF EXISTS Expand (box2d,float8);
DROP FUNCTION IF EXISTS Expand (box3d,float8);
DROP FUNCTION IF EXISTS Expand (geometry,float8);
DROP FUNCTION IF EXISTS Find_Extent (text,text);
DROP FUNCTION IF EXISTS Find_Extent (text,text,text);
DROP FUNCTION IF EXISTS EndPoint (geometry);
DROP FUNCTION IF EXISTS ExteriorRing (geometry);
DROP FUNCTION IF EXISTS Force_2d (geometry);
DROP FUNCTION IF EXISTS Force_3d (geometry);
DROP FUNCTION IF EXISTS Force_3dm (geometry);
DROP FUNCTION IF EXISTS Force_3dz (geometry);
DROP FUNCTION IF EXISTS Force_4d (geometry);
DROP FUNCTION IF EXISTS Force_Collection (geometry);
DROP FUNCTION IF EXISTS ForceRHR (geometry);
DROP FUNCTION IF EXISTS GeomCollFromText (text, int4);
DROP FUNCTION IF EXISTS GeomCollFromText (text);
DROP FUNCTION IF EXISTS GeomCollFromWKB (bytea, int);
DROP FUNCTION IF EXISTS GeomCollFromWKB (bytea);
DROP FUNCTION IF EXISTS GeometryN (geometry,integer);
DROP FUNCTION IF EXISTS GeomUnion (geometry,geometry);
DROP FUNCTION IF EXISTS getbbox (geometry);
DROP FUNCTION IF EXISTS intersects (geometry,geometry);
DROP FUNCTION IF EXISTS IsRing (geometry);
DROP FUNCTION IF EXISTS IsSimple (geometry);
DROP FUNCTION IF EXISTS length_spheroid (geometry, spheroid);
DROP FUNCTION IF EXISTS length2d_spheroid (geometry, spheroid);
DROP FUNCTION IF EXISTS length3d_spheroid (geometry, spheroid);
DROP FUNCTION IF EXISTS LineMerge (geometry);
DROP FUNCTION IF EXISTS locate_along_measure (geometry, float8);
DROP FUNCTION IF EXISTS MakeBox2d (geometry, geometry);
DROP FUNCTION IF EXISTS MakePolygon (geometry, geometry[]);
DROP FUNCTION IF EXISTS MakePolygon (geometry);
DROP FUNCTION IF EXISTS MPolyFromWKB (bytea);
DROP FUNCTION IF EXISTS multi (geometry);
DROP FUNCTION IF EXISTS MultiPolyFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MultiPolyFromWKB (bytea);
DROP FUNCTION IF EXISTS InteriorRingN (geometry,integer);
DROP FUNCTION IF EXISTS intersection (geometry,geometry);
DROP FUNCTION IF EXISTS IsClosed (geometry);
DROP FUNCTION IF EXISTS IsEmpty (geometry);
DROP FUNCTION IF EXISTS IsValid (geometry);
DROP FUNCTION IF EXISTS length3d (geometry);
DROP FUNCTION IF EXISTS length2d (geometry);
DROP FUNCTION IF EXISTS length (geometry);
DROP FUNCTION IF EXISTS line_interpolate_point (geometry, float8);
DROP FUNCTION IF EXISTS line_locate_point (geometry, geometry);
DROP FUNCTION IF EXISTS line_substring (geometry, float8, float8);
DROP FUNCTION IF EXISTS LineFromText (text);
DROP FUNCTION IF EXISTS LineFromText (text, int4);
DROP FUNCTION IF EXISTS LineFromMultiPoint (geometry);
DROP FUNCTION IF EXISTS LineFromWKB (bytea, int);
DROP FUNCTION IF EXISTS LineFromWKB (bytea);
DROP FUNCTION IF EXISTS LineStringFromText (text);
DROP FUNCTION IF EXISTS LineStringFromText (text, int4);
DROP FUNCTION IF EXISTS LinestringFromWKB (bytea, int);
DROP FUNCTION IF EXISTS LinestringFromWKB (bytea);
DROP FUNCTION IF EXISTS locate_between_measures (geometry, float8, float8);
DROP FUNCTION IF EXISTS M (geometry);
DROP FUNCTION IF EXISTS MakeBox3d (geometry, geometry);
DROP FUNCTION IF EXISTS makeline_garray  (geometry[]);
DROP FUNCTION IF EXISTS MakeLine (geometry, geometry);
DROP FUNCTION IF EXISTS MakePoint (float8, float8);
DROP FUNCTION IF EXISTS MakePoint (float8, float8, float8);
DROP FUNCTION IF EXISTS MakePoint (float8, float8, float8, float8);
DROP FUNCTION IF EXISTS MakePointM (float8, float8, float8);
DROP FUNCTION IF EXISTS max_distance (geometry,geometry);
DROP FUNCTION IF EXISTS mem_size (geometry);
DROP FUNCTION IF EXISTS MLineFromText (text, int4);
DROP FUNCTION IF EXISTS MLineFromText (text);
DROP FUNCTION IF EXISTS MLineFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MLineFromWKB (bytea);
DROP FUNCTION IF EXISTS MPointFromText (text, int4);
DROP FUNCTION IF EXISTS MPointFromText (text);
DROP FUNCTION IF EXISTS MPointFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MPointFromWKB (bytea);
DROP FUNCTION IF EXISTS MPolyFromText (text, int4);
DROP FUNCTION IF EXISTS MPolyFromText (text);
DROP FUNCTION IF EXISTS MPolyFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MultiLineFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MultiLineFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MultiLineFromWKB (bytea);
DROP FUNCTION IF EXISTS MultiLineStringFromText (text);
DROP FUNCTION IF EXISTS MultiLineStringFromText (text, int4);
DROP FUNCTION IF EXISTS MultiPointFromText (text);
DROP FUNCTION IF EXISTS MultiPointFromText (text);
DROP FUNCTION IF EXISTS MultiPointFromText (text, int4);
DROP FUNCTION IF EXISTS MultiPointFromWKB (bytea, int);
DROP FUNCTION IF EXISTS MultiPointFromWKB (bytea);
DROP FUNCTION IF EXISTS MultiPolygonFromText (text, int4);
DROP FUNCTION IF EXISTS MultiPolygonFromText (text);
DROP FUNCTION IF EXISTS NumInteriorRing (geometry);
DROP FUNCTION IF EXISTS NumInteriorRings (geometry);
DROP FUNCTION IF EXISTS npoints (geometry);
DROP FUNCTION IF EXISTS nrings (geometry);
DROP FUNCTION IF EXISTS NumGeometries (geometry);
DROP FUNCTION IF EXISTS NumPoints (geometry);
DROP FUNCTION IF EXISTS overlaps (geometry,geometry);
DROP FUNCTION IF EXISTS perimeter3d (geometry);
DROP FUNCTION IF EXISTS perimeter2d (geometry);
DROP FUNCTION IF EXISTS point_inside_circle (geometry,float8,float8,float8);
DROP FUNCTION IF EXISTS PointFromText (text);
DROP FUNCTION IF EXISTS PointFromText (text, int4);
DROP FUNCTION IF EXISTS PointFromWKB (bytea);
DROP FUNCTION IF EXISTS PointFromWKB (bytea, int);
DROP FUNCTION IF EXISTS PointN (geometry,integer);
DROP FUNCTION IF EXISTS PointOnSurface (geometry);
DROP FUNCTION IF EXISTS PolyFromText (text);
DROP FUNCTION IF EXISTS PolyFromText (text, int4);
DROP FUNCTION IF EXISTS PolyFromWKB (bytea, int);
DROP FUNCTION IF EXISTS PolyFromWKB (bytea);
DROP FUNCTION IF EXISTS PolygonFromText (text, int4);
DROP FUNCTION IF EXISTS PolygonFromText (text);
DROP FUNCTION IF EXISTS PolygonFromWKB (bytea, int);
DROP FUNCTION IF EXISTS PolygonFromWKB (bytea);
DROP FUNCTION IF EXISTS Polygonize_GArray  (geometry[]);
DROP FUNCTION IF EXISTS relate (geometry,geometry);
DROP FUNCTION IF EXISTS relate (geometry,geometry,text);
DROP FUNCTION IF EXISTS RemovePoint (geometry, integer);
DROP FUNCTION IF EXISTS reverse (geometry);
DROP FUNCTION IF EXISTS Segmentize (geometry, float8);
DROP FUNCTION IF EXISTS SetPoint (geometry, integer, geometry);
DROP FUNCTION IF EXISTS shift_longitude (geometry);
DROP FUNCTION IF EXISTS Simplify (geometry, float8);
DROP FUNCTION IF EXISTS SnapToGrid (geometry, float8, float8, float8, float8);
DROP FUNCTION IF EXISTS SnapToGrid (geometry, float8);
DROP FUNCTION IF EXISTS SnapToGrid (geometry, geometry, float8, float8, float8, float8);
DROP FUNCTION IF EXISTS SnapToGrid (geometry, float8, float8);
DROP FUNCTION IF EXISTS ST_MakeLine_GArray  (geometry[]);
DROP FUNCTION IF EXISTS StartPoint (geometry);
DROP FUNCTION IF EXISTS symdifference (geometry,geometry);
DROP FUNCTION IF EXISTS symmetricdifference (geometry,geometry);
DROP FUNCTION IF EXISTS summary (geometry);
DROP FUNCTION IF EXISTS transform (geometry,integer);
DROP FUNCTION IF EXISTS touches (geometry,geometry);
DROP FUNCTION IF EXISTS within (geometry,geometry);
DROP FUNCTION IF EXISTS X (geometry);
DROP FUNCTION IF EXISTS xmax (box3d);
DROP FUNCTION IF EXISTS xmin (box3d);
DROP FUNCTION IF EXISTS Y (geometry);
DROP FUNCTION IF EXISTS ymax (box3d);
DROP FUNCTION IF EXISTS ymin (box3d);
DROP FUNCTION IF EXISTS Z (geometry);
DROP FUNCTION IF EXISTS zmax (box3d);
DROP FUNCTION IF EXISTS zmin (box3d);
DROP FUNCTION IF EXISTS zmflag (geometry);
DROP FUNCTION IF EXISTS collect (geometry, geometry);
DROP FUNCTION IF EXISTS combine_bbox (box2d,geometry);
DROP FUNCTION IF EXISTS combine_bbox (box3d,geometry);
DROP FUNCTION IF EXISTS ST_Polygonize_GArray  (geometry[]);
DROP FUNCTION IF EXISTS ST_unite_garray  (geometry[]);
DROP FUNCTION IF EXISTS unite_garray  (geometry[]);
DROP FUNCTION IF EXISTS ST_Length3D (geometry);
DROP FUNCTION IF EXISTS ST_Length_spheroid3D (geometry, spheroid);
DROP FUNCTION IF EXISTS ST_Perimeter3D (geometry);
DROP FUNCTION IF EXISTS ST_MakeBox3D (geometry, geometry);
-- Drop all types.
-- Drop all functions needed for types definition.
-- Drop all schemas.

COMMIT;
