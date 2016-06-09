-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- $Id: topology.sql.in.c 10643 2012-11-05 10:25:41Z strk $
--
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2010, 2011 Sandro Santilli <strk@keybit.net>
-- Copyright (C) 2005 Refractions Research Inc.
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Sandro Santilli <strk@keybit.net>
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- STATUS:
--
--  All objects are created in the 'topology' schema.
--
--  We have PostGIS-specific objects and SQL/MM objects.
--  PostGIS-specific objects have no prefix, SQL/MM ones
--  have the ``ST_' prefix.
--  
-- [PostGIS-specific]
--
-- TABLE topology
--  Table storing topology info (name, srid, precision)
--
-- TYPE TopoGeometry
--  Complex type storing topology_id, layer_id, geometry type
--  and topogeometry id.
--
-- DOMAIN TopoElement
--  An array of two elements: element_id and element_type.
--  In fact, an array of integers.
--
-- DOMAIN TopoElementArray
--  An array of element_id,element_type values.
--  In fact, a bidimensional array of integers:
--  '{{id,type}, {id,type}, ...}'
--
-- FUNCTION CreateTopology(name, [srid], [precision])
--  Initialize a new topology (creating schema with
--  edge,face,node,relation) and add a record into
--  the topology.topology table.
--  TODO: add triggers (or rules, or whatever) enforcing
--        precision to the edge and node tables.
--  
-- FUNCTION DropTopology(name)
--    Delete a topology removing reference from the
--    topology.topology table
--
-- FUNCTION GetTopologyId(name)
-- FUNCTION GetTopologySRID(name)
-- FUNCTION GetTopologyName(id)
--    Return info about a Topology
--
-- FUNCTION AddTopoGeometryColumn(toponame, schema, table, column, geomtype)
--    Add a TopoGeometry column to a table, making it a topology layer.
--    Returns created layer id.
--
-- FUNCTION DropTopoGeometryColumn(schema, table, column)
--    Drop a TopoGeometry column, unregister the associated layer,
--    cleanup the relation table.
--
-- FUNCTION CreateTopoGeom(toponame, geomtype, layer_id, topo_objects)
--    Create a TopoGeometry object from existing Topology elements.
--    The "topo_objects" parameter is of TopoElementArray type.
--
-- FUNCTION GetTopoGeomElementArray(toponame, layer_id, topogeom_id)
-- FUNCTION GetTopoGeomElementArray(TopoGeometry)
--    Returns a TopoElementArray object containing the topological
--    elements of the given TopoGeometry.
--
-- FUNCTION GetTopoGeomElements(toponame, layer_id, topogeom_id)
-- FUNCTION GetTopoGeomElements(TopoGeometry)
--    Returns a set of TopoElement objects containing the
--    topological elements of the given TopoGeometry (primitive
--    elements)
--
-- FUNCTION ValidateTopology(toponame)
--    Run validity checks on the topology, returning, for each
--    detected error, a 3-columns row containing error string
--    and references to involved topo elements: error, id1, id2
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Overloaded functions for TopoGeometry inputs
--
-- FUNCTION intersects(TopoGeometry, TopoGeometry)
-- FUNCTION equals(TopoGeometry, TopoGeometry)
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- FUNCTION TopoGeo_AddPoint(toponame, point)
--    Add a Point geometry to the topology
--    TODO: accept a topology/layer id
--          rework to use existing node if existent
--
-- FUNCTION TopoGeo_AddLinestring(toponame, line)
--    Add a LineString geometry to the topology
--    TODO: accept a topology/layer id
--          rework to use existing nodes/edges
--          splitting them if required
--
-- FUNCTION TopoGeo_AddPolygon(toponame, polygon)
--    Add a Polygon geometry to the topology
--    TODO: implement
--
-- TYPE GetFaceEdges_ReturnType
--    Complex type used to return tuples from ST_GetFaceEdges
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- [SQL/MM]
--
--   ST_InitTopoGeo
--    Done, can be modified to include explicit sequences or
--    more constraints. Very noisy due to implicit index creations
--    for primary keys and sequences for serial fields...
--  
--   ST_CreateTopoGeo
--    Complete
--  
--   ST_AddIsoNode
--    Complete
--  
--   ST_RemoveIsoNode
--    Complete
--  
--   ST_MoveIsoNode
--    Complete
--  
--   ST_AddIsoEdge
--    Complete
--
--   ST_RemoveIsoEdge
--    Complete, exceptions untested
--  
--   ST_ChangeEdgeGeom
--    Complete
--
--   ST_NewEdgesSplit
--    Complete
--    Also updates the Relation table
--
--   ST_ModEdgeSplit
--    Complete
--    Also updates the Relation table
--
--   ST_AddEdgeNewFaces
--    Complete
--    Also updates the Relation table
--
--   ST_AddEdgeModFace
--    Complete
--    Also updates the Relation table
--  
--   ST_GetFaceEdges
--    Complete
--
--   ST_ModEdgeHeal
--    Complete
--    Also updates the Relation table
--
--   ST_NewEdgeHeal
--    Complete
--    Also updates the Relation table
--  
--   ST_GetFaceGeometry
--    Implemented using ST_BuildArea()
--  
--   ST_RemEdgeNewFace
--    Complete
--    Also updates the Relation table
--  
--   ST_RemEdgeModFace
--    Complete
--    Also updates the Relation table
--  
--   ST_ValidateTopoGeo
--    Unimplemented (probably a wrapper around ValidateTopology)
--  
--  

-- Uninstalling previous installation isn't really a good habit ...
-- Let people decide about that
-- DROP SCHEMA topology CASCADE;






































CREATE SCHEMA topology;

-- Doing everything outside of a transaction helps
-- upgrading in the best case.
BEGIN;

--={ ----------------------------------------------------------------
--  POSTGIS-SPECIFIC block
--
--  This part contains function NOT in the SQL/MM specification
--
---------------------------------------------------------------------

--
-- Topology table.
-- Stores id,name,precision and SRID of topologies.
--
CREATE TABLE topology.topology (
  id SERIAL NOT NULL PRIMARY KEY,
  name VARCHAR NOT NULL UNIQUE,
  SRID INTEGER NOT NULL,
  precision FLOAT8 NOT NULL,
  hasz BOOLEAN NOT NULL DEFAULT false
);

--{ LayerTrigger()
--
-- Layer integrity trigger
--
CREATE OR REPLACE FUNCTION topology.LayerTrigger()
  RETURNS trigger
AS
$$
DECLARE
  rec RECORD;
  ok BOOL;
  toponame varchar;
  query TEXT;
BEGIN

  --RAISE NOTICE 'LayerTrigger called % % at % level', TG_WHEN, TG_OP, TG_LEVEL;


  IF TG_OP = 'INSERT' THEN
    RAISE EXCEPTION 'LayerTrigger not meant to be called on INSERT';
  ELSIF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'The topology.layer table cannot be updated';
  END IF;


  -- Check for existance of any feature column referencing
  -- this layer
  FOR rec IN SELECT * FROM pg_namespace n, pg_class c, pg_attribute a
    WHERE text(n.nspname) = OLD.schema_name
    AND c.relnamespace = n.oid
    AND text(c.relname) = OLD.table_name
    AND a.attrelid = c.oid
    AND text(a.attname) = OLD.feature_column
  LOOP
    query = 'SELECT * '
      || ' FROM ' || quote_ident(OLD.schema_name)
      || '.' || quote_ident(OLD.table_name)
      || ' WHERE layer_id('
      || quote_ident(OLD.feature_column)||') '
      || '=' || OLD.layer_id
      || ' LIMIT 1';
    --RAISE NOTICE '%', query;
    FOR rec IN EXECUTE query
    LOOP
      RAISE NOTICE 'A feature referencing layer % of topology % still exists in %.%.%', OLD.layer_id, OLD.topology_id, OLD.schema_name, OLD.table_name, OLD.feature_column;
      RETURN NULL;
    END LOOP;
  END LOOP;


  -- Get topology name
  SELECT name FROM topology.topology INTO toponame
    WHERE id = OLD.topology_id;

  IF toponame IS NULL THEN
    RAISE NOTICE 'Could not find name of topology with id %',
      OLD.layer_id;
  END IF;

  -- Check if any record in the relation table references this layer
  FOR rec IN SELECT * FROM pg_namespace
    WHERE text(nspname) = toponame
  LOOP
    query = 'SELECT * '
      || ' FROM ' || quote_ident(toponame)
      || '.relation '
      || ' WHERE layer_id = '|| OLD.layer_id
      || ' LIMIT 1';
    --RAISE NOTICE '%', query;
    FOR rec IN EXECUTE query
    LOOP
      RAISE NOTICE 'A record in %.relation still references layer %', toponame, OLD.layer_id;
      RETURN NULL;
    END LOOP;
  END LOOP;

  RETURN OLD;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
--} LayerTrigger()


--{
-- Layer table.
-- Stores topology layer informations
--
CREATE TABLE topology.layer (
  topology_id INTEGER NOT NULL
    REFERENCES topology.topology(id),
  layer_id integer NOT NULL,
  schema_name VARCHAR NOT NULL,
  table_name VARCHAR NOT NULL,
  feature_column VARCHAR NOT NULL,
  feature_type integer NOT NULL,
  level INTEGER NOT NULL DEFAULT 0,
  child_id INTEGER DEFAULT NULL,
  UNIQUE(schema_name, table_name, feature_column),
  PRIMARY KEY(topology_id, layer_id)
);

CREATE TRIGGER layer_integrity_checks BEFORE UPDATE OR DELETE
ON topology.layer FOR EACH ROW EXECUTE PROCEDURE topology.LayerTrigger();

--} Layer table.

--
-- Type returned by ValidateTopology 
--
CREATE TYPE topology.ValidateTopology_ReturnType AS (
  error varchar,
  id1 integer,
  id2 integer
);

--
-- TopoGeometry type
--
CREATE TYPE topology.TopoGeometry AS (
  topology_id integer,
  layer_id integer,
  id integer,
  type integer -- 1: [multi]point, 2: [multi]line,
               -- 3: [multi]polygon, 4: collection
);

--
-- TopoElement domain
-- 
-- This is an array of two elements: element_id and element_type.
--
-- When used to define _simple_ TopoGeometries,
-- element_type can be:
--  0: a node
--  1: an edge
--  2: a face
-- and element_id will be the node, edge or face identifier
--
-- When used to define _hierarchical_ TopoGeometries,
-- element_type will be the child layer identifier and
-- element_id will be composing TopoGoemetry identifier
--
CREATE DOMAIN topology.TopoElement AS integer[]
  CONSTRAINT DIMENSIONS CHECK (
    array_upper(VALUE, 2) IS NULL
    AND array_upper(VALUE, 1) = 2
  );
ALTER DOMAIN topology.TopoElement ADD
        CONSTRAINT lower_dimension CHECK (
    array_lower(VALUE, 1) = 1
  );
ALTER DOMAIN topology.TopoElement DROP CONSTRAINT 



  type_range;
ALTER DOMAIN topology.TopoElement ADD
        CONSTRAINT type_range CHECK (
    VALUE[2] > 0 
  );

--
-- TopoElementArray domain
--
CREATE DOMAIN topology.TopoElementArray AS integer[][]
  CONSTRAINT DIMENSIONS CHECK (
    array_upper(VALUE, 2) IS NOT NULL
    AND array_upper(VALUE, 2) = 2
    AND array_upper(VALUE, 3) IS NULL
  );

--{ RelationTrigger()
--
-- Relation integrity trigger
--
CREATE OR REPLACE FUNCTION topology.RelationTrigger()
  RETURNS trigger
AS
$$
DECLARE
  toponame varchar;
  topoid integer;
  plyr RECORD; -- parent layer
  rec RECORD;
  ok BOOL;

BEGIN
  IF TG_NARGS != 2 THEN
    RAISE EXCEPTION 'RelationTrigger called with wrong number of arguments';
  END IF;

  topoid = TG_ARGV[0];
  toponame = TG_ARGV[1];

  --RAISE NOTICE 'RelationTrigger called % % on %.relation for a %', TG_WHEN, TG_OP, toponame, TG_LEVEL;


  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'RelationTrigger not meant to be called on DELETE';
  END IF;

  -- Get layer info (and verify it exists)
  ok = false;
  FOR plyr IN EXECUTE 'SELECT * FROM topology.layer '
    || 'WHERE '
    || ' topology_id = ' || topoid
    || ' AND'
    || ' layer_id = ' || NEW.layer_id
  LOOP
    ok = true;
    EXIT;
  END LOOP;
  IF NOT ok THEN
    RAISE EXCEPTION 'Layer % does not exist in topology %',
      NEW.layer_id, topoid;
    RETURN NULL;
  END IF;

  IF plyr.level > 0 THEN -- this is hierarchical layer

    -- ElementType must be the layer child id
    IF NEW.element_type != plyr.child_id THEN
      RAISE EXCEPTION 'Type of elements in layer % must be set to its child layer id %', plyr.layer_id, plyr.child_id;
      RETURN NULL;
    END IF;

    -- ElementId must be an existent TopoGeometry in child layer
    ok = false;
    FOR rec IN EXECUTE 'SELECT topogeo_id FROM '
      || quote_ident(toponame) || '.relation '
      || ' WHERE layer_id = ' || plyr.child_id 
      || ' AND topogeo_id = ' || NEW.element_id
    LOOP
      ok = true;
      EXIT;
    END LOOP;
    IF NOT ok THEN
      RAISE EXCEPTION 'TopoGeometry % does not exist in the child layer %', NEW.element_id, plyr.child_id;
      RETURN NULL;
    END IF;

  ELSE -- this is a basic layer

    -- ElementType must be compatible with layer type
    IF plyr.feature_type != 4
      AND plyr.feature_type != NEW.element_type
    THEN
      RAISE EXCEPTION 'Element of type % is not compatible with layer of type %', NEW.element_type, plyr.feature_type;
      RETURN NULL;
    END IF;

    --
    -- Now lets see if the element is consistent, which
    -- is it exists in the topology tables.
    --

    --
    -- Element is a Node
    --
    IF NEW.element_type = 1 
    THEN
      ok = false;
      FOR rec IN EXECUTE 'SELECT node_id FROM '
        || quote_ident(toponame) || '.node '
        || ' WHERE node_id = ' || NEW.element_id
      LOOP
        ok = true;
        EXIT;
      END LOOP;
      IF NOT ok THEN
        RAISE EXCEPTION 'Node % does not exist in topology %', NEW.element_id, toponame;
        RETURN NULL;
      END IF;

    --
    -- Element is an Edge
    --
    ELSIF NEW.element_type = 2 
    THEN
      ok = false;
      FOR rec IN EXECUTE 'SELECT edge_id FROM '
        || quote_ident(toponame) || '.edge_data '
        || ' WHERE edge_id = ' || abs(NEW.element_id)
      LOOP
        ok = true;
        EXIT;
      END LOOP;
      IF NOT ok THEN
        RAISE EXCEPTION 'Edge % does not exist in topology %', NEW.element_id, toponame;
        RETURN NULL;
      END IF;

    --
    -- Element is a Face
    --
    ELSIF NEW.element_type = 3 
    THEN
      IF NEW.element_id = 0 THEN
        RAISE EXCEPTION 'Face % cannot be associated with any feature', NEW.element_id;
        RETURN NULL;
      END IF;
      ok = false;
      FOR rec IN EXECUTE 'SELECT face_id FROM '
        || quote_ident(toponame) || '.face '
        || ' WHERE face_id = ' || NEW.element_id
      LOOP
        ok = true;
        EXIT;
      END LOOP;
      IF NOT ok THEN
        RAISE EXCEPTION 'Face % does not exist in topology %', NEW.element_id, toponame;
        RETURN NULL;
      END IF;
    END IF;

  END IF;
  
  RETURN NEW;
END;
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
--} RelationTrigger()

--{
--  AddTopoGeometryColumn(toponame, schema, table, colum, type, [child])
--
--  Add a TopoGeometry column to a table, making it a topology layer.
--  Returns created layer id.
--
--
CREATE OR REPLACE FUNCTION topology.AddTopoGeometryColumn(toponame varchar, schema varchar, tbl varchar, col varchar, ltype varchar, child integer)
  RETURNS integer
AS
$$
DECLARE
  intltype integer;
  newlevel integer;
  topoid integer;
  rec RECORD;
  newlayer_id integer;
  query text;
BEGIN

        -- Get topology id
        SELECT id FROM topology.topology into topoid
                WHERE name = toponame;

  IF topoid IS NULL THEN
    RAISE EXCEPTION 'Topology % does not exist', toponame;
  END IF;

  IF ltype ILIKE '%POINT%' OR ltype ILIKE 'PUNTAL' THEN
    intltype = 1;
  ELSIF ltype ILIKE '%LINE%' OR ltype ILIKE 'LINEAL' THEN
    intltype = 2;
  ELSIF ltype ILIKE '%POLYGON%' OR ltype ILIKE 'AREAL' THEN
    intltype = 3;
  ELSIF ltype ILIKE '%COLLECTION%' OR ltype ILIKE 'GEOMETRY' THEN
    intltype = 4;
  ELSE
    RAISE EXCEPTION 'Layer type must be one of POINT,LINE,POLYGON,COLLECTION';
  END IF;

  --
  -- Add new TopoGeometry column in schema.table
  --
  EXECUTE 'ALTER TABLE ' || quote_ident(schema)
    || '.' || quote_ident(tbl) 
    || ' ADD COLUMN ' || quote_ident(col)
    || ' topology.TopoGeometry;';


  --
  -- See if child id exists and extract its level
  --
  IF child IS NOT NULL THEN
    SELECT level + 1 FROM topology.layer
      WHERE layer_id = child
      INTO newlevel;
    IF newlevel IS NULL THEN
      RAISE EXCEPTION 'Child layer % does not exist in topology "%"', child, toponame;
    END IF;
  END IF;

  --
  -- Get new layer id from sequence
  --
  EXECUTE 'SELECT nextval(' ||
    quote_literal(
      quote_ident(toponame) || '.layer_id_seq'
    ) || ')' INTO STRICT newlayer_id;

  EXECUTE 'INSERT INTO ' 
    || 'topology.layer(topology_id, '
    || 'layer_id, level, child_id, schema_name, '
    || 'table_name, feature_column, feature_type) '
    || 'VALUES ('
    || topoid || ','
    || newlayer_id || ',' || COALESCE(newlevel, 0) || ','
    || COALESCE(child::text, 'NULL') || ','
    || quote_literal(schema) || ','
    || quote_literal(tbl) || ','
    || quote_literal(col) || ','
    || intltype || ');';


  --
  -- Create a sequence for TopoGeometries in this new layer
  --
  EXECUTE 'CREATE SEQUENCE ' || quote_ident(toponame)
    || '.topogeo_s_' || newlayer_id;

  --
  -- Add constraints on TopoGeom column
  --
  EXECUTE 'ALTER TABLE ' || quote_ident(schema)
    || '.' || quote_ident(tbl) 
    || ' ADD CONSTRAINT "check_topogeom_' || col || '" CHECK ('
    || 'topology_id(' || quote_ident(col) || ') = ' || topoid
    || ' AND '
    || 'layer_id(' || quote_ident(col) || ') = ' || newlayer_id
    || ' AND '
    || 'type(' || quote_ident(col) || ') = ' || intltype
    || ');';

  --
  -- Add dependency of the feature column on the topology schema
  --
  query = 'INSERT INTO pg_catalog.pg_depend SELECT '
    || 'fcat.oid, fobj.oid, fsub.attnum, tcat.oid, '
    || 'tobj.oid, 0, ''n'' '
    || 'FROM pg_class fcat, pg_namespace fnsp, '
    || ' pg_class fobj, pg_attribute fsub, '
    || ' pg_class tcat, pg_namespace tobj '
    || ' WHERE fcat.relname = ''pg_class'' '
    || ' AND fnsp.nspname = ' || quote_literal(schema)
    || ' AND fobj.relnamespace = fnsp.oid '
    || ' AND fobj.relname = ' || quote_literal(tbl)
    || ' AND fsub.attrelid = fobj.oid '
    || ' AND fsub.attname = ' || quote_literal(col)
    || ' AND tcat.relname = ''pg_namespace'' '
    || ' AND tobj.nspname = ' || quote_literal(toponame);

--
-- The only reason to add this dependency is to avoid
-- simple drop of a feature column. Still, drop cascade
-- will remove both the feature column and the sequence
-- corrupting the topology anyway ...
--


  RETURN newlayer_id;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;
--}{ AddTopoGeometryColumn

CREATE OR REPLACE FUNCTION topology.AddTopoGeometryColumn(varchar, varchar, varchar, varchar, varchar)
  RETURNS integer
AS
$$
  SELECT topology.AddTopoGeometryColumn($1, $2, $3, $4, $5, NULL);
$$
LANGUAGE 'sql' VOLATILE;

--
--} AddTopoGeometryColumn

--{
--  DropTopoGeometryColumn(schema, table, colum)
--
--  Drop a TopoGeometry column, unregister the associated layer,
--  cleanup the relation table.
--
--
CREATE OR REPLACE FUNCTION topology.DropTopoGeometryColumn(schema varchar, tbl varchar, col varchar)
  RETURNS text
AS
$$
DECLARE
  rec RECORD;
  lyrinfo RECORD;
  ok BOOL;
  result text;
BEGIN

        -- Get layer and topology info
  ok = false;
  FOR rec IN EXECUTE 'SELECT t.name as toponame, l.* FROM '
    || 'topology.topology t, topology.layer l '
    || ' WHERE l.topology_id = t.id'
    || ' AND l.schema_name = ' || quote_literal(schema)
    || ' AND l.table_name = ' || quote_literal(tbl)
    || ' AND l.feature_column = ' || quote_literal(col)
  LOOP
    ok = true;
    lyrinfo = rec;
  END LOOP;

  -- Layer not found
  IF NOT ok THEN
    RAISE EXCEPTION 'No layer registered on %.%.%',
      schema,tbl,col;
  END IF;
    
  -- Clean up the topology schema
  FOR rec IN SELECT * FROM pg_namespace
    WHERE text(nspname) = lyrinfo.toponame
  LOOP
    -- Cleanup the relation table
    EXECUTE 'DELETE FROM ' || quote_ident(lyrinfo.toponame)
      || '.relation '
      || ' WHERE '
      || 'layer_id = ' || lyrinfo.layer_id;

    -- Drop the sequence for topogeoms in this layer
    EXECUTE 'DROP SEQUENCE ' || quote_ident(lyrinfo.toponame)
      || '.topogeo_s_' || lyrinfo.layer_id;

  END LOOP;

  ok = false;
  FOR rec IN SELECT * FROM pg_namespace n, pg_class c, pg_attribute a
    WHERE text(n.nspname) = schema
    AND c.relnamespace = n.oid
    AND text(c.relname) = tbl
    AND a.attrelid = c.oid
    AND text(a.attname) = col
  LOOP
    ok = true;
    EXIT;
  END LOOP;


  IF ok THEN
    -- Set feature column to NULL to bypass referential integrity
    -- checks
    EXECUTE 'UPDATE ' || quote_ident(schema) || '.'
      || quote_ident(tbl)
      || ' SET ' || quote_ident(col)
      || ' = NULL';
  END IF;

  -- Delete the layer record
  EXECUTE 'DELETE FROM topology.layer '
    || ' WHERE topology_id = ' || lyrinfo.topology_id
    || ' AND layer_id = ' || lyrinfo.layer_id;

  IF ok THEN
    -- Drop the layer column
    EXECUTE 'ALTER TABLE ' || quote_ident(schema) || '.'
      || quote_ident(tbl)
      || ' DROP ' || quote_ident(col)
      || ' cascade';
  END IF;

  result = 'Layer ' || lyrinfo.layer_id || ' ('
    || schema || '.' || tbl || '.' || col
    || ') dropped';

  RETURN result;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;
--
--} DropTopoGeometryColumn


--{
-- CreateTopoGeom(topology_name, topogeom_type, layer_id, elements)
--
-- Create a TopoGeometry object from Topology elements.
-- The elements parameter is a two-dimensional array. 
-- Every element of the array is either a Topology element represented by
-- (id, type) or a TopoGeometry element represented by (id, layer).
-- The actual semantic depends on the TopoGeometry layer, either at
-- level 0 (elements are topological primitives) or higer (elements
-- are TopoGeoms from child layer).
-- 
-- @param toponame Topology name
--
-- @param tg_type Spatial type of geometry
--          1:[multi]point (puntal)
--          2:[multi]line  (lineal)
--          3:[multi]poly  (areal)
--          4:collection   (mixed)
--
-- @param layer_id Layer identifier
--
-- @param tg_objs Array of components
-- 
-- Return a topology.TopoGeometry object.
--
CREATE OR REPLACE FUNCTION topology.CreateTopoGeom(toponame varchar, tg_type integer, layer_id integer, tg_objs topology.TopoElementArray)
  RETURNS topology.TopoGeometry
AS
$$
DECLARE
  i integer;
  dims varchar;
  outerdims varchar;
  innerdims varchar;
  obj_type integer;
  obj_id integer;
  ret topology.TopoGeometry;
  rec RECORD;
  layertype integer;
  layerlevel integer;
  layerchild integer;
BEGIN

  IF tg_type < 1 OR tg_type > 4 THEN
    RAISE EXCEPTION 'Invalid TopoGeometry type % (must be in the range 1..4)', tg_type;
  END IF;

  -- Get topology id into return TopoGeometry
  SELECT id FROM topology.topology into ret.topology_id
    WHERE name = toponame;

  --
  -- Get layer info
  --
  layertype := NULL;
  FOR rec IN EXECUTE 'SELECT * FROM topology.layer'
    || ' WHERE topology_id = ' || ret.topology_id
    || ' AND layer_id = ' || layer_id
  LOOP
    layertype = rec.feature_type;
    layerlevel = rec.level;
    layerchild = rec.child_id;
  END LOOP;

  -- Check for existence of given layer id
  IF layertype IS NULL THEN
    RAISE EXCEPTION 'No layer with id % is registered with topology %', layer_id, toponame;
  END IF;

  -- Verify compatibility between layer geometry type and
  -- TopoGeom requested geometry type
  IF layertype != 4 and layertype != tg_type THEN
    RAISE EXCEPTION 'A Layer of type % cannot contain a TopoGeometry of type %', layertype, tg_type;
  END IF;

  -- Set layer id and type in return object
  ret.layer_id = layer_id;
  ret.type = tg_type;

  --
  -- Get new TopoGeo id from sequence
  --
  FOR rec IN EXECUTE 'SELECT nextval(' ||
    quote_literal(
      quote_ident(toponame) || '.topogeo_s_' || layer_id
    ) || ')'
  LOOP
    ret.id = rec.nextval;
  END LOOP;

  -- Loop over outer dimension
  i = array_lower(tg_objs, 1);
  LOOP
    obj_id = tg_objs[i][1];
    obj_type = tg_objs[i][2];

    -- Elements of type 0 represent emptiness, just skip them
    IF obj_type = 0 THEN
      IF obj_id != 0 THEN
        RAISE EXCEPTION 'Malformed empty topo element {0,%} -- id must be 0 as well', obj_id;
      END IF;
    ELSE
      IF layerlevel = 0 THEN -- array specifies lower-level objects
        IF tg_type != 4 and tg_type != obj_type THEN
          RAISE EXCEPTION 'A TopoGeometry of type % cannot contain topology elements of type %', tg_type, obj_type;
        END IF;
      ELSE -- array specifies lower-level topogeometries
        IF obj_type != layerchild THEN
          RAISE EXCEPTION 'TopoGeom element layer do not match TopoGeom child layer';
        END IF;
        -- TODO: verify that the referred TopoGeometry really
        -- exists in the relation table ?
      END IF;

      --RAISE NOTICE 'obj:% type:% id:%', i, obj_type, obj_id;

      --
      -- Insert record into the Relation table
      --
      EXECUTE 'INSERT INTO '||quote_ident(toponame)
        || '.relation(topogeo_id, layer_id, '
        || 'element_id,element_type) '
        || ' VALUES ('||ret.id
        ||','||ret.layer_id
        || ',' || obj_id || ',' || obj_type || ');';
    END IF;

    i = i+1;
    IF i > array_upper(tg_objs, 1) THEN
      EXIT;
    END IF;
  END LOOP;

  RETURN ret;

END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
--} CreateTopoGeom(toponame,topogeom_type, layer_id, TopoElementArray)

--{
-- CreateTopoGeom(topology_name, topogeom_type, layer_id) - creates the empty topogeom
CREATE OR REPLACE FUNCTION topology.CreateTopoGeom(toponame varchar, tg_type integer, layer_id integer)
  RETURNS topology.TopoGeometry
AS
$$
  SELECT topology.CreateTopoGeom($1,$2,$3,'{{0,0}}');
$$ LANGUAGE 'sql' VOLATILE STRICT;
--} CreateTopoGeom(toponame, topogeom_type, layer_id)

--{
-- GetTopologyName(topology_id)
--
-- TODO: rewrite in SQL ?
--
CREATE OR REPLACE FUNCTION topology.GetTopologyName(topoid integer)
  RETURNS varchar
AS
$$
DECLARE
  ret varchar;
BEGIN
        SELECT name FROM topology.topology into ret
                WHERE id = topoid;
  RETURN ret;
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} GetTopologyName(topoid)

--{
-- GetTopologyId(toponame)
--
-- TODO: rewrite in SQL ?
--
CREATE OR REPLACE FUNCTION topology.GetTopologyId(toponame varchar)
  RETURNS integer
AS
$$
DECLARE
  ret integer;
BEGIN
        SELECT id FROM topology.topology into ret
                WHERE name = toponame;
  RETURN ret;
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} GetTopologyId(toponame)

--{
-- GetTopologySRID(toponame)
--
CREATE OR REPLACE FUNCTION topology.GetTopologySRID(toponame varchar)
  RETURNS integer
AS $$
  SELECT SRID FROM topology.topology WHERE name = $1;
$$ LANGUAGE 'sql' STABLE STRICT;
--} GetTopologySRID(toponame)
  


--{
-- GetTopoGeomElementArray(toponame, layer_id, topogeom_id)
-- GetTopoGeomElementArray(TopoGeometry)
--
-- Returns a set of element_id,element_type
--
CREATE OR REPLACE FUNCTION topology.GetTopoGeomElementArray(toponame varchar, layer_id integer, tgid integer)
  RETURNS topology.TopoElementArray
AS
$$
DECLARE
  rec RECORD;
  tg_objs varchar := '{';
  i integer;
  query text;
BEGIN

  query = 'SELECT * FROM topology.GetTopoGeomElements('
    || quote_literal(toponame) || ','
    || quote_literal(layer_id) || ','
    || quote_literal(tgid)
    || ') as obj ORDER BY obj';





  -- TODO: why not using array_agg here ?

  i = 1;
  FOR rec IN EXECUTE query
  LOOP
    IF i > 1 THEN
      tg_objs = tg_objs || ',';
    END IF;
    tg_objs = tg_objs || '{'
      || rec.obj[1] || ',' || rec.obj[2]
      || '}';
    i = i+1;
  END LOOP;

  tg_objs = tg_objs || '}';

  RETURN tg_objs;
END;
$$
LANGUAGE 'plpgsql' STABLE STRICT;

CREATE OR REPLACE FUNCTION topology.GetTopoGeomElementArray(tg topology.TopoGeometry)
  RETURNS topology.TopoElementArray
AS
$$
DECLARE
  toponame varchar;
BEGIN
  toponame = topology.GetTopologyName(tg.topology_id);
  RETURN topology.GetTopoGeomElementArray(toponame, tg.layer_id, tg.id);
END;
$$
LANGUAGE 'plpgsql' STABLE STRICT;

--} GetTopoGeomElementArray()

--{
-- GetTopoGeomElements(toponame, layer_id, topogeom_id)
-- GetTopoGeomElements(TopoGeometry)
--
-- Returns a set of element_id,element_type
--
CREATE OR REPLACE FUNCTION topology.GetTopoGeomElements(toponame varchar, layerid integer, tgid integer)
  RETURNS SETOF topology.TopoElement
AS
$$
DECLARE
  ret topology.TopoElement;
  rec RECORD;
  rec2 RECORD;
  query text;
  query2 text;
  lyr RECORD;
  ok bool;
BEGIN

  -- Get layer info
  ok = false;
  FOR rec IN EXECUTE 'SELECT * FROM '
    || ' topology.layer '
    || ' WHERE layer_id = ' || layerid
  LOOP
    lyr = rec;
    ok = true;
  END LOOP;

  IF NOT ok THEN
    RAISE EXCEPTION 'Layer % does not exist', layerid;
  END IF;


  query = 'SELECT abs(element_id) as element_id, element_type FROM '
    || quote_ident(toponame) || '.relation WHERE '
    || ' layer_id = ' || layerid
    || ' AND topogeo_id = ' || quote_literal(tgid)
    || ' ORDER BY element_type, element_id';

  --RAISE NOTICE 'Query: %', query;

  FOR rec IN EXECUTE query
  LOOP
    IF lyr.level > 0 THEN
      query2 = 'SELECT * from topology.GetTopoGeomElements('
        || quote_literal(toponame) || ','
        || rec.element_type
        || ','
        || rec.element_id
        || ') as ret;';
      --RAISE NOTICE 'Query2: %', query2;
      FOR rec2 IN EXECUTE query2
      LOOP
        RETURN NEXT rec2.ret;
      END LOOP;
    ELSE
      ret = '{' || rec.element_id || ',' || rec.element_type || '}';
      RETURN NEXT ret;
    END IF;
  
  END LOOP;

  RETURN;
END;
$$
LANGUAGE 'plpgsql' STABLE STRICT;

CREATE OR REPLACE FUNCTION topology.GetTopoGeomElements(tg topology.TopoGeometry)
  RETURNS SETOF topology.TopoElement
AS
$$
DECLARE
  toponame varchar;
  rec RECORD;
BEGIN
  toponame = topology.GetTopologyName(tg.topology_id);
  FOR rec IN SELECT * FROM topology.GetTopoGeomElements(toponame,
    tg.layer_id,tg.id) as ret
  LOOP
    RETURN NEXT rec.ret;
  END LOOP;
  RETURN;
END;
$$
LANGUAGE 'plpgsql' STABLE STRICT;

--} GetTopoGeomElements()

--{
-- Geometry(TopoGeometry)
--
-- Construct a Geometry from a TopoGeometry.
-- 
-- }{
CREATE OR REPLACE FUNCTION topology.Geometry(topogeom topology.TopoGeometry)
  RETURNS Geometry
AS $$
DECLARE
  toponame varchar;
  geom geometry;
  rec RECORD;
  plyr RECORD;
  clyr RECORD;
  sql TEXT;
BEGIN

  -- Get topology name
  SELECT name FROM topology.topology
  WHERE id = topogeom.topology_id
  INTO toponame;
  IF toponame IS NULL THEN
    RAISE EXCEPTION 'Invalid TopoGeometry (unexistent topology id %)', topogeom.topology_id;
  END IF;

  -- Get layer info
  SELECT * FROM topology.layer
    WHERE topology_id = topogeom.topology_id
    AND layer_id = topogeom.layer_id
    INTO plyr;
  IF plyr IS NULL THEN
    RAISE EXCEPTION 'Could not find TopoGeometry layer % in topology %', topogeom.layer_id, topogeom.topology_id;
  END IF;

  --
  -- If this feature layer is on any level > 0 we will
  -- compute the topological union of all child features
  -- in fact recursing.
  --
  IF plyr.level > 0 THEN -- {

    -- Get child layer info
    SELECT * FROM topology.layer WHERE layer_id = plyr.child_id
      AND topology_id = topogeom.topology_id
      INTO clyr;
    IF clyr IS NULL THEN
      RAISE EXCEPTION 'Invalid layer % in topology % (unexistent child layer %)', topogeom.layer_id, topogeom.topology_id, plyr.child_id;
    END IF;

    sql := 'SELECT st_multi(st_union(topology.Geometry('
      || quote_ident(clyr.feature_column)
      || '))) as geom FROM '
      || quote_ident(clyr.schema_name) || '.'
      || quote_ident(clyr.table_name)
      || ', ' || quote_ident(toponame) || '.relation pr'
      || ' WHERE '
      || ' pr.topogeo_id = ' || topogeom.id
      || ' AND '
      || ' pr.layer_id = ' || topogeom.layer_id
      || ' AND '
      || ' id('||quote_ident(clyr.feature_column)
      || ') = pr.element_id '
      || ' AND '
      || 'layer_id('||quote_ident(clyr.feature_column)
      || ') = pr.element_type ';
    --RAISE DEBUG '%', query;
    EXECUTE sql INTO geom;
      
  ELSIF topogeom.type = 3 THEN -- [multi]polygon -- }{

    sql := 'SELECT st_multi(st_union('
      || 'topology.ST_GetFaceGeometry('
      || quote_literal(toponame) || ','
      || 'element_id))) as g FROM ' 
      || quote_ident(toponame)
      || '.relation WHERE topogeo_id = '
      || topogeom.id || ' AND layer_id = '
      || topogeom.layer_id || ' AND element_type = 3 ';
    EXECUTE sql INTO geom;

  ELSIF topogeom.type = 2 THEN -- [multi]line -- }{

    sql := 
      'SELECT st_multi(ST_LineMerge(ST_Collect(e.geom))) as g FROM '
      || quote_ident(toponame) || '.edge e, '
      || quote_ident(toponame) || '.relation r '
      || ' WHERE r.topogeo_id = ' || topogeom.id
      || ' AND r.layer_id = ' || topogeom.layer_id
      || ' AND r.element_type = 2 '
      || ' AND abs(r.element_id) = e.edge_id';
    EXECUTE sql INTO geom;
  
  ELSIF topogeom.type = 1 THEN -- [multi]point -- }{

    sql :=
      'SELECT st_multi(st_union(n.geom)) as g FROM '
      || quote_ident(toponame) || '.node n, '
      || quote_ident(toponame) || '.relation r '
      || ' WHERE r.topogeo_id = ' || topogeom.id
      || ' AND r.layer_id = ' || topogeom.layer_id
      || ' AND r.element_type = 1 '
      || ' AND r.element_id = n.node_id';
    EXECUTE sql INTO geom;

  ELSIF topogeom.type = 4 THEN -- mixed collection -- }{

    sql := 'WITH areas AS ( SELECT ST_Union('
      || 'topology.ST_GetFaceGeometry('
      || quote_literal(toponame) || ','
      || 'element_id)) as g FROM ' 
      || quote_ident(toponame)
      || '.relation WHERE topogeo_id = '
      || topogeom.id || ' AND layer_id = '
      || topogeom.layer_id || ' AND element_type = 3), '
      || 'lines AS ( SELECT ST_LineMerge(ST_Collect(e.geom)) as g FROM '
      || quote_ident(toponame) || '.edge e, '
      || quote_ident(toponame) || '.relation r '
      || ' WHERE r.topogeo_id = ' || topogeom.id
      || ' AND r.layer_id = ' || topogeom.layer_id
      || ' AND r.element_type = 2 '
      || ' AND abs(r.element_id) = e.edge_id ), '
      || ' points as ( SELECT st_union(n.geom) as g FROM '
      || quote_ident(toponame) || '.node n, '
      || quote_ident(toponame) || '.relation r '
      || ' WHERE r.topogeo_id = ' || topogeom.id
      || ' AND r.layer_id = ' || topogeom.layer_id
      || ' AND r.element_type = 1 '
      || ' AND r.element_id = n.node_id ), '
      || ' un as ( SELECT g FROM areas UNION ALL SELECT g FROM lines '
      || '          UNION ALL SELECT g FROM points ) '
      || 'SELECT ST_Multi(ST_Collect(g)) FROM un';
    EXECUTE sql INTO geom;

  ELSE -- }{

    RAISE EXCEPTION 'Invalid TopoGeometries (unknown type %)', topogeom.type;

  END IF; -- }

  IF geom IS NULL THEN
    IF topogeom.type = 3 THEN -- [multi]polygon
      geom := 'MULTIPOLYGON EMPTY';
    ELSIF topogeom.type = 2 THEN -- [multi]line
      geom := 'MULTILINESTRING EMPTY';
    ELSIF topogeom.type = 1 THEN -- [multi]point
      geom := 'MULTIPOINT EMPTY';
    ELSE
      geom := 'GEOMETRYCOLLECTION EMPTY';
    END IF;
  END IF;

  RETURN geom;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
--} Geometry(TopoGeometry)

-- 7.3+ explicit cast 
CREATE CAST (topology.TopoGeometry AS Geometry) WITH FUNCTION topology.Geometry(topology.TopoGeometry) AS IMPLICIT;

--{
--  ValidateTopology(toponame)
--
--  Return a Set of ValidateTopology_ReturnType containing
--  informations on all topology inconsistencies
--
CREATE OR REPLACE FUNCTION topology.ValidateTopology(toponame varchar)
  RETURNS setof topology.ValidateTopology_ReturnType
AS
$$
DECLARE
  retrec topology.ValidateTopology_ReturnType;
  rec RECORD;
  rec2 RECORD;
  i integer;
  invalid_edges integer[];
  invalid_faces integer[];
  sql text;
BEGIN

  -- Check for coincident nodes
  FOR rec IN EXECUTE 'SELECT a.node_id as id1, b.node_id as id2 FROM '
    || quote_ident(toponame) || '.node a, '
    || quote_ident(toponame) || '.node b '
    || 'WHERE a.node_id < b.node_id '
    || ' AND ST_DWithin(a.geom, b.geom, 0)' -- NOTE: see #1625 and #1789
  LOOP
    retrec.error = 'coincident nodes';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
  END LOOP;

  -- Check for edge crossed nodes
  -- TODO: do this in the single edge loop
  FOR rec IN EXECUTE 'SELECT n.node_id as id1, e.edge_id as id2 FROM '
    || quote_ident(toponame) || '.node n, '
    || quote_ident(toponame) || '.edge e '
    || 'WHERE e.start_node != n.node_id '
    || 'AND e.end_node != n.node_id '
    || 'AND ST_Within(n.geom, e.geom)'
  LOOP
    retrec.error = 'edge crosses node';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
  END LOOP;

  -- Scan all edges 
  FOR rec IN EXECUTE 'SELECT e.geom, e.edge_id as id1, e.left_face, e.right_face FROM '
    || quote_ident(toponame) || '.edge e ORDER BY edge_id'
  LOOP

    -- Any invalid edge becomes a cancer for higher level complexes
    IF NOT ST_IsValid(rec.geom) THEN

      retrec.error = 'invalid edge';
      retrec.id1 = rec.id1;
      retrec.id2 = NULL;
      RETURN NEXT retrec;
      invalid_edges := array_append(invalid_edges, rec.id1);

      IF invalid_faces IS NULL OR NOT rec.left_face = ANY ( invalid_faces )
      THEN
        invalid_faces := array_append(invalid_faces, rec.left_face);
      END IF;

      IF rec.right_face != rec.left_face AND ( invalid_faces IS NULL OR
            NOT rec.right_face = ANY ( invalid_faces ) )
      THEN
        invalid_faces := array_append(invalid_faces, rec.right_face);
      END IF;

      CONTINUE;

    END IF;

    IF NOT ST_IsSimple(rec.geom) THEN
      retrec.error = 'edge not simple';
      retrec.id1 = rec.id1;
      retrec.id2 = NULL;
      RETURN NEXT retrec;
    END IF;

  END LOOP;

  -- Check for edge crossing
  sql := 'SELECT e1.edge_id as id1, e2.edge_id as id2, '
    || ' e1.geom as g1, e2.geom as g2, '
    || 'ST_Relate(e1.geom, e2.geom) as im FROM '
    || quote_ident(toponame) || '.edge e1, '
    || quote_ident(toponame) || '.edge e2 '
    || 'WHERE e1.edge_id < e2.edge_id '
    || ' AND e1.geom && e2.geom ';
  IF invalid_edges IS NOT NULL THEN
    sql := sql || ' AND NOT e1.edge_id = ANY ('
               || quote_literal(invalid_edges) || ')'
               || ' AND NOT e2.edge_id = ANY ('
               || quote_literal(invalid_edges) || ')';
  END IF;

  FOR rec IN EXECUTE sql
  LOOP
    IF ST_RelateMatch(rec.im, 'FF1F**1*2') THEN
      CONTINUE; -- no interior intersection
    END IF;

    --
    -- Closed lines have no boundary, so endpoint
    -- intersection would be considered interior
    -- See http://trac.osgeo.org/postgis/ticket/770
    -- See also full explanation in topology.AddEdge
    --

    IF ST_RelateMatch(rec.im, 'FF10F01F2') THEN
      -- first line (g1) is open, second (g2) is closed
      -- first boundary has puntual intersection with second interior
      --
      -- compute intersection, check it equals second endpoint
      IF ST_Equals(ST_Intersection(rec.g2, rec.g1),
                   ST_StartPoint(rec.g2))
      THEN
        CONTINUE;
      END IF;
    END IF;

    IF ST_RelateMatch(rec.im, 'F01FFF102') THEN
      -- second line (g2) is open, first (g1) is closed
      -- second boundary has puntual intersection with first interior
      -- 
      -- compute intersection, check it equals first endpoint
      IF ST_Equals(ST_Intersection(rec.g2, rec.g1),
                   ST_StartPoint(rec.g1))
      THEN
        CONTINUE;
      END IF;
    END IF;

    IF ST_RelateMatch(rec.im, '0F1FFF1F2') THEN
      -- both lines are closed (boundary intersects nothing)
      -- they have puntual intersection between interiors
      -- 
      -- compute intersection, check it's a single point
      -- and equals first StartPoint _and_ second StartPoint
      IF ST_Equals(ST_Intersection(rec.g1, rec.g2),
                   ST_StartPoint(rec.g1)) AND
         ST_Equals(ST_StartPoint(rec.g1), ST_StartPoint(rec.g2))
      THEN
        CONTINUE;
      END IF;
    END IF;

    retrec.error = 'edge crosses edge';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
  END LOOP;

  -- Check for edge start_node geometry mis-match
  -- TODO: move this in the first edge table scan 
  FOR rec IN EXECUTE 'SELECT e.edge_id as id1, n.node_id as id2 FROM '
    || quote_ident(toponame) || '.edge e, '
    || quote_ident(toponame) || '.node n '
    || 'WHERE e.start_node = n.node_id '
    || 'AND NOT ST_Equals(ST_StartPoint(e.geom), n.geom)'
  LOOP
    retrec.error = 'edge start node geometry mis-match';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
  END LOOP;

  -- Check for edge end_node geometry mis-match
  -- TODO: move this in the first edge table scan 
  FOR rec IN EXECUTE 'SELECT e.edge_id as id1, n.node_id as id2 FROM '
    || quote_ident(toponame) || '.edge e, '
    || quote_ident(toponame) || '.node n '
    || 'WHERE e.end_node = n.node_id '
    || 'AND NOT ST_Equals(ST_EndPoint(e.geom), n.geom)'
  LOOP
    retrec.error = 'edge end node geometry mis-match';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
  END LOOP;

  -- Check for faces w/out edges
  FOR rec IN EXECUTE 'SELECT face_id as id1 FROM '
    || quote_ident(toponame) || '.face '
    || 'WHERE face_id > 0 EXCEPT ( SELECT left_face FROM '
    || quote_ident(toponame) || '.edge '
    || ' UNION SELECT right_face FROM '
    || quote_ident(toponame) || '.edge '
    || ')'
  LOOP
    retrec.error = 'face without edges';
    retrec.id1 = rec.id1;
    retrec.id2 = NULL;
    RETURN NEXT retrec;
  END LOOP;

  -- Now create a temporary table to construct all face geometries
  -- for checking their consistency

  sql := 'CREATE TEMP TABLE face_check ON COMMIT DROP AS '
    || 'SELECT face_id, topology.ST_GetFaceGeometry('
    || quote_literal(toponame) || ', face_id) as geom, mbr FROM '
    || quote_ident(toponame) || '.face WHERE face_id > 0';
  IF invalid_faces IS NOT NULL THEN
    sql := sql || ' AND NOT face_id = ANY ('
               || quote_literal(invalid_faces) || ')';
  END IF;
  EXECUTE sql;

  -- Build a gist index on geom
  EXECUTE 'CREATE INDEX "face_check_gist" ON '
    || 'face_check USING gist (geom);';

  -- Build a btree index on id
  EXECUTE 'CREATE INDEX "face_check_bt" ON ' 
    || 'face_check (face_id);';

  -- Scan the table looking for NULL geometries
  FOR rec IN EXECUTE
    'SELECT f1.face_id FROM '
    || 'face_check f1 WHERE f1.geom IS NULL'
  LOOP
    -- Face missing !
    retrec.error := 'face has no rings';
    retrec.id1 := rec.face_id;
    retrec.id2 := NULL;
    RETURN NEXT retrec;
  END LOOP;


  -- Scan the table looking for overlap or containment
  -- TODO: also check for MBR consistency
  FOR rec IN EXECUTE
    'SELECT f1.geom, f1.face_id as id1, f2.face_id as id2, '
    || ' ST_Relate(f1.geom, f2.geom) as im'
    || ' FROM '
    || 'face_check f1, '
    || 'face_check f2 '
    || 'WHERE f1.face_id < f2.face_id'
    || ' AND f1.geom && f2.geom'
  LOOP

    -- Face overlap
    IF ST_RelateMatch(rec.im, 'T*T***T**') THEN
    retrec.error = 'face overlaps face';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
    END IF;

    -- Face 1 is within face 2 
    IF ST_RelateMatch(rec.im, 'T*F**F***') THEN
    retrec.error = 'face within face';
    retrec.id1 = rec.id1;
    retrec.id2 = rec.id2;
    RETURN NEXT retrec;
    END IF;

    -- Face 1 contains face 2
    IF ST_RelateMatch(rec.im, 'T*****FF*') THEN
    retrec.error = 'face within face';
    retrec.id1 = rec.id2;
    retrec.id2 = rec.id1;
    RETURN NEXT retrec;
    END IF;

  END LOOP;



  DROP TABLE face_check;

  RETURN;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
-- } ValidateTopology(toponame)

--{
--  CreateTopology(name, SRID, precision, hasZ)
--
-- Create a topology schema, add a topology info record
-- in the topology.topology relation, return it's numeric
-- id. 
--
CREATE OR REPLACE FUNCTION topology.CreateTopology(atopology varchar, srid integer, prec float8, hasZ boolean)
RETURNS integer
AS
$$
DECLARE
  rec RECORD;
  topology_id integer;
  ndims integer;
BEGIN

--  FOR rec IN SELECT * FROM pg_namespace WHERE text(nspname) = atopology
--  LOOP
--    RAISE EXCEPTION 'SQL/MM Spatial exception - schema already exists';
--  END LOOP;

  ndims = 2;
  IF hasZ THEN ndims = 3; END IF;

  ------ Fetch next id for the new topology
  FOR rec IN SELECT nextval('topology.topology_id_seq')
  LOOP
    topology_id = rec.nextval;
  END LOOP;


  EXECUTE 'CREATE SCHEMA ' || quote_ident(atopology);

  -------------{ face CREATION
  EXECUTE 
  'CREATE TABLE ' || quote_ident(atopology) || '.face ('
  || 'face_id SERIAL,'
  || ' CONSTRAINT face_primary_key PRIMARY KEY(face_id)'
  || ');';

  -- Add mbr column to the face table 
  EXECUTE
  'SELECT AddGeometryColumn('||quote_literal(atopology)
  ||',''face'',''mbr'','||quote_literal(srid)
  ||',''POLYGON'',2)'; -- 2d only mbr is good enough

  -------------} END OF face CREATION


  --------------{ node CREATION

  EXECUTE 
  'CREATE TABLE ' || quote_ident(atopology) || '.node ('
  || 'node_id SERIAL,'
  --|| 'geom GEOMETRY,'
  || 'containing_face INTEGER,'

  || 'CONSTRAINT node_primary_key PRIMARY KEY(node_id),'

  --|| 'CONSTRAINT node_geometry_type CHECK '
  --|| '( GeometryType(geom) = ''POINT'' ),'

  || 'CONSTRAINT face_exists FOREIGN KEY(containing_face) '
  || 'REFERENCES ' || quote_ident(atopology) || '.face(face_id)'

  || ');';

  -- Add geometry column to the node table 
  EXECUTE
  'SELECT AddGeometryColumn('||quote_literal(atopology)
  ||',''node'',''geom'','||quote_literal(srid)
  ||',''POINT'',' || ndims || ')';

  --------------} END OF node CREATION

  --------------{ edge CREATION

  -- edge_data table
  EXECUTE 
  'CREATE TABLE ' || quote_ident(atopology) || '.edge_data ('
  || 'edge_id SERIAL NOT NULL PRIMARY KEY,'
  || 'start_node INTEGER NOT NULL,'
  || 'end_node INTEGER NOT NULL,'
  || 'next_left_edge INTEGER NOT NULL,'
  || 'abs_next_left_edge INTEGER NOT NULL,'
  || 'next_right_edge INTEGER NOT NULL,'
  || 'abs_next_right_edge INTEGER NOT NULL,'
  || 'left_face INTEGER NOT NULL,'
  || 'right_face INTEGER NOT NULL,'
  --|| 'geom GEOMETRY NOT NULL,'

  --|| 'CONSTRAINT edge_geometry_type CHECK '
  --|| '( GeometryType(geom) = ''LINESTRING'' ),'

  || 'CONSTRAINT start_node_exists FOREIGN KEY(start_node)'
  || ' REFERENCES ' || quote_ident(atopology) || '.node(node_id),'

  || 'CONSTRAINT end_node_exists FOREIGN KEY(end_node) '
  || ' REFERENCES ' || quote_ident(atopology) || '.node(node_id),'

  || 'CONSTRAINT left_face_exists FOREIGN KEY(left_face) '
  || 'REFERENCES ' || quote_ident(atopology) || '.face(face_id),'

  || 'CONSTRAINT right_face_exists FOREIGN KEY(right_face) '
  || 'REFERENCES ' || quote_ident(atopology) || '.face(face_id),'

  || 'CONSTRAINT next_left_edge_exists FOREIGN KEY(abs_next_left_edge)'
  || ' REFERENCES ' || quote_ident(atopology)
  || '.edge_data(edge_id)'
  || ' DEFERRABLE INITIALLY DEFERRED,'

  || 'CONSTRAINT next_right_edge_exists '
  || 'FOREIGN KEY(abs_next_right_edge)'
  || ' REFERENCES ' || quote_ident(atopology)
  || '.edge_data(edge_id) '
  || ' DEFERRABLE INITIALLY DEFERRED'
  || ');';

  -- Add geometry column to the edge_data table 
  EXECUTE
  'SELECT AddGeometryColumn('||quote_literal(atopology)
  ||',''edge_data'',''geom'','||quote_literal(srid)
  ||',''LINESTRING'',' || ndims || ')';


  -- edge standard view (select rule)
  EXECUTE 'CREATE VIEW ' || quote_ident(atopology)
    || '.edge AS SELECT '
    || ' edge_id, start_node, end_node, next_left_edge, '
    || ' next_right_edge, '
    || ' left_face, right_face, geom FROM '
    || quote_ident(atopology) || '.edge_data';

  -- edge standard view description
  EXECUTE 'COMMENT ON VIEW ' || quote_ident(atopology)
    || '.edge IS '
    || '''Contains edge topology primitives''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.edge_id IS '
    || '''Unique identifier of the edge''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.start_node IS '
    || '''Unique identifier of the node at the start of the edge''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.end_node IS '
    || '''Unique identifier of the node at the end of the edge''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.next_left_edge IS '
    || '''Unique identifier of the next edge of the face on the left (when looking in the direction from START_NODE to END_NODE), moving counterclockwise around the face boundary''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.next_right_edge IS '
    || '''Unique identifier of the next edge of the face on the right (when looking in the direction from START_NODE to END_NODE), moving counterclockwise around the face boundary''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.left_face IS '
    || '''Unique identifier of the face on the left side of the edge when looking in the direction from START_NODE to END_NODE''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.right_face IS '
    || '''Unique identifier of the face on the right side of the edge when looking in the direction from START_NODE to END_NODE''';
  EXECUTE 'COMMENT ON COLUMN ' || quote_ident(atopology)
    || '.edge.geom IS '
    || '''The geometry of the edge''';

  -- edge standard view (insert rule)
  EXECUTE 'CREATE RULE edge_insert_rule AS ON INSERT '
          || 'TO ' || quote_ident(atopology)
    || '.edge DO INSTEAD '
                || ' INSERT into ' || quote_ident(atopology)
    || '.edge_data '
                || ' VALUES (NEW.edge_id, NEW.start_node, NEW.end_node, '
    || ' NEW.next_left_edge, abs(NEW.next_left_edge), '
    || ' NEW.next_right_edge, abs(NEW.next_right_edge), '
    || ' NEW.left_face, NEW.right_face, NEW.geom);';

  --------------} END OF edge CREATION

  --------------{ layer sequence 
  EXECUTE 'CREATE SEQUENCE '
    || quote_ident(atopology) || '.layer_id_seq;';
  --------------} layer sequence

  --------------{ relation CREATION
  --
  EXECUTE 
  'CREATE TABLE ' || quote_ident(atopology) || '.relation ('
  || ' topogeo_id integer NOT NULL, '
  || ' layer_id integer NOT NULL, ' 
  || ' element_id integer NOT NULL, '
  || ' element_type integer NOT NULL, '
  || ' UNIQUE(layer_id,topogeo_id,element_id,element_type));';

  EXECUTE 
  'CREATE TRIGGER relation_integrity_checks '
  ||'BEFORE UPDATE OR INSERT ON '
  || quote_ident(atopology) || '.relation FOR EACH ROW '
  || ' EXECUTE PROCEDURE topology.RelationTrigger('
  ||topology_id||','||quote_literal(atopology)||')';
  --------------} END OF relation CREATION

  
  ------- Default (world) face
  EXECUTE 'INSERT INTO ' || quote_ident(atopology) || '.face(face_id) VALUES(0);';

  ------- GiST index on face
  EXECUTE 'CREATE INDEX face_gist ON '
    || quote_ident(atopology)
    || '.face using gist (mbr);';

  ------- GiST index on node
  EXECUTE 'CREATE INDEX node_gist ON '
    || quote_ident(atopology)
    || '.node using gist (geom);';

  ------- GiST index on edge
  EXECUTE 'CREATE INDEX edge_gist ON '
    || quote_ident(atopology)
    || '.edge_data using gist (geom);';

  ------- Indexes on left_face and right_face of edge_data
  ------- NOTE: these indexes speed up GetFaceGeometry (and thus
  -------       TopoGeometry::Geometry) by a factor of 10 !
  -------       See http://trac.osgeo.org/postgis/ticket/806
  EXECUTE 'CREATE INDEX edge_left_face_idx ON '
    || quote_ident(atopology)
    || '.edge_data (left_face);';
  EXECUTE 'CREATE INDEX edge_right_face_idx ON '
    || quote_ident(atopology)
    || '.edge_data (right_face);';

  ------- Indexes on start_node and end_node of edge_data
  ------- NOTE: this indexes speed up node deletion
  -------       by a factor of 1000 !
  -------       See http://trac.osgeo.org/postgis/ticket/2082
  EXECUTE 'CREATE INDEX edge_start_node_idx ON '
    || quote_ident(atopology)
    || '.edge_data (start_node);';
  EXECUTE 'CREATE INDEX edge_end_node_idx ON '
    || quote_ident(atopology)
    || '.edge_data (end_node);';

  -- TODO: consider also adding an index on node.containing_face 

  ------- Add record to the "topology" metadata table
  EXECUTE 'INSERT INTO topology.topology '
    || '(id, name, srid, precision, hasZ) VALUES ('
    || quote_literal(topology_id) || ','
    || quote_literal(atopology) || ','
    || quote_literal(srid) || ',' || quote_literal(prec)
    || ',' || hasZ
    || ')';

  RETURN topology_id;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

--} CreateTopology

--{ CreateTopology wrappers for unspecified srid or precision or hasZ

--  CreateTopology(name, SRID, precision) -- hasZ = false
CREATE OR REPLACE FUNCTION topology.CreateTopology(toponame varchar, srid integer, prec float8)
RETURNS integer AS
' SELECT topology.CreateTopology($1, $2, $3, false);'
LANGUAGE 'sql' VOLATILE STRICT;

--  CreateTopology(name, SRID) -- precision = 0
CREATE OR REPLACE FUNCTION topology.CreateTopology(varchar, integer)
RETURNS integer AS
' SELECT topology.CreateTopology($1, $2, 0); '
LANGUAGE 'sql' VOLATILE STRICT;

--  CreateTopology(name) -- srid = unknown, precision = 0
CREATE OR REPLACE FUNCTION topology.CreateTopology(varchar)
RETURNS integer AS
$$ SELECT topology.CreateTopology($1, ST_SRID('POINT EMPTY'::geometry), 0); $$
LANGUAGE 'sql' VOLATILE STRICT;

--} CreateTopology

--{
--  DropTopology(name)
--
-- Drops a topology schema getting rid of every dependent object.
--
CREATE OR REPLACE FUNCTION topology.DropTopology(atopology varchar)
RETURNS text
AS
$$
DECLARE
  topoid integer;
  rec RECORD;
BEGIN

  -- Get topology id
        SELECT id FROM topology.topology into topoid
                WHERE name = atopology;


  IF topoid IS NOT NULL THEN

    RAISE NOTICE 'Dropping all layers from topology % (%)',
      atopology, topoid;

    -- Drop all layers in the topology
    FOR rec IN EXECUTE 'SELECT * FROM topology.layer WHERE '
      || ' topology_id = ' || topoid
    LOOP

      EXECUTE 'SELECT topology.DropTopoGeometryColumn('
        || quote_literal(rec.schema_name)
        || ','
        || quote_literal(rec.table_name)
        || ','
        || quote_literal(rec.feature_column)
        || ')';
    END LOOP;

    -- Delete record from topology.topology
    EXECUTE 'DELETE FROM topology.topology WHERE id = '
      || topoid;

  END IF;


  -- Drop the schema (if it exists)
  FOR rec IN SELECT * FROM pg_namespace WHERE text(nspname) = atopology
  LOOP
    EXECUTE 'DROP SCHEMA '||quote_ident(atopology)||' CASCADE';
  END LOOP;

  RETURN 'Topology ' || quote_literal(atopology) || ' dropped';
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
--} DropTopology


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--  TopologySummary(name)
--
-- Print an overview about a topology
--
CREATE OR REPLACE FUNCTION topology.TopologySummary(atopology varchar)
RETURNS text
AS
$$
DECLARE
  rec RECORD;
  rec2 RECORD;
  var_topology_id integer;
  n int4;
  ret text;
BEGIN

  ret := 'Topology ' || quote_ident(atopology) ;

  BEGIN
    SELECT * FROM topology.topology WHERE name = atopology INTO STRICT rec;
    -- TODO: catch <no_rows> to give a nice error message
    var_topology_id := rec.id;

    ret := ret || ' (' || rec.id || '), ';
    ret := ret || 'SRID ' || rec.srid || ', '
               || 'precision ' || rec.precision;
    IF rec.hasz THEN ret := ret || ', has Z'; END IF;
    ret := ret || E'\n';
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      ret := ret || E' (X)\n';
  END;

  BEGIN

  BEGIN
    EXECUTE 'SELECT count(node_id) FROM ' || quote_ident(atopology)
      || '.node ' INTO STRICT n;
    ret = ret || n || ' nodes, ';
  EXCEPTION
    WHEN UNDEFINED_TABLE OR INVALID_SCHEMA_NAME THEN
      ret = ret || 'X nodes, ';
  END;

  BEGIN
    EXECUTE 'SELECT count(edge_id) FROM ' || quote_ident(atopology)
      || '.edge_data ' INTO STRICT n;
    ret = ret || n || ' edges, ';
  EXCEPTION
    WHEN UNDEFINED_TABLE OR INVALID_SCHEMA_NAME THEN
      ret = ret || 'X edges, ';
  END;

  BEGIN
    EXECUTE 'SELECT count(face_id) FROM ' || quote_ident(atopology)
      || '.face WHERE face_id != 0' INTO STRICT n;
    ret = ret || n || ' faces, ';
  EXCEPTION
    WHEN UNDEFINED_TABLE OR INVALID_SCHEMA_NAME THEN
      ret = ret || 'X faces, ';
  END;

  BEGIN

    EXECUTE 'SELECT count(*) FROM (SELECT DISTINCT layer_id,topogeo_id FROM '
      || quote_ident(atopology) || '.relation ) foo ' INTO STRICT n;
    ret = ret || n || ' topogeoms in ';

    EXECUTE 'SELECT count(*) FROM (SELECT DISTINCT layer_id FROM '
      || quote_ident(atopology) || '.relation ) foo ' INTO STRICT n;
    ret = ret || n || ' layers' || E'\n';
  EXCEPTION
    WHEN UNDEFINED_TABLE OR INVALID_SCHEMA_NAME THEN
      ret = ret || 'X topogeoms in X layers' || E'\n';
  END;

  -- TODO: print informations about layers
  FOR rec IN SELECT * FROM topology.layer l
    WHERE l.topology_id = var_topology_id
    ORDER by layer_id
  LOOP -- {
    ret = ret || 'Layer ' || rec.layer_id || ', type ';
    CASE
      WHEN rec.feature_type = 1 THEN
        ret = ret || 'Puntal';
      WHEN rec.feature_type = 2 THEN
        ret = ret || 'Lineal';
      WHEN rec.feature_type = 3 THEN
        ret = ret || 'Polygonal';
      WHEN rec.feature_type = 4 THEN
        ret = ret || 'Mixed';
      ELSE 
        ret = ret || '???';
    END CASE;

    ret = ret || ' (' || rec.feature_type || '), ';

    BEGIN

      EXECUTE 'SELECT count(*) FROM ( SELECT DISTINCT topogeo_id FROM '
        || quote_ident(atopology)
        || '.relation r WHERE r.layer_id = ' || rec.layer_id
        || ' ) foo ' INTO STRICT n;

      ret = ret || n || ' topogeoms' || E'\n';

    EXCEPTION WHEN UNDEFINED_TABLE THEN
      ret = ret || 'X topogeoms' || E'\n';
    END;

      IF rec.level > 0 THEN
        ret = ret || ' Hierarchy level ' || rec.level 
                  || ', child layer ' || rec.child_id || E'\n';
      END IF;

      ret = ret || ' Deploy: ';
      IF rec.feature_column != '' THEN
        ret = ret || quote_ident(rec.schema_name) || '.'
                  || quote_ident(rec.table_name) || '.'
                  || quote_ident(rec.feature_column)
                  || E'\n';
      ELSE
        ret = ret || E'NONE (detached)\n';
      END IF;

  END LOOP; -- }

  EXCEPTION
    WHEN INVALID_SCHEMA_NAME THEN
      ret = ret || E'\n- missing schema - ';
    WHEN OTHERS THEN
      RAISE EXCEPTION 'Got % (%)', SQLERRM, SQLSTATE;
  END;


  RETURN ret;
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;

--} TopologySummary

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
-- CopyTopology(name_source, name_target)
--
-- Makes a copy of a topology (primitives + topogeometry collections) .
-- Returns the new topology id.
--
CREATE OR REPLACE FUNCTION topology.CopyTopology(atopology varchar, newtopo varchar)
RETURNS int
AS
$$
DECLARE
  rec RECORD;
  rec2 RECORD;
  oldtopo_id integer;
  newtopo_id integer;
  n int4;
  ret text;
BEGIN

  SELECT * FROM topology.topology where name = atopology
  INTO strict rec;
  oldtopo_id = rec.id;
  -- TODO: more gracefully handle unexistent topology

  SELECT topology.CreateTopology(newtopo, rec.SRID, rec.precision, rec.hasZ)
  INTO strict newtopo_id;

  -- Copy faces
  EXECUTE 'INSERT INTO ' || quote_ident(newtopo)
    || '.face SELECT * FROM ' || quote_ident(atopology)
    || '.face WHERE face_id != 0';
  -- Update faces sequence
  EXECUTE 'SELECT setval(' || quote_literal(
      quote_ident(newtopo) || '.face_face_id_seq'
    ) || ', (SELECT last_value FROM ' 
    || quote_ident(atopology) || '.face_face_id_seq))';

  -- Copy nodes
  EXECUTE 'INSERT INTO ' || quote_ident(newtopo)
    || '.node SELECT * FROM ' || quote_ident(atopology)
    || '.node';
  -- Update node sequence
  EXECUTE 'SELECT setval(' || quote_literal(
      quote_ident(newtopo) || '.node_node_id_seq'
    ) || ', (SELECT last_value FROM ' 
    || quote_ident(atopology) || '.node_node_id_seq))';

  -- Copy edges
  EXECUTE 'INSERT INTO ' || quote_ident(newtopo)
    || '.edge_data SELECT * FROM ' || quote_ident(atopology)
    || '.edge_data';
  -- Update edge sequence
  EXECUTE 'SELECT setval(' || quote_literal(
      quote_ident(newtopo) || '.edge_data_edge_id_seq'
    ) || ', (SELECT last_value FROM ' 
    || quote_ident(atopology) || '.edge_data_edge_id_seq))';

  -- Copy layers and their TopoGeometry sequences 
  FOR rec IN SELECT * FROM topology.layer WHERE topology_id = oldtopo_id
  LOOP
    INSERT INTO topology.layer (topology_id, layer_id, feature_type,
      level, child_id, schema_name, table_name, feature_column) 
      VALUES (newtopo_id, rec.layer_id, rec.feature_type,
              rec.level, rec.child_id, newtopo,
              'LAYER' ||  rec.layer_id, '');
    -- Create layer's TopoGeometry sequences
    EXECUTE 'SELECT last_value FROM ' 
      || quote_ident(atopology) || '.topogeo_s_' || rec.layer_id 
      INTO STRICT n;
    EXECUTE 'CREATE SEQUENCE ' || quote_ident(newtopo)
      || '.topogeo_s_' || rec.layer_id;
    EXECUTE 'SELECT setval(' || quote_literal(
      quote_ident(newtopo) || '.topogeo_s_' || rec.layer_id
      ) || ', ' || n || ')';
  END LOOP;

  -- Copy TopoGeometry definitions
  EXECUTE 'INSERT INTO ' || quote_ident(newtopo)
    || '.relation SELECT * FROM ' || quote_ident(atopology)
    || '.relation';

  RETURN newtopo_id;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

--} TopologySummary


-- Spatial predicates

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011-2012 Sandro Santilli <strk@keybit.net>
-- Copyright (C) 2005 Refractions Research Inc.
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Sandro Santilli <strk@keybit.net>
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--  Overloaded spatial predicates for TopoGeometry inputs
--
--   FUNCTION intersects(TopoGeometry, TopoGeometry)
--   FUNCTION equals(TopoGeometry, TopoGeometry)
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
-- intersects(TopoGeometry, TopoGeometry)
--
CREATE OR REPLACE FUNCTION topology.intersects(tg1 topology.TopoGeometry, tg2 topology.TopoGeometry)
  RETURNS bool
AS
$$
DECLARE
  tgbuf topology.TopoGeometry;
  rec RECORD;
  toponame varchar;
  query text;
BEGIN
  IF tg1.topology_id != tg2.topology_id THEN
    -- TODO: revert to ::geometry instead ?
    RAISE EXCEPTION 'Cannot compute intersection between TopoGeometries from different topologies';
  END IF;

  -- Order TopoGeometries so that tg1 has less-or-same
  -- dimensionality of tg1 (point,line,polygon,collection)
  IF tg1.type > tg2.type THEN
    tgbuf := tg2;
    tg2 := tg1;
    tg1 := tgbuf;
  END IF;

  --RAISE NOTICE 'tg1.id:% tg2.id:%', tg1.id, tg2.id;
  -- Geometry collection are not currently supported
  IF tg2.type = 4 THEN
    RAISE EXCEPTION 'GeometryCollection are not supported by intersects()';
  END IF;

        -- Get topology name
        SELECT name FROM topology.topology into toponame
                WHERE id = tg1.topology_id;

  -- Hierarchical TopoGeometries are not currently supported
  query = 'SELECT level FROM topology.layer'
    || ' WHERE '
    || ' topology_id = ' || tg1.topology_id
    || ' AND '
    || '( layer_id = ' || tg1.layer_id
    || ' OR layer_id = ' || tg2.layer_id
    || ' ) '
    || ' AND level > 0 ';

  --RAISE NOTICE '%', query;

  FOR rec IN EXECUTE query
  LOOP
    -- TODO: revert to ::geometry instead ?
    RAISE EXCEPTION 'Hierarchical TopoGeometries are not currently supported by intersects()';
  END LOOP;

  IF tg1.type = 1 THEN -- [multi]point


    IF tg2.type = 1 THEN -- point/point
  ---------------------------------------------------------
  -- 
  --  Two [multi]point features intersect if they share
  --  any Node 
  --
  --
  --
      query =
        'SELECT a.topogeo_id FROM '
        || quote_ident(toponame) ||
        '.relation a, '
        || quote_ident(toponame) ||
        '.relation b '
        || 'WHERE a.layer_id = ' || tg1.layer_id
        || ' AND b.layer_id = ' || tg2.layer_id
        || ' AND a.topogeo_id = ' || tg1.id
        || ' AND b.topogeo_id = ' || tg2.id
        || ' AND a.element_id = b.element_id '
        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        RETURN TRUE; -- they share an element
      END LOOP;
      RETURN FALSE; -- no elements shared
  --
  ---------------------------------------------------------
      

    ELSIF tg2.type = 2 THEN -- point/line
  ---------------------------------------------------------
  -- 
  --  A [multi]point intersects a [multi]line if they share
  --  any Node. 
  --
  --
  --
      query =
        'SELECT a.topogeo_id FROM '
        || quote_ident(toponame) ||
        '.relation a, '
        || quote_ident(toponame) ||
        '.relation b, '
        || quote_ident(toponame) ||
        '.edge_data e '
        || 'WHERE a.layer_id = ' || tg1.layer_id
        || ' AND b.layer_id = ' || tg2.layer_id
        || ' AND a.topogeo_id = ' || tg1.id
        || ' AND b.topogeo_id = ' || tg2.id
        || ' AND abs(b.element_id) = e.edge_id '
        || ' AND ( '
          || ' e.start_node = a.element_id '
          || ' OR '
          || ' e.end_node = a.element_id '
        || ' )'
        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        RETURN TRUE; -- they share an element
      END LOOP;
      RETURN FALSE; -- no elements shared
  --
  ---------------------------------------------------------

    ELSIF tg2.type = 3 THEN -- point/polygon
  ---------------------------------------------------------
  -- 
  --  A [multi]point intersects a [multi]polygon if any
  --  Node of the point is contained in any face of the
  --  polygon OR ( is end_node or start_node of any edge
  --  of any polygon face ).
  --
  --  We assume the Node-in-Face check is faster becasue
  --  there will be less Faces then Edges in any polygon.
  --
  --
  --
  --
      -- Check if any node is contained in a face
      query =
        'SELECT n.node_id as id FROM '
        || quote_ident(toponame) ||
        '.relation r1, '
        || quote_ident(toponame) ||
        '.relation r2, '
        || quote_ident(toponame) ||
        '.node n '
        || 'WHERE r1.layer_id = ' || tg1.layer_id
        || ' AND r2.layer_id = ' || tg2.layer_id
        || ' AND r1.topogeo_id = ' || tg1.id
        || ' AND r2.topogeo_id = ' || tg2.id
        || ' AND n.node_id = r1.element_id '
        || ' AND r2.element_id = n.containing_face '
        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        --RAISE NOTICE 'Node % in polygon face', rec.id;
        RETURN TRUE; -- one (or more) nodes are
                     -- contained in a polygon face
      END LOOP;

      -- Check if any node is start or end of any polygon
      -- face edge
      query =
        'SELECT n.node_id as nid, e.edge_id as eid '
        || ' FROM '
        || quote_ident(toponame) ||
        '.relation r1, '
        || quote_ident(toponame) ||
        '.relation r2, '
        || quote_ident(toponame) ||
        '.edge_data e, '
        || quote_ident(toponame) ||
        '.node n '
        || 'WHERE r1.layer_id = ' || tg1.layer_id
        || ' AND r2.layer_id = ' || tg2.layer_id
        || ' AND r1.topogeo_id = ' || tg1.id
        || ' AND r2.topogeo_id = ' || tg2.id
        || ' AND n.node_id = r1.element_id '
        || ' AND ( '
        || ' e.left_face = r2.element_id '
        || ' OR '
        || ' e.right_face = r2.element_id '
        || ' ) '
        || ' AND ( '
        || ' e.start_node = r1.element_id '
        || ' OR '
        || ' e.end_node = r1.element_id '
        || ' ) '
        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        --RAISE NOTICE 'Node % on edge % bound', rec.nid, rec.eid;
        RETURN TRUE; -- one node is start or end
                     -- of a face edge
      END LOOP;

      RETURN FALSE; -- no intersection
  --
  ---------------------------------------------------------

    ELSIF tg2.type = 4 THEN -- point/collection
      RAISE EXCEPTION 'Intersection point/collection not implemented yet';

    ELSE
      RAISE EXCEPTION 'Invalid TopoGeometry type', tg2.type;
    END IF;

  ELSIF tg1.type = 2 THEN -- [multi]line
    IF tg2.type = 2 THEN -- line/line
  ---------------------------------------------------------
  -- 
  --  A [multi]line intersects a [multi]line if they share
  --  any Node. 
  --
  --
  --
      query =
        'SELECT e1.start_node FROM '
        || quote_ident(toponame) ||
        '.relation r1, '
        || quote_ident(toponame) ||
        '.relation r2, '
        || quote_ident(toponame) ||
        '.edge_data e1, '
        || quote_ident(toponame) ||
        '.edge_data e2 '
        || 'WHERE r1.layer_id = ' || tg1.layer_id
        || ' AND r2.layer_id = ' || tg2.layer_id
        || ' AND r1.topogeo_id = ' || tg1.id
        || ' AND r2.topogeo_id = ' || tg2.id
        || ' AND abs(r1.element_id) = e1.edge_id '
        || ' AND abs(r2.element_id) = e2.edge_id '
        || ' AND ( '
        || ' e1.start_node = e2.start_node '
        || ' OR '
        || ' e1.start_node = e2.end_node '
        || ' OR '
        || ' e1.end_node = e2.start_node '
        || ' OR '
        || ' e1.end_node = e2.end_node '
        || ' )'
        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        RETURN TRUE; -- they share an element
      END LOOP;
      RETURN FALSE; -- no elements shared
  --
  ---------------------------------------------------------

    ELSIF tg2.type = 3 THEN -- line/polygon
  ---------------------------------------------------------
  -- 
  -- A [multi]line intersects a [multi]polygon if they share
  -- any Node (touch-only case), or if any line edge has any
  -- polygon face on the left or right (full-containment case
  -- + edge crossing case).
  --
  --
      -- E1 are line edges, E2 are polygon edges
      -- R1 are line relations.
      -- R2 are polygon relations.
      -- R2.element_id are FACE ids
      query =
        'SELECT e1.edge_id'
        || ' FROM '
        || quote_ident(toponame) ||
        '.relation r1, '
        || quote_ident(toponame) ||
        '.relation r2, '
        || quote_ident(toponame) ||
        '.edge_data e1, '
        || quote_ident(toponame) ||
        '.edge_data e2 '
        || 'WHERE r1.layer_id = ' || tg1.layer_id
        || ' AND r2.layer_id = ' || tg2.layer_id
        || ' AND r1.topogeo_id = ' || tg1.id
        || ' AND r2.topogeo_id = ' || tg2.id

        -- E1 are line edges
        || ' AND e1.edge_id = abs(r1.element_id) '

        -- E2 are face edges
        || ' AND ( e2.left_face = r2.element_id '
        || '   OR e2.right_face = r2.element_id ) '

        || ' AND ( '

        -- Check if E1 have left-or-right face 
        -- being part of R2.element_id
        || ' e1.left_face = r2.element_id '
        || ' OR '
        || ' e1.right_face = r2.element_id '

        -- Check if E1 share start-or-end node
        -- with any E2.
        || ' OR '
        || ' e1.start_node = e2.start_node '
        || ' OR '
        || ' e1.start_node = e2.end_node '
        || ' OR '
        || ' e1.end_node = e2.start_node '
        || ' OR '
        || ' e1.end_node = e2.end_node '

        || ' ) '

        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        RETURN TRUE; -- either common node
                     -- or edge-in-face
      END LOOP;

      RETURN FALSE; -- no intersection
  --
  ---------------------------------------------------------

    ELSIF tg2.type = 4 THEN -- line/collection
      RAISE EXCEPTION 'Intersection line/collection not implemented yet', tg1.type, tg2.type;

    ELSE
      RAISE EXCEPTION 'Invalid TopoGeometry type', tg2.type;
    END IF;


  ELSIF tg1.type = 3 THEN -- [multi]polygon

    IF tg2.type = 3 THEN -- polygon/polygon
  ---------------------------------------------------------
  -- 
  -- A [multi]polygon intersects a [multi]polygon if they share
  -- any Node (touch-only case), or if any face edge has any of the
  -- other polygon face on the left or right (full-containment case
  -- + edge crossing case).
  --
  --
      -- E1 are poly1 edges.
      -- E2 are poly2 edges
      -- R1 are poly1 relations.
      -- R2 are poly2 relations.
      -- R1.element_id are poly1 FACE ids
      -- R2.element_id are poly2 FACE ids
      query =
        'SELECT e1.edge_id'
        || ' FROM '
        || quote_ident(toponame) ||
        '.relation r1, '
        || quote_ident(toponame) ||
        '.relation r2, '
        || quote_ident(toponame) ||
        '.edge_data e1, '
        || quote_ident(toponame) ||
        '.edge_data e2 '
        || 'WHERE r1.layer_id = ' || tg1.layer_id
        || ' AND r2.layer_id = ' || tg2.layer_id
        || ' AND r1.topogeo_id = ' || tg1.id
        || ' AND r2.topogeo_id = ' || tg2.id

        -- E1 are poly1 edges
        || ' AND ( e1.left_face = r1.element_id '
        || '   OR e1.right_face = r1.element_id ) '

        -- E2 are poly2 edges
        || ' AND ( e2.left_face = r2.element_id '
        || '   OR e2.right_face = r2.element_id ) '

        || ' AND ( '

        -- Check if any edge from a polygon face
        -- has any of the other polygon face
        -- on the left or right 
        || ' e1.left_face = r2.element_id '
        || ' OR '
        || ' e1.right_face = r2.element_id '
        || ' OR '
        || ' e2.left_face = r1.element_id '
        || ' OR '
        || ' e2.right_face = r1.element_id '

        -- Check if E1 share start-or-end node
        -- with any E2.
        || ' OR '
        || ' e1.start_node = e2.start_node '
        || ' OR '
        || ' e1.start_node = e2.end_node '
        || ' OR '
        || ' e1.end_node = e2.start_node '
        || ' OR '
        || ' e1.end_node = e2.end_node '

        || ' ) '

        || ' LIMIT 1';
      --RAISE NOTICE '%', query;
      FOR rec IN EXECUTE query
      LOOP
        RETURN TRUE; -- either common node
                     -- or edge-in-face
      END LOOP;

      RETURN FALSE; -- no intersection
  --
  ---------------------------------------------------------

    ELSIF tg2.type = 4 THEN -- polygon/collection
      RAISE EXCEPTION 'Intersection poly/collection not implemented yet', tg1.type, tg2.type;

    ELSE
      RAISE EXCEPTION 'Invalid TopoGeometry type', tg2.type;
    END IF;

  ELSIF tg1.type = 4 THEN -- collection
    IF tg2.type = 4 THEN -- collection/collection
      RAISE EXCEPTION 'Intersection collection/collection not implemented yet', tg1.type, tg2.type;
    ELSE
      RAISE EXCEPTION 'Invalid TopoGeometry type', tg2.type;
    END IF;

  ELSE
    RAISE EXCEPTION 'Invalid TopoGeometry type %', tg1.type;
  END IF;
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} intersects(TopoGeometry, TopoGeometry)

--{
--  equals(TopoGeometry, TopoGeometry)
--
CREATE OR REPLACE FUNCTION topology.equals(tg1 topology.TopoGeometry, tg2 topology.TopoGeometry)
  RETURNS bool
AS
$$
DECLARE
  rec RECORD;
  toponame varchar;
  query text;
BEGIN

  IF tg1.topology_id != tg2.topology_id THEN
    -- TODO: revert to ::geometry instead ?
    RAISE EXCEPTION 'Cannot compare TopoGeometries from different topologies';
  END IF;

  -- Not the same type, not equal
  IF tg1.type != tg2.type THEN
    RETURN FALSE;
  END IF;

  -- Geometry collection are not currently supported
  IF tg2.type = 4 THEN
    RAISE EXCEPTION 'GeometryCollection are not supported by equals()';
  END IF;

        -- Get topology name
        SELECT name FROM topology.topology into toponame
                WHERE id = tg1.topology_id;

  -- Two geometries are equal if they are composed by 
  -- the same TopoElements
  FOR rec IN EXECUTE 'SELECT * FROM '
    || ' topology.GetTopoGeomElements('
    || quote_literal(toponame) || ', '
    || tg1.layer_id || ',' || tg1.id || ') '
    || ' EXCEPT SELECT * FROM '
    || ' topology.GetTopogeomElements('
    || quote_literal(toponame) || ', '
    || tg2.layer_id || ',' || tg2.id || ');'
  LOOP
    RETURN FALSE;
  END LOOP;

  FOR rec IN EXECUTE 'SELECT * FROM '
    || ' topology.GetTopoGeomElements('
    || quote_literal(toponame) || ', '
    || tg2.layer_id || ',' || tg2.id || ')'
    || ' EXCEPT SELECT * FROM '
    || ' topology.GetTopogeomElements('
    || quote_literal(toponame) || ', '
    || tg1.layer_id || ',' || tg1.id || '); '
  LOOP
    RETURN FALSE;
  END LOOP;
  RETURN TRUE;
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} equals(TopoGeometry, TopoGeometry)



--  Querying

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Andrea Peri <aperi2007@gmail.com>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

-- {
--
-- Andrea Peri (19 Jan 2011) creation
-- Andrea Peri (14 Feb 2011) minor issues
--
-- getnodebypoint(atopology, point, tolerance)
--
-- Retrieve a Node ID given a POINT and a tolerance
-- tolerance = 0 mean exactly intersection
--
-- Returns the integer ID if there is a Node on the Point.
--
-- If there isn't any node in the Point, GetNodeByPoint return 0.
--
-- if near the point there are two or more nodes it throw an exception.
--
CREATE OR REPLACE FUNCTION topology.GetNodeByPoint(atopology varchar, apoint geometry, tol1 float8)
	RETURNS int
AS
$$
DECLARE
	sql text;
	idnode int;
BEGIN
	--
	-- Atopology and apoint are required
	-- 
	IF atopology IS NULL OR apoint IS NULL THEN
		RAISE EXCEPTION 'Invalid null argument';
	END IF;

	--
	-- Apoint must be a point
	--
	IF substring(geometrytype(apoint), 1, 5) != 'POINT'
	THEN
		RAISE EXCEPTION 'Node geometry must be a point';
	END IF;

	--
	-- Tolerance must be >= 0
	--
	IF tol1 < 0
	THEN
		RAISE EXCEPTION 'Tolerance must be >=0';
	END IF;


    if tol1 = 0 then
    	sql := 'SELECT a.node_id FROM ' 
        || quote_ident(atopology) 
        || '.node as a WHERE '
        || '(a.geom && ' || quote_literal(apoint::text)||'::geometry) '
        || ' AND (ST_Intersects(a.geom,' || quote_literal(apoint::text)||'::geometry) );';
    else
    	sql := 'SELECT a.node_id FROM ' 
        || quote_ident(atopology) 
        || '.node as a WHERE '
        || '(ST_DWithin(a.geom,' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ') );';
    end if;

    BEGIN
    EXECUTE sql INTO STRICT idnode;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            idnode = 0;
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Two or more nodes found';
    END;

	RETURN idnode;
	
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} GetNodeByPoint


-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Andrea Peri <aperi2007@gmail.com>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- Andrea Peri (19 Jan 2011) creation
-- Andrea Peri (14 Feb 2011) minor issues
--
-- GetEdgeByPoint(atopology, point, tol)
--
-- Retrieve an Edge ID given a POINT and a tolerance
-- tolerance = 0 mean exactly intersection
--
-- Returns return the integer ID if there is an edge on the Point.
--
-- When the Point is even a Node it raise an exception. 
-- This case is testable with the GetNodeByPoint(atopology, apoint, tol)
--
-- If there isn't any edge in the Point, GetEdgeByPoint return 0.
--
-- if near the point there are two or more edges it throw an exception.
--
CREATE OR REPLACE FUNCTION topology.GetEdgeByPoint(atopology varchar, apoint geometry, tol1 float8)
	RETURNS int
AS
$$
DECLARE
	sql text;
	idedge int;
BEGIN
	--
	-- Atopology and apoint are required
	-- 
	IF atopology IS NULL OR apoint IS NULL THEN
		RAISE EXCEPTION 'Invalid null argument';
	END IF;

	--
	-- Apoint must be a point
	--
	IF substring(geometrytype(apoint), 1, 5) != 'POINT'
	THEN
		RAISE EXCEPTION 'Node geometry must be a point';
	END IF;

	--
	-- Tolerance must be >= 0
	--
	IF tol1 < 0
	THEN
		RAISE EXCEPTION 'Tolerance must be >=0';
	END IF;


    if tol1 = 0 then
    	sql := 'SELECT a.edge_id FROM ' 
        || quote_ident(atopology) 
        || '.edge_data as a WHERE '
        || '(a.geom && ' || quote_literal(apoint::text)||'::geometry) '
        || ' AND (ST_Intersects(a.geom,' || quote_literal(apoint::text)||'::geometry) );';
    else
    	sql := 'SELECT a.edge_id FROM ' 
        || quote_ident(atopology) 
        || '.edge_data as a WHERE '
        || '(ST_DWithin(a.geom,' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ') );';
    end if;

    BEGIN
    EXECUTE sql INTO STRICT idedge;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            idedge = 0;
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Two or more edges found';
    END;

	RETURN idedge;
	
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} GetEdgeByPoint

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Andrea Peri <aperi2007@gmail.com>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- Andrea Peri (27 Feb 2011) creation
--
-- GetFaceByPoint(atopology, point, tol)
--
-- Retrieve a Face ID given a POINT and a tolerance
-- tolerance = 0 mean exactly intersection
--
-- Returns return the integer ID if there is a face on the Point.
--
-- When the Point is even a Node it raise an exception. 
-- This case is testable with the GetNodeByPoint(atopology, apoint, tol)
--
-- If there isn't any face in the Point, GetFaceByPoint return 0.
--
-- if near the point there are two or more faces it throw an exception.
--
CREATE OR REPLACE FUNCTION topology.GetFaceByPoint(atopology varchar, apoint geometry, tol1 float8)
	RETURNS int
AS
$$
DECLARE
	sql text;
	idface int;
BEGIN

    idface := -1;

	--
	-- Atopology and apoint are required
	-- 
	IF atopology IS NULL OR apoint IS NULL THEN
		RAISE EXCEPTION 'Invalid null argument';
	END IF;

	--
	-- Apoint must be a point
	--
	IF substring(geometrytype(apoint), 1, 5) != 'POINT'
	THEN
		RAISE EXCEPTION 'Node geometry must be a point';
	END IF;

	--
	-- Tolerance must be >= 0
	--
	IF tol1 < 0
	THEN
		RAISE EXCEPTION 'Tolerance must be >=0';
	END IF;
    --
    -- first test is to check if there is inside an mbr
    --
    if tol1 = 0 then
    	sql := 'SELECT a.face_id FROM ' 
        || quote_ident(atopology) 
        || '.face as a WHERE '
        || '(a.mbr && ' || quote_literal(apoint::text)||'::geometry) '
        || 'LIMIT 1;';
    else
    	sql := 'SELECT a.face_id FROM ' 
        || quote_ident(atopology) 
        || '.face as a WHERE '
        || '(ST_DWithin(a.mbr,' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ') ) '
        || 'LIMIT 1;';
    end if;

    BEGIN
    EXECUTE sql INTO STRICT idface;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            idface = 0;
    END;

    if idface > 0 then
        --
        -- probably there is something so now check the exact test
        --

    	if tol1 = 0 then
        	sql := 'SELECT e.face_id FROM ('
            || 'SELECT d.face_id,ST_BuildArea(ST_Union(geom)) as geom FROM ('
    		|| 'SELECT b.edge_id as edge_id,b.left_face as face_id,b.geom as geom FROM '
        	|| quote_ident(atopology) || '.edge_data as b,'
            || '(SELECT a.face_id FROM '
    		|| quote_ident(atopology) || '.face as a '
        	|| 'WHERE ST_Intersects(a.mbr,' || quote_literal(apoint::text)||'::geometry)=true'
            || ') as c '
    		|| 'WHERE (b.left_face = c.face_id) '
        	|| ' UNION ALL '
            || 'SELECT b.edge_id as edge_id, b.right_face as face_id, b.geom as geom FROM '
    		|| quote_ident(atopology) || '.edge_data as b,'
        	|| '(SELECT a.face_id FROM '
            || quote_ident(atopology) || '.face as a '
    		|| 'WHERE ST_Intersects(a.mbr,' || quote_literal(apoint::text)||'::geometry)=true'
        	|| ') as c '
            || 'WHERE (b.right_face = c.face_id) '
    		|| ') as d '
        	|| 'GROUP BY face_id '
            || ') as e '
    		|| 'WHERE ST_Intersects(e.geom, ' || quote_literal(apoint::text)||'::geometry)=true;';
        else
        	sql := 'SELECT e.face_id FROM ('
            || 'SELECT d.face_id,ST_BuildArea(ST_Union(geom)) as geom FROM ('
    		|| 'SELECT b.edge_id as edge_id,b.left_face as face_id,b.geom as geom FROM '
        	|| quote_ident(atopology) || '.edge_data as b,'
            || '(SELECT a.face_id FROM '
    		|| quote_ident(atopology) || '.face as a '
        	|| 'WHERE ST_DWithin(a.mbr,' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ')=true'
            || ') as c '
    		|| 'WHERE (b.left_face = c.face_id) '
        	|| ' UNION ALL '
            || 'SELECT b.edge_id as edge_id, b.right_face as face_id, b.geom as geom FROM '
    		|| quote_ident(atopology) || '.edge_data as b,'
        	|| '(SELECT a.face_id FROM '
            || quote_ident(atopology) || '.face as a '
    		|| 'WHERE ST_DWithin(a.mbr,' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ')=true'
        	|| ') as c '
            || 'WHERE (b.right_face = c.face_id) '
    		|| ') as d '
        	|| 'GROUP BY face_id '
            || ') as e '
    		|| 'WHERE ST_DWithin(e.geom, ' || quote_literal(apoint::text)||'::geometry,' || tol1::text || ')=true;';
        end if;
	
    	RAISE DEBUG ' ==> %',sql;

        BEGIN
            EXECUTE sql INTO STRICT idface;
        	EXCEPTION
        	    WHEN NO_DATA_FOUND THEN
                    idface = 0;
                WHEN TOO_MANY_ROWS THEN
                    RAISE EXCEPTION 'Two or more faces found';
            END;

    end if;
    
    RETURN idface;
	
END
$$
LANGUAGE 'plpgsql' STABLE STRICT;
--} GetFaceByPoint



--  Populating

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2010-2012 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Functions used to populate a topology
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -



-- {
--  Compute the max size of the double-precision floating point grid
--  cell required to cover the given geometry
--
--
-- A pragmatic test conducted using algoritm shown here:
-- http://stackoverflow.com/questions/7408407/generate-next-largest-or-smallest-representable-floating-point-number-without-bi
-- showed the "tolerance" growing by an order of magnitude proportionally
-- with the order of magnitude of the input, starting with something like
-- 3.5527136788005009294e-15 for the starting value of 9.0
--
-- }{
CREATE OR REPLACE FUNCTION topology._st_mintolerance(ageom Geometry)
  RETURNS float8
AS $$
    SELECT 3.6 * power(10,  - ( 15 - log(coalesce(
      nullif(
        greatest(abs(ST_xmin($1)), abs(ST_ymin($1)),
                 abs(ST_xmax($1)), abs(ST_ymax($1))),
        0),
      1)) ));
$$ LANGUAGE 'sql' IMMUTABLE STRICT;
-- }

-- {
-- Get tolerance for a given topology
-- and if zero the minimum for the given geometry
--
-- }{
CREATE OR REPLACE FUNCTION topology._st_mintolerance(atopology varchar, ageom Geometry)
  RETURNS float8
AS $$
DECLARE
  ret FLOAT8;
BEGIN
  SELECT COALESCE(
    NULLIF(precision, 0),
    topology._st_mintolerance($2))
  FROM topology.topology
  WHERE name = $1 INTO ret;
  IF NOT FOUND THEN
    RAISE EXCEPTION
      'No topology with name "%" in topology.topology', atopology;
  END IF;
  return ret;
END;
$$ LANGUAGE 'plpgsql' STABLE STRICT;
-- }

--{
--
-- AddNode(atopology, point, allowEdgeSplitting, setContainingFace)
--
-- Add a node primitive to a topology and get its identifier.
-- Returns an existing node at the same location, if any.
--
-- When adding a _new_ node it checks for the existance of any
-- edge crossing the given point, raising an exception if found.
--
-- The newly added nodes have no containing face.
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
--
-- }{
CREATE OR REPLACE FUNCTION topology.AddNode(atopology varchar, apoint geometry, allowEdgeSplitting boolean, setContainingFace boolean DEFAULT false)
	RETURNS int
AS
$$
DECLARE
	nodeid int;
	rec RECORD;
  containing_face int;
BEGIN
	--
	-- Atopology and apoint are required
	-- 
	IF atopology IS NULL OR apoint IS NULL THEN
		RAISE EXCEPTION 'Invalid null argument';
	END IF;

	--
	-- Apoint must be a point
	--
	IF substring(geometrytype(apoint), 1, 5) != 'POINT'
	THEN
		RAISE EXCEPTION 'Node geometry must be a point';
	END IF;

	--
	-- Check if a coincident node already exists
	-- 
	-- We use index AND x/y equality
	--
	FOR rec IN EXECUTE 'SELECT node_id FROM '
		|| quote_ident(atopology) || '.node ' ||
		'WHERE geom && ' || quote_literal(apoint::text) || '::geometry'
		||' AND ST_X(geom) = ST_X('||quote_literal(apoint::text)||'::geometry)'
		||' AND ST_Y(geom) = ST_Y('||quote_literal(apoint::text)||'::geometry)'
	LOOP
		RETURN  rec.node_id;
	END LOOP;

	--
	-- Check if any edge crosses this node
	-- (endpoints are fine)
	--
	FOR rec IN EXECUTE 'SELECT edge_id FROM '
		|| quote_ident(atopology) || '.edge ' 
		|| 'WHERE ST_DWithin('
		|| quote_literal(apoint::text) 
		|| ', geom, 0) AND NOT ST_Equals('
		|| quote_literal(apoint::text)
		|| ', ST_StartPoint(geom)) AND NOT ST_Equals('
		|| quote_literal(apoint::text)
		|| ', ST_EndPoint(geom))'
	LOOP
    IF allowEdgeSplitting THEN
      RETURN ST_ModEdgeSplit(atopology, rec.edge_id, apoint);
    ELSE
		  RAISE EXCEPTION 'An edge crosses the given node.';
    END IF;
	END LOOP;

  IF setContainingFace THEN
    containing_face := topology.GetFaceByPoint(atopology, apoint, 0);



  ELSE
    containing_face := NULL;
  END IF;

	--
	-- Get new node id from sequence
	--
	FOR rec IN EXECUTE 'SELECT nextval(' ||
		quote_literal(
			quote_ident(atopology) || '.node_node_id_seq'
		) || ')'
	LOOP
		nodeid = rec.nextval;
	END LOOP;

	--
	-- Insert the new row
	--
	EXECUTE 'INSERT INTO ' || quote_ident(atopology)
		|| '.node(node_id, containing_face, geom) 
		VALUES(' || nodeid || ',' || coalesce(containing_face::text, 'NULL') || ','
    || quote_literal(apoint::text) || ')';

	RETURN nodeid;
	
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} AddNode

--{
--
-- AddNode(atopology, point)
--
CREATE OR REPLACE FUNCTION topology.AddNode(atopology varchar, apoint geometry)
	RETURNS int
AS
$$
  SELECT topology.AddNode($1, $2, false, false);
$$
LANGUAGE 'sql' VOLATILE;
--} AddNode

--{
--
-- AddEdge(atopology, line)
--
-- Add an edge primitive to a topology and get its identifier.
-- Edge endpoints will be added as nodes if missing.
-- Returns an existing edge at the same location, if any.
--
-- An exception is raised if the given line crosses an existing
-- node or interects with an existing edge on anything but endnodes.
--
-- The newly added edge has "universe" face on both sides
-- and links to itself as per next left/right edge.
-- Calling code is expected to do further linking.
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
-- 
CREATE OR REPLACE FUNCTION topology.AddEdge(atopology varchar, aline geometry)
	RETURNS int
AS
$$
DECLARE
	edgeid int;
	rec RECORD;
  ix geometry; 
BEGIN
	--
	-- Atopology and apoint are required
	-- 
	IF atopology IS NULL OR aline IS NULL THEN
		RAISE EXCEPTION 'Invalid null argument';
	END IF;

	--
	-- Aline must be a linestring
	--
	IF substring(geometrytype(aline), 1, 4) != 'LINE'
	THEN
		RAISE EXCEPTION 'Edge geometry must be a linestring';
	END IF;

	--
	-- Check there's no face registered in the topology
	--
	FOR rec IN EXECUTE 'SELECT count(face_id) FROM '
		|| quote_ident(atopology) || '.face '
		|| ' WHERE face_id != 0 LIMIT 1'
	LOOP
		IF rec.count > 0 THEN
			RAISE EXCEPTION 'AddEdge can only be used against topologies with no faces defined';
		END IF;
	END LOOP;

	--
	-- Check if the edge crosses an existing node
	--
	FOR rec IN EXECUTE 'SELECT node_id FROM '
		|| quote_ident(atopology) || '.node '
		|| 'WHERE ST_Crosses('
		|| quote_literal(aline::text) || '::geometry, geom'
		|| ')'
	LOOP
		RAISE EXCEPTION 'Edge crosses node %', rec.node_id;
	END LOOP;

	--
	-- Check if the edge intersects an existing edge
	-- on anything but endpoints
	--
	-- Following DE-9 Intersection Matrix represent
	-- the only relation we accept. 
	--
	--    F F 1
	--    F * *
	--    1 * 2
	--
	-- Example1: linestrings touching at one endpoint
	--    FF1 F00 102
	--    FF1 F** 1*2 <-- our match
	--
	-- Example2: linestrings touching at both endpoints
	--    FF1 F0F 1F2
	--    FF1 F** 1*2 <-- our match
	--
	FOR rec IN EXECUTE 'SELECT edge_id, geom, ST_Relate('
		|| quote_literal(aline::text)
		|| '::geometry, geom, 2) as im'
		|| ' FROM '
		|| quote_ident(atopology) || '.edge '
		|| 'WHERE '
		|| quote_literal(aline::text) || '::geometry && geom'

	LOOP

	  IF ST_RelateMatch(rec.im, 'FF1F**1*2') THEN
	    CONTINUE; -- no interior intersection
	  END IF;

	  -- Reuse an EQUAL edge (be it closed or not)
	  IF ST_RelateMatch(rec.im, '1FFF*FFF2') THEN



	      RETURN rec.edge_id;
	  END IF;

	  -- WARNING: the constructive operation might throw an exception
	  BEGIN
	    ix = ST_Intersection(rec.geom, aline);
	  EXCEPTION
	  WHEN OTHERS THEN
	    RAISE NOTICE 'Could not compute intersection between input edge (%) and edge % (%)', aline::text, rec.edge_id, rec.geom::text;
	  END;

	  RAISE EXCEPTION 'Edge intersects (not on endpoints) with existing edge % at or near point %', rec.edge_id, ST_AsText(ST_PointOnSurface(ix));

	END LOOP;

	--
	-- Get new edge id from sequence
	--
	FOR rec IN EXECUTE 'SELECT nextval(' ||
		quote_literal(
			quote_ident(atopology) || '.edge_data_edge_id_seq'
		) || ')'
	LOOP
		edgeid = rec.nextval;
	END LOOP;

	--
	-- Insert the new row
	--
	EXECUTE 'INSERT INTO '
		|| quote_ident(atopology)
		|| '.edge(edge_id, start_node, end_node, '
		|| 'next_left_edge, next_right_edge, '
		|| 'left_face, right_face, '
		|| 'geom) '
		|| ' VALUES('

		-- edge_id
		|| edgeid ||','

		-- start_node
		|| 'topology.addNode('
		|| quote_literal(atopology)
		|| ', ST_StartPoint('
		|| quote_literal(aline::text)
		|| ')) ,'

		-- end_node
		|| 'topology.addNode('
		|| quote_literal(atopology)
		|| ', ST_EndPoint('
		|| quote_literal(aline::text)
		|| ')) ,'

		-- next_left_edge
		|| -edgeid ||','

		-- next_right_edge
		|| edgeid ||','

		-- left_face
		|| '0,'

		-- right_face
		|| '0,'

		-- geom
		||quote_literal(aline::text)
		|| ')';

	RETURN edgeid;
	
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} AddEdge

--{
--
-- AddFace(atopology, poly, [<force_new>=true])
--
-- Add a face primitive to a topology and get its identifier.
-- Returns an existing face at the same location, if any, unless
-- true is passed as the force_new argument
--
-- For a newly added face, its edges will be appropriately
-- linked (marked as left-face or right-face), and any contained
-- edges and nodes would also be marked as such.
--
-- When forcing re-registration of an existing face, no action will be
-- taken to deal with the face being substituted. Which means
-- a record about the old face and any record in the relation table
-- referencing the existing face will remain untouched, effectively
-- leaving the topology in a possibly invalid state.
-- It is up to the caller to deal with that.
--
-- The target topology is assumed to be valid (containing no
-- self-intersecting edges).
--
-- An exception is raised if:
--  o The polygon boundary is not fully defined by existing edges.
--  o The polygon overlaps an existing face.
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
-- 
CREATE OR REPLACE FUNCTION topology.AddFace(atopology varchar, apoly geometry, force_new boolean DEFAULT FALSE)
	RETURNS int
AS
$$
DECLARE
  bounds geometry;
  symdif geometry;
  faceid int;
  rec RECORD;
  rrec RECORD;
  relate text;
  right_edges int[];
  left_edges int[];
  all_edges geometry;
  old_faceid int;
  old_edgeid int;
  sql text;
  right_side bool;
  edgeseg geometry;
  p1 geometry;
  p2 geometry;
  p3 geometry;
  loc float8;
  segnum int;
  numsegs int;
BEGIN
  --
  -- Atopology and apoly are required
  -- 
  IF atopology IS NULL OR apoly IS NULL THEN
    RAISE EXCEPTION 'Invalid null argument';
  END IF;

  --
  -- Aline must be a polygon
  --
  IF substring(geometrytype(apoly), 1, 4) != 'POLY'
  THEN
    RAISE EXCEPTION 'Face geometry must be a polygon';
  END IF;

  for rrec IN SELECT (ST_DumpRings(ST_ForceRHR(apoly))).geom
  LOOP -- {
    --
    -- Find all bounds edges, forcing right-hand-rule
    -- to know what's left and what's right...
    --
    bounds = ST_Boundary(rrec.geom);

    sql := 'SELECT e.geom, e.edge_id, '
      || 'e.left_face, e.right_face FROM '
      || quote_ident(atopology) || '.edge e, (SELECT '
      || quote_literal(bounds::text)
      || '::geometry as geom) r WHERE '
      || 'r.geom && e.geom'
    ;
    -- RAISE DEBUG 'SQL: %', sql;
    FOR rec IN EXECUTE sql
    LOOP -- {
      --RAISE DEBUG 'Edge % has bounding box intersection', rec.edge_id;

      -- Find first non-empty segment of the edge
      numsegs = ST_NumPoints(rec.geom);
      segnum = 1;
      WHILE segnum < numsegs LOOP
        p1 = ST_PointN(rec.geom, segnum);
        p2 = ST_PointN(rec.geom, segnum+1);
        IF ST_Distance(p1, p2) > 0 THEN
          EXIT;
        END IF;
        segnum = segnum + 1;
      END LOOP;

      IF segnum = numsegs THEN
        RAISE WARNING 'Edge % is collapsed', rec.edge_id;
        CONTINUE; -- we don't want to spend time on it
      END IF;

      edgeseg = ST_MakeLine(p1, p2);

      -- Skip non-covered edges
      IF NOT ST_Equals(p2, ST_EndPoint(rec.geom)) THEN
        IF NOT ( _ST_Intersects(bounds, p1) AND _ST_Intersects(bounds, p2) )
        THEN
          --RAISE DEBUG 'Edge % has points % and % not intersecting with ring bounds', rec.edge_id, st_astext(p1), st_astext(p2);
          CONTINUE;
        END IF;
      ELSE
        -- must be a 2-points only edge, let's use Covers (more expensive)
        IF NOT _ST_Covers(bounds, edgeseg) THEN
          --RAISE DEBUG 'Edge % is not covered by ring', rec.edge_id;
          CONTINUE;
        END IF;
      END IF;

      p3 = ST_StartPoint(bounds);
      IF ST_DWithin(edgeseg, p3, 0) THEN
        -- Edge segment covers ring endpoint, See bug #874
        loc = ST_Line_Locate_Point(edgeseg, p3);
        -- WARNING: this is as robust as length of edgeseg allows...
        IF loc > 0.9 THEN
          -- shift last point down 
          p2 = ST_Line_Interpolate_Point(edgeseg, loc - 0.1);
        ELSIF loc < 0.1 THEN
          -- shift first point up
          p1 = ST_Line_Interpolate_Point(edgeseg, loc + 0.1); 
        ELSE
          -- when ring start point is in between, we swap the points
          p3 = p1; p1 = p2; p2 = p3;
        END IF;
      END IF;

      right_side = ST_Line_Locate_Point(bounds, p1) < 
                   ST_Line_Locate_Point(bounds, p2);
  





      IF right_side THEN
        right_edges := array_append(right_edges, rec.edge_id);
        old_faceid = rec.right_face;
      ELSE
        left_edges := array_append(left_edges, rec.edge_id);
        old_faceid = rec.left_face;
      END IF;

      IF faceid IS NULL OR faceid = 0 THEN
        faceid = old_faceid;
        old_edgeid = rec.edge_id;
      ELSIF faceid != old_faceid THEN
        RAISE EXCEPTION 'Edge % has face % registered on the side of this face, while edge % has face % on the same side', rec.edge_id, old_faceid, old_edgeid, faceid;
      END IF;

      -- Collect all edges for final full coverage check
      all_edges = ST_Collect(all_edges, rec.geom);

    END LOOP; -- }
  END LOOP; -- }

  IF all_edges IS NULL THEN
    RAISE EXCEPTION 'Found no edges on the polygon boundary';
  END IF;








  --
  -- Check that all edges found, taken togheter,
  -- fully match the ring boundary and nothing more
  --
  -- If the test fail either we need to add more edges
  -- from the polygon ring or we need to split
  -- some of the existing ones.
  -- 
  bounds = ST_Boundary(apoly);
  IF NOT ST_isEmpty(ST_SymDifference(bounds, all_edges)) THEN
    IF NOT ST_isEmpty(ST_Difference(bounds, all_edges)) THEN
      RAISE EXCEPTION 'Polygon boundary is not fully defined by existing edges at or near point %', ST_AsText(ST_PointOnSurface(ST_Difference(bounds, all_edges)));
    ELSE
      RAISE EXCEPTION 'Existing edges cover polygon boundary and more at or near point % (invalid topology?)', ST_AsText(ST_PointOnSurface(ST_Difference(all_edges, bounds)));
    END IF;
  END IF;

  IF faceid IS NOT NULL AND faceid != 0 THEN
    IF NOT force_new THEN



      RETURN faceid;
    ELSE



    END IF;
  END IF;

  --
  -- Get new face id from sequence
  --
  FOR rec IN EXECUTE 'SELECT nextval(' ||
    quote_literal(
      quote_ident(atopology) || '.face_face_id_seq'
    ) || ')'
  LOOP
    faceid = rec.nextval;
  END LOOP;

  --
  -- Insert new face 
  --
  EXECUTE 'INSERT INTO '
    || quote_ident(atopology)
    || '.face(face_id, mbr) VALUES('
    -- face_id
    || faceid || ','
    -- minimum bounding rectangle
    || quote_literal(ST_Envelope(apoly)::text)
    || ')';

  --
  -- Update all edges having this face on the left
  --
  IF left_edges IS NOT NULL THEN
    EXECUTE 'UPDATE '
    || quote_ident(atopology)
    || '.edge_data SET left_face = '
    || quote_literal(faceid)
    || ' WHERE edge_id = ANY('
    || quote_literal(left_edges)
    || ') ';
  END IF;

  --
  -- Update all edges having this face on the right
  --
  IF right_edges IS NOT NULL THEN
    EXECUTE 'UPDATE '
    || quote_ident(atopology)
    || '.edge_data SET right_face = '
    || quote_literal(faceid)
    || ' WHERE edge_id = ANY('
    || quote_literal(right_edges)
    || ') ';
  END IF;


  --
  -- Set left_face/right_face of any contained edge 
  --
  EXECUTE 'UPDATE '
    || quote_ident(atopology)
    || '.edge_data SET right_face = '
    || quote_literal(faceid)
    || ', left_face = '
    || quote_literal(faceid)
    || ' WHERE ST_Contains('
    || quote_literal(apoly::text)
    || ', geom)';

  -- 
  -- Set containing_face of any contained node 
  -- 
  EXECUTE 'UPDATE '
    || quote_ident(atopology)
    || '.node SET containing_face = '
    || quote_literal(faceid)
    || ' WHERE containing_face IS NOT NULL AND ST_Contains('
    || quote_literal(apoly::text)
    || ', geom)';

  RETURN faceid;
	
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} AddFace

-- ----------------------------------------------------------------------------
-- 
-- Functions to incrementally populate a topology
-- 
-- ----------------------------------------------------------------------------

--{
--  TopoGeo_AddPoint(toponame, pointgeom, tolerance)
--
--  Add a Point into a topology, with an optional tolerance 
--
CREATE OR REPLACE FUNCTION topology.TopoGeo_AddPoint(atopology varchar, apoint geometry, tolerance float8 DEFAULT 0)
	RETURNS int AS
$$
DECLARE
  id integer;
  rec RECORD;
  sql text;
  prj GEOMETRY;
  snapedge GEOMETRY;
  snaptol FLOAT8;
  tol FLOAT8;
  z FLOAT8;
BEGIN

  -- 0. Check arguments
  IF geometrytype(apoint) != 'POINT' THEN
    RAISE EXCEPTION 'Invalid geometry type (%) passed to TopoGeo_AddPoint, expected POINT', geometrytype(apoint);
  END IF;

  -- Get tolerance, if 0 was given
  tol := COALESCE( NULLIF(tolerance, 0), topology._st_mintolerance(atopology, apoint) );

  -- 1. Check if any existing node falls within tolerance
  --    and if so pick the closest
  sql := 'SELECT a.node_id FROM ' 
    || quote_ident(atopology) 
    || '.node as a WHERE ST_DWithin(a.geom,'
    || quote_literal(apoint::text) || '::geometry,'
    || tol || ') ORDER BY ST_Distance('
    || quote_literal(apoint::text)
    || '::geometry, a.geom) LIMIT 1;';



  EXECUTE sql INTO id;
  IF id IS NOT NULL THEN
    RETURN id;
  END IF;





  -- 2. Check if any existing edge falls within tolerance
  --    and if so split it by a point projected on it
  sql := 'SELECT a.edge_id, a.geom FROM ' 
    || quote_ident(atopology) 
    || '.edge as a WHERE ST_DWithin(a.geom,'
    || quote_literal(apoint::text) || '::geometry,'
    || tol || ') ORDER BY ST_Distance('
    || quote_literal(apoint::text)
    || '::geometry, a.geom) LIMIT 1;';



  EXECUTE sql INTO rec;
  IF rec IS NOT NULL THEN
    -- project point to line, split edge by point
    prj := ST_ClosestPoint(rec.geom, apoint);
    -- This is a workaround for ClosestPoint lack of Z support:
    -- http://trac.osgeo.org/postgis/ticket/2033
    z := ST_Z(apoint);
    IF z IS NOT NULL THEN
      prj := ST_Translate(ST_Force_3DZ(prj), 0, 0, z); -- no ST_SetZ ...
    END IF;



    IF NOT ST_Contains(rec.geom, prj) THEN



      -- The tolerance must be big enough for snapping to happen
      -- and small enough to snap only to the projected point.
      -- Unfortunately ST_Distance returns 0 because it also uses
      -- a projected point internally, so we need another way.
      snaptol := topology._st_mintolerance(prj);



      snapedge := ST_Snap(rec.geom, prj, snaptol);

      -- Snapping currently snaps the first point below tolerance
      -- so may possibly move first point. See ticket #1631
      IF NOT ST_Equals(ST_StartPoint(rec.geom), ST_StartPoint(snapedge))
      THEN



        snapedge := ST_MakeLine(ST_StartPoint(rec.geom), snapedge);
      END IF;






      PERFORM ST_ChangeEdgeGeom(atopology, rec.edge_id, snapedge);
    END IF;
    id := topology.ST_ModEdgeSplit(atopology, rec.edge_id, prj);
  ELSE



    id := topology.ST_AddIsoNode(atopology, NULL, apoint);
  END IF;

  RETURN id;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} TopoGeo_AddPoint

--{
--  TopoGeo_addLinestring(toponame, linegeom, tolerance)
--
--  Add a LineString into a topology 
--
-- }{
CREATE OR REPLACE FUNCTION topology.TopoGeo_addLinestring(atopology varchar, aline geometry, tolerance float8 DEFAULT 0)
	RETURNS SETOF int AS
$$
DECLARE
  rec RECORD;
  rec2 RECORD;
  sql TEXT;
  set1 GEOMETRY;
  set2 GEOMETRY;
  snapped GEOMETRY;
  noded GEOMETRY;
  start_node INTEGER;
  end_node INTEGER;
  id INTEGER; 
  inodes GEOMETRY;
  iedges GEOMETRY;
  tol float8;
BEGIN

  -- 0. Check arguments
  IF geometrytype(aline) != 'LINESTRING' THEN
    RAISE EXCEPTION 'Invalid geometry type (%) passed to TopoGeo_AddLinestring, expected LINESTRING', geometrytype(aline);
  END IF;

  -- Get tolerance, if 0 was given
  tol := COALESCE( NULLIF(tolerance, 0), topology._st_mintolerance(atopology, aline) );

  -- 1. Self-node
  noded := ST_UnaryUnion(aline);




  -- 2. Node to edges falling within tol distance
  sql := 'WITH nearby AS ( SELECT e.geom FROM '
    || quote_ident(atopology) 
    || '.edge e WHERE ST_DWithin(e.geom, '
    || quote_literal(noded::text)
    || '::geometry, '
    || tol || ') ) SELECT st_collect(geom) FROM nearby;';



  EXECUTE sql INTO iedges;
  IF iedges IS NOT NULL THEN





    snapped := ST_Snap(noded, iedges, tol);




    noded := ST_Difference(snapped, iedges);




    set1 := ST_Intersection(snapped, iedges);




    set2 := ST_LineMerge(set1);




    noded := ST_Union(noded, set2);




  END IF;

  -- 2.1. Node with existing nodes within tol
  -- TODO: check if we should be only considering _isolated_ nodes!
  sql := 'WITH nearby AS ( SELECT n.geom FROM '
    || quote_ident(atopology) 
    || '.node n WHERE ST_DWithin(n.geom, '
    || quote_literal(noded::text)
    || '::geometry, '
    || tol || ') ) SELECT st_collect(geom) FROM nearby;';



  EXECUTE sql INTO inodes;

  IF inodes IS NOT NULL THEN -- {




    -- TODO: consider snapping once against all elements
    ---      (rather than once with edges and once with nodes)
    noded := ST_Snap(noded, inodes, tol);




    FOR rec IN SELECT (ST_Dump(inodes)).*
    LOOP
        -- Use the node to split edges
        SELECT ST_Collect(geom) 
        FROM ST_Dump(ST_Split(noded, rec.geom))
        INTO STRICT noded;



    END LOOP;




    -- re-node to account for ST_Snap introduced self-intersections
    -- See http://trac.osgeo.org/postgis/ticket/1714
    -- TODO: consider running UnaryUnion once after all noding 
    noded := ST_UnaryUnion(noded);



  END IF; -- }

  -- 3. For each (now-noded) segment, insert an edge
  FOR rec IN SELECT (ST_Dump(noded)).geom LOOP

    -- TODO: skip point elements ?





    start_node := topology.TopoGeo_AddPoint(atopology,
                                          ST_StartPoint(rec.geom),
                                          tol);




    end_node := topology.TopoGeo_AddPoint(atopology,
                                        ST_EndPoint(rec.geom),
                                        tol);




    -- Added endpoints may have drifted due to tolerance, so
    -- we need to re-snap the edge to the new nodes before adding it
    sql := 'SELECT n1.geom as sn, n2.geom as en FROM ' || quote_ident(atopology)
      || '.node n1, ' || quote_ident(atopology)
      || '.node n2 WHERE n1.node_id = '
      || start_node || ' AND n2.node_id = ' || end_node;




    EXECUTE sql INTO STRICT rec2;

    snapped := ST_SetPoint(
                 ST_SetPoint(rec.geom, ST_NPoints(rec.geom)-1, rec2.en),
                 0, rec2.sn);

    
    snapped := ST_CollectionExtract(ST_MakeValid(snapped), 2);





    -- Check if the so-snapped edge collapsed (see #1650)
    IF ST_IsEmpty(snapped) THEN



      CONTINUE;
    END IF;

    -- Check if the so-snapped edge _now_ exists
    sql := 'SELECT edge_id FROM ' || quote_ident(atopology)
      || '.edge_data WHERE ST_Equals(geom, ' || quote_literal(snapped::text)
      || '::geometry)';



    EXECUTE sql INTO id;
    IF id IS NULL THEN
      id := topology.ST_AddEdgeModFace(atopology, start_node, end_node,
                                       snapped);



    ELSE



    END IF;

    RETURN NEXT id;

  END LOOP;

  RETURN;
END
$$
LANGUAGE 'plpgsql';
--} TopoGeo_addLinestring

--{
--  TopoGeo_AddPolygon(toponame, polygeom, tolerance)
--
--  Add a Polygon into a topology 
--
-- }{
CREATE OR REPLACE FUNCTION topology.TopoGeo_AddPolygon(atopology varchar, apoly geometry, tolerance float8 DEFAULT 0)
	RETURNS SETOF int AS
$$
DECLARE
  boundary GEOMETRY;
  fgeom GEOMETRY;
  rec RECORD;
  edges INTEGER[];
  sql TEXT;
  tol FLOAT8;
BEGIN

  -- 0. Check arguments
  IF geometrytype(apoly) != 'POLYGON' THEN
    RAISE EXCEPTION 'Invalid geometry type (%) passed to TopoGeo_AddPolygon, expected POLYGON', geometrytype(apoly);
  END IF;

  -- Get tolerance, if 0 was given
  tol := COALESCE( NULLIF(tolerance, 0), topology._st_mintolerance(atopology, apoly) );

  -- 1. Extract boundary
  boundary := ST_Boundary(apoly);




  -- 2. Add boundaries as edges
  FOR rec IN SELECT (ST_Dump(boundary)).geom LOOP
    edges := array_cat(edges, array_agg(x)) FROM ( select topology.TopoGeo_addLinestring(atopology, rec.geom, tol) as x ) as foo;



  END LOOP;

  -- 3. Find faces covered by input polygon
  --    NOTE: potential snapping changed polygon edges
  sql := 'SELECT DISTINCT f.face_id FROM ' || quote_ident(atopology)
    || '.face f WHERE f.mbr && '
    || quote_literal(apoly::text)
    || '::geometry';



  FOR rec IN EXECUTE sql LOOP
    -- check for actual containment
    fgeom := ST_PointOnSurface(ST_GetFaceGeometry(atopology, rec.face_id));
    IF NOT ST_Covers(apoly, fgeom) THEN



      CONTINUE;
    END IF;
    RETURN NEXT rec.face_id;
  END LOOP;

END
$$
LANGUAGE 'plpgsql';
--} TopoGeo_AddPolygon

--{
--  TopoGeo_AddGeometry(toponame, geom, tolerance)
--
--  Add a Geometry into a topology 
--
CREATE OR REPLACE FUNCTION topology.TopoGeo_AddGeometry(atopology varchar, ageom geometry, tolerance float8 DEFAULT 0)
	RETURNS void AS
$$
DECLARE
BEGIN
	RAISE EXCEPTION 'TopoGeo_AddGeometry not implemented yet';
END
$$
LANGUAGE 'plpgsql';
--} TopoGeo_AddGeometry

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Functions used to polygonize topology edges
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- int Polygonize(toponame)
--
-- TODO: allow restricting polygonization to a bounding box
--
-- }{
CREATE OR REPLACE FUNCTION topology.polygonize(toponame varchar)
  RETURNS text
AS
$$
DECLARE
  sql text;
  rec RECORD;
  faces int;
BEGIN

  sql := 'SELECT (st_dump(st_polygonize(geom))).geom from '
         || quote_ident(toponame) || '.edge_data';

  faces = 0;
  FOR rec in EXECUTE sql LOOP
    BEGIN
      PERFORM topology.AddFace(toponame, rec.geom);
      faces = faces + 1;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE WARNING 'Error registering face % (%)', rec.geom, SQLERRM;
    END;
  END LOOP;
  RETURN faces || ' faces registered';
END
$$
LANGUAGE 'plpgsql';
--} Polygonize(toponame)


--  TopoElement

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2010, 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- TopoElement management functions 
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- TopoElementArray TopoElementArray_append(<TopoElement>)
--
-- Append a TopoElement to a TopoElementArray
--
CREATE OR REPLACE FUNCTION topology.TopoElementArray_append(topology.TopoElementArray, topology.TopoElement)
	RETURNS topology.TopoElementArray
AS
$$
	SELECT CASE
		WHEN $1 IS NULL THEN
			topology.TopoElementArray('{' || $2::text || '}')
		ELSE
			topology.TopoElementArray($1::int[][]||$2::int[])
		END;
$$
LANGUAGE 'sql' IMMUTABLE;
--} TopoElementArray_append

--{
--
-- TopoElementArray TopoElementArray_agg(<setof TopoElement>)
--
-- Aggregates a set of TopoElement values into a TopoElementArray
--
DROP AGGREGATE IF EXISTS topology.TopoElementArray_agg(topology.TopoElement);
CREATE AGGREGATE topology.TopoElementArray_agg(
	sfunc = topology.TopoElementArray_append,
	basetype = topology.TopoElement,
	stype = topology.TopoElementArray
	);
--} TopoElementArray_agg


--  TopoGeometry

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

-- {
--  Override geometrytype() for topogeometry objects
--
--  Note: For performance reasons, this function always assumes
--        TopoGeometry are of the MULTI type. This may not always
--        be the case if you convert the TopoGeometry to an actual
--        Geometry.
--
-- }{
CREATE OR REPLACE FUNCTION topology.GeometryType(tg topology.TopoGeometry)
	RETURNS text
AS
$$
	SELECT CASE
		WHEN type($1) = 1 THEN 'MULTIPOINT'
		WHEN type($1) = 2 THEN 'MULTILINESTRING'
		WHEN type($1) = 3 THEN 'MULTIPOLYGON'
		WHEN type($1) = 4 THEN 'GEOMETRYCOLLECTION'
		ELSE 'UNEXPECTED'
		END;
$$
LANGUAGE 'sql' STABLE STRICT;
-- }

-- {
--  Override st_geometrytype() for topogeometry objects
--
--  Note: For performance reasons, this function always assumes
--        TopoGeometry are of the MULTI type. This may not always
--        be the case if you convert the TopoGeometry to an actual
--        Geometry.
--
-- }{
CREATE OR REPLACE FUNCTION topology.ST_GeometryType(tg topology.TopoGeometry)
	RETURNS text
AS
$$
	SELECT CASE
		WHEN type($1) = 1 THEN 'ST_MultiPoint'
		WHEN type($1) = 2 THEN 'ST_MultiLinestring'
		WHEN type($1) = 3 THEN 'ST_MultiPolygon'
		WHEN type($1) = 4 THEN 'ST_GeometryCollection'
		ELSE 'ST_Unexpected'
		END;
$$
LANGUAGE 'sql' STABLE STRICT;
-- }

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011-2012 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

-- {
--  Convert a simple geometry to a topologically-defined one
--
--  See http://trac.osgeo.org/postgis/ticket/1017
--
-- }{
CREATE OR REPLACE FUNCTION topology.toTopoGeom(ageom Geometry, atopology varchar, alayer int, atolerance float8 DEFAULT 0)
  RETURNS topology.TopoGeometry
AS
$$
DECLARE
  layer_info RECORD;
  topology_info RECORD;
  rec RECORD;
  tg topology.TopoGeometry;
  elems topology.TopoElementArray = '{{0,0}}';
  sql TEXT;
  typ TEXT;
  tolerance FLOAT8;
BEGIN

  -- Get topology information
  BEGIN
    SELECT *
    FROM topology.topology
      INTO STRICT topology_info WHERE name = atopology;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'No topology with name "%" in topology.topology',
        atopology;
  END;

  -- Get tolerance, if 0 was given
  tolerance := COALESCE( NULLIF(atolerance, 0), topology._st_mintolerance(atopology, ageom) );

  -- Get layer information
  BEGIN
    SELECT *, CASE
      WHEN feature_type = 1 THEN 'puntal'
      WHEN feature_type = 2 THEN 'lineal'
      WHEN feature_type = 3 THEN 'areal'
      WHEN feature_type = 4 THEN 'mixed'
      ELSE 'unexpected_'||feature_type
      END as typename
    FROM topology.layer l
      INTO STRICT layer_info
      WHERE l.layer_id = alayer
      AND l.topology_id = topology_info.id;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'No layer with id "%" in topology "%"',
        alayer, atopology;
  END;

  -- Can't convert to a hierarchical topogeometry
  IF layer_info.level > 0 THEN
      RAISE EXCEPTION 'Layer "%" of topology "%" is hierarchical, cannot convert to it.',
        alayer, atopology;
  END IF;


  -- 
  -- Check type compatibility and create empty TopoGeometry
  -- 1:puntal, 2:lineal, 3:areal, 4:collection
  --
  typ = geometrytype(ageom);
  IF typ = 'GEOMETRYCOLLECTION' THEN
    --  A collection can only go collection layer
    IF layer_info.feature_type != 4 THEN
      RAISE EXCEPTION
        'Layer "%" of topology "%" is %, cannot hold a collection feature.',
        layer_info.layer_id, topology_info.name, layer_info.typename;
    END IF;
    tg := topology.CreateTopoGeom(atopology, 4, alayer);
  ELSIF typ = 'POINT' OR typ = 'MULTIPOINT' THEN -- puntal
    --  A point can go in puntal or collection layer
    IF layer_info.feature_type != 4 and layer_info.feature_type != 1 THEN
      RAISE EXCEPTION
        'Layer "%" of topology "%" is %, cannot hold a puntal feature.',
        layer_info.layer_id, topology_info.name, layer_info.typename;
    END IF;
    tg := topology.CreateTopoGeom(atopology, 1, alayer);
  ELSIF typ = 'LINESTRING' or typ = 'MULTILINESTRING' THEN -- lineal
    --  A line can go in lineal or collection layer
    IF layer_info.feature_type != 4 and layer_info.feature_type != 2 THEN
      RAISE EXCEPTION
        'Layer "%" of topology "%" is %, cannot hold a lineal feature.',
        layer_info.layer_id, topology_info.name, layer_info.typename;
    END IF;
    tg := topology.CreateTopoGeom(atopology, 2, alayer);
  ELSIF typ = 'POLYGON' OR typ = 'MULTIPOLYGON' THEN -- areal
    --  An area can go in areal or collection layer
    IF layer_info.feature_type != 4 and layer_info.feature_type != 3 THEN
      RAISE EXCEPTION
        'Layer "%" of topology "%" is %, cannot hold an areal feature.',
        layer_info.layer_id, topology_info.name, layer_info.typename;
    END IF;
    tg := topology.CreateTopoGeom(atopology, 3, alayer);
  ELSE
      -- Should never happen
      RAISE EXCEPTION
        'Unsupported feature type %', typ;
  END IF;

  -- Now that we have a topogeometry, we loop over distinct components 
  -- and add them to the definition of it. We add them as soon
  -- as possible so that each element can further edit the
  -- definition by splitting
  FOR rec IN SELECT DISTINCT id(tg), alayer as lyr,
    CASE WHEN ST_Dimension(geom) = 0 THEN 1
         WHEN ST_Dimension(geom) = 1 THEN 2
         WHEN ST_Dimension(geom) = 2 THEN 3
    END as type,
    CASE WHEN ST_Dimension(geom) = 0 THEN
           topology.topogeo_addPoint(atopology, geom, tolerance)
         WHEN ST_Dimension(geom) = 1 THEN
           topology.topogeo_addLineString(atopology, geom, tolerance)
         WHEN ST_Dimension(geom) = 2 THEN
           topology.topogeo_addPolygon(atopology, geom, tolerance)
    END as primitive
    FROM (SELECT (ST_Dump(ageom)).geom) as f
    WHERE NOT ST_IsEmpty(geom)
  LOOP
    -- TODO: consider use a single INSERT statement for the whole thing
    sql := 'INSERT INTO ' || quote_ident(atopology)
        || '.relation(topogeo_id, layer_id, element_type, element_id) VALUES ('
        || rec.id || ',' || rec.lyr || ',' || rec.type
        || ',' || rec.primitive || ')';



    EXECUTE sql;
  END LOOP;

  RETURN tg;

END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
-- }


--  GML

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2010, 2011 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Functions used for topology GML output
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- INTERNAL FUNCTION
-- text _AsGMLNode(id, point, nsprefix, precision, options, idprefix, gmlver)
--
-- }{
CREATE OR REPLACE FUNCTION topology._AsGMLNode(id int, point geometry, nsprefix_in text, prec int, options int, idprefix text, gmlver int)
  RETURNS text
AS
$$
DECLARE
  nsprefix text;
  gml text;
BEGIN

  nsprefix := 'gml:';
  IF NOT nsprefix_in IS NULL THEN
    IF nsprefix_in = '' THEN
      nsprefix = nsprefix_in;
    ELSE
      nsprefix = nsprefix_in || ':';
    END IF;
  END IF;

  gml := '<' || nsprefix || 'Node ' || nsprefix
    || 'id="' || idprefix || 'N' || id || '"';
  IF point IS NOT NULL THEN
    gml = gml || '>'
              || '<' || nsprefix || 'pointProperty>'
              || ST_AsGML(gmlver, point, prec, options, nsprefix_in)
              || '</' || nsprefix || 'pointProperty>'
              || '</' || nsprefix || 'Node>';
  ELSE
    gml = gml || '/>';
  END IF;
  RETURN gml;
END
$$
LANGUAGE 'plpgsql' IMMUTABLE;
--} _AsGMLNode(id, point, nsprefix, precision, options, idprefix, gmlVersion)

--{
--
-- INTERNAL FUNCTION
-- text _AsGMLEdge(edge_id, start_node, end_node, line, visitedTable,
--                 nsprefix, precision, options, idprefix, gmlVersion)
--
-- }{
CREATE OR REPLACE FUNCTION topology._AsGMLEdge(edge_id int, start_node int,end_node int, line geometry, visitedTable regclass, nsprefix_in text, prec int, options int, idprefix text, gmlver int)
  RETURNS text
AS
$$
DECLARE
  visited bool;
  nsprefix text;
  gml text;
BEGIN

  nsprefix := 'gml:';
  IF nsprefix_in IS NOT NULL THEN
    IF nsprefix_in = '' THEN
      nsprefix = nsprefix_in;
    ELSE
      nsprefix = nsprefix_in || ':';
    END IF;
  END IF;

  gml := '<' || nsprefix || 'Edge ' || nsprefix
    || 'id="' || idprefix || 'E' || edge_id || '">';

  -- Start node
  gml = gml || '<' || nsprefix || 'directedNode orientation="-"';
  -- Do visited bookkeeping if visitedTable was given
  visited = NULL;
  IF visitedTable IS NOT NULL THEN
    EXECUTE 'SELECT true FROM '
            || visitedTable::text
            || ' WHERE element_type = 1 AND element_id = '
            || start_node LIMIT 1 INTO visited;
    IF visited IS NOT NULL THEN
      gml = gml || ' xlink:href="#' || idprefix || 'N' || start_node || '" />';
    ELSE
      -- Mark as visited 
      EXECUTE 'INSERT INTO ' || visitedTable::text
        || '(element_type, element_id) VALUES (1, '
        || start_node || ')';
    END IF;
  END IF;
  IF visited IS NULL THEN
    gml = gml || '>';
    gml = gml || topology._AsGMLNode(start_node, NULL, nsprefix_in,
                                     prec, options, idprefix, gmlver);
    gml = gml || '</' || nsprefix || 'directedNode>';
  END IF;

  -- End node
  gml = gml || '<' || nsprefix || 'directedNode';
  -- Do visited bookkeeping if visitedTable was given
  visited = NULL;
  IF visitedTable IS NOT NULL THEN
    EXECUTE 'SELECT true FROM '
            || visitedTable::text
            || ' WHERE element_type = 1 AND element_id = '
            || end_node LIMIT 1 INTO visited;
    IF visited IS NOT NULL THEN
      gml = gml || ' xlink:href="#' || idprefix || 'N' || end_node || '" />';
    ELSE
      -- Mark as visited 
      EXECUTE 'INSERT INTO ' || visitedTable::text
        || '(element_type, element_id) VALUES (1, '
        || end_node || ')';
    END IF;
  END IF;
  IF visited IS NULL THEN
    gml = gml || '>';
    gml = gml || topology._AsGMLNode(end_node, NULL, nsprefix_in,
                                     prec, options, idprefix, gmlver);
    gml = gml || '</' || nsprefix || 'directedNode>';
  END IF;

  IF line IS NOT NULL THEN
    gml = gml || '<' || nsprefix || 'curveProperty>'
              || ST_AsGML(gmlver, line, prec, options, nsprefix_in)
              || '</' || nsprefix || 'curveProperty>';
  END IF;

  gml = gml || '</' || nsprefix || 'Edge>';

  RETURN gml;
END
$$
LANGUAGE 'plpgsql' VOLATILE; -- writes into visitedTable
--} _AsGMLEdge(id, start_node, end_node, line, visitedTable, nsprefix, precision, options, idprefix, gmlver)

--{
--
-- INTERNAL FUNCTION
-- text _AsGMLFace(toponame, face_id, visitedTable,
--                 nsprefix, precision, options, idprefix, gmlVersion)
--
-- }{
CREATE OR REPLACE FUNCTION topology._AsGMLFace(toponame text, face_id int, visitedTable regclass, nsprefix_in text, prec int, options int, idprefix text, gmlver int)
  RETURNS text
AS
$$
DECLARE
  visited bool;
  nsprefix text;
  gml text;
  rec RECORD;
  rec2 RECORD;
  bounds geometry;
  side int;
BEGIN

  nsprefix := 'gml:';
  IF nsprefix_in IS NOT NULL THEN
    IF nsprefix_in = '' THEN
      nsprefix = nsprefix_in;
    ELSE
      nsprefix = nsprefix_in || ':';
    END IF;
  END IF;

  gml := '<' || nsprefix || 'Face ' || nsprefix
    || 'id="' || idprefix || 'F' || face_id || '">';

  -- Construct the face geometry, then for each polygon:
  FOR rec IN SELECT (ST_DumpRings((ST_Dump(ST_ForceRHR(
    topology.ST_GetFaceGeometry(toponame, face_id)))).geom)).geom
  LOOP

      -- Contents of a directed face are the list of edges
      -- that cover the specific ring
      bounds = ST_Boundary(rec.geom);

      FOR rec2 IN EXECUTE
        'SELECT e.*, ST_Line_Locate_Point('
        || quote_literal(bounds::text)
        || ', ST_Line_Interpolate_Point(e.geom, 0.2)) as pos'
        || ', ST_Line_Locate_Point('
        || quote_literal(bounds::text)
        || ', ST_Line_Interpolate_Point(e.geom, 0.8)) as pos2 FROM '
        || quote_ident(toponame)
        || '.edge e WHERE ( e.left_face = ' || face_id
        || ' OR e.right_face = ' || face_id
        || ') AND ST_Covers('
        || quote_literal(bounds::text)
        || ', e.geom) ORDER BY pos'
      LOOP

        gml = gml || '<' || nsprefix || 'directedEdge';

        -- if this edge goes in same direction to the
        --       ring bounds, make it with negative orientation
        IF rec2.pos2 > rec2.pos THEN -- edge goes in same direction
          gml = gml || ' orientation="-"';
        END IF;

        -- Do visited bookkeeping if visitedTable was given
        IF visitedTable IS NOT NULL THEN

          EXECUTE 'SELECT true FROM '
            || visitedTable::text
            || ' WHERE element_type = 2 AND element_id = '
            || rec2.edge_id LIMIT 1 INTO visited;
          IF visited THEN
            -- Use xlink:href if visited
            gml = gml || ' xlink:href="#' || idprefix || 'E'
                      || rec2.edge_id || '" />';
            CONTINUE;
          ELSE
            -- Mark as visited otherwise
            EXECUTE 'INSERT INTO ' || visitedTable::text
              || '(element_type, element_id) VALUES (2, '
              || rec2.edge_id || ')';
          END IF;

        END IF;

        gml = gml || '>';

        gml = gml || topology._AsGMLEdge(rec2.edge_id, rec2.start_node,
                                        rec2.end_node, rec2.geom,
                                        visitedTable, nsprefix_in,
                                        prec, options, idprefix, gmlver);
        gml = gml || '</' || nsprefix || 'directedEdge>';

      END LOOP;
    END LOOP;

  gml = gml || '</' || nsprefix || 'Face>';

  RETURN gml;
END
$$
LANGUAGE 'plpgsql' VOLATILE; -- writes into visited table
--} _AsGMLFace(toponame, id, visitedTable, nsprefix, precision, options, idprefix, gmlver)

--{
--
-- API FUNCTION
--
-- text AsGML(TopoGeometry, nsprefix, precision, options, visitedTable, idprefix, gmlver)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, nsprefix_in text, precision_in int, options_in int, visitedTable regclass, idprefix text, gmlver int)
  RETURNS text
AS
$$
DECLARE
  nsprefix text;
  precision int;
  options int;
  visited bool;
  toponame text;
  gml text;
  sql text;
  rec RECORD;
  rec2 RECORD;
  --bounds geometry;
  side int;
BEGIN

  nsprefix := 'gml:';
  IF nsprefix_in IS NOT NULL THEN
    IF nsprefix_in = '' THEN
      nsprefix = nsprefix_in;
    ELSE
      nsprefix = nsprefix_in || ':';
    END IF;
  END IF;

  precision := 15;
  IF precision_in IS NOT NULL THEN
    precision = precision_in;
  END IF;

  options := 1;
  IF options_in IS NOT NULL THEN
    options = options_in;
  END IF;

  -- Get topology name (for subsequent queries)
  SELECT name FROM topology.topology into toponame
              WHERE id = tg.topology_id;

  -- Puntual TopoGeometry
  IF tg.type = 1 THEN
    gml = '<' || nsprefix || 'TopoPoint>';
    -- For each defining node, print a directedNode
    FOR rec IN  EXECUTE 'SELECT r.element_id, n.geom from '
      || quote_ident(toponame) || '.relation r LEFT JOIN '
      || quote_ident(toponame) || '.node n ON (r.element_id = n.node_id)'
      || ' WHERE r.layer_id = ' || tg.layer_id
      || ' AND r.topogeo_id = ' || tg.id
    LOOP
      gml = gml || '<' || nsprefix || 'directedNode';
      -- Do visited bookkeeping if visitedTable was given
      IF visitedTable IS NOT NULL THEN
        EXECUTE 'SELECT true FROM '
                || visitedTable::text
                || ' WHERE element_type = 1 AND element_id = '
                || rec.element_id LIMIT 1 INTO visited;
        IF visited IS NOT NULL THEN
          gml = gml || ' xlink:href="#' || idprefix || 'N' || rec.element_id || '" />';
          CONTINUE;
        ELSE
          -- Mark as visited 
          EXECUTE 'INSERT INTO ' || visitedTable::text
            || '(element_type, element_id) VALUES (1, '
            || rec.element_id || ')';
        END IF;
      END IF;
      gml = gml || '>';
      gml = gml || topology._AsGMLNode(rec.element_id, rec.geom, nsprefix_in, precision, options, idprefix, gmlver);
      gml = gml || '</' || nsprefix || 'directedNode>';
    END LOOP;
    gml = gml || '</' || nsprefix || 'TopoPoint>';
    RETURN gml;

  ELSIF tg.type = 2 THEN -- lineal
    gml = '<' || nsprefix || 'TopoCurve>';

    FOR rec IN SELECT (ST_Dump(topology.Geometry(tg))).geom
    LOOP
      FOR rec2 IN EXECUTE
        'SELECT e.*, ST_Line_Locate_Point('
        || quote_literal(rec.geom::text)
        || ', ST_Line_Interpolate_Point(e.geom, 0.2)) as pos'
        || ', ST_Line_Locate_Point('
        || quote_literal(rec.geom::text)
        || ', ST_Line_Interpolate_Point(e.geom, 0.8)) as pos2 FROM '
        || quote_ident(toponame)
        || '.edge e WHERE ST_Covers('
        || quote_literal(rec.geom::text)
        || ', e.geom) ORDER BY pos'
        -- TODO: add relation to the conditional, to reduce load ?
      LOOP

        gml = gml || '<' || nsprefix || 'directedEdge';

        -- if this edge goes in opposite direction to the
        --       line, make it with negative orientation
        IF rec2.pos2 < rec2.pos THEN -- edge goes in opposite direction
          gml = gml || ' orientation="-"';
        END IF;

        -- Do visited bookkeeping if visitedTable was given
        IF visitedTable IS NOT NULL THEN

          EXECUTE 'SELECT true FROM '
            || visitedTable::text
            || ' WHERE element_type = 2 AND element_id = '
            || rec2.edge_id LIMIT 1 INTO visited;
          IF visited THEN
            -- Use xlink:href if visited
            gml = gml || ' xlink:href="#' || idprefix || 'E' || rec2.edge_id || '" />';
            CONTINUE;
          ELSE
            -- Mark as visited otherwise
            EXECUTE 'INSERT INTO ' || visitedTable::text
              || '(element_type, element_id) VALUES (2, '
              || rec2.edge_id || ')';
          END IF;

        END IF;


        gml = gml || '>';

        gml = gml || topology._AsGMLEdge(rec2.edge_id,
                                        rec2.start_node,
                                        rec2.end_node, rec2.geom,
                                        visitedTable,
                                        nsprefix_in, precision,
                                        options, idprefix, gmlver);


        gml = gml || '</' || nsprefix || 'directedEdge>';
      END LOOP;
    END LOOP;

    gml = gml || '</' || nsprefix || 'TopoCurve>';
    return gml;

  ELSIF tg.type = 3 THEN -- areal
    gml = '<' || nsprefix || 'TopoSurface>';

    -- For each defining face, print a directedFace
    FOR rec IN  EXECUTE 'SELECT f.face_id from '
      || quote_ident(toponame) || '.relation r LEFT JOIN '
      || quote_ident(toponame) || '.face f ON (r.element_id = f.face_id)'
      || ' WHERE r.layer_id = ' || tg.layer_id
      || ' AND r.topogeo_id = ' || tg.id
    LOOP
      gml = gml || '<' || nsprefix || 'directedFace';
      -- Do visited bookkeeping if visitedTable was given
      IF visitedTable IS NOT NULL THEN
        EXECUTE 'SELECT true FROM '
                || visitedTable::text
                || ' WHERE element_type = 3 AND element_id = '
                || rec.face_id LIMIT 1 INTO visited;
        IF visited IS NOT NULL THEN
          gml = gml || ' xlink:href="#' || idprefix || 'F' || rec.face_id || '" />';
          CONTINUE;
        ELSE
          -- Mark as visited 
          EXECUTE 'INSERT INTO ' || visitedTable::text
            || '(element_type, element_id) VALUES (3, '
            || rec.face_id || ')';
        END IF;
      END IF;
      gml = gml || '>';
      gml = gml || topology._AsGMLFace(toponame, rec.face_id, visitedTable,
                                       nsprefix_in, precision,
                                       options, idprefix, gmlver);
      gml = gml || '</' || nsprefix || 'directedFace>';
    END LOOP;
    gml = gml || '</' || nsprefix || 'TopoSurface>';
    RETURN gml;

  ELSIF tg.type = 4 THEN -- collection
    RAISE EXCEPTION 'Collection TopoGeometries are not supported by AsGML';

  END IF;
	

  RETURN gml;
	
END
$$
LANGUAGE 'plpgsql' VOLATILE; -- writes into visited table
--} AsGML(TopoGeometry, nsprefix, precision, options, visitedTable, idprefix, gmlver)

--{
--
-- API FUNCTION
--
-- text AsGML(TopoGeometry, nsprefix, precision, options, visitedTable,
--            idprefix)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry,nsprefix text, prec int, options int, visitedTable regclass, idprefix text)
  RETURNS text
AS
$$
 SELECT topology.AsGML($1, $2, $3, $4, $5, $6, 3);
$$
LANGUAGE 'sql' VOLATILE; -- writes into visited table
--} AsGML(TopoGeometry, nsprefix, precision, options, visitedTable, idprefix)

--{
--
-- API FUNCTION 
--
-- text AsGML(TopoGeometry, nsprefix, precision, options, visitedTable)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, nsprefix text, prec int, options int, vis regclass)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, $2, $3, $4, $5, '');
$$ LANGUAGE 'sql' VOLATILE; -- writes into visited table
-- } AsGML(TopoGeometry, nsprefix, precision, options)


--{
--
-- API FUNCTION 
--
-- text AsGML(TopoGeometry, nsprefix, precision, options)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, nsprefix text, prec int, opts int)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, $2, $3, $4, NULL);
$$ LANGUAGE 'sql' STABLE; -- does NOT write into visited table 
-- } AsGML(TopoGeometry, nsprefix, precision, options)

--{
--
-- API FUNCTION 
--
-- text AsGML(TopoGeometry, nsprefix)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, nsprefix text)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, $2, 15, 1, NULL);
$$ LANGUAGE 'sql' STABLE; -- does NOT write into visited table 
-- } AsGML(TopoGeometry, nsprefix)

--{
--
-- API FUNCTION
--
-- text AsGML(TopoGeometry, visited_table)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, visitedTable regclass)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, 'gml', 15, 1, $2);
$$ LANGUAGE 'sql' VOLATILE; -- writes into visited table
-- } AsGML(TopoGeometry, visited_table)

--{
--
-- API FUNCTION
--
-- text AsGML(TopoGeometry, visited_table, nsprefix)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry, visitedTable regclass, nsprefix text)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, $3, 15, 1, $2);
$$ LANGUAGE 'sql' VOLATILE; -- writes into visited table
-- } AsGML(TopoGeometry, visited_table, nsprefix)


--{
--
-- API FUNCTION
--
-- text AsGML(TopoGeometry)
--
-- }{
CREATE OR REPLACE FUNCTION topology.AsGML(tg topology.TopoGeometry)
  RETURNS text AS
$$
 SELECT topology.AsGML($1, 'gml');
$$ LANGUAGE 'sql' STABLE; -- does NOT write into visited table
-- } AsGML(TopoGeometry)



--=}  POSTGIS-SPECIFIC block

--  SQL/MM block

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2010, 2011, 2012 Sandro Santilli <strk@keybit.net>
-- Copyright (C) 2005 Refractions Research Inc.
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Sandro Santilli <strk@keybit.net>
--  
-- 



--={ ----------------------------------------------------------------
--  SQL/MM block
--
--  This part contains function in the SQL/MM specification
--
---------------------------------------------------------------------

--
-- Type returned by ST_GetFaceEdges
--
CREATE TYPE topology.GetFaceEdges_ReturnType AS (
  sequence integer,
  edge integer
);


--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.5
--
--  ST_GetFaceEdges(atopology, aface)
--
--
-- 
CREATE OR REPLACE FUNCTION topology.ST_GetFaceEdges(toponame varchar, face_id integer)
  RETURNS SETOF topology.GetFaceEdges_ReturnType
AS
$$
DECLARE
  rec RECORD;
  bounds geometry;
  retrec topology.GetFaceEdges_ReturnType;
  n int;
  sql TEXT;
BEGIN
  --
  -- toponame and face_id are required
  -- 
  IF toponame IS NULL OR face_id IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  IF toponame = '' THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END IF;

  n := 1;

  -- Construct the face geometry, then for each ring of each polygon:
  sql := 'SELECT (ST_DumpRings((ST_Dump(ST_ForceRHR('
    || 'ST_BuildArea(ST_Collect(geom))))).geom)).geom FROM '
    || quote_ident(toponame) || '.edge_data WHERE left_face = '
    || face_id || ' OR right_face = ' || face_id;
  FOR rec IN EXECUTE sql 
  LOOP -- {

    -- Find the edges constituting its boundary
    bounds = ST_Boundary(rec.geom);


    sql := 'WITH er2 AS ( ' 
      || 'WITH er AS ( SELECT ' 
      || 'min(e.edge_id) over (), count(*) over () as cnt, e.edge_id, '
      || 'ST_Line_Locate_Point('
      || quote_literal(bounds::text)
      || ', ST_Line_Interpolate_Point(e.geom, 0.2)) as pos'
      || ', ST_Line_Locate_Point('
      || quote_literal(bounds::text)
      || ', ST_Line_Interpolate_Point(e.geom, 0.8)) as pos2 FROM '
      || quote_ident(toponame)
      || '.edge e WHERE ( e.left_face = ' || face_id
      || ' OR e.right_face = ' || face_id
      || ') AND ST_Covers('
      || quote_literal(bounds::text)
      || ', e.geom)';
    IF face_id = 0 THEN
      sql := sql || ' ORDER BY POS ASC) ';
    ELSE
      sql := sql || ' ORDER BY POS DESC) ';
    END IF;

    -- Reorder rows so to start with the one with smaller edge_id
    sql := sql || 'SELECT row_number() over () - 1 as rn, * FROM er ) '
               || 'SELECT *, ( rn + cnt - ( select rn FROM er2 WHERE edge_id = min ) ) % cnt AS reord FROM er2 ORDER BY reord';


    --RAISE DEBUG 'SQL: %', sql;

    FOR rec IN EXECUTE sql
    LOOP






      retrec.sequence = n;
      retrec.edge = rec.edge_id;

      IF face_id = 0 THEN
        -- if this edge goes in opposite direction to the
        --       ring bounds, make it with negative orientation
        IF rec.pos2 < rec.pos THEN -- edge goes in opposite direction
          retrec.edge = -retrec.edge;
        END IF;
      ELSE
        -- if this edge goes in same direction to the
        --       ring bounds, make it with negative orientation
        IF rec.pos2 > rec.pos THEN -- edge goes in same direction
          retrec.edge = -retrec.edge;
        END IF;
      END IF;

      RETURN NEXT retrec;

      n = n+1;

    END LOOP;
  END LOOP; -- }

  RETURN;
EXCEPTION
  WHEN INVALID_SCHEMA_NAME THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
END
$$
LANGUAGE 'plpgsql' STABLE;
--} ST_GetFaceEdges

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.10
--
--  ST_NewEdgeHeal(atopology, anedge, anotheredge)
--
-- Not in the specs:
-- * Refuses to heal two edges if any of the two is closed 
-- * Raise an exception when trying to heal an edge with itself
-- * Raise an exception if any TopoGeometry is defined by only one
--   of the two edges
-- * Update references in the Relation table.
-- 
CREATE OR REPLACE FUNCTION topology.ST_NewEdgeHeal(toponame varchar, e1id integer, e2id integer)
  RETURNS int
AS
$$
DECLARE
  e1rec RECORD;
  e2rec RECORD;
  rec RECORD;
  newedgeid int;
  connectededges int[];
  commonnode int;
  caseno int;
  topoid int;
  sql text;
  e2sign int;
  eidary int[];
BEGIN
  --
  -- toponame and face_id are required
  -- 
  IF toponame IS NULL OR e1id IS NULL OR e2id IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  -- NOT IN THE SPECS: see if the same edge is given twice..
  IF e1id = e2id THEN
    RAISE EXCEPTION 'Cannot heal edge % with itself, try with another', e1id;
  END IF;

  -- Get topology id
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = toponame;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e1id
      INTO STRICT e1rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e1id;
      WHEN INVALID_SCHEMA_NAME THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing edge_data table)',
          toponame;
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e2id
      INTO STRICT e2rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e2id;
    -- NOTE: checks for INVALID_SCHEMA_NAME or UNDEFINED_TABLE done before
  END;


  -- NOT IN THE SPECS: See if any of the two edges are closed.
  IF e1rec.start_node = e1rec.end_node THEN
    RAISE EXCEPTION 'Edge % is closed, cannot heal to edge %', e1id, e2id;
  END IF;
  IF e2rec.start_node = e2rec.end_node THEN
    RAISE EXCEPTION 'Edge % is closed, cannot heal to edge %', e2id, e1id;
  END IF;

  -- Find common node

  IF e1rec.end_node = e2rec.start_node THEN
    commonnode = e1rec.end_node;
    caseno = 1;
  ELSIF e1rec.end_node = e2rec.end_node THEN
    commonnode = e1rec.end_node;
    caseno = 2;
  END IF;

  -- Check if any other edge is connected to the common node
  IF commonnode IS NOT NULL THEN
    FOR rec IN EXECUTE 'SELECT edge_id FROM ' || quote_ident(toponame)
      || '.edge_data WHERE ( edge_id != ' || e1id
      || ' AND edge_id != ' || e2id || ') AND ( start_node = '
      || commonnode || ' OR end_node = ' || commonnode || ' )'
    LOOP
      commonnode := NULL;
      connectededges = connectededges || rec.edge_id;
    END LOOP;
  END IF;

  IF commonnode IS NULL THEN
    IF e1rec.start_node = e2rec.start_node THEN
      commonnode = e1rec.start_node;
      caseno = 3;
    ELSIF e1rec.start_node = e2rec.end_node THEN
      commonnode = e1rec.start_node;
      caseno = 4;
    END IF;

    -- Check if any other edge is connected to the common node
    IF commonnode IS NOT NULL THEN
      FOR rec IN EXECUTE 'SELECT edge_id FROM ' || quote_ident(toponame)
        || '.edge_data WHERE ( edge_id != ' || e1id
        || ' AND edge_id != ' || e2id || ') AND ( start_node = '
        || commonnode || ' OR end_node = ' || commonnode || ' )'
      LOOP
        commonnode := NULL;
        connectededges = connectededges || rec.edge_id;
      END LOOP;
    END IF;
  END IF;

  IF commonnode IS NULL THEN
    IF connectededges IS NOT NULL THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - other edges connected (%)', array_to_string(connectededges, ',');
    ELSE
      RAISE EXCEPTION 'SQL/MM Spatial exception - non-connected edges';
    END IF;
  END IF;

  -- NOT IN THE SPECS:
  -- check if any topo_geom is defined only by one of the
  -- input edges. In such case there would be no way to adapt
  -- the definition in case of healing, so we'd have to bail out
  eidary = ARRAY[e1id, e2id];
  sql := 'SELECT t.* from ('
    || 'SELECT r.topogeo_id, r.layer_id'
    || ', l.schema_name, l.table_name, l.feature_column'
    || ', array_agg(abs(r.element_id)) as elems '
    || 'FROM topology.layer l INNER JOIN '
    || quote_ident(toponame)
    || '.relation r ON (l.layer_id = r.layer_id) '
    || 'WHERE l.level = 0 AND l.feature_type = 2 '
    || ' AND l.topology_id = ' || topoid
    || ' AND abs(r.element_id) IN (' || e1id || ',' || e2id || ') '
    || 'group by r.topogeo_id, r.layer_id, l.schema_name, l.table_name, '
    || ' l.feature_column ) t WHERE NOT t.elems @> '
    || quote_literal(eidary);
  --RAISE DEBUG 'SQL: %', sql;
  FOR rec IN EXECUTE sql LOOP
    RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented healing edges % and %',
          rec.topogeo_id, rec.layer_id,
          rec.schema_name, rec.table_name, rec.feature_column,
          e1id, e2id;
  END LOOP;

  -- Create new edge {
  rec := e1rec;
  IF caseno = 1 THEN -- e1.end = e2.start
    rec.geom = ST_MakeLine(e1rec.geom, e2rec.geom);
    rec.end_node = e2rec.end_node;
    rec.next_left_edge = e2rec.next_left_edge;
    e2sign = 1;
  ELSIF caseno = 2 THEN -- e1.end = e2.end
    rec.geom = ST_MakeLine(e1rec.geom, st_reverse(e2rec.geom));
    rec.end_node = e2rec.start_node;
    rec.next_left_edge = e2rec.next_right_edge;
    e2sign = -1;
  ELSIF caseno = 3 THEN -- e1.start = e2.start
    rec.geom = ST_MakeLine(st_reverse(e2rec.geom), e1rec.geom);
    rec.start_node = e2rec.end_node;
    rec.next_right_edge = e2rec.next_left_edge;
    e2sign = -1;
  ELSIF caseno = 4 THEN -- e1.start = e2.end
    rec.geom = ST_MakeLine(e2rec.geom, e1rec.geom);
    rec.start_node = e2rec.start_node;
    rec.next_right_edge = e2rec.next_right_edge;
    e2sign = 1;
  END IF;
  -- }

  -- Insert new edge {
  EXECUTE 'SELECT nextval(' || quote_literal(
      quote_ident(toponame) || '.edge_data_edge_id_seq'
    ) || ')' INTO STRICT newedgeid;
  EXECUTE 'INSERT INTO ' || quote_ident(toponame)
    || '.edge VALUES(' || newedgeid
    || ',' || rec.start_node
    || ',' || rec.end_node
    || ',' || rec.next_left_edge
    || ',' || rec.next_right_edge
    || ',' || rec.left_face
    || ',' || rec.right_face
    || ',' || quote_literal(rec.geom::text)
    || ')';
  -- End of new edge insertion }

  -- Update next_left_edge/next_right_edge for
  -- any edge having them still pointing at the edges being removed
  -- (e2id)
  --
  -- NOTE:
  -- *(next_XXX_edge/e2id) serves the purpose of extracting existing
  -- sign from the value, while *e2sign changes that sign again if we
  -- reverted edge2 direction
  --
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_left_edge = ' || newedgeid
    || ', next_left_edge = ' || e2sign*newedgeid
    || '*(next_left_edge/'
    || e2id || ')  WHERE abs_next_left_edge = ' || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_right_edge = ' || newedgeid
    || ', next_right_edge = ' || e2sign*newedgeid
    || '*(next_right_edge/'
    || e2id || ') WHERE abs_next_right_edge = ' || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;

  -- New edge has the same direction as old edge 1
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_left_edge = ' || newedgeid
    || ', next_left_edge = ' || newedgeid
    || '*(next_left_edge/'
    || e1id || ')  WHERE abs_next_left_edge = ' || e1id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_right_edge = ' || newedgeid
    || ', next_right_edge = ' || newedgeid
    || '*(next_right_edge/'
    || e1id || ') WHERE abs_next_right_edge = ' || e1id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;

  --
  -- NOT IN THE SPECS:
  -- Replace composition rows involving the two
  -- edges as one involving the new edge.
  -- It takes a DELETE and an UPDATE to do all
  sql := 'DELETE FROM ' || quote_ident(toponame)
    || '.relation r USING topology.layer l '
    || 'WHERE l.level = 0 AND l.feature_type = 2'
    || ' AND l.topology_id = ' || topoid
    || ' AND l.layer_id = r.layer_id AND abs(r.element_id) = '
    || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.relation r '
    || ' SET element_id = ' || newedgeid || '*(element_id/'
    || e1id
    || ') FROM topology.layer l WHERE l.level = 0 AND l.feature_type = 2'
    || ' AND l.topology_id = ' || topoid
    || ' AND l.layer_id = r.layer_id AND abs(r.element_id) = '
    || e1id
  ;



  EXECUTE sql;


  -- Delete both edges
  EXECUTE 'DELETE FROM ' || quote_ident(toponame)
    || '.edge_data WHERE edge_id = ' || e2id;
  EXECUTE 'DELETE FROM ' || quote_ident(toponame)
    || '.edge_data WHERE edge_id = ' || e1id;

  -- Delete the common node 
  BEGIN
    EXECUTE 'DELETE FROM ' || quote_ident(toponame)
            || '.node WHERE node_id = ' || commonnode;
    EXCEPTION
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing node table)',
          toponame;
  END;

  RETURN newedgeid;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_NewEdgeHeal

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.11
--
--  ST_ModEdgeHeal(atopology, anedge, anotheredge)
--
-- Not in the specs:
-- * Returns the id of the node being removed
-- * Refuses to heal two edges if any of the two is closed 
-- * Raise an exception when trying to heal an edge with itself
-- * Raise an exception if any TopoGeometry is defined by only one
--   of the two edges
-- * Update references in the Relation table.
-- 
CREATE OR REPLACE FUNCTION topology.ST_ModEdgeHeal(toponame varchar, e1id integer, e2id integer)
  RETURNS int
AS
$$
DECLARE
  e1rec RECORD;
  e2rec RECORD;
  rec RECORD;
  connectededges int[];
  commonnode int;
  caseno int;
  topoid int;
  sql text;
  e2sign int;
  eidary int[];
BEGIN
  --
  -- toponame and face_id are required
  -- 
  IF toponame IS NULL OR e1id IS NULL OR e2id IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  -- NOT IN THE SPECS: see if the same edge is given twice..
  IF e1id = e2id THEN
    RAISE EXCEPTION 'Cannot heal edge % with itself, try with another', e1id;
  END IF;

  -- Get topology id
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = toponame;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e1id
      INTO STRICT e1rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e1id;
      WHEN INVALID_SCHEMA_NAME THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing edge_data table)',
          toponame;
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e2id
      INTO STRICT e2rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e2id;
    -- NOTE: checks for INVALID_SCHEMA_NAME or UNDEFINED_TABLE done before
  END;


  -- NOT IN THE SPECS: See if any of the two edges are closed.
  IF e1rec.start_node = e1rec.end_node THEN
    RAISE EXCEPTION 'Edge % is closed, cannot heal to edge %', e1id, e2id;
  END IF;
  IF e2rec.start_node = e2rec.end_node THEN
    RAISE EXCEPTION 'Edge % is closed, cannot heal to edge %', e2id, e1id;
  END IF;

  -- Find common node

  IF e1rec.end_node = e2rec.start_node THEN
    commonnode = e1rec.end_node;
    caseno = 1;
  ELSIF e1rec.end_node = e2rec.end_node THEN
    commonnode = e1rec.end_node;
    caseno = 2;
  END IF;

  -- Check if any other edge is connected to the common node
  IF commonnode IS NOT NULL THEN
    FOR rec IN EXECUTE 'SELECT edge_id FROM ' || quote_ident(toponame)
      || '.edge_data WHERE ( edge_id != ' || e1id
      || ' AND edge_id != ' || e2id || ') AND ( start_node = '
      || commonnode || ' OR end_node = ' || commonnode || ' )'
    LOOP
      commonnode := NULL;
      connectededges = connectededges || rec.edge_id;
    END LOOP;
  END IF;

  IF commonnode IS NULL THEN
    IF e1rec.start_node = e2rec.start_node THEN
      commonnode = e1rec.start_node;
      caseno = 3;
    ELSIF e1rec.start_node = e2rec.end_node THEN
      commonnode = e1rec.start_node;
      caseno = 4;
    END IF;

    -- Check if any other edge is connected to the common node
    IF commonnode IS NOT NULL THEN
      FOR rec IN EXECUTE 'SELECT edge_id FROM ' || quote_ident(toponame)
        || '.edge_data WHERE ( edge_id != ' || e1id
        || ' AND edge_id != ' || e2id || ') AND ( start_node = '
        || commonnode || ' OR end_node = ' || commonnode || ' )'
      LOOP
        commonnode := NULL;
        connectededges = connectededges || rec.edge_id;
      END LOOP;
    END IF;
  END IF;

  IF commonnode IS NULL THEN
    IF connectededges IS NOT NULL THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - other edges connected (%)', array_to_string(connectededges, ',');
    ELSE
      RAISE EXCEPTION 'SQL/MM Spatial exception - non-connected edges';
    END IF;
  END IF;

  -- NOT IN THE SPECS:
  -- check if any topo_geom is defined only by one of the
  -- input edges. In such case there would be no way to adapt
  -- the definition in case of healing, so we'd have to bail out
  eidary = ARRAY[e1id, e2id];
  sql := 'SELECT t.* from ('
    || 'SELECT r.topogeo_id, r.layer_id'
    || ', l.schema_name, l.table_name, l.feature_column'
    || ', array_agg(abs(r.element_id)) as elems '
    || 'FROM topology.layer l INNER JOIN '
    || quote_ident(toponame)
    || '.relation r ON (l.layer_id = r.layer_id) '
    || 'WHERE l.level = 0 AND l.feature_type = 2 '
    || ' AND l.topology_id = ' || topoid
    || ' AND abs(r.element_id) IN (' || e1id || ',' || e2id || ') '
    || 'group by r.topogeo_id, r.layer_id, l.schema_name, l.table_name, '
    || ' l.feature_column ) t WHERE NOT t.elems @> '
    || quote_literal(eidary);
  --RAISE DEBUG 'SQL: %', sql;
  FOR rec IN EXECUTE sql LOOP
    RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented healing edges % and %',
          rec.topogeo_id, rec.layer_id,
          rec.schema_name, rec.table_name, rec.feature_column,
          e1id, e2id;
  END LOOP;

  -- Update data of the first edge {
  rec := e1rec;
  IF caseno = 1 THEN -- e1.end = e2.start
    rec.geom = ST_MakeLine(e1rec.geom, e2rec.geom);
    rec.end_node = e2rec.end_node;
    rec.next_left_edge = e2rec.next_left_edge;
    e2sign = 1;
  ELSIF caseno = 2 THEN -- e1.end = e2.end
    rec.geom = ST_MakeLine(e1rec.geom, st_reverse(e2rec.geom));
    rec.end_node = e2rec.start_node;
    rec.next_left_edge = e2rec.next_right_edge;
    e2sign = -1;
  ELSIF caseno = 3 THEN -- e1.start = e2.start
    rec.geom = ST_MakeLine(st_reverse(e2rec.geom), e1rec.geom);
    rec.start_node = e2rec.end_node;
    rec.next_right_edge = e2rec.next_left_edge;
    e2sign = -1;
  ELSIF caseno = 4 THEN -- e1.start = e2.end
    rec.geom = ST_MakeLine(e2rec.geom, e1rec.geom);
    rec.start_node = e2rec.start_node;
    rec.next_right_edge = e2rec.next_right_edge;
    e2sign = 1;
  END IF;
  EXECUTE 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET geom = ' || quote_literal(rec.geom::text)
    || ', start_node = ' || rec.start_node
    || ', end_node = ' || rec.end_node
    || ', next_left_edge = ' || rec.next_left_edge
    || ', abs_next_left_edge = ' || abs(rec.next_left_edge)
    || ', next_right_edge = ' || rec.next_right_edge
    || ', abs_next_right_edge = ' || abs(rec.next_right_edge)
    || ' WHERE edge_id = ' || e1id;
  -- End of first edge update }

  -- Update next_left_edge/next_right_edge for
  -- any edge having them still pointing at the edge being removed (e2id)
  --
  -- NOTE:
  -- *(next_XXX_edge/e2id) serves the purpose of extracting existing
  -- sign from the value, while *e2sign changes that sign again if we
  -- reverted edge2 direction
  --
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_left_edge = ' || e1id
    || ', next_left_edge = ' || e2sign*e1id
    || '*(next_left_edge/'
    || e2id || ')  WHERE abs_next_left_edge = ' || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET abs_next_right_edge = ' || e1id
    || ', next_right_edge = ' || e2sign*e1id
    || '*(next_right_edge/'
    || e2id || ') WHERE abs_next_right_edge = ' || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;

  -- Delete the second edge
  EXECUTE 'DELETE FROM ' || quote_ident(toponame)
    || '.edge_data WHERE edge_id = ' || e2id;

  -- Delete the common node 
  BEGIN
    EXECUTE 'DELETE FROM ' || quote_ident(toponame)
            || '.node WHERE node_id = ' || commonnode;
    EXCEPTION
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing node table)',
          toponame;
  END;

  --
  -- NOT IN THE SPECS:
  -- Drop composition rows involving second
  -- edge, as the first edge took its space,
  -- and all affected TopoGeom have been previously checked
  -- for being composed by both edges.
  sql := 'DELETE FROM ' || quote_ident(toponame)
    || '.relation r USING topology.layer l '
    || 'WHERE l.level = 0 AND l.feature_type = 2'
    || ' AND l.topology_id = ' || topoid
    || ' AND l.layer_id = r.layer_id AND abs(r.element_id) = '
    || e2id;
  --RAISE DEBUG 'SQL: %', sql;
  EXECUTE sql;

  RETURN commonnode;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_ModEdgeHeal

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.14
--
--  ST_RemEdgeNewFace(atopology, anedge)
--
-- Not in the specs:
-- * Raise an exception if any TopoGeometry is defined by only one
--   of the two faces that will dissolve.
-- * Raise an exception if any TopoGeometry is defined by 
--   the edge being removed.
-- * Properly set containg_face on nodes that remains isolated by the drop
-- * Update containg_face for isolated nodes in the dissolved faces
-- * Update references in the Relation table
-- 
-- }{
CREATE OR REPLACE FUNCTION topology.ST_RemEdgeNewFace(toponame varchar, e1id integer)
  RETURNS int
AS
$$
DECLARE
  e1rec RECORD;
  rec RECORD;
  fidary int[];
  topoid int;
  sql text;
  newfaceid int;
  newfacecreated bool;
  elink int;
BEGIN
  --
  -- toponame and face_id are required
  -- 
  IF toponame IS NULL OR e1id IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  -- Get topology id
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = toponame;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e1id
      INTO STRICT e1rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e1id;
      WHEN INVALID_SCHEMA_NAME THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing edge_data table)',
          toponame;
  END;

  -- Check that no TopoGeometry references the edge being removed
  sql := 'SELECT r.topogeo_id, r.layer_id'
      || ', l.schema_name, l.table_name, l.feature_column '
      || 'FROM topology.layer l INNER JOIN '
      || quote_ident(toponame)
      || '.relation r ON (l.layer_id = r.layer_id) '
      || 'WHERE l.level = 0 AND l.feature_type = 2 '
      || ' AND l.topology_id = ' || topoid
      || ' AND abs(r.element_id) = ' || e1id ;



  FOR rec IN EXECUTE sql LOOP
    RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented dropping edge %',
            rec.topogeo_id, rec.layer_id,
            rec.schema_name, rec.table_name, rec.feature_column,
            e1id;
  END LOOP;

  -- Update next_left_edge and next_right_edge face
  -- for all edges bounding the new face
  RAISE NOTICE 'Updating next_{right,left}_face of ring edges...';

  -- TODO: reduce the following to 2 UPDATE rather than 4

  -- Update next_left_edge of previous edges in left face -- {

  elink := e1rec.next_left_edge;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_left_edge = '
    || elink
    || ', abs_next_left_edge = '
    || abs(elink)
    || ' WHERE next_left_edge < 0 AND abs(next_left_edge) = '
    || e1id;



  EXECUTE sql;

  -- If the edge being removed links to self,
  -- we use the other face
  IF e1rec.abs_next_right_edge = e1rec.edge_id THEN
    elink := e1rec.next_left_edge;
  ELSE
    elink := e1rec.next_right_edge;
  END IF;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_left_edge = '
    || elink
    || ', abs_next_left_edge = '
    || abs(elink)
    || ' WHERE next_left_edge > 0 AND abs(next_left_edge) = '
    || e1id;



  EXECUTE sql;

  -- }

  -- Update next_right_edge of previous edges in right face -- {

  elink := e1rec.next_left_edge;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_right_edge = '
    || elink
    || ', abs_next_right_edge = '
    || abs(elink)
    || ' WHERE next_right_edge < 0 AND abs(next_right_edge) = '
    || e1id;



  EXECUTE sql;

  -- If the edge being removed links to self,
  -- we use the other face
  IF e1rec.abs_next_right_edge = e1rec.edge_id THEN
    elink := e1rec.next_left_edge;
  ELSE
    elink := e1rec.next_right_edge;
  END IF;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_right_edge = '
    || elink
    || ', abs_next_right_edge = '
    || abs(elink)
    || ' WHERE next_right_edge > 0 AND abs(next_right_edge) = '
    || e1id;



  EXECUTE sql;

  -- }

  IF e1rec.left_face = e1rec.right_face THEN -- {

    RAISE NOTICE 'Deletion of edge % affects no face',
                    e1rec.edge_id;

    newfaceid := e1rec.left_face; -- TODO: or what should we return ?
    newfacecreated := false;

  ELSE -- }{

    RAISE NOTICE 'Deletion of edge % joins faces % and %',
                    e1rec.edge_id, e1rec.left_face, e1rec.right_face;

    -- NOT IN THE SPECS:
    -- check if any topo_geom is defined only by one of the
    -- joined faces. In such case there would be no way to adapt
    -- the definition in case of healing, so we'd have to bail out
    --
    -- TODO: use an internal function and share with ST_RemEdgeNewFace
    --
    -- 
    fidary = ARRAY[e1rec.left_face, e1rec.right_face];
    sql := 'SELECT t.* from ('
      || 'SELECT r.topogeo_id, r.layer_id'
      || ', l.schema_name, l.table_name, l.feature_column'
      || ', array_agg(r.element_id) as elems '
      || 'FROM topology.layer l INNER JOIN '
      || quote_ident(toponame)
      || '.relation r ON (l.layer_id = r.layer_id) '
      || 'WHERE l.level = 0 AND l.feature_type = 3 '
      || ' AND l.topology_id = ' || topoid
      || ' AND r.element_id = ANY (' || quote_literal(fidary)
      || ') group by r.topogeo_id, r.layer_id, l.schema_name, l.table_name, '
      || ' l.feature_column ) t';

    -- No surface can be defined by universal face 
    IF e1rec.left_face != 0 AND e1rec.right_face != 0 THEN -- {
      sql := sql || ' WHERE NOT t.elems @> ' || quote_literal(fidary);
    END IF; -- }





    FOR rec IN EXECUTE sql LOOP
      RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented healing faces % and %',
            rec.topogeo_id, rec.layer_id,
            rec.schema_name, rec.table_name, rec.feature_column,
            e1rec.right_face, e1rec.left_face;
    END LOOP;

    IF e1rec.left_face = 0 OR e1rec.right_face = 0 THEN -- {

      --
      -- We won't add any new face, but rather let the universe
      -- flood the removed face.
      --

      newfaceid := 0;
      newfacecreated := false;

    ELSE -- }{

      --
      -- Insert the new face 
      --

      sql := 'SELECT nextval(' || quote_literal(
          quote_ident(toponame) || '.face_face_id_seq'
        ) || ')';

      EXECUTE sql INTO STRICT newfaceid;
      newfacecreated := true;

      sql := 'INSERT INTO '
        || quote_ident(toponame)
        || '.face(face_id, mbr) SELECT '
        -- face_id
        || newfaceid  || ', '
        -- minimum bounding rectangle is the union of the old faces mbr
        -- (doing this without GEOS would be faster)
        || 'ST_Envelope(ST_Union(mbr)) FROM '
        || quote_ident(toponame)
        || '.face WHERE face_id IN (' 
        || e1rec.left_face || ',' || e1rec.right_face 
        || ')';



      EXECUTE sql;

    END IF; -- }

    -- Update left_face for all edges still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.edge_data SET left_face = ' || newfaceid 
      || ' WHERE left_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- Update right_face for all edges still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.edge_data SET right_face = ' || newfaceid 
      || ' WHERE right_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- Update containing_face for all nodes still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.node SET containing_face = ' || newfaceid 
      || ' WHERE containing_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- NOT IN THE SPECS:
    -- Replace composition rows involving the two
    -- faces as one involving the new face.
    -- It takes a DELETE and an UPDATE to do all
    sql := 'DELETE FROM ' || quote_ident(toponame)
      || '.relation r USING topology.layer l '
      || 'WHERE l.level = 0 AND l.feature_type = 3'
      || ' AND l.topology_id = ' || topoid
      || ' AND l.layer_id = r.layer_id AND abs(r.element_id) = '
      || e1rec.left_face;



    EXECUTE sql;
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.relation r '
      || ' SET element_id = ' || newfaceid 
      || ' FROM topology.layer l WHERE l.level = 0 AND l.feature_type = 3'
      || ' AND l.topology_id = ' || topoid
      || ' AND l.layer_id = r.layer_id AND r.element_id = '
      || e1rec.right_face;



    EXECUTE sql;

  END IF; -- } two faces healed...

  -- Delete the edge
  sql := 'DELETE FROM ' || quote_ident(toponame)
    || '.edge_data WHERE edge_id = ' || e1id;



  EXECUTE sql;

  -- Check if any of the edge nodes remains isolated, 
  -- set containing_face  = newfaceid in that case
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.node n SET containing_face = ' || newfaceid
    || ' WHERE node_id IN ('
    || e1rec.start_node || ','
    || e1rec.end_node || ') AND NOT EXISTS (SELECT edge_id FROM '
    || quote_ident(toponame)
    || '.edge_data WHERE start_node = n.node_id OR end_node = n.node_id)';



  EXECUTE sql;

  IF e1rec.right_face != e1rec.left_face THEN -- {

    -- Delete left face, if not universe
    IF e1rec.left_face != 0 THEN
      sql := 'DELETE FROM ' || quote_ident(toponame)
        || '.face WHERE face_id = ' || e1rec.left_face; 



      EXECUTE sql;
    END IF;

    -- Delete right face, if not universe
    IF e1rec.right_face != 0
    THEN
      sql := 'DELETE FROM ' || quote_ident(toponame)
        || '.face WHERE face_id = ' || e1rec.right_face;



      EXECUTE sql;
    END IF;

  END IF; -- }

  IF newfacecreated THEN
    RETURN newfaceid;
  ELSE
    RETURN NULL; -- -newfaceid;
  END IF;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_RemEdgeNewFace

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.15
--
--  ST_RemEdgeModFace(atopology, anedge)
--
-- Not in the specs:
-- * Raise an exception if any TopoGeometry is defined by only one
--   of the two faces that will dissolve.
-- * Raise an exception if any TopoGeometry is defined by 
--   the edge being removed.
-- * Properly set containg_face on nodes that remains isolated by the drop
-- * Update containg_face for isolated nodes in the dissolved faces
-- * Update references in the Relation table
-- * Return id of the face taking up the removed edge space
--
-- }{
CREATE OR REPLACE FUNCTION topology.ST_RemEdgeModFace(toponame varchar, e1id integer)
  RETURNS int
AS
$$
DECLARE
  e1rec RECORD;
  rec RECORD;
  fidary int[];
  topoid int;
  sql text;
  floodfaceid int;
  elink int;
BEGIN
  --
  -- toponame and face_id are required
  -- 
  IF toponame IS NULL OR e1id IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  -- Get topology id
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = toponame;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(toponame)
      || '.edge_data WHERE edge_id = ' || e1id
      INTO STRICT e1rec;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', e1id;
      WHEN INVALID_SCHEMA_NAME THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
      WHEN UNDEFINED_TABLE THEN
        RAISE EXCEPTION 'corrupted topology "%" (missing edge_data table)',
          toponame;
  END;

  -- Check that no TopoGeometry references the edge being removed
  sql := 'SELECT r.topogeo_id, r.layer_id'
      || ', l.schema_name, l.table_name, l.feature_column '
      || 'FROM topology.layer l INNER JOIN '
      || quote_ident(toponame)
      || '.relation r ON (l.layer_id = r.layer_id) '
      || 'WHERE l.level = 0 AND l.feature_type = 2 '
      || ' AND l.topology_id = ' || topoid
      || ' AND abs(r.element_id) = ' || e1id ;



  FOR rec IN EXECUTE sql LOOP
    RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented dropping edge %',
            rec.topogeo_id, rec.layer_id,
            rec.schema_name, rec.table_name, rec.feature_column,
            e1id;
  END LOOP;

  -- Update next_left_edge and next_right_edge face
  -- for all edges bounding the new face
  RAISE NOTICE 'Updating next_{right,left}_face of ring edges...';

  -- TODO: reduce the following to 2 UPDATE rather than 4

  -- Update next_left_edge of previous edges in left face -- {

  elink := e1rec.next_left_edge;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_left_edge = '
    || elink
    || ', abs_next_left_edge = '
    || abs(elink)
    || ' WHERE next_left_edge < 0 AND abs(next_left_edge) = '
    || e1id;



  EXECUTE sql;

  -- If the edge being removed links to self,
  -- we use the other face
  IF e1rec.abs_next_right_edge = e1rec.edge_id THEN
    elink := e1rec.next_left_edge;
  ELSE
    elink := e1rec.next_right_edge;
  END IF;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_left_edge = '
    || elink
    || ', abs_next_left_edge = '
    || abs(elink)
    || ' WHERE next_left_edge > 0 AND abs(next_left_edge) = '
    || e1id;



  EXECUTE sql;

  -- }

  -- Update next_right_edge of previous edges in right face -- {

  elink := e1rec.next_left_edge;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_right_edge = '
    || elink
    || ', abs_next_right_edge = '
    || abs(elink)
    || ' WHERE next_right_edge < 0 AND abs(next_right_edge) = '
    || e1id;



  EXECUTE sql;

  -- If the edge being removed links to self,
  -- we use the other face
  IF e1rec.abs_next_right_edge = e1rec.edge_id THEN
    elink := e1rec.next_left_edge;
  ELSE
    elink := e1rec.next_right_edge;
  END IF;

  sql := 'UPDATE ' || quote_ident(toponame)
    || '.edge_data SET next_right_edge = '
    || elink
    || ', abs_next_right_edge = '
    || abs(elink)
    || ' WHERE next_right_edge > 0 AND abs(next_right_edge) = '
    || e1id;



  EXECUTE sql;

  -- }

  IF e1rec.left_face = e1rec.right_face THEN -- {

    RAISE NOTICE 'Deletion of edge % affects no face',
                    e1rec.edge_id;

    floodfaceid = e1rec.left_face; 

  ELSE -- }{

    RAISE NOTICE 'Deletion of edge % joins faces % and %',
                    e1rec.edge_id, e1rec.left_face, e1rec.right_face;

    -- NOT IN THE SPECS:
    -- check if any topo_geom is defined only by one of the
    -- joined faces. In such case there would be no way to adapt
    -- the definition in case of healing, so we'd have to bail out
    --
    -- TODO: use an internal function and share with ST_RemEdgeNewFace
    --
    fidary = ARRAY[e1rec.left_face, e1rec.right_face];
    sql := 'SELECT t.* from ('
      || 'SELECT r.topogeo_id, r.layer_id'
      || ', l.schema_name, l.table_name, l.feature_column'
      || ', array_agg(r.element_id) as elems '
      || 'FROM topology.layer l INNER JOIN '
      || quote_ident(toponame)
      || '.relation r ON (l.layer_id = r.layer_id) '
      || 'WHERE l.level = 0 AND l.feature_type = 3 '
      || ' AND l.topology_id = ' || topoid
      || ' AND r.element_id = ANY (' || quote_literal(fidary) 
      || ') group by r.topogeo_id, r.layer_id, l.schema_name, l.table_name, '
      || ' l.feature_column ) t';

    -- No surface can be defined by universal face 
    IF NOT 0 = ANY ( fidary ) THEN -- {
      sql := sql || ' WHERE NOT t.elems @> ' || quote_literal(fidary);
    END IF; -- }





    FOR rec IN EXECUTE sql LOOP
      RAISE EXCEPTION 'TopoGeom % in layer % (%.%.%) cannot be represented healing faces % and %',
            rec.topogeo_id, rec.layer_id,
            rec.schema_name, rec.table_name, rec.feature_column,
            e1rec.right_face, e1rec.left_face;
    END LOOP;

    IF e1rec.left_face = 0 OR e1rec.right_face = 0 THEN -- {

      --
      -- We won't add any new face, but rather let the universe
      -- flood the removed face.
      --

      floodfaceid = 0;

    ELSE -- }{

      -- we choose right face as the face that will remain
      -- to be symmetric with ST_AddEdgeModFace 
      floodfaceid = e1rec.right_face;

      sql := 'UPDATE '
        || quote_ident(toponame)
        || '.face SET mbr = (SELECT '
        -- minimum bounding rectangle is the union of the old faces mbr
        -- (doing this without GEOS would be faster)
        || 'ST_Envelope(ST_Union(mbr)) FROM '
        || quote_ident(toponame)
        || '.face WHERE face_id IN (' 
        || e1rec.left_face || ',' || e1rec.right_face 
        || ') ) WHERE face_id = ' || floodfaceid ;



      EXECUTE sql;

    END IF; -- }

    -- Update left_face for all edges still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.edge_data SET left_face = ' || floodfaceid 
      || ' WHERE left_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- Update right_face for all edges still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.edge_data SET right_face = ' || floodfaceid 
      || ' WHERE right_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- Update containing_face for all nodes still referencing old faces
    sql := 'UPDATE ' || quote_ident(toponame)
      || '.node SET containing_face = ' || floodfaceid 
      || ' WHERE containing_face IN ('
      || e1rec.left_face || ',' || e1rec.right_face 
      || ')';



    EXECUTE sql;

    -- NOT IN THE SPECS:
    -- Replace composition rows involving the two
    -- faces as one involving the new face.
    -- It takes a single DELETE to do that.
    sql := 'DELETE FROM ' || quote_ident(toponame)
      || '.relation r USING topology.layer l '
      || 'WHERE l.level = 0 AND l.feature_type = 3'
      || ' AND l.topology_id = ' || topoid
      || ' AND l.layer_id = r.layer_id AND abs(r.element_id) IN ('
      || e1rec.left_face || ',' || e1rec.right_face
      || ') AND abs(r.element_id) != '
      || floodfaceid; -- could be optimized..



    EXECUTE sql;

  END IF; -- } two faces healed...

  -- Delete the edge
  sql := 'DELETE FROM ' || quote_ident(toponame)
    || '.edge_data WHERE edge_id = ' || e1id;



  EXECUTE sql;

  -- Check if any of the edge nodes remains isolated, 
  -- set containing_face  = floodfaceid in that case
  sql := 'UPDATE ' || quote_ident(toponame)
    || '.node n SET containing_face = ' || floodfaceid
    || ' WHERE node_id IN ('
    || e1rec.start_node || ','
    || e1rec.end_node || ') AND NOT EXISTS (SELECT edge_id FROM '
    || quote_ident(toponame)
    || '.edge_data WHERE start_node = n.node_id OR end_node = n.node_id)';



  EXECUTE sql;

  IF e1rec.right_face != e1rec.left_face THEN -- {

    -- Delete left face, if not universe and not "flood" face
    IF e1rec.left_face != 0 AND e1rec.left_face != floodfaceid
    THEN
      sql := 'DELETE FROM ' || quote_ident(toponame)
        || '.face WHERE face_id = ' || e1rec.left_face; 



      EXECUTE sql;
    END IF;

    -- Delete right face, if not universe and not "flood" face
    IF e1rec.right_face != 0 AND e1rec.right_face != floodfaceid
    THEN
      sql := 'DELETE FROM ' || quote_ident(toponame)
        || '.face WHERE face_id = ' || e1rec.right_face;



      EXECUTE sql;
    END IF;

  END IF; -- }

  RETURN floodfaceid;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_RemEdgeModFace


--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.16
--
--  ST_GetFaceGeometry(atopology, aface)
-- 
CREATE OR REPLACE FUNCTION topology.ST_GetFaceGeometry(toponame varchar, aface integer)
  RETURNS GEOMETRY AS
$$
DECLARE
  rec RECORD;
  sql TEXT;
BEGIN

  --
  -- toponame and aface are required
  -- 
  IF toponame IS NULL OR aface IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  IF toponame = '' THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END IF;

  IF aface = 0 THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - universal face has no geometry';
  END IF;

  BEGIN

    -- No such face
    sql := 'SELECT NOT EXISTS (SELECT * from ' || quote_ident(toponame)
      || '.face WHERE face_id = ' || aface
      || ') as none';
    EXECUTE sql INTO rec;
    IF rec.none THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent face.';
    END IF;

    --
    -- Construct face 
    -- 
    sql :=
      'SELECT ST_BuildArea(ST_Collect(geom)) as geom FROM '
      || quote_ident(toponame)
      || '.edge_data WHERE left_face = ' || aface
      || ' OR right_face = ' || aface;
    FOR rec IN EXECUTE sql
    LOOP
      RETURN rec.geom;
    END LOOP;

  EXCEPTION
    WHEN INVALID_SCHEMA_NAME THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
    WHEN UNDEFINED_TABLE THEN
      RAISE EXCEPTION 'corrupted topology "%"', toponame;
  END;

  RETURN NULL;
END
$$
LANGUAGE 'plpgsql' STABLE;
--} ST_GetFaceGeometry


--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.1 
--
--  ST_AddIsoNode(atopology, aface, apoint)
--
CREATE OR REPLACE FUNCTION topology.ST_AddIsoNode(atopology varchar, aface integer, apoint geometry)
  RETURNS INTEGER AS
$$
DECLARE
  rec RECORD;
  nodeid integer;
  sql text;
  containingface integer;
BEGIN

  --
  -- Atopology and apoint are required
  -- 
  IF atopology IS NULL OR apoint IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Apoint must be a point
  --
  IF substring(geometrytype(apoint), 1, 5) != 'POINT'
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - invalid point';
  END IF;

  --
  -- Check if a coincident node already exists
  -- 
  -- We use index AND x/y equality
  --
  FOR rec IN EXECUTE 'SELECT node_id FROM '
    || quote_ident(atopology) || '.node ' ||
    'WHERE ST_Equals(geom, ' || quote_literal(apoint::text) || '::geometry)'
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - coincident node';
  END LOOP;

  --
  -- Check if any edge crosses (intersects) this node
  -- I used _intersects_ here to include boundaries (endpoints)
  --
  FOR rec IN EXECUTE 'SELECT edge_id FROM '
    || quote_ident(atopology) || '.edge ' 
    || 'WHERE ST_Intersects(geom, ' || quote_literal(apoint::text)
    || '::geometry)'
  LOOP
    RAISE EXCEPTION
    'SQL/MM Spatial exception - edge crosses node.';
  END LOOP;

  -- retrieve the face that contains (eventually) the point
  
  --
  -- first test is to check if there is inside an mbr (more fast)
  --
  sql := 'SELECT f.face_id FROM ' 
        || quote_ident(atopology) 
        || '.face f WHERE f.face_id > 0 AND f.mbr && '
        || quote_literal(apoint::text)
        || '::geometry AND ST_Contains(topology.ST_GetFaceGeometry('
        || quote_literal(atopology) 
        || ', f.face_id), '
        || quote_literal(apoint::text)
        || '::geometry)';
  IF aface IS NOT NULL AND aface != 0 THEN
    sql := sql || ' AND f.face_id = ' || aface;
  END IF;




  EXECUTE sql INTO containingface;

  -- If aface was specified, check that it was correct
  IF aface IS NOT NULL THEN -- {
    IF aface = 0 THEN -- {
      IF containingface IS NOT NULL THEN -- {
        RAISE EXCEPTION
          'SQL/MM Spatial exception - within face % (not universe)',
          containingface;
      ELSE -- }{
        containingface := 0;
      END IF; -- }
    ELSE -- }{ -- aface != 0
      IF containingface IS NULL OR containingface != aface THEN -- {
        RAISE EXCEPTION 'SQL/MM Spatial exception - not within face';
      END IF; -- }
    END IF; -- }
  ELSE -- }{ -- aface is null
    containingface := COALESCE(containingface, 0);
  END IF; -- }

  --
  -- Insert the new row
  --
  sql := 'INSERT INTO '
      || quote_ident(atopology)
      || '.node(node_id, geom, containing_face) SELECT nextval('
      || quote_literal( quote_ident(atopology) || '.node_node_id_seq' )
      || '),'
      ||quote_literal(apoint::text)
      || '::geometry,' || containingface
      || ' RETURNING node_id';




  EXECUTE sql INTO nodeid;

  RETURN nodeid;
EXCEPTION
  -- TODO: avoid the EXCEPTION handling here ?
  WHEN INVALID_SCHEMA_NAME THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_AddIsoNode

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.2 
--
--  ST_MoveIsoNode(atopology, anode, apoint)
--
CREATE OR REPLACE FUNCTION topology.ST_MoveIsoNode(atopology character varying, anode integer, apoint geometry)
  RETURNS text AS
$$
DECLARE
  rec RECORD;
BEGIN

  --
  -- All arguments are required
  -- 
  IF atopology IS NULL OR anode IS NULL OR apoint IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Apoint must be a point
  --
  IF substring(geometrytype(apoint), 1, 5) != 'POINT'
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - invalid point';
  END IF;

  --
  -- Check node isolation.
  -- 
  FOR rec IN EXECUTE 'SELECT edge_id FROM '
    || quote_ident(atopology) || '.edge ' ||
    ' WHERE start_node =  ' || anode ||
    ' OR end_node = ' || anode 
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - not isolated node';
  END LOOP;

  --
  -- Check if a coincident node already exists
  -- 
  -- We use index AND x/y equality
  --
  FOR rec IN EXECUTE 'SELECT node_id FROM '
    || quote_ident(atopology) || '.node ' ||
    'WHERE geom && ' || quote_literal(apoint::text) || '::geometry'
    ||' AND ST_X(geom) = ST_X('||quote_literal(apoint::text)||'::geometry)'
    ||' AND ST_Y(geom) = ST_Y('||quote_literal(apoint::text)||'::geometry)'
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - coincident node';
  END LOOP;

  --
  -- Check if any edge crosses (intersects) this node
  -- I used _intersects_ here to include boundaries (endpoints)
  --
  FOR rec IN EXECUTE 'SELECT edge_id FROM '
    || quote_ident(atopology) || '.edge ' 
    || 'WHERE geom && ' || quote_literal(apoint::text) 
    || ' AND ST_Intersects(geom, ' || quote_literal(apoint::text)
    || '::geometry)'
  LOOP
    RAISE EXCEPTION
    'SQL/MM Spatial exception - edge crosses node.';
  END LOOP;

  --
  -- Update node point
  --
  EXECUTE 'UPDATE ' || quote_ident(atopology) || '.node '
    || ' SET geom = ' || quote_literal(apoint::text) 
    || ' WHERE node_id = ' || anode;

  RETURN 'Isolated Node ' || anode || ' moved to location '
    || ST_X(apoint) || ',' || ST_Y(apoint);
END
$$
  LANGUAGE plpgsql VOLATILE;
--} ST_MoveIsoNode

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.3 
--
--  ST_RemoveIsoNode(atopology, anode)
--
CREATE OR REPLACE FUNCTION topology.ST_RemoveIsoNode(atopology varchar, anode integer)
  RETURNS TEXT AS
$$
DECLARE
  rec RECORD;
BEGIN

  --
  -- Atopology and apoint are required
  -- 
  IF atopology IS NULL OR anode IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Check node isolation.
  -- 
  FOR rec IN EXECUTE 'SELECT edge_id FROM '
    || quote_ident(atopology) || '.edge_data ' ||
    ' WHERE start_node =  ' || anode ||
    ' OR end_node = ' || anode 
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - not isolated node';
  END LOOP;

  EXECUTE 'DELETE FROM ' || quote_ident(atopology) || '.node '
    || ' WHERE node_id = ' || anode;

  RETURN 'Isolated node ' || anode || ' removed';
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_RemoveIsoNode

--{
-- According to http://trac.osgeo.org/postgis/ticket/798
-- ST_RemoveIsoNode was renamed to ST_RemIsoNode in the final ISO
-- document
--
CREATE OR REPLACE FUNCTION topology.ST_RemIsoNode(varchar, integer)
  RETURNS TEXT AS
$$
  SELECT topology.ST_RemoveIsoNode($1, $2)
$$ LANGUAGE 'sql' VOLATILE;

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.7 
--
--  ST_RemoveIsoEdge(atopology, anedge)
--
CREATE OR REPLACE FUNCTION topology.ST_RemoveIsoEdge(atopology varchar, anedge integer)
  RETURNS TEXT AS
$$
DECLARE
  edge RECORD;
  rec RECORD;
  ok BOOL;
BEGIN

  --
  -- Atopology and anedge are required
  -- 
  IF atopology IS NULL OR anedge IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Check node existance
  -- 
  ok = false;
  FOR edge IN EXECUTE 'SELECT * FROM '
    || quote_ident(atopology) || '.edge_data ' ||
    ' WHERE edge_id =  ' || anedge
  LOOP
    ok = true;
  END LOOP;
  IF NOT ok THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - non-existent edge';
  END IF;

  --
  -- Check node isolation
  -- 
  IF edge.left_face != edge.right_face THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - not isolated edge';
  END IF;

  FOR rec IN EXECUTE 'SELECT * FROM '
    || quote_ident(atopology) || '.edge_data ' 
    || ' WHERE edge_id !=  ' || anedge
    || ' AND ( start_node = ' || edge.start_node
    || ' OR start_node = ' || edge.end_node
    || ' OR end_node = ' || edge.start_node
    || ' OR end_node = ' || edge.end_node
    || ' ) '
  LOOP
    RAISE EXCEPTION
      'SQL/MM Spatial exception - not isolated edge';
  END LOOP;

  --
  -- Delete the edge
  --
  EXECUTE 'DELETE FROM ' || quote_ident(atopology) || '.edge_data '
    || ' WHERE edge_id = ' || anedge;

  RETURN 'Isolated edge ' || anedge || ' removed';
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_RemoveIsoEdge

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.8 
--
--  ST_NewEdgesSplit(atopology, anedge, apoint)
--
-- Not in the specs:
-- * Update references in the Relation table.
--
CREATE OR REPLACE FUNCTION topology.ST_NewEdgesSplit(atopology varchar, anedge integer, apoint geometry)
  RETURNS INTEGER AS
$$
DECLARE
  oldedge RECORD;
  rec RECORD;
  tmp integer;
  topoid integer;
  nodeid integer;
  nodepos float8;
  edgeid1 integer;
  edgeid2 integer;
  edge1 geometry;
  edge2 geometry;
  ok BOOL;
BEGIN

  --
  -- All args required
  -- 
  IF atopology IS NULL OR anedge IS NULL OR apoint IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Check node existance
  -- 
  ok = false;
  FOR oldedge IN EXECUTE 'SELECT * FROM '
    || quote_ident(atopology) || '.edge_data ' ||
    ' WHERE edge_id =  ' || anedge
  LOOP
    ok = true;
  END LOOP;
  IF NOT ok THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - non-existent edge';
  END IF;

  --
  -- Check that given point is Within(anedge.geom)
  -- 
  IF NOT ST_Within(apoint, oldedge.geom) THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - point not on edge';
  END IF;

  --
  -- Check if a coincident node already exists
  --
  FOR rec IN EXECUTE 'SELECT node_id FROM '
    || quote_ident(atopology) || '.node '
    || 'WHERE geom && '
    || quote_literal(apoint::text) || '::geometry'
    || ' AND ST_X(geom) = ST_X('
    || quote_literal(apoint::text) || '::geometry)'
    || ' AND ST_Y(geom) = ST_Y('
    || quote_literal(apoint::text) || '::geometry)'
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - coincident node';
  END LOOP;

  --
  -- Get new node id
  --
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.node_node_id_seq'')'
  LOOP
    nodeid = rec.nextval;
  END LOOP;

  --RAISE NOTICE 'Next node id = % ', nodeid;

  --
  -- Add the new node 
  --
  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.node(node_id, geom) 
    VALUES(' || nodeid || ','
    || quote_literal(apoint::text)
    || ')';

  --
  -- Delete the old edge
  --
  EXECUTE 'DELETE FROM ' || quote_ident(atopology) || '.edge_data '
    || ' WHERE edge_id = ' || anedge;

  --
  -- Compute new edges
  --
  edge2 := ST_Split(oldedge.geom, apoint);
  edge1 := ST_GeometryN(edge2, 1);
  edge2 := ST_GeometryN(edge2, 2);

  --
  -- Get ids for the new edges 
  --
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.edge_data_edge_id_seq'')'
  LOOP
    edgeid1 = rec.nextval;
  END LOOP;
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.edge_data_edge_id_seq'')'
  LOOP
    edgeid2 = rec.nextval;
  END LOOP;





  --RAISE NOTICE 'EdgeId1 % EdgeId2 %', edgeid1, edgeid2;

  --RAISE DEBUG 'oldedge.next_left_edge: %', oldedge.next_left_edge;
  --RAISE DEBUG 'oldedge.next_right_edge: %', oldedge.next_right_edge;

  --
  -- Insert the two new edges
  --
  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.edge VALUES('
    || edgeid1                                -- edge_id
    || ',' || oldedge.start_node              -- start_node
    || ',' || nodeid                          -- end_node
    || ',' || edgeid2                         -- next_left_edge
    || ',' || CASE                            -- next_right_edge
               WHEN 
                oldedge.next_right_edge = anedge
               THEN edgeid1
               WHEN
                oldedge.next_right_edge = -anedge
               THEN -edgeid2
               ELSE oldedge.next_right_edge
              END
    || ',' || oldedge.left_face               -- left_face
    || ',' || oldedge.right_face              -- right_face
    || ',' || quote_literal(edge1::text)      -- geom
    ||')';

  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.edge VALUES('
    || edgeid2                                -- edge_id
    || ',' || nodeid                          -- start_node
    || ',' || oldedge.end_node                -- end_node
    || ',' || CASE                            -- next_left_edge
               WHEN 
                oldedge.next_left_edge =
                -anedge
               THEN -edgeid2
               WHEN 
                oldedge.next_left_edge =
                anedge
               THEN edgeid1
               ELSE oldedge.next_left_edge
              END
    || ',' || -edgeid1                        -- next_right_edge
    || ',' || oldedge.left_face               -- left_face
    || ',' || oldedge.right_face              -- right_face
    || ',' || quote_literal(edge2::text)      -- geom
    ||')';

  --
  -- Update all next edge references to match new layout
  --

  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET next_right_edge = '
    || edgeid2
    || ','
    || ' abs_next_right_edge = ' || edgeid2
    || ' WHERE next_right_edge = ' || anedge
    || ' AND edge_id NOT IN (' || edgeid1 || ',' || edgeid2 || ')'
    ;
  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET next_right_edge = '
    || -edgeid1
    || ','
    || ' abs_next_right_edge = ' || edgeid1
    || ' WHERE next_right_edge = ' || -anedge
    || ' AND edge_id NOT IN (' || edgeid1 || ',' || edgeid2 || ')'
    ;

  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET next_left_edge = '
    || edgeid1
    || ','
    || ' abs_next_left_edge = ' || edgeid1
    || ' WHERE next_left_edge = ' || anedge
    || ' AND edge_id NOT IN (' || edgeid1 || ',' || edgeid2 || ')'
    ;
  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET '
    || ' next_left_edge = ' || -edgeid2
    || ','
    || ' abs_next_left_edge = ' || edgeid2
    || ' WHERE next_left_edge = ' || -anedge
    || ' AND edge_id NOT IN (' || edgeid1 || ',' || edgeid2 || ')'
    ;

  -- Get topology id
        SELECT id FROM topology.topology into topoid
                WHERE name = atopology;
  IF topoid IS NULL THEN
    RAISE EXCEPTION 'No topology % registered',
      quote_ident(atopology);
  END IF;

  --
  -- Update references in the Relation table.
  -- We only take into considerations non-hierarchical
  -- TopoGeometry here, for obvious reasons.
  --
  FOR rec IN EXECUTE 'SELECT r.* FROM '
    || quote_ident(atopology)
    || '.relation r, topology.layer l '
    || ' WHERE '
    || ' l.topology_id = ' || topoid
    || ' AND l.level = 0 '
    || ' AND l.layer_id = r.layer_id '
    || ' AND abs(r.element_id) = ' || anedge
    || ' AND r.element_type = 2'
  LOOP
    --RAISE NOTICE 'TopoGeometry % in layer % contains the edge being split', rec.topogeo_id, rec.layer_id;

    -- Delete old reference
    EXECUTE 'DELETE FROM ' || quote_ident(atopology)
      || '.relation '
      || ' WHERE '
      || 'layer_id = ' || rec.layer_id
      || ' AND '
      || 'topogeo_id = ' || rec.topogeo_id
      || ' AND '
      || 'element_type = ' || rec.element_type
      || ' AND '
      || 'abs(element_id) = ' || anedge;

    -- Add new reference to edge1
    IF rec.element_id < 0 THEN
      tmp = -edgeid1;
    ELSE
      tmp = edgeid1;
    END IF;
    EXECUTE 'INSERT INTO ' || quote_ident(atopology)
      || '.relation '
      || ' VALUES( '
      || rec.topogeo_id
      || ','
      || rec.layer_id
      || ','
      || tmp
      || ','
      || rec.element_type
      || ')';

    -- Add new reference to edge2
    IF rec.element_id < 0 THEN
      tmp = -edgeid2;
    ELSE
      tmp = edgeid2;
    END IF;
    EXECUTE 'INSERT INTO ' || quote_ident(atopology)
      || '.relation '
      || ' VALUES( '
      || rec.topogeo_id
      || ','
      || rec.layer_id
      || ','
      || tmp
      || ','
      || rec.element_type
      || ')';
      
  END LOOP;

  --RAISE NOTICE 'Edge % split in edges % and % by node %',
  --  anedge, edgeid1, edgeid2, nodeid;

  RETURN nodeid; 
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_NewEdgesSplit

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.9 
--
--  ST_ModEdgeSplit(atopology, anedge, apoint)
--
-- Not in the specs:
-- * Update references in the Relation table.
--
CREATE OR REPLACE FUNCTION topology.ST_ModEdgeSplit(atopology varchar, anedge integer, apoint geometry)
  RETURNS INTEGER AS
$$
DECLARE
  oldedge RECORD;
  rec RECORD;
  tmp integer;
  topoid integer;
  nodeid integer;
  nodepos float8;
  newedgeid integer;
  newedge1 geometry;
  newedge2 geometry;
  query text;
  ok BOOL;
BEGIN

  --
  -- All args required
  -- 
  IF atopology IS NULL OR anedge IS NULL OR apoint IS NULL THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  -- Get topology id
  SELECT id FROM topology.topology into topoid
    WHERE name = atopology;

  --
  -- Check node existance
  -- 
  ok = false;
  FOR oldedge IN EXECUTE 'SELECT * FROM '
    || quote_ident(atopology) || '.edge_data ' ||
    ' WHERE edge_id =  ' || anedge
  LOOP
    ok = true;
  END LOOP;
  IF NOT ok THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - non-existent edge';
  END IF;

  --
  -- Check that given point is Within(anedge.geom)
  -- 
  IF NOT ST_Within(apoint, oldedge.geom) THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - point not on edge';
  END IF;

  --
  -- Check if a coincident node already exists
  --
  FOR rec IN EXECUTE 'SELECT node_id FROM '
    || quote_ident(atopology) || '.node ' ||
    'WHERE geom && '
    || quote_literal(apoint::text) || '::geometry'
    ||' AND ST_X(geom) = ST_X('
    || quote_literal(apoint::text) || '::geometry)'
    ||' AND ST_Y(geom) = ST_Y('
    ||quote_literal(apoint::text)||'::geometry)'
  LOOP
    RAISE EXCEPTION
     'SQL/MM Spatial exception - coincident node';
  END LOOP;

  --
  -- Get new node id
  --
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.node_node_id_seq'')'
  LOOP
    nodeid = rec.nextval;
  END LOOP;

  --RAISE NOTICE 'Next node id = % ', nodeid;

  --
  -- Add the new node 
  --
  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.node(node_id, geom) 
    VALUES('||nodeid||','||quote_literal(apoint::text)||
    ')';

  --
  -- Compute new edge
  --
  newedge2 := ST_Split(oldedge.geom, apoint);
  newedge1 := ST_GeometryN(newedge2, 1);
  newedge2 := ST_GeometryN(newedge2, 2);

  --
  -- Get ids for the new edge
  --
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.edge_data_edge_id_seq'')'
  LOOP
    newedgeid = rec.nextval;
  END LOOP;





  --
  -- Insert the new edge
  --
  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.edge '
    || '(edge_id, start_node, end_node,'
    || 'next_left_edge, next_right_edge,'
    || 'left_face, right_face, geom) '
    || 'VALUES('
    || newedgeid
    || ',' || nodeid
    || ',' || oldedge.end_node
    || ',' || COALESCE(                      -- next_left_edge
                NULLIF(
                  oldedge.next_left_edge,
                  -anedge
                ),
                -newedgeid
              )
    || ',' || -anedge                        -- next_right_edge
    || ',' || oldedge.left_face              -- left_face
    || ',' || oldedge.right_face             -- right_face
    || ',' || quote_literal(newedge2::text)  -- geom
    ||')';

  --
  -- Update the old edge
  --
  EXECUTE 'UPDATE ' || quote_ident(atopology) || '.edge_data '
    || ' SET geom = ' || quote_literal(newedge1::text)
    || ','
    || ' next_left_edge = ' || newedgeid
    || ', abs_next_left_edge = ' || newedgeid
    || ','
    || ' end_node = ' || nodeid
    || ' WHERE edge_id = ' || anedge;


  --
  -- Update all next edge references to match new layout
  --

  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET next_right_edge = '
    || -newedgeid 
    || ','
    || ' abs_next_right_edge = ' || newedgeid
    || ' WHERE edge_id != ' || newedgeid
    || ' AND next_right_edge = ' || -anedge;

  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.edge_data SET '
    || ' next_left_edge = ' || -newedgeid
    || ','
    || ' abs_next_left_edge = ' || newedgeid
    || ' WHERE edge_id != ' || newedgeid
    || ' AND next_left_edge = ' || -anedge;

  --
  -- Update references in the Relation table.
  -- We only take into considerations non-hierarchical
  -- TopoGeometry here, for obvious reasons.
  --
  FOR rec IN EXECUTE 'SELECT r.* FROM '
    || quote_ident(atopology)
    || '.relation r, topology.layer l '
    || ' WHERE '
    || ' l.topology_id = ' || topoid
    || ' AND l.level = 0 '
    || ' AND l.layer_id = r.layer_id '
    || ' AND abs(r.element_id) = ' || anedge
    || ' AND r.element_type = 2'
  LOOP
    --RAISE NOTICE 'TopoGeometry % in layer % contains the edge being split (%) - updating to add new edge %', rec.topogeo_id, rec.layer_id, anedge, newedgeid;

    -- Add new reference to edge1
    IF rec.element_id < 0 THEN
      tmp = -newedgeid;
    ELSE
      tmp = newedgeid;
    END IF;
    query = 'INSERT INTO ' || quote_ident(atopology)
      || '.relation '
      || ' VALUES( '
      || rec.topogeo_id
      || ','
      || rec.layer_id
      || ','
      || tmp
      || ','
      || rec.element_type
      || ')';

    --RAISE NOTICE '%', query;
    EXECUTE query;
  END LOOP;

  --RAISE NOTICE 'Edge % split in edges % and % by node %',
  --  anedge, anedge, newedgeid, nodeid;

  RETURN nodeid; 
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_ModEdgesSplit

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.4 
--
--  ST_AddIsoEdge(atopology, anode, anothernode, acurve)
-- 
-- Not in the specs:
-- * Reset containing_face for starting and ending point,
--   as they stop being isolated nodes
-- * Refuse to add a closed edge, as it would not be isolated
--   (ie: would create a ring)
--
-- }{
--
CREATE OR REPLACE FUNCTION topology.ST_AddIsoEdge(atopology varchar, anode integer, anothernode integer, acurve geometry)
  RETURNS INTEGER AS
$$
DECLARE
  aface INTEGER;
  face GEOMETRY;
  snodegeom GEOMETRY;
  enodegeom GEOMETRY;
  count INTEGER;
  rec RECORD;
  edgeid INTEGER;
BEGIN

  --
  -- All arguments required
  -- 
  IF atopology IS NULL
     OR anode IS NULL
     OR anothernode IS NULL
     OR acurve IS NULL
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  -- NOT IN THE SPECS:
  -- A closed edge is never isolated (as it forms a face)
  IF anode = anothernode THEN
      RAISE EXCEPTION
       'Closed edges would not be isolated, try ST_AddEdgeNewFaces';
  END IF;

  --
  -- Acurve must be a LINESTRING
  --
  IF substring(geometrytype(acurve), 1, 4) != 'LINE'
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - invalid curve';
  END IF;

  --
  -- Acurve must be simple
  --
  IF NOT ST_IsSimple(acurve)
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - curve not simple';
  END IF;

  --
  -- Check for:
  --    existence of nodes
  --    nodes faces match
  -- Extract:
  --    nodes face id
  --    nodes geoms
  --
  aface := NULL;
  count := 0;
  FOR rec IN EXECUTE 'SELECT geom, containing_face, node_id FROM '
    || quote_ident(atopology) || '.node
    WHERE node_id = ' || anode ||
    ' OR node_id = ' || anothernode
  LOOP 

    IF rec.containing_face IS NULL THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - not isolated node';
    END IF;

    IF aface IS NULL THEN
      aface := rec.containing_face;
    ELSE
      IF aface != rec.containing_face THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - nodes in different faces';
      END IF;
    END IF;

    -- Get nodes geom
    IF rec.node_id = anode THEN
      snodegeom = rec.geom;
    ELSE
      enodegeom = rec.geom;
    END IF;

    count = count+1;

  END LOOP;

  -- TODO: don't need count, can do with snodegeom/enodegeom instead..
  IF count < 2 THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent node';
  END IF;


  --
  -- l) Check that start point of acurve match start node
  -- geoms.
  -- 
  IF ST_X(snodegeom) != ST_X(ST_StartPoint(acurve)) OR
     ST_Y(snodegeom) != ST_Y(ST_StartPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - start node not geometry start point.';
  END IF;

  --
  -- m) Check that end point of acurve match end node
  -- geoms.
  -- 
  IF ST_X(enodegeom) != ST_X(ST_EndPoint(acurve)) OR
     ST_Y(enodegeom) != ST_Y(ST_EndPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - end node not geometry end point.';
  END IF;

  --
  -- n) Check if curve crosses (contains) any node
  -- I used _contains_ here to leave endpoints out
  -- 
  FOR rec IN EXECUTE 'SELECT node_id FROM '
    || quote_ident(atopology) || '.node '
    || ' WHERE geom && ' || quote_literal(acurve::text) 
    || ' AND ST_Contains(' || quote_literal(acurve::text)
    || ',geom)'
  LOOP
    RAISE EXCEPTION
      'SQL/MM Spatial exception - geometry crosses a node';
  END LOOP;

  --
  -- o) Check if curve intersects any other edge
  -- 
  FOR rec IN EXECUTE 'SELECT * FROM '
    || quote_ident(atopology) || '.edge_data
    WHERE ST_Intersects(geom, ' || quote_literal(acurve::text) || '::geometry)'
  LOOP
    RAISE EXCEPTION 'SQL/MM Spatial exception - geometry intersects an edge';
  END LOOP;

  --
  -- Get new edge id from sequence
  --
  FOR rec IN EXECUTE 'SELECT nextval(''' ||
    atopology || '.edge_data_edge_id_seq'')'
  LOOP
    edgeid = rec.nextval;
  END LOOP;

  -- TODO: this should likely be an exception instead !
  IF aface IS NULL THEN
    aface := 0;
  END IF;

  --
  -- Insert the new row
  --
  EXECUTE 'INSERT INTO ' || quote_ident(atopology)
    || '.edge VALUES(' || edgeid || ',' || anode
    || ',' || anothernode || ',' || (-edgeid)
    || ',' || edgeid || ','
    || aface || ',' || aface || ','
    || quote_literal(acurve::text) || ')';

  --
  -- Update Node containing_face values
  --
  -- the nodes anode and anothernode are no more isolated
  -- because now there is an edge connecting them
  -- 
  EXECUTE 'UPDATE ' || quote_ident(atopology)
    || '.node SET containing_face = NULL where (node_id ='
    || anode
    || ' OR node_id='
    || anothernode
    || ')';

  RETURN edgeid;

END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_AddIsoEdge

-- Internal function used by ST_ChangeEdgeGeom to compare
-- adjacent edges of an edge endpoint
--
-- @param anode the node to use edge end star of
-- @param anedge the directed edge to get adjacents from
--        if positive `anode' is assumed to be its start node
--        if negative `anode' is assumed to be its end node
--      
-- {
CREATE OR REPLACE FUNCTION topology._ST_AdjacentEdges(atopology varchar, anode integer, anedge integer)
RETURNS integer[] AS
$$
DECLARE
  ret integer[];
BEGIN
  WITH edgestar AS (
    SELECT *, count(*) over () AS cnt
    FROM GetNodeEdges(atopology, anode)
  )
  SELECT ARRAY[ (
      SELECT p.edge AS prev FROM edgestar p
      WHERE p.sequence = CASE WHEN m.sequence-1 < 1 THEN cnt
                         ELSE m.sequence-1 END
    ), (
      SELECT p.edge AS prev FROM edgestar p WHERE p.sequence = ((m.sequence)%cnt)+1
    ) ]
  FROM edgestar m
  WHERE edge = anedge
  INTO ret;

  RETURN ret;
END
$$
LANGUAGE 'plpgsql' STABLE;
--}

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.6
--
--  ST_ChangeEdgeGeom(atopology, anedge, acurve)
--
-- Not in the specs:
-- * Raise an exception if given a non-existent edge
-- * Raise an exception if movement is not topologically isomorphic
--
-- }{
CREATE OR REPLACE FUNCTION topology.ST_ChangeEdgeGeom(atopology varchar, anedge integer, acurve geometry)
  RETURNS TEXT AS
$$
DECLARE
  rec RECORD;
  rng_info RECORD; -- movement range info
  oldedge RECORD;
  range GEOMETRY; -- movement range
  tmp1 GEOMETRY;
  snode_info RECORD;
  enode_info RECORD;
  sql TEXT;
  iscw BOOLEAN;
BEGIN

  --
  -- All arguments required
  -- 
  IF atopology IS NULL
     OR anedge IS NULL
     OR acurve IS NULL
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Acurve must be a LINESTRING
  --
  IF substring(geometrytype(acurve), 1, 4) != 'LINE'
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - invalid curve';
  END IF;

  --
  -- Acurve must be a simple
  --
  IF NOT ST_IsSimple(acurve)
  THEN
    RAISE EXCEPTION
     'SQL/MM Spatial exception - curve not simple';
  END IF;

  --
  -- Get data about existing edge
  --
  BEGIN
    EXECUTE 'SELECT * FROM ' || quote_ident(atopology) || '.edge_data  '
      || ' WHERE edge_id = ' || anedge
    INTO STRICT oldedge;
  EXCEPTION
    -- NOT IN THE SPECS: check given edge existance
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent edge %', anedge;
  END;

  --
  -- e) Check StartPoint consistency
  --
  IF NOT ST_Equals(ST_StartPoint(acurve), ST_StartPoint(oldedge.geom)) THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - start node not geometry start point.';
  END IF;

  IF oldedge.start_node = oldedge.end_node THEN -- {

    -- Not in the specs:
    -- if the edge is closed, check we didn't change winding !
    --       (should be part of isomorphism checking)
    range := ST_MakePolygon(oldedge.geom);
    iscw := ST_OrderingEquals(range, ST_ForceRHR(range));

    IF ST_NumPoints(ST_RemoveRepeatedPoints(acurve)) < 3 THEN
      RAISE EXCEPTION 'Invalid edge (no two distinct vertices exist)';
    END IF;
    range := ST_MakePolygon(acurve);

    IF iscw != ST_OrderingEquals(range, ST_ForceRHR(range)) THEN
      RAISE EXCEPTION 'Edge twist at node %',
        ST_AsText(ST_StartPoint(oldedge.geom));
    END IF;

  ELSE -- }{

    --
    -- f) Check EndPoint consistency
    --
    IF NOT ST_Equals(ST_EndPoint(acurve), ST_EndPoint(oldedge.geom)) THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - end node not geometry end point.';
    END IF;

  END IF; -- }

  --
  -- g) Check if curve crosses any node
  -- 
  FOR rec IN EXECUTE
    'SELECT node_id, ST_Relate(geom, '
    || quote_literal(acurve::text) || '::geometry, 2) as relate FROM '
    || quote_ident(atopology)
    || '.node WHERE geom && '
    || quote_literal(acurve::text)
    || '::geometry AND node_id NOT IN ('
    || oldedge.start_node || ',' || oldedge.end_node
    || ')'
  LOOP
    IF ST_RelateMatch(rec.relate, 'T********') THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - geometry crosses a node';
    END IF;
  END LOOP;

  --
  -- h) Check if this geometry has any interaction with any existing edge
  --
  sql := 'SELECT edge_id, ST_Relate(geom,' 
    || quote_literal(acurve::text)
    || '::geometry, 2) as im FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE edge_id != ' || anedge || ' AND geom && '
    || quote_literal(acurve::text) || '::geometry';
  FOR rec IN EXECUTE sql LOOP -- {

    --RAISE DEBUG 'IM=%',rec.im;

    IF ST_RelateMatch(rec.im, 'F********') THEN
      CONTINUE; -- no interior-interior intersection
    END IF;

    IF ST_RelateMatch(rec.im, '1FFF*FFF2') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - coincident edge %', rec.edge_id;
    END IF;

    -- NOT IN THE SPECS: geometry touches an edge
    IF ST_RelateMatch(rec.im, '1********') THEN
      RAISE EXCEPTION
        'Spatial exception - geometry intersects edge %', rec.edge_id;
    END IF;

    IF ST_RelateMatch(rec.im, 'T********') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - geometry crosses edge %', rec.edge_id;
    END IF;

  END LOOP; -- }

  --
  -- Not in the specs:
  -- Check topological isomorphism 
  --

  -- Check that the "motion range" doesn't include any node 
  --{

  sql := 'SELECT ST_Collect(geom) as nodes, '
    || 'null::geometry as r1, null::geometry as r2 FROM '
    || quote_ident(atopology)
    || '.node WHERE geom && '
    || quote_literal(ST_Collect(ST_Envelope(oldedge.geom),
                                ST_Envelope(acurve))::text)
    || '::geometry AND node_id NOT IN ( '
    || oldedge.start_node || ',' || oldedge.end_node || ')';



  EXECUTE sql INTO rng_info;

  -- There's no collision if there's no nodes in the combined
  -- bbox of old and new edges.
  --
  IF NOT ST_IsEmpty(rng_info.nodes) THEN -- {








    tmp1 := ST_MakeLine(ST_EndPoint(oldedge.geom), ST_StartPoint(oldedge.geom));




    rng_info.r1 := ST_MakeLine(oldedge.geom, tmp1);
    IF ST_NumPoints(rng_info.r1) < 4 THEN
      rng_info.r1 := ST_AddPoint(rng_info.r1, ST_StartPoint(oldedge.geom));
    END IF;



    rng_info.r1 := ST_CollectionExtract(
                       ST_MakeValid(ST_MakePolygon(rng_info.r1)), 3);




    rng_info.r2 := ST_MakeLine(acurve, tmp1);
    IF ST_NumPoints(rng_info.r2) < 4 THEN
      rng_info.r2 := ST_AddPoint(rng_info.r2, ST_StartPoint(oldedge.geom));
    END IF;



    rng_info.r2 := ST_CollectionExtract(
                       ST_MakeValid(ST_MakePolygon(rng_info.r2)), 3);




    FOR rec IN WITH
      nodes AS ( SELECT * FROM ST_Dump(rng_info.nodes) ),
      inr1 AS ( SELECT path[1] FROM nodes WHERE ST_Contains(rng_info.r1, geom) ),
      inr2 AS ( SELECT path[1] FROM nodes WHERE ST_Contains(rng_info.r2, geom) )
      ( SELECT * FROM inr1
          EXCEPT
        SELECT * FROM inr2
      ) UNION 
      ( SELECT * FROM inr2
          EXCEPT
        SELECT * FROM inr1
      )
    LOOP
      RAISE EXCEPTION 'Edge motion collision at %',
                     ST_AsText(ST_GeometryN(rng_info.nodes, rec.path));
    END LOOP;

  END IF; -- }

  --} motion range checking end

  -- 
  -- Check edge adjacency before
  --{

  SELECT topology._ST_AdjacentEdges(
      atopology, oldedge.start_node, anedge
    ) as pre, NULL::integer[] as post
  INTO STRICT snode_info;




  SELECT topology._ST_AdjacentEdges(
      atopology, oldedge.end_node, -anedge
    ) as pre, NULL::integer[] as post
  INTO STRICT enode_info;




  --}

  --
  -- Update edge geometry
  --
  EXECUTE 'UPDATE ' || quote_ident(atopology) || '.edge_data '
    || ' SET geom = ' || quote_literal(acurve::text) 
    || ' WHERE edge_id = ' || anedge;

  -- 
  -- Check edge adjacency after
  --{

  snode_info.post := topology._ST_AdjacentEdges(
      atopology, oldedge.start_node, anedge
    );




  enode_info.post := topology._ST_AdjacentEdges(
      atopology, oldedge.end_node, -anedge
    );




  IF snode_info.pre != snode_info.post THEN
    RAISE EXCEPTION 'Edge changed disposition around start node %',
      oldedge.start_node;
  END IF;

  IF enode_info.pre != enode_info.post THEN
    RAISE EXCEPTION 'Edge changed disposition around end node %',
      oldedge.end_node;
  END IF;

  --}

  -- Update faces MBR of left and right faces
  -- TODO: think about ways to optimize this part, like see if
  --       the old edge geometry partecipated in the definition
  --       of the current MBR (for shrinking) or the new edge MBR
  --       would be larger than the old face MBR...
  --
  IF oldedge.left_face != 0 THEN
    sql := 'UPDATE ' || quote_ident(atopology) || '.face '
      || ' SET mbr = ' || quote_literal(
        ST_Envelope(ST_GetFaceGeometry(atopology, oldedge.left_face))::text
        )
      || '::geometry WHERE face_id = ' || oldedge.left_face;
    EXECUTE sql;
  END IF;
  IF oldedge.right_face != 0 AND oldedge.right_face != oldedge.left_face THEN
    sql := 'UPDATE ' || quote_ident(atopology) || '.face '
      || ' SET mbr = ' || quote_literal(
        ST_Envelope(ST_GetFaceGeometry(atopology, oldedge.right_face))::text
        )
      || '::geometry WHERE face_id = ' || oldedge.right_face;
    EXECUTE sql;
  END IF;
  

  RETURN 'Edge ' || anedge || ' changed';

END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_ChangeEdgeGeom

--
-- _ST_AddFaceSplit
--
-- Add a split face by walking on the edge side.
--
-- @param atopology topology name
-- @param anedge edge id and walking side (left:positive right:negative)
-- @param oface the face in which the edge identifier is known to be
-- @param mbr_only do not create a new face but update MBR of the current
--
-- The created face, if any, will be at the left hand of the walking path
--
-- Return:
--  NULL: if mbr_only was requested
--     0: if the edge does not form a ring
--  NULL: if it is impossible to create a face on the requested side
--        ( new face on the side is the universe )
--   >0 : id of newly added face
--
-- {
CREATE OR REPLACE FUNCTION topology._ST_AddFaceSplit(atopology varchar, anedge integer, oface integer, mbr_only bool)
  RETURNS INTEGER AS
$$
DECLARE
  fan RECORD;
  newface INTEGER;
  sql TEXT;
  isccw BOOLEAN;
  ishole BOOLEAN;

BEGIN

  IF oface = 0 AND mbr_only THEN



    RETURN NULL;
  END IF;

  SELECT null::int[] as newring_edges,
         null::geometry as shell
  INTO fan;

  SELECT array_agg(edge)
  FROM topology.getringedges(atopology, anedge)
  INTO STRICT fan.newring_edges;





  -- You can't get to the other side of an edge forming a ring 
  IF fan.newring_edges @> ARRAY[-anedge] THEN



    RETURN 0;
  END IF;





  sql := 'WITH ids as ( select row_number() over () as seq, edge from unnest('
    || quote_literal(fan.newring_edges::text)
    || '::int[] ) u(edge) ), edges AS ( select CASE WHEN i.edge < 0 THEN ST_Reverse(e.geom) ELSE e.geom END as g FROM ids i left join '
    || quote_ident(atopology) || '.edge_data e ON(e.edge_id = abs(i.edge)) ORDER BY seq) SELECT ST_MakePolygon(ST_MakeLine(g.g)) FROM edges g;';



  EXECUTE sql INTO fan.shell;





  isccw := NOT ST_OrderingEquals(fan.shell, ST_ForceRHR(fan.shell));





  IF oface = 0 THEN
    IF NOT isccw THEN



      RETURN NULL;
    END IF;
  END IF;

  IF mbr_only AND oface != 0 THEN
    -- Update old face mbr (nothing to do if we're opening an hole)
    IF isccw THEN -- {
      sql := 'UPDATE '
        || quote_ident(atopology) || '.face SET mbr = '
        || quote_literal(ST_Envelope(fan.shell)::text)
        || '::geometry WHERE face_id = ' || oface;



    	EXECUTE sql;
    END IF; -- }
    RETURN NULL;
  END IF;

  IF oface != 0 AND NOT isccw THEN -- {
    -- Face created an hole in an outer face
    sql := 'INSERT INTO '
      || quote_ident(atopology) || '.face(mbr) SELECT mbr FROM '
      || quote_ident(atopology)
      || '.face WHERE face_id = ' || oface
      || ' RETURNING face_id';
  ELSE
    sql := 'INSERT INTO '
      || quote_ident(atopology) || '.face(mbr) VALUES ('
      || quote_literal(ST_Envelope(fan.shell)::text)
      || '::geometry) RETURNING face_id';
  END IF; -- }

  -- Insert new face



  EXECUTE sql INTO STRICT newface;

  -- Update forward edges
  sql := 'UPDATE '
    || quote_ident(atopology) || '.edge_data SET left_face = ' || newface
    || ' WHERE left_face = ' || oface || ' AND edge_id = ANY ('
    || quote_literal(array( select +(x) from unnest(fan.newring_edges) u(x) )::text)
    || ')';



  EXECUTE sql;

  -- Update backward edges
  sql := 'UPDATE '
    || quote_ident(atopology) || '.edge_data SET right_face = ' || newface
    || ' WHERE right_face = ' || oface || ' AND edge_id = ANY ('
    || quote_literal(array( select -(x) from unnest(fan.newring_edges) u(x) )::text)
    || ')';



  EXECUTE sql;

  IF oface != 0 AND NOT isccw THEN -- {
    -- face shrinked, must update all non-contained edges and nodes



    ishole := true;
  ELSE



    ishole := false;
  END IF; -- }

  -- Update edges bounding the old face
  sql := 'UPDATE '
    || quote_ident(atopology)
    || '.edge_data SET left_face = CASE WHEN left_face = '
    || oface || ' THEN ' || newface
    || ' ELSE left_face END, right_face = CASE WHEN right_face = '
    || oface || ' THEN ' || newface
    || ' ELSE right_face END WHERE ( left_face = ' || oface
    || ' OR right_face = ' || oface
    || ') AND NOT edge_id = ANY ('
    || quote_literal( array(
        select abs(x) from unnest(fan.newring_edges) u(x)
       )::text )
    || ') AND ';
  IF ishole THEN sql := sql || 'NOT '; END IF;
  sql := sql || 'ST_Contains(' || quote_literal(fan.shell::text)
    -- We only need to check a single point, but must not be an endpoint
    || '::geometry, ST_Line_Interpolate_Point(geom, 0.2))';



  EXECUTE sql;

  -- Update isolated nodes in new new face 
  sql := 'UPDATE '
    || quote_ident(atopology) || '.node SET containing_face = ' || newface
    || ' WHERE containing_face = ' || oface 
    || ' AND ';
  IF ishole THEN sql := sql || 'NOT '; END IF;
  sql := sql || 'ST_Contains(' || quote_literal(fan.shell::text) || '::geometry, geom)';



  EXECUTE sql;

  RETURN newface;

END
$$
LANGUAGE 'plpgsql' VOLATILE;
--}

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.12
--
--  ST_AddEdgeNewFaces(atopology, anode, anothernode, acurve)
--
-- Not in the specs:
-- * Reset containing_face for starting and ending point,
--   as they stop being isolated nodes
-- * Update references in the Relation table.
--
CREATE OR REPLACE FUNCTION topology.ST_AddEdgeNewFaces(atopology varchar, anode integer, anothernode integer, acurve geometry)
  RETURNS INTEGER AS
$$
DECLARE
  rec RECORD;
  i INTEGER;
  topoid INTEGER;
  az FLOAT8;
  span RECORD; -- start point analysis data
  epan RECORD; --   end point analysis data
  fan RECORD; -- face analisys
  newedge RECORD; -- informations about new edge
  sql TEXT;
  newfaces INTEGER[];
  newface INTEGER;
BEGIN

  --
  -- All args required
  -- 
  IF atopology IS NULL
    OR anode IS NULL
    OR anothernode IS NULL
    OR acurve IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Acurve must be a LINESTRING
  --
  IF substring(geometrytype(acurve), 1, 4) != 'LINE'
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - invalid curve';
  END IF;
  
  --
  -- Curve must be simple
  --
  IF NOT ST_IsSimple(acurve) THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - curve not simple';
  END IF;

  --
  -- Get topology id
  --
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = atopology;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  -- Initialize new edge info (will be filled up more later)
  SELECT anode as start_node, anothernode as end_node, acurve as geom,
    NULL::int as next_left_edge, NULL::int as next_right_edge,
    NULL::int as left_face, NULL::int as right_face, NULL::int as edge_id,
    NULL::int as prev_left_edge, NULL::int as prev_right_edge, -- convenience
    anode = anothernode as isclosed, -- convenience
    false as start_node_isolated, -- convenience
    false as end_node_isolated, -- convenience
    NULL::geometry as start_node_geom, -- convenience
    NULL::geometry as end_node_geom, -- convenience
    ST_RemoveRepeatedPoints(acurve) as cleangeom -- convenience
  INTO newedge;

  -- Compute azimuth of first edge end on start node
  SELECT null::int AS nextCW, null::int AS nextCCW,
         null::float8 AS minaz, null::float8 AS maxaz,
         false AS was_isolated,
         ST_Azimuth(ST_StartPoint(newedge.cleangeom),
                    ST_PointN(newedge.cleangeom, 2)) AS myaz
  INTO span;
  IF span.myaz IS NULL THEN
    RAISE EXCEPTION 'Invalid edge (no two distinct vertices exist)';
  END IF;

  -- Compute azimuth of last edge end on end node
  SELECT null::int AS nextCW, null::int AS nextCCW,
         null::float8 AS minaz, null::float8 AS maxaz,
         false AS was_isolated,
         ST_Azimuth(ST_EndPoint(newedge.cleangeom),
                    ST_PointN(newedge.cleangeom,
                              ST_NumPoints(newedge.cleangeom)-1)) AS myaz
  INTO epan;
  IF epan.myaz IS NULL THEN
    RAISE EXCEPTION 'Invalid edge (no two distinct vertices exist)';
  END IF;


  -- 
  -- Check endpoints existance, match with Curve geometry
  -- and get face information (if any)
  --
  i := 0;
  FOR rec IN EXECUTE 'SELECT node_id, containing_face, geom FROM '
    || quote_ident(atopology)
    || '.node WHERE node_id IN ( '
    || anode || ',' || anothernode
    || ')'
  LOOP
    IF rec.containing_face IS NOT NULL THEN




      IF newedge.left_face IS NULL THEN
        newedge.left_face := rec.containing_face;
        newedge.right_face := rec.containing_face;
      ELSE
        IF newedge.left_face != rec.containing_face THEN
          RAISE EXCEPTION
            'SQL/MM Spatial exception - geometry crosses an edge (endnodes in faces % and %)', newedge.left_face, rec.containing_face;
        END IF;
      END IF;
    END IF;

    IF rec.node_id = anode THEN
      newedge.start_node_geom = rec.geom;
    END IF;

    IF rec.node_id = anothernode THEN
      newedge.end_node_geom = rec.geom;
    END IF;

    i := i + 1;
  END LOOP;

  IF newedge.start_node_geom IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent node';
  ELSIF NOT Equals(newedge.start_node_geom, ST_StartPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - start node not geometry start point.';
  END IF;

  IF newedge.end_node_geom IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent node';
  ELSIF NOT Equals(newedge.end_node_geom, ST_EndPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - end node not geometry end point.';
  END IF;

  RAISE DEBUG 'All Checked !';

  --
  -- Check if this geometry crosses any node
  --
  FOR rec IN EXECUTE
    'SELECT node_id, ST_Relate(geom, '
    || quote_literal(acurve::text) || '::geometry, 2) as relate FROM '
    || quote_ident(atopology)
    || '.node WHERE geom && '
    || quote_literal(acurve::text)
    || '::geometry'
  LOOP
    IF ST_RelateMatch(rec.relate, 'T********') THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - geometry crosses a node';
    END IF;
  END LOOP;

  --
  -- Check if this geometry has any interaction with any existing edge
  --
  FOR rec IN EXECUTE 'SELECT edge_id, ST_Relate(geom,' 
    || quote_literal(acurve::text)
    || '::geometry, 2) as im FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE geom && '
    || quote_literal(acurve::text) || '::geometry'
  LOOP

    --RAISE DEBUG 'IM=%',rec.im;

    IF ST_RelateMatch(rec.im, 'F********') THEN
      CONTINUE; -- no interior intersection
    END IF;

    IF ST_RelateMatch(rec.im, '1FFF*FFF2') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - coincident edge %', rec.edge_id;
    END IF;

    -- NOT IN THE SPECS: geometry touches an edge
    IF ST_RelateMatch(rec.im, '1********') THEN
      RAISE EXCEPTION
        'Spatial exception - geometry intersects edge %', rec.edge_id;
    END IF;

    IF ST_RelateMatch(rec.im, 'T********') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - geometry crosses edge %', rec.edge_id;
    END IF;

  END LOOP;

  ---------------------------------------------------------------
  --
  -- All checks passed, time to prepare the new edge
  --
  ---------------------------------------------------------------

  EXECUTE 'SELECT nextval(' || quote_literal(
      quote_ident(atopology) || '.edge_data_edge_id_seq') || ')'
  INTO STRICT newedge.edge_id;


  -- Find links on start node -- {





  sql :=
    'SELECT edge_id, -1 AS end_node, start_node, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) as geom FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE start_node = ' || anode
    || ' UNION SELECT edge_id, end_node, -1, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE end_node = ' || anode;
  IF newedge.isclosed THEN
    sql := sql || ' UNION SELECT '
      || newedge.edge_id || ',' || newedge.end_node
      || ',-1,0,0,' -- pretend we start elsewhere
      || quote_literal(newedge.cleangeom::text);
  END IF;
  i := 0;
  FOR rec IN EXECUTE sql
  LOOP -- incident edges {

    i := i + 1;

    IF rec.start_node = anode THEN
      --
      -- Edge starts at our node, we compute
      -- azimuth from node to its second point
      --
      az := ST_Azimuth(ST_StartPoint(rec.geom), ST_PointN(rec.geom, 2));

    ELSE
      --
      -- Edge ends at our node, we compute
      -- azimuth from node to its second-last point
      --
      az := ST_Azimuth(ST_EndPoint(rec.geom),
                       ST_PointN(rec.geom, ST_NumPoints(rec.geom)-1));
      rec.edge_id := -rec.edge_id;

    END IF;

    IF az IS NULL THEN
      RAISE EXCEPTION 'Invalid edge % found (no two distinct nodes exist)',
        rec.edge_id;
    END IF;






    az = az - span.myaz;
    IF az < 0 THEN
      az := az + 2*PI();
    END IF;

    -- RAISE DEBUG ' normalized az %', az;

    IF span.maxaz IS NULL OR az > span.maxaz THEN
      span.maxaz := az;
      span.nextCCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.left_face := rec.left_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.left_face := rec.right_face;
        END IF;
      END IF;
    END IF;

    IF span.minaz IS NULL OR az < span.minaz THEN
      span.minaz := az;
      span.nextCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.right_face := rec.right_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.right_face := rec.left_face;
        END IF;
      END IF;
    END IF;

    --RAISE DEBUG 'Closest edges: CW:%(%) CCW:%(%)', span.nextCW, span.minaz, span.nextCCW, span.maxaz;

  END LOOP; -- incident edges }




  IF newedge.isclosed THEN
    IF i < 2 THEN span.was_isolated = true; END IF;
  ELSE
    IF i < 1 THEN span.was_isolated = true; END IF;
  END IF;

  IF span.nextCW IS NULL THEN
    -- This happens if the destination node is isolated
    newedge.next_right_edge := newedge.edge_id;
    newedge.prev_left_edge := -newedge.edge_id;
  ELSE
    newedge.next_right_edge := span.nextCW;
    newedge.prev_left_edge := -span.nextCCW;
  END IF;



  -- } start_node analysis


  -- Find links on end_node {
      




  sql :=
    'SELECT edge_id, -1 as end_node, start_node, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) as geom FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE start_node = ' || anothernode
    || 'UNION SELECT edge_id, end_node, -1, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE end_node = ' || anothernode;
  IF newedge.isclosed THEN
    sql := sql || ' UNION SELECT '
      || newedge.edge_id || ',' || -1 -- pretend we end elsewhere
      || ',' || newedge.start_node || ',0,0,'
      || quote_literal(newedge.cleangeom::text);
  END IF;
  i := 0;
  FOR rec IN EXECUTE sql
  LOOP -- incident edges {

    i := i + 1;

    IF rec.start_node = anothernode THEN
      --
      -- Edge starts at our node, we compute
      -- azimuth from node to its second point
      --
      az := ST_Azimuth(ST_StartPoint(rec.geom),
                       ST_PointN(rec.geom, 2));

    ELSE
      --
      -- Edge ends at our node, we compute
      -- azimuth from node to its second-last point
      --
      az := ST_Azimuth(ST_EndPoint(rec.geom),
        ST_PointN(rec.geom, ST_NumPoints(rec.geom)-1));
      rec.edge_id := -rec.edge_id;

    END IF;





    az := az - epan.myaz;
    IF az < 0 THEN
      az := az + 2*PI();
    END IF;

    -- RAISE DEBUG ' normalized az %', az;

    IF epan.maxaz IS NULL OR az > epan.maxaz THEN
      epan.maxaz := az;
      epan.nextCCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.right_face := rec.left_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.right_face := rec.right_face;
        END IF;
      END IF;
    END IF;

    IF epan.minaz IS NULL OR az < epan.minaz THEN
      epan.minaz := az;
      epan.nextCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.left_face := rec.right_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.left_face := rec.left_face;
        END IF;
      END IF;
    END IF;

    --RAISE DEBUG 'Closest edges: CW:%(%) CCW:%(%)', epan.nextCW, epan.minaz, epan.nextCCW, epan.maxaz;

  END LOOP; -- incident edges }




  IF newedge.isclosed THEN
    IF i < 2 THEN epan.was_isolated = true; END IF;
  ELSE
    IF i < 1 THEN epan.was_isolated = true; END IF;
  END IF;

  IF epan.nextCW IS NULL THEN
    -- This happens if the destination node is isolated
    newedge.next_left_edge := -newedge.edge_id;
    newedge.prev_right_edge := newedge.edge_id;
  ELSE
    newedge.next_left_edge := epan.nextCW;
    newedge.prev_right_edge := -epan.nextCCW;
  END IF;

  -- } end_node analysis



  ----------------------------------------------------------------------
  --
  -- If we don't have faces setup by now we must have encountered
  -- a malformed topology (no containing_face on isolated nodes, no
  -- left/right faces on adjacent edges or mismatching values)
  --
  ----------------------------------------------------------------------
  IF newedge.left_face != newedge.right_face THEN
    RAISE EXCEPTION 'Left(%)/right(%) faces mismatch: invalid topology ?', 
      newedge.left_face, newedge.right_face;
  END IF;
  IF newedge.left_face IS NULL THEN
    RAISE EXCEPTION 'Could not derive edge face from linked primitives: invalid topology ?';
  END IF;

  ----------------------------------------------------------------------
  --
  -- Insert the new edge, and update all linking
  --
  ----------------------------------------------------------------------

  -- Insert the new edge with what we have so far
  EXECUTE 'INSERT INTO ' || quote_ident(atopology) 
    || '.edge VALUES(' || newedge.edge_id
    || ',' || newedge.start_node
    || ',' || newedge.end_node
    || ',' || newedge.next_left_edge
    || ',' || newedge.next_right_edge
    || ',' || newedge.left_face
    || ',' || newedge.right_face
    || ',' || quote_literal(newedge.geom::geometry::text)
    || ')';

  -- Link prev_left_edge to us 
  -- (if it's not us already)
  IF abs(newedge.prev_left_edge) != newedge.edge_id THEN
    IF newedge.prev_left_edge > 0 THEN
      -- its next_left_edge is us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_left_edge = '
        || newedge.edge_id
        || ', abs_next_left_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || newedge.prev_left_edge;
    ELSE
      -- its next_right_edge is us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_right_edge = '
        || newedge.edge_id
        || ', abs_next_right_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || -newedge.prev_left_edge;
    END IF;
  END IF;

  -- Link prev_right_edge to us 
  -- (if it's not us already)
  IF abs(newedge.prev_right_edge) != newedge.edge_id THEN
    IF newedge.prev_right_edge > 0 THEN
      -- its next_left_edge is -us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_left_edge = '
        || -newedge.edge_id
        || ', abs_next_left_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || newedge.prev_right_edge;
    ELSE
      -- its next_right_edge is -us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_right_edge = '
        || -newedge.edge_id
        || ', abs_next_right_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || -newedge.prev_right_edge;
    END IF;
  END IF;

  -- NOT IN THE SPECS...
  -- set containing_face = null for start_node and end_node
  -- if they where isolated 
  IF span.was_isolated OR epan.was_isolated THEN
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.node SET containing_face = null WHERE node_id IN ('
        || anode || ',' || anothernode || ')';
  END IF;

  --------------------------------------------
  -- Check face splitting
  --------------------------------------------





  SELECT topology._ST_AddFaceSplit(atopology, -newedge.edge_id, newedge.left_face, false)
  INTO newface;

  IF newface = 0 THEN



    RETURN newedge.edge_id; 
  END IF;

  newfaces[1] := newface;




  SELECT topology._ST_AddFaceSplit(atopology, newedge.edge_id, newedge.left_face, false)
  INTO newface;

  newfaces[2] := newface;

  IF newedge.left_face != 0 THEN -- {

    -- NOT IN THE SPECS:
    -- update TopoGeometry compositions to substitute oldface with newfaces
    sql := 'UPDATE '
      || quote_ident(atopology)
      || '.relation r set element_id = ' || newfaces[1]
      || ' FROM topology.layer l '
      || ' WHERE l.topology_id = ' || topoid
      || ' AND l.level = 0 '
      || ' AND l.layer_id = r.layer_id '
      || ' AND r.element_id = ' || newedge.left_face
      || ' AND r.element_type = 3 RETURNING r.topogeo_id, r.layer_id';
    --RAISE DEBUG 'SQL: %', sql;
    FOR rec IN EXECUTE sql
    LOOP




      -- Add reference to the other face
      sql := 'INSERT INTO ' || quote_ident(atopology)
        || '.relation VALUES( ' || rec.topogeo_id
        || ',' || rec.layer_id || ',' || newfaces[2] || ', 3)';
      --RAISE DEBUG 'SQL: %', sql;
      EXECUTE sql;

    END LOOP;

    -- drop old face from faces table
    sql := 'DELETE FROM ' || quote_ident(atopology)
      || '.face WHERE face_id = ' || newedge.left_face;
    EXECUTE sql;

  END IF; -- }

  RETURN newedge.edge_id;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_AddEdgeNewFaces

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.13
--
--  ST_AddEdgeModFace(atopology, anode, anothernode, acurve)
--
-- Not in the specs:
-- * Reset containing_face for starting and ending point,
--   as they stop being isolated nodes
-- * Update references in the Relation table.
--
CREATE OR REPLACE FUNCTION topology.ST_AddEdgeModFace(atopology varchar, anode integer, anothernode integer, acurve geometry)
  RETURNS INTEGER AS
$$
DECLARE
  rec RECORD;
  rrec RECORD;
  i INTEGER;
  topoid INTEGER;
  az FLOAT8;
  span RECORD; -- start point analysis data
  epan RECORD; --   end point analysis data
  fan RECORD; -- face analisys
  newedge RECORD; -- informations about new edge
  sql TEXT;
  newfaces INTEGER[];
  newface INTEGER;
BEGIN

  --
  -- All args required
  -- 
  IF atopology IS NULL
    OR anode IS NULL
    OR anothernode IS NULL
    OR acurve IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  --
  -- Acurve must be a LINESTRING
  --
  IF substring(geometrytype(acurve), 1, 4) != 'LINE'
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - invalid curve';
  END IF;
  
  --
  -- Curve must be simple
  --
  IF NOT ST_IsSimple(acurve) THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - curve not simple';
  END IF;

  --
  -- Get topology id
  --
  BEGIN
    SELECT id FROM topology.topology
      INTO STRICT topoid WHERE name = atopology;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  -- Initialize new edge info (will be filled up more later)
  SELECT anode as start_node, anothernode as end_node, acurve as geom,
    NULL::int as next_left_edge, NULL::int as next_right_edge,
    NULL::int as left_face, NULL::int as right_face, NULL::int as edge_id,
    NULL::int as prev_left_edge, NULL::int as prev_right_edge, -- convenience
    anode = anothernode as isclosed, -- convenience
    false as start_node_isolated, -- convenience
    false as end_node_isolated, -- convenience
    NULL::geometry as start_node_geom, -- convenience
    NULL::geometry as end_node_geom, -- convenience
    ST_RemoveRepeatedPoints(acurve) as cleangeom -- convenience
  INTO newedge;

  -- Compute azimut of first edge end on start node
  SELECT null::int AS nextCW, null::int AS nextCCW,
         null::float8 AS minaz, null::float8 AS maxaz,
         false AS was_isolated,
         ST_Azimuth(ST_StartPoint(newedge.cleangeom),
                    ST_PointN(newedge.cleangeom, 2)) AS myaz
  INTO span;
  IF span.myaz IS NULL THEN
    RAISE EXCEPTION 'Invalid edge (no two distinct vertices exist)';
  END IF;

  -- Compute azimuth of last edge end on end node
  SELECT null::int AS nextCW, null::int AS nextCCW,
         null::float8 AS minaz, null::float8 AS maxaz,
         false AS was_isolated,
         ST_Azimuth(ST_EndPoint(newedge.cleangeom),
                    ST_PointN(newedge.cleangeom,
                              ST_NumPoints(newedge.cleangeom)-1)) AS myaz
  INTO epan;
  IF epan.myaz IS NULL THEN
    RAISE EXCEPTION 'Invalid edge (no two distinct vertices exist)';
  END IF;


  -- 
  -- Check endpoints existance, match with Curve geometry
  -- and get face information (if any)
  --
  i := 0;
  FOR rec IN EXECUTE 'SELECT node_id, containing_face, geom FROM '
    || quote_ident(atopology)
    || '.node WHERE node_id IN ( '
    || anode || ',' || anothernode
    || ')'
  LOOP
    IF rec.containing_face IS NOT NULL THEN




      IF newedge.left_face IS NULL THEN
        newedge.left_face := rec.containing_face;
        newedge.right_face := rec.containing_face;
      ELSE
        IF newedge.left_face != rec.containing_face THEN
          RAISE EXCEPTION
            'SQL/MM Spatial exception - geometry crosses an edge (endnodes in faces % and %)', newedge.left_face, rec.containing_face;
        END IF;
      END IF;
    END IF;

    IF rec.node_id = anode THEN
      newedge.start_node_geom = rec.geom;
    END IF;

    IF rec.node_id = anothernode THEN
      newedge.end_node_geom = rec.geom;
    END IF;

    i := i + 1;
  END LOOP;

  IF newedge.start_node_geom IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent node';
  ELSIF NOT Equals(newedge.start_node_geom, ST_StartPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - start node not geometry start point.';
  END IF;

  IF newedge.end_node_geom IS NULL
  THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent node';
  ELSIF NOT Equals(newedge.end_node_geom, ST_EndPoint(acurve))
  THEN
    RAISE EXCEPTION
      'SQL/MM Spatial exception - end node not geometry end point.';
  END IF;

  --
  -- Check if this geometry crosses any node
  --
  FOR rec IN EXECUTE
    'SELECT node_id, ST_Relate(geom, '
    || quote_literal(acurve::text) || '::geometry, 2) as relate FROM '
    || quote_ident(atopology)
    || '.node WHERE geom && '
    || quote_literal(acurve::text)
    || '::geometry'
  LOOP
    IF ST_RelateMatch(rec.relate, 'T********') THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - geometry crosses a node';
    END IF;
  END LOOP;

  --
  -- Check if this geometry has any interaction with any existing edge
  --
  FOR rec IN EXECUTE 'SELECT edge_id, ST_Relate(geom,' 
    || quote_literal(acurve::text)
    || '::geometry, 2) as im FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE geom && '
    || quote_literal(acurve::text) || '::geometry'
  LOOP

    --RAISE DEBUG 'IM=%',rec.im;

    IF ST_RelateMatch(rec.im, 'F********') THEN
      CONTINUE; -- no interior intersection
    END IF;

    IF ST_RelateMatch(rec.im, '1FFF*FFF2') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - coincident edge %', rec.edge_id;
    END IF;

    -- NOT IN THE SPECS: geometry touches an edge
    IF ST_RelateMatch(rec.im, '1********') THEN
      RAISE EXCEPTION
        'Spatial exception - geometry intersects edge %', rec.edge_id;
    END IF;

    IF ST_RelateMatch(rec.im, 'T********') THEN
      RAISE EXCEPTION
        'SQL/MM Spatial exception - geometry crosses edge %', rec.edge_id;
    END IF;

  END LOOP;

  ---------------------------------------------------------------
  --
  -- All checks passed, time to prepare the new edge
  --
  ---------------------------------------------------------------

  EXECUTE 'SELECT nextval(' || quote_literal(
      quote_ident(atopology) || '.edge_data_edge_id_seq') || ')'
  INTO STRICT newedge.edge_id;


  -- Find links on start node -- {





  sql :=
    'SELECT edge_id, -1 AS end_node, start_node, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) as geom FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE start_node = ' || anode
    || ' UNION SELECT edge_id, end_node, -1, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE end_node = ' || anode;
  IF newedge.isclosed THEN
    sql := sql || ' UNION SELECT '
      || newedge.edge_id || ',' || newedge.end_node
      || ',-1,0,0,' -- pretend we start elsewhere
      || quote_literal(newedge.cleangeom::text);
  END IF;
  i := 0;
  FOR rec IN EXECUTE sql
  LOOP -- incident edges {

    i := i + 1;

    IF rec.start_node = anode THEN
      --
      -- Edge starts at our node, we compute
      -- azimuth from node to its second point
      --
      az := ST_Azimuth(ST_StartPoint(rec.geom), ST_PointN(rec.geom, 2));

    ELSE
      --
      -- Edge ends at our node, we compute
      -- azimuth from node to its second-last point
      --
      az := ST_Azimuth(ST_EndPoint(rec.geom),
                       ST_PointN(rec.geom, ST_NumPoints(rec.geom)-1));
      rec.edge_id := -rec.edge_id;

    END IF;

    IF az IS NULL THEN
      RAISE EXCEPTION 'Invalid edge % found (no two distinct nodes exist)',
        rec.edge_id;
    END IF;






    az = az - span.myaz;
    IF az < 0 THEN
      az := az + 2*PI();
    END IF;

    -- RAISE DEBUG ' normalized az %', az;

    IF span.maxaz IS NULL OR az > span.maxaz THEN
      span.maxaz := az;
      span.nextCCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.left_face := rec.left_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.left_face := rec.right_face;
        END IF;
      END IF;
    END IF;

    IF span.minaz IS NULL OR az < span.minaz THEN
      span.minaz := az;
      span.nextCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.right_face := rec.right_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.right_face := rec.left_face;
        END IF;
      END IF;
    END IF;

    --RAISE DEBUG 'Closest edges: CW:%(%) CCW:%(%)', span.nextCW, span.minaz, span.nextCCW, span.maxaz;

  END LOOP; -- incident edges }




  IF newedge.isclosed THEN
    IF i < 2 THEN span.was_isolated = true; END IF;
  ELSE
    IF i < 1 THEN span.was_isolated = true; END IF;
  END IF;

  IF span.nextCW IS NULL THEN
    -- This happens if the destination node is isolated
    newedge.next_right_edge := newedge.edge_id;
    newedge.prev_left_edge := -newedge.edge_id;
  ELSE
    newedge.next_right_edge := span.nextCW;
    newedge.prev_left_edge := -span.nextCCW;
  END IF;



  -- } start_node analysis


  -- Find links on end_node {
      




  sql :=
    'SELECT edge_id, -1 as end_node, start_node, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) as geom FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE start_node = ' || anothernode
    || 'UNION SELECT edge_id, end_node, -1, left_face, right_face, '
    || 'ST_RemoveRepeatedPoints(geom) FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE end_node = ' || anothernode;
  IF newedge.isclosed THEN
    sql := sql || ' UNION SELECT '
      || newedge.edge_id || ',' || -1 -- pretend we end elsewhere
      || ',' || newedge.start_node || ',0,0,'
      || quote_literal(newedge.cleangeom::text);
  END IF;
  i := 0;
  FOR rec IN EXECUTE sql
  LOOP -- incident edges {

    i := i + 1;

    IF rec.start_node = anothernode THEN
      --
      -- Edge starts at our node, we compute
      -- azimuth from node to its second point
      --
      az := ST_Azimuth(ST_StartPoint(rec.geom),
                       ST_PointN(rec.geom, 2));

    ELSE
      --
      -- Edge ends at our node, we compute
      -- azimuth from node to its second-last point
      --
      az := ST_Azimuth(ST_EndPoint(rec.geom),
        ST_PointN(rec.geom, ST_NumPoints(rec.geom)-1));
      rec.edge_id := -rec.edge_id;

    END IF;





    az := az - epan.myaz;
    IF az < 0 THEN
      az := az + 2*PI();
    END IF;

    -- RAISE DEBUG ' normalized az %', az;

    IF epan.maxaz IS NULL OR az > epan.maxaz THEN
      epan.maxaz := az;
      epan.nextCCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.right_face := rec.left_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.right_face := rec.right_face;
        END IF;
      END IF;
    END IF;

    IF epan.minaz IS NULL OR az < epan.minaz THEN
      epan.minaz := az;
      epan.nextCW := rec.edge_id;
      IF abs(rec.edge_id) != newedge.edge_id THEN
        IF rec.edge_id < 0 THEN
          -- TODO: check for mismatch ?
          newedge.left_face := rec.right_face;
        ELSE
          -- TODO: check for mismatch ?
          newedge.left_face := rec.left_face;
        END IF;
      END IF;
    END IF;

    --RAISE DEBUG 'Closest edges: CW:%(%) CCW:%(%)', epan.nextCW, epan.minaz, epan.nextCCW, epan.maxaz;

  END LOOP; -- incident edges }




  IF newedge.isclosed THEN
    IF i < 2 THEN epan.was_isolated = true; END IF;
  ELSE
    IF i < 1 THEN epan.was_isolated = true; END IF;
  END IF;

  IF epan.nextCW IS NULL THEN
    -- This happens if the destination node is isolated
    newedge.next_left_edge := -newedge.edge_id;
    newedge.prev_right_edge := newedge.edge_id;
  ELSE
    newedge.next_left_edge := epan.nextCW;
    newedge.prev_right_edge := -epan.nextCCW;
  END IF;

  -- } end_node analysis



  ----------------------------------------------------------------------
  --
  -- If we don't have faces setup by now we must have encountered
  -- a malformed topology (no containing_face on isolated nodes, no
  -- left/right faces on adjacent edges or mismatching values)
  --
  ----------------------------------------------------------------------
  IF newedge.left_face != newedge.right_face THEN
    RAISE EXCEPTION 'Left(%)/right(%) faces mismatch: invalid topology ?', 
      newedge.left_face, newedge.right_face;
  END IF;
  IF newedge.left_face IS NULL THEN
    RAISE EXCEPTION 'Could not derive edge face from linked primitives: invalid topology ?';
  END IF;

  ----------------------------------------------------------------------
  --
  -- Insert the new edge, and update all linking
  --
  ----------------------------------------------------------------------

  -- Insert the new edge with what we have so far
  EXECUTE 'INSERT INTO ' || quote_ident(atopology) 
    || '.edge VALUES(' || newedge.edge_id
    || ',' || newedge.start_node
    || ',' || newedge.end_node
    || ',' || newedge.next_left_edge
    || ',' || newedge.next_right_edge
    || ',' || newedge.left_face
    || ',' || newedge.right_face
    || ',' || quote_literal(newedge.geom::geometry::text)
    || ')';

  -- Link prev_left_edge to us 
  -- (if it's not us already)
  IF abs(newedge.prev_left_edge) != newedge.edge_id THEN
    IF newedge.prev_left_edge > 0 THEN
      -- its next_left_edge is us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_left_edge = '
        || newedge.edge_id
        || ', abs_next_left_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || newedge.prev_left_edge;
    ELSE
      -- its next_right_edge is us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_right_edge = '
        || newedge.edge_id
        || ', abs_next_right_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || -newedge.prev_left_edge;
    END IF;
  END IF;

  -- Link prev_right_edge to us 
  -- (if it's not us already)
  IF abs(newedge.prev_right_edge) != newedge.edge_id THEN
    IF newedge.prev_right_edge > 0 THEN
      -- its next_left_edge is -us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_left_edge = '
        || -newedge.edge_id
        || ', abs_next_left_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || newedge.prev_right_edge;
    ELSE
      -- its next_right_edge is -us
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.edge_data SET next_right_edge = '
        || -newedge.edge_id
        || ', abs_next_right_edge = '
        || newedge.edge_id
        || ' WHERE edge_id = ' 
        || -newedge.prev_right_edge;
    END IF;
  END IF;

  -- NOT IN THE SPECS...
  -- set containing_face = null for start_node and end_node
  -- if they where isolated 
  IF span.was_isolated OR epan.was_isolated THEN
      EXECUTE 'UPDATE ' || quote_ident(atopology)
        || '.node SET containing_face = null WHERE node_id IN ('
        || anode || ',' || anothernode || ')';
  END IF;

  --------------------------------------------
  -- Check face splitting
  --------------------------------------------




  SELECT topology._ST_AddFaceSplit(atopology, newedge.edge_id, newedge.left_face, false)
  INTO newface;
  IF newface = 0 THEN



    RETURN newedge.edge_id; 
  END IF;

  IF newface IS NULL THEN -- must be forming a maximal ring in universal face



    SELECT topology._ST_AddFaceSplit(atopology, -newedge.edge_id, newedge.left_face, false)
    INTO newface;
  ELSE



    PERFORM topology._ST_AddFaceSplit(atopology, -newedge.edge_id, newedge.left_face, true);
  END IF;

  IF newface IS NULL THEN



    RETURN newedge.edge_id; 
  END IF;

  --------------------------------------------
  -- Update topogeometries, if needed
  --------------------------------------------

  IF newedge.left_face != 0 THEN -- {

    -- NOT IN THE SPECS:
    -- update TopoGeometry compositions to add newface
    sql := 'SELECT r.topogeo_id, r.layer_id FROM '
      || quote_ident(atopology)
      || '.relation r, topology.layer l '
      || ' WHERE l.topology_id = ' || topoid
      || ' AND l.level = 0 '
      || ' AND l.layer_id = r.layer_id '
      || ' AND r.element_id = ' || newedge.left_face
      || ' AND r.element_type = 3 ';
    --RAISE DEBUG 'SQL: %', sql;
    FOR rec IN EXECUTE sql
    LOOP




      -- Add reference to the other face
      sql := 'INSERT INTO ' || quote_ident(atopology)
        || '.relation VALUES( ' || rec.topogeo_id
        || ',' || rec.layer_id || ',' || newface || ', 3)';
      --RAISE DEBUG 'SQL: %', sql;
      EXECUTE sql;

    END LOOP;

  END IF; -- }

  RETURN newedge.edge_id;
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_AddEdgeModFace

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.17
--
--  ST_InitTopoGeo(atopology)
--
CREATE OR REPLACE FUNCTION topology.ST_InitTopoGeo(atopology varchar)
RETURNS text
AS
$$
DECLARE
  rec RECORD;
  topology_id numeric;
BEGIN
  IF atopology IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  FOR rec IN SELECT * FROM pg_namespace WHERE text(nspname) = atopology
  LOOP
    RAISE EXCEPTION 'SQL/MM Spatial exception - schema already exists';
  END LOOP;

  FOR rec IN EXECUTE 'SELECT topology.CreateTopology('
    ||quote_literal(atopology)|| ') as id'
  LOOP
    topology_id := rec.id;
  END LOOP;

  RETURN 'Topology-Geometry ' || quote_literal(atopology)
    || ' (id:' || topology_id || ') created.';
END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_InitTopoGeo

--{
-- Topo-Geo and Topo-Net 3: Routine Details
-- X.3.18
--
--  ST_CreateTopoGeo(atopology, acollection)
--}{
CREATE OR REPLACE FUNCTION topology.ST_CreateTopoGeo(atopology varchar, acollection geometry)
RETURNS text
AS
$$
DECLARE
  typ char(4);
  rec RECORD;
  ret int;
  nodededges GEOMETRY;
  points GEOMETRY;
  snode_id int;
  enode_id int;
  tolerance FLOAT8;
  topoinfo RECORD;
BEGIN

  IF atopology IS NULL OR acollection IS NULL THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - null argument';
  END IF;

  -- Get topology information
  BEGIN
    SELECT * FROM topology.topology
      INTO STRICT topoinfo WHERE name = atopology;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
  END;

  -- Check SRID compatibility
  IF ST_SRID(acollection) != topoinfo.SRID THEN
    RAISE EXCEPTION 'Geometry SRID (%) does not match topology SRID (%)',
      ST_SRID(acollection), topoinfo.SRID;
  END IF;

  -- Verify pre-conditions (valid, empty topology schema exists)
  BEGIN -- {

    -- Verify the topology views in the topology schema to be empty
    FOR rec in EXECUTE
      'SELECT count(*) FROM '
      || quote_ident(atopology) || '.edge_data '
      || ' UNION ' ||
      'SELECT count(*) FROM '
      || quote_ident(atopology) || '.node '
    LOOP
      IF rec.count > 0 THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-empty view';
      END IF;
    END LOOP;

    -- face check is separated as it will contain a single (world)
    -- face record
    FOR rec in EXECUTE
      'SELECT count(*) FROM '
      || quote_ident(atopology) || '.face '
    LOOP
      IF rec.count != 1 THEN
    RAISE EXCEPTION 'SQL/MM Spatial exception - non-empty face view';
      END IF;
    END LOOP;

  EXCEPTION
    WHEN INVALID_SCHEMA_NAME THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - invalid topology name';
    WHEN UNDEFINED_TABLE THEN
      RAISE EXCEPTION 'SQL/MM Spatial exception - non-existent view';

  END; -- }





  --
  -- Node input linework with itself
  --
  WITH components AS ( SELECT geom FROM ST_Dump(acollection) )
  SELECT ST_UnaryUnion(ST_Collect(geom)) FROM (
    SELECT geom FROM components
      WHERE ST_Dimension(geom) = 1
    UNION ALL
    SELECT ST_Boundary(geom) FROM components
      WHERE ST_Dimension(geom) = 2
  ) as linework INTO STRICT nodededges;





  --
  -- Linemerge the resulting edges, to reduce the working set
  -- NOTE: this is more of a workaround for GEOS splitting overlapping
  --       lines to each of the segments.
  --
  SELECT ST_LineMerge(nodededges) INTO STRICT nodededges;






  --
  -- Collect input points and input lines endpoints
  --
  WITH components AS ( SELECT geom FROM ST_Dump(acollection) )
  SELECT ST_Union(geom) FROM (
    SELECT geom FROM components
      WHERE ST_Dimension(geom) = 0
    UNION ALL
    SELECT ST_Boundary(geom) FROM components
      WHERE ST_Dimension(geom) = 1
  ) as nodes INTO STRICT points;





  --
  -- Further split edges by points
  -- TODO: optimize this adding ST_Split support for multiline/multipoint
  --
  FOR rec IN SELECT geom FROM ST_Dump(points)
  LOOP
    -- Use the node to split edges
    SELECT ST_Collect(geom) 
    FROM ST_Dump(ST_Split(nodededges, rec.geom))
    INTO STRICT nodededges;
  END LOOP;
  SELECT ST_UnaryUnion(nodededges) INTO STRICT nodededges;






  --
  -- Collect all nodes (from points and noded linework endpoints)
  --

  WITH edges AS ( SELECT geom FROM ST_Dump(nodededges) )
  SELECT ST_Union( -- TODO: ST_UnaryUnion ?
          COALESCE(ST_UnaryUnion(ST_Collect(geom)), 
            ST_SetSRID('POINT EMPTY'::geometry, topoinfo.SRID)),
          COALESCE(points,
            ST_SetSRID('POINT EMPTY'::geometry, topoinfo.SRID))
         )
  FROM (
    SELECT ST_StartPoint(geom) as geom FROM edges
      UNION ALL
    SELECT ST_EndPoint(geom) FROM edges
  ) as endpoints INTO points;





  --
  -- Add all nodes as isolated so that 
  -- later calls to AddEdgeModFace will tweak their being
  -- isolated or not...
  --
  FOR rec IN SELECT geom FROM ST_Dump(points)
  LOOP
    PERFORM topology.ST_AddIsoNode(atopology, 0, rec.geom);
  END LOOP;
  

  FOR rec IN SELECT geom FROM ST_Dump(nodededges)
  LOOP
    SELECT topology.GetNodeByPoint(atopology, st_startpoint(rec.geom), 0)
      INTO STRICT snode_id;
    SELECT topology.GetNodeByPoint(atopology, st_endpoint(rec.geom), 0)
      INTO STRICT enode_id;
    PERFORM topology.ST_AddEdgeModFace(atopology, snode_id, enode_id, rec.geom);
  END LOOP;

  RETURN 'Topology ' || atopology || ' populated';

END
$$
LANGUAGE 'plpgsql' VOLATILE;
--} ST_CreateTopoGeo

--=}  SQL/MM block



-- The following files needs getfaceedges_returntype, defined in sqlmm.sql

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2011 2012 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- Developed by Sandro Santilli <strk@keybit.net>
-- for Faunalia (http://www.faunalia.it) with funding from
-- Regione Toscana - Sistema Informativo per la Gestione del Territorio
-- e dell' Ambiente [RT-SIGTA].
-- For the project: "Sviluppo strumenti software per il trattamento di dati
-- geografici basati su QuantumGIS e Postgis (CIG 0494241492)"
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- Return a list of edges (sequence, id) resulting by starting from the
-- given edge and following the leftmost turn at each encountered node.
--
-- Edge ids are signed, they are negative if traversed backward. 
-- Sequence numbers start with 1.
--
-- Use a negative starting_edge to follow its rigth face rather than
-- left face (to start traversing it in reverse).
--
-- Optionally pass a limit on the number of edges to traverse. This is a
-- safety measure against not-properly linked topologies, where you may
-- end up looping forever (single edge loops edge are detected but longer
-- ones are not). Default is no limit (good luck!)
--
-- GetRingEdges(atopology, anedge, [maxedges])
--
CREATE OR REPLACE FUNCTION topology.GetRingEdges(atopology varchar, anedge int, maxedges int DEFAULT null)
	RETURNS SETOF topology.GetFaceEdges_ReturnType
AS
$$
DECLARE
  rec RECORD;
  retrec topology.GetFaceEdges_ReturnType;
  n int;
  sql text;
BEGIN
  sql := 'WITH RECURSIVE edgering AS ( SELECT '
    || anedge
    || ' as signed_edge_id, edge_id, next_left_edge, next_right_edge FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE edge_id = '
    || abs(anedge)
    || ' UNION '
    || ' SELECT CASE WHEN p.signed_edge_id < 0 THEN p.next_right_edge '
    || ' ELSE p.next_left_edge END, e.edge_id, e.next_left_edge, e.next_right_edge '
    || ' FROM ' || quote_ident(atopology)
    || '.edge_data e, edgering p WHERE e.edge_id = CASE WHEN p.signed_edge_id < 0 '
    || 'THEN abs(p.next_right_edge) ELSE abs(p.next_left_edge) END ) SELECT * FROM edgering';

  n := 1;
  FOR rec IN EXECUTE sql
  LOOP
    retrec.sequence := n;
    retrec.edge := rec.signed_edge_id;
    RETURN NEXT retrec;

    n := n + 1;

    IF n > maxedges THEN
      RAISE EXCEPTION 'Max traversing limit hit: %', maxedges;
    END IF;
  END LOOP;

END
$$
LANGUAGE 'plpgsql' STABLE;
--} GetRingEdges

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.refractions.net
--
-- Copyright (C) 2012 Sandro Santilli <strk@keybit.net>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--
-- Return a list of edges (sequence, id) incident to the given node.
--
-- Edge ids are signed, they are negative if the node is their endpoint.
-- Sequence numbers start with 1 ordering edges by azimuth (clockwise).
--
-- GetNodeEdges(atopology, anode)
--
CREATE OR REPLACE FUNCTION topology.GetNodeEdges(atopology varchar, anode int)
	RETURNS SETOF topology.GetFaceEdges_ReturnType
AS
$$
DECLARE
  curedge int;
  nextedge int;
  rec RECORD;
  retrec topology.GetFaceEdges_ReturnType;
  n int;
  sql text;
BEGIN

  n := 0;
  sql :=
    'WITH incident_edges AS ( SELECT edge_id, start_node, end_node, ST_RemoveRepeatedPoints(geom) as geom FROM '
    || quote_ident(atopology)
    || '.edge_data WHERE start_node = ' || anode
    || ' or end_node = ' || anode
    || ') SELECT edge_id, ST_Azimuth(ST_StartPoint(geom), ST_PointN(geom, 2)) as az FROM  incident_edges WHERE start_node = ' || anode
    || ' UNION ALL SELECT -edge_id, ST_Azimuth(ST_EndPoint(geom), ST_PointN(geom, ST_NumPoints(geom)-1)) FROM incident_edges WHERE end_node = ' || anode
    || ' ORDER BY az';




  FOR rec IN EXECUTE sql
  LOOP -- incident edges {




    n := n + 1;
    retrec.sequence := n;
    retrec.edge := rec.edge_id;
    RETURN NEXT retrec;
  END LOOP; -- incident edges }

END
$$
LANGUAGE 'plpgsql' STABLE;
--} GetRingEdges


--general management --

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- PostGIS - Spatial Types for PostgreSQL
-- http://www.postgis.org
--
-- Copyright (C) 2011 Regina Obe <lr@pcorp.us>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

--{
--  AddToSearchPath(schema_name)
--
-- Adds the specified schema to the database search path
-- if it is not already in the database search path
-- This is a helper function for upgrade/install
-- We may want to move this function as a generic helper
CREATE OR REPLACE FUNCTION topology.AddToSearchPath(a_schema_name varchar)
RETURNS text
AS
$$
DECLARE
	var_result text;
	var_cur_search_path text;
BEGIN
	SELECT reset_val INTO var_cur_search_path FROM pg_settings WHERE name = 'search_path';
	IF var_cur_search_path LIKE '%' || quote_ident(a_schema_name) || '%' THEN
		var_result := a_schema_name || ' already in database search_path';
	ELSE
		EXECUTE 'ALTER DATABASE ' || quote_ident(current_database()) || ' SET search_path = ' || var_cur_search_path || ', ' || quote_ident(a_schema_name); 
		var_result := a_schema_name || ' has been added to end of database search_path ';
	END IF;
  
  RETURN var_result;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;

--} AddToSearchPath


CREATE OR REPLACE FUNCTION postgis_topology_scripts_installed() RETURNS text
	AS $$ SELECT '2.0.3'::text || ' r' || 11128::text AS version $$
	LANGUAGE 'sql' IMMUTABLE;

-- Make sure topology is in database search path --
SELECT topology.AddToSearchPath('topology');

COMMIT;

