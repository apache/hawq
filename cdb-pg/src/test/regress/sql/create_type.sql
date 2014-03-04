--
-- CREATE_TYPE
--

--
-- Note: widget_in/out were CREATEd in CREATE_function_1, without any
-- prior shell-type creation.  These commands therefore complete a test
-- of the "old style" approach of making the functions first.
--

-- start_ignore
DROP SCHEMA IF EXISTS create_type_icg cascade;
CREATE schema create_type_icg;
SET search_path=create_type_icg;
-- end_ignore

-- Test creation and destruction of shell types
CREATE TYPE shell;
CREATE TYPE shell;   -- fail, type already present
DROP TYPE shell;
DROP TYPE shell;     -- fail, type not exist

--
-- Test type-related default VALUES (broken in releases before PG 7.2)
--
-- This part of the test also exercises the "new style" approach of making
-- a shell type and then filling it in.
--
CREATE TYPE int42;
CREATE TYPE text_w_default;

-- Make dummy I/O routines using the existing internal support for int4, text
CREATE FUNCTION int42_in(cstring)
   RETURNS int42
   AS 'int4in'
   LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION int42_out(int42)
   RETURNS cstring
   AS 'int4out'
   LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION text_w_default_in(cstring)
   RETURNS text_w_default
   AS 'textin'
   LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION text_w_default_out(text_w_default)
   RETURNS cstring
   AS 'textout'
   LANGUAGE internal IMMUTABLE STRICT;

CREATE TYPE int42 (
   internallength = 4,
   input = int42_in,
   output = int42_out,
   alignment = int4,
   default = 42,
   passedbyvalue
);

CREATE TYPE text_w_default (
   internallength = variable,
   input = text_w_default_in,
   output = text_w_default_out,
   alignment = int4,
   default = 'zippo'
);

CREATE TABLE default_test (f1 text_w_default, f2 int42);

INSERT INTO default_test DEFAULT VALUES;

SELECT * FROM default_test;

-- Test stand-alone composite type

CREATE TYPE default_test_row AS (f1 text_w_default, f2 int42);

CREATE FUNCTION get_default_test() RETURNS SETOF default_test_row AS '
  SELECT * FROM default_test;
' LANGUAGE SQL;

SELECT * FROM get_default_test();

-- Test comments
COMMENT ON TYPE bad IS 'bad comment';
COMMENT ON TYPE default_test_row IS 'good comment';
COMMENT ON TYPE default_test_row IS NULL;

-- Check shell type CREATE for existing types
CREATE TYPE text_w_default;		-- should fail

DROP TYPE default_test_row CASCADE;

DROP TABLE default_test;

-- Create & Drop type as non-superuser
CREATE USER user_bob;
SET SESSION AUTHORIZATION user_bob;
CREATE TYPE shell;
DROP TYPE shell;
CREATE TYPE compfoo as (f1 int, f2 text);
DROP TYPE compfoo;
RESET SESSION AUTHORIZATION;
DROP USER user_bob;

--udt arrays
CREATE TABLE aTABLE(k int, a int42[]);
INSERT INTO aTABLE VALUES(1, '{1, 3}');
INSERT INTO aTABLE VALUES(2, '{2, 3}');
INSERT INTO aTABLE VALUES(3, '{3, 3}');
INSERT INTO aTABLE VALUES(4, '{4, 3}');
SELECT a FROM aTABLE WHERE k=1;   
SELECT a FROM aTABLE WHERE k=4;   

--functions
CREATE FUNCTION echo_aTABLE(int) returns int42[]
as 'select a from aTABLE where k = $1;'
LANGUAGE SQL;
SELECT echo_aTABLE(1);
SELECT echo_aTABLE(2);


CREATE TABLE sensors(f1 text, f2 int42);
INSERT INTO sensors VALUES ('sensor1', '21');
INSERT INTO sensors VALUES ('sensor2', '22');
INSERT INTO sensors VALUES ('sensor3', '23');
INSERT INTO sensors VALUES ('sensor4', '24');

CREATE FUNCTION get_sensor_point(text) returns int42
as 'select f2 from sensors where f1=$1;'
language sql;
SELECT get_sensor_point('sensor2');

CREATE FUNCTION get_sensor_points() returns setof int42
as 'select f2 from sensors;'
language sql;
SELECT get_sensor_pointis();

-- start_ignore
DROP SCHEMA create_type_icg cascade;
-- end_ignore
