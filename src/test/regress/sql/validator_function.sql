-- --------------------------------------
-- test validator function cve-2014-061
-- --------------------------------------
BEGIN;
-- Create test function foo()
CREATE OR REPLACE FUNCTION foo() RETURNS boolean AS $$
BEGIN
  RETURN true;
END;
$$ LANGUAGE 'plpgsql';
-- Remove public access to foo()
REVOKE ALL ON FUNCTION foo() FROM PUBLIC;
COMMIT;

BEGIN;
-- Create user user1
CREATE USER user1;
-- user1 cannot executre foo()
REVOKE EXECUTE ON FUNCTION foo() FROM user1;
COMMIT;

-- DEBUG info
SELECT usesuper FROM pg_user WHERE usename = current_user;
-- foo() works
SELECT foo();

-- --------------------------------------
-- Cannot run validator on functions
-- defined in another language
-- --------------------------------------
-- Correct validator on foo() works for default admin user
SELECT plpgsql_validator(oid)
FROM (
  SELECT oid FROM pg_proc
  WHERE proname = 'foo'
) AS bar;
-- Wrong validator of a different language fails on foo()
SELECT fmgr_sql_validator(oid)
FROM (
  SELECT oid FROM pg_proc
  WHERE proname = 'foo'
) AS bar;

-- --------------------------------------
-- Cannot run validator on functions to
-- which user has no privilege to execute
-- --------------------------------------
SET SESSION AUTHORIZATION user1;
-- DEBUG info
SELECT usesuper FROM pg_user WHERE usename = 'user1' AND current_user = 'user1';
-- foo() no longer works
SELECT foo();
-- Can no longer run validator function due to user1
-- not having access to foo()
SELECT plpgsql_validator(oid)
FROM (
  SELECT oid FROM pg_proc
  WHERE proname = 'foo'
) AS bar;

-- cleanup
-- switch to superuser
\c -
DROP USER user1;
DROP FUNCTION foo();
