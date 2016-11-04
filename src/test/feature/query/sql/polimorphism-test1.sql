-- Legend:
-----------
-- A = type is ANY
-- P = type is polymorphic
-- N = type is non-polymorphic
-- B = aggregate base type
-- S = aggregate state type
-- R = aggregate return type
-- 1 = arg1 of a function
-- 2 = arg2 of a function
-- ag = aggregate
-- tf = trans (state) function
-- ff = final function
-- rt = return type of a function
-- -> = implies
-- => = allowed
-- !> = not allowed
-- E  = exists
-- NE = not-exists
--
-- Possible states:
-- ----------------
-- B = (A || P || N)
--   when (B = A) -> (tf2 = NE)
-- S = (P || N)
-- ff = (E || NE)
-- tf1 = (P || N)
-- tf2 = (NE || P || N)
-- R = (P || N)

-- polymorphic single arg transfn
CREATE FUNCTION stfp(anyarray) RETURNS anyarray AS 'select $1' LANGUAGE SQL;
-- non-polymorphic single arg transfn
CREATE FUNCTION stfnp(int[]) RETURNS int[] AS 'select $1' LANGUAGE SQL;
-- dual polymorphic transfn
CREATE FUNCTION tfp(anyarray,anyelement) RETURNS anyarray AS 'select $1 || $2' LANGUAGE SQL;
-- dual non-polymorphic transfn
CREATE FUNCTION tfnp(int[],int) RETURNS int[] AS 'select $1 || $2' LANGUAGE SQL;
-- arg1 only polymorphic transfn
CREATE FUNCTION tf1p(anyarray,int) RETURNS anyarray AS 'select $1' LANGUAGE SQL;
-- arg2 only polymorphic transfn
CREATE FUNCTION tf2p(int[],anyelement) RETURNS int[] AS 'select $1' LANGUAGE SQL;
-- multi-arg polymorphic
CREATE FUNCTION sum3(anyelement,anyelement,anyelement) RETURNS anyelement AS 'select $1+$2+$3' LANGUAGE SQL STRICT;
-- finalfn polymorphic
CREATE FUNCTION ffp(anyarray) RETURNS anyarray AS 'select $1' LANGUAGE SQL;
-- finalfn non-polymorphic
CREATE FUNCTION ffnp(int[]) RETURNS int[] AS 'select $1' LANGUAGE SQL;