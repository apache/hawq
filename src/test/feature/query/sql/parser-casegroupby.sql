--
-- CASE ... WHEN IS NOT DISTINCT FROM ...
--
INSERT INTO mytable values 	(1,2,'t'),
							(2,3,'e'),
							(3,4,'o'),
							(4,5,'o'),
							(4,4,'o'),
							(5,5,'t'),
							(6,6,'t'),
							(7,6,'a'),
							(8,7,'t'),
							(9,8,'a');

CREATE OR REPLACE FUNCTION negate(int) RETURNS int 
AS 'SELECT $1 * (-1)'
LANGUAGE sql
IMMUTABLE
RETURNS null ON null input;

DROP VIEW IF EXISTS myview;
CREATE VIEW myview AS 
   SELECT a,b, CASE a WHEN IS NOT DISTINCT FROM b THEN b*10
                      WHEN IS NOT DISTINCT FROM b+1 THEN b*100 
                      WHEN b-1 THEN b*1000
                      WHEN b*10 THEN b*10000
                      WHEN negate(b) THEN b*(-1.0)
                      ELSE b END AS newb
     FROM mytable;
SELECT * FROM myview ORDER BY a,b;


-- Test deparse
select pg_get_viewdef('myview',true); 

-- User-defined DECODE function
CREATE OR REPLACE FUNCTION "decode"(int, int, int) RETURNS int
AS 'select $1 * $2 - $3;'
LANGUAGE sql
IMMUTABLE
RETURNS null ON null input;

SELECT decode(11,8,11);
SELECT "decode"(11,8,11);

-- Test CASE x WHEN IS NOT DISTINCT FROM y with DECODE
SELECT a,b,decode(a,1,1), 
		CASE decode(a,1,1) WHEN IS NOT DISTINCT FROM 1 THEN b*100
                  		   WHEN IS NOT DISTINCT FROM 4 THEN b*1000 ELSE b END as newb
  FROM mytable ORDER BY a,b; 

-- Test CASE WHEN x IS NOT DISTINCT FROM y with DECODE
SELECT a,b,decode(a,1,1), 
		CASE WHEN decode(a,1,1) IS NOT DISTINCT FROM 1 THEN b*100
			 WHEN decode(a,1,1) IS NOT DISTINCT FROM 4 THEN b*1000 ELSE b END as newb
  FROM mytable ORDER BY a,b; 

SELECT a,b,"decode"(a,1,1), 
			CASE WHEN "decode"(a,1,1) IS NOT DISTINCT FROM 1 THEN b*100
                 WHEN "decode"(a,1,1) IS NOT DISTINCT FROM 4 THEN b*1000 ELSE b END as newb
  FROM mytable ORDER BY a,b; 

--
-- Case expression in group by
--
SELECT
        CASE t.field1
        WHEN IS NOT DISTINCT FROM ''::text THEN 'Undefined'::character varying
        ELSE t.field1
        END AS field1
        FROM ( SELECT 'test value'::text AS field1) t
        GROUP BY
        CASE t.field1
                WHEN IS NOT DISTINCT FROM ''::text THEN 'Undefined'::character varying
                ELSE t.field1
        END;

--
-- Variant of case expression in group by
--
SELECT
        CASE t.field1
        WHEN IS NOT DISTINCT FROM ''::text THEN 'Undefined'::character varying
        ELSE t.field1
        END AS field1
        FROM ( SELECT 'test value'::text AS field1) t
        GROUP BY 1;

--
-- decode in group by
--
SELECT
        decode(t.field1, ''::text, 'Undefined'::character varying, t.field1) as field1
        FROM ( SELECT 'test value'::text AS field1) t
        GROUP BY
        decode(t.field1, ''::text, 'Undefined'::character varying, t.field1);

--
-- variant of decode in group by
--
        SELECT
        decode(t.field1, ''::text, 'Undefined'::character varying, t.field1) as field1
        FROM ( SELECT 'test value'::text AS field1) t
        GROUP BY 1;

--
-- clean up
--
DROP FUNCTION "decode"(int, int, int);
