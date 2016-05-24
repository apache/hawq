PREPARE q2(text) AS
        SELECT datname, datistemplate, datallowconn
        FROM pg_database WHERE datname = $1;

EXECUTE q2('postgres');

PREPARE q3(text, int, float, boolean, oid, smallint) AS
        SELECT * FROM test1 WHERE string4 = $1 AND (four = $2 OR
        ten = $3::bigint OR true = $4 OR oid = $5 OR odd = $6::int) ORDER BY 1,2,3,4;

EXECUTE q3('AAAAxx', 5::smallint, 10.5::float, false, 500::oid, 4::bigint);

EXECUTE q3('bool');

EXECUTE q3('bytea', 5::smallint, 10.5::float, false, 500::oid, 4::bigint, true);

EXECUTE q3(5::smallint, 10.5::float, false, 500::oid, 4::bigint, 'bytea');

PREPARE q4(nonexistenttype) AS SELECT $1;

PREPARE q5(int, text) AS
        SELECT * FROM test1 WHERE unique1 = $1 OR stringu1 = $2;
CREATE TEMPORARY TABLE q5_prep_results AS EXECUTE q5(200, 'DTAAAA');
SELECT * FROM q5_prep_results ORDER BY 1,2,3,4;

PREPARE q6 AS
    SELECT * FROM test1 WHERE unique1 = $1 AND stringu1 = $2;
PREPARE q7(unknown) AS
    SELECT * FROM test2 WHERE thepath = $1;

SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY name;
