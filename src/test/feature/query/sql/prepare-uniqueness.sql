SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY 1,2,3;

PREPARE q1 AS SELECT 1 AS a;
EXECUTE q1;

PREPARE q1 AS SELECT 2;

DEALLOCATE q1;
PREPARE q1 AS SELECT 2;
EXECUTE q1;
PREPARE q2 AS SELECT 2 AS b;

SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY 1,2,3;

DEALLOCATE PREPARE q1;
DEALLOCATE PREPARE q2;

SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY 1,2,3;