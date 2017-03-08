CREATE FUNCTION scube_accum(numeric, numeric) RETURNS numeric AS 'select $1 + $2 * $2 * $2' LANGUAGE SQL IMMUTABLE RETURNS NULL ON NULL INPUT;

