set session role=usertest7;
CREATE OR REPLACE FUNCTION normalize_si(text) RETURNS text AS $$ BEGIN RETURN substring($1, 9, 2) || substring($1, 7, 2) || substring($1, 5, 2) || substring($1, 1, 4); END; $$LANGUAGE 'plpgsql' IMMUTABLE;

