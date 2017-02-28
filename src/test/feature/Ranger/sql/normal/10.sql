set session role=usertest10;
CREATE OR REPLACE FUNCTION si_same(text, text) RETURNS int AS $$ BEGIN IF normalize_si($1) < normalize_si($2) THEN RETURN -1; END IF; END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

