set session role=usertest8;
CREATE OR REPLACE FUNCTION si_lt(text, text) RETURNS boolean AS $$ BEGIN RETURN normalize_si($1) < normalize_si($2); END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

