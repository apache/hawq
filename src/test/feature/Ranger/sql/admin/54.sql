CREATE FUNCTION getfoo() RETURNS SETOF mytype AS $$ SELECT i, i FROM a order by i $$ LANGUAGE SQL;

