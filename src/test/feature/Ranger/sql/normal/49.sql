set session role=usertest49;
CREATE AGGREGATE scube(numeric) ( SFUNC = scube_accum, STYPE = numeric, INITCOND = 0 );

