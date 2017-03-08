set session role=usersuper3;
CREATE OR REPLACE FUNCTION f4() RETURNS TEXT AS $$ plpy.execute("select * from a order by i") $$ LANGUAGE plpythonu VOLATILE;

