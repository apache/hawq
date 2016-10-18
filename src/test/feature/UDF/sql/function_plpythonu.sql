CREATE OR REPLACE FUNCTION plpythonu_max (a INTEGER, b INTEGER)
RETURNS INTEGER
AS $$
  if a > b:
    return a
  return b
$$ LANGUAGE plpythonu;

SELECT plpythonu_max(1, 10);
