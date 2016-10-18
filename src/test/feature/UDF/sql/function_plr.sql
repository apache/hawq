CREATE OR REPLACE FUNCTION r_max (INTEGER, INTEGER)
RETURNS INTEGER
AS $$
	if (arg1 > arg2)
		return(arg1)
	else
		return(arg2)
$$
LANGUAGE plr STRICT;

SELECT r_max(1, 10);
