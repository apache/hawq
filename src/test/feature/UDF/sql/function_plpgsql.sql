CREATE OR REPLACE FUNCTION plpgsql_increment(i INTEGER)
RETURNS INTEGER
AS $$
    BEGIN
        RETURN i + 1;
    END;
$$ LANGUAGE plpgsql;

SELECT plpgsql_increment(6);
