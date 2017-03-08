set session role=usertest46;
PREPARE fooplan (int) AS INSERT INTO a VALUES($1);EXECUTE fooplan(1);DEALLOCATE fooplan;

