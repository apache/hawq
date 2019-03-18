--set session role=usertest21;
insert into foo(i) values(1234);

--set session role=usertest24;
--COPY (select * from a) to '/tmp/a.txt';
--COPY a FROM '/tmp/a.txt';

--set session role=usertest25;
COPY a TO STDOUT WITH DELIMITER '|';

--set session role=usertest27;
--select * from ext_t order by N_NATIONKEY;   


--set session role=usertest29;
--insert into ext_t2(i) values(234);

--set session role=usertest38;
insert into a values(1);

--set session role=usertest39;
insert into a values(1);

--set session role=usertest4;
select * from f4();

--set session role=usertest40;
insert into a VALUES (nextval('myseq'));

--set session role=usertest41;
select * from pg_database, a order by oid, i limit 1;

--set session role=usertest42;
select generate_series(1,3);

--set session role=usertest43;
select * from av;

--set session role=usertest44;
SELECT setval('myseq', 1);

--set session role=usertest45;
--SELECT * INTO aaa FROM a WHERE i > 0 order by i;

--set session role=usertest46;
PREPARE fooplan (int) AS INSERT INTO a VALUES($1);EXECUTE fooplan(1);DEALLOCATE fooplan;

--set session role=usertest47;
explain select * from a;

--set session role=usertest55;
select getfoo();

--set session role=usertest57;
begin; DECLARE mycursor CURSOR FOR SELECT * FROM a order by i; FETCH FORWARD 2 FROM mycursor; commit;

--set session role=usertest58;
BEGIN; INSERT INTO a VALUES (1); SAVEPOINT my_savepoint; INSERT INTO a VALUES (1); RELEASE SAVEPOINT my_savepoint; COMMIT;

--set session role=usertest59;
\d

--set session role=usertest60;
analyze a;

--set session role=usertest61;
--analyze;

--set session role=usertest62;
vacuum aa;

--set session role=usertest63;
--vacuum analyze;
