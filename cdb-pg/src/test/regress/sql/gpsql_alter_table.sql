-- AO
CREATE TABLE altable(a int, b text, c int);
INSERT INTO altable SELECT i, i::text, i FROM generate_series(1, 10)i;

ALTER TABLE altable ADD COLUMN x int;
ALTER TABLE altable ADD COLUMN x int DEFAULT 1;
SELECT a, b, c, x FROM altable;

ALTER TABLE altable ALTER COLUMN c SET DEFAULT 10 - 1;
INSERT INTO altable(a, b) VALUES(11, '11');
SELECT a, b, c FROM altable WHERE a = 11;

ALTER TABLE altable ALTER COLUMN c DROP DEFAULT;
BEGIN;
INSERT INTO altable(a, b) VALUES(12, '12');
SELECT a, b, c FROM altable WHERE a = 12;
ROLLBACK;

ALTER TABLE altable ALTER COLUMN c SET NOT NULL;
INSERT INTO altable(a, b) VALUES(13, '13');

ALTER TABLE altable ALTER COLUMN c DROP NOT NULL;
INSERT INTO altable(a, b) VALUES(13, '13');
SELECT a, b, c FROM altable WHERE a = 13;

ALTER TABLE altable ALTER COLUMN c SET STATISTICS 100;

ALTER TABLE altable ALTER COLUMN b SET STORAGE PLAIN;

ALTER TABLE altable DROP COLUMN b;
SELECT a, c FROM altable;

ALTER TABLE altable ADD CONSTRAINT c_check CHECK (c < 10);

ALTER TABLE altable ADD CONSTRAINT c_check CHECK (c > 0);
INSERT INTO altable(a, c) VALUES(0, -10);

ALTER TABLE altable DROP CONSTRAINT c_check;
INSERT INTO altable(a, c) VALUES(0, -10);

ALTER TABLE altable ALTER COLUMN c TYPE bigint;

CREATE USER alt_user;
CREATE TABLE altable_owner(a, b) AS VALUES(1, 10),(2,20);
ALTER TABLE altable_owner OWNER to alt_user;
SET ROLE alt_user;
SELECT a, b FROM altable_owner;
RESET ROLE;
DROP TABLE altable_owner;
DROP USER alt_user;

ALTER TABLE altable CLUSTER ON some_index;

ALTER TABLE altable SET WITHOUT OIDS;

ALTER TABLE altable SET TABLESPACE pg_default;

ALTER TABLE altable SET (fillfactor = 90);

ALTER TABLE altable ENABLE TRIGGER ALL;

ALTER TABLE altable DISABLE TRIGGER ALL;

-- CO
CREATE TABLE altablec(a int, b text, c int) WITH (appendonly=true, orientation=column);
INSERT INTO altablec SELECT i, i::text, i FROM generate_series(1, 10)i;

ALTER TABLE altablec ADD COLUMN x int;
ALTER TABLE altablec ADD COLUMN x int DEFAULT 1;
SELECT a, b, c, x FROM altablec;

ALTER TABLE altablec ALTER COLUMN c SET DEFAULT 10 - 1;
INSERT INTO altablec(a, b) VALUES(11, '11');
SELECT a, b, c FROM altablec WHERE a = 11;

ALTER TABLE altablec ALTER COLUMN c DROP DEFAULT;
BEGIN;
INSERT INTO altablec(a, b) VALUES(12, '12');
SELECT a, b, c FROM altablec WHERE a = 12;
ROLLBACK;

ALTER TABLE altablec ALTER COLUMN c SET NOT NULL;
INSERT INTO altablec(a, b) VALUES(13, '13');

ALTER TABLE altablec ALTER COLUMN c DROP NOT NULL;
INSERT INTO altablec(a, b) VALUES(13, '13');
SELECT a, b, c FROM altablec WHERE a = 13;

ALTER TABLE altablec ALTER COLUMN c SET STATISTICS 100;

ALTER TABLE altablec ALTER COLUMN b SET STORAGE PLAIN;

ALTER TABLE altablec DROP COLUMN b;
SELECT a, c FROM altablec;

ALTER TABLE altablec ADD CONSTRAINT c_check CHECK (c < 10);

ALTER TABLE altablec ADD CONSTRAINT c_check CHECK (c > 0);
INSERT INTO altablec(a, c) VALUES(0, -10);

ALTER TABLE altablec DROP CONSTRAINT c_check;
INSERT INTO altablec(a, c) VALUES(0, -10);

ALTER TABLE altablec ALTER COLUMN c TYPE bigint;

CREATE TABLE palt(a int, b text, c int)
PARTITION BY RANGE(c)
(START (1) END (100) EVERY (10));
INSERT INTO palt SELECT i, i::text, i FROM generate_series(1, 99)i;

CREATE TABLE palt_child(a int, b text, c int);
ALTER TABLE palt EXCHANGE PARTITION FOR (51) WITH TABLE palt_child;

ALTER TABLE palt ADD PARTITION START (100) END (110);
INSERT INTO palt VALUES(100, '100', 100);
SELECT count(*) FROM palt;

ALTER TABLE palt RENAME PARTITION FOR (100) TO part100;

ALTER TABLE palt DROP PARTITION FOR (100);
INSERT INTO palt VALUES(100, '100', 100);
SELECT count(*) FROM palt;

ALTER TABLE palt TRUNCATE PARTITION FOR (41);
SELECT count(*) FROM palt;

ALTER TABLE palt ADD COLUMN x text DEFAULT 'default';
SELECT count(*) FROM palt;

--
-- cover more column/attribute types
--
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp (initial int4);

COMMENT ON TABLE tmp_wrong IS 'table comment';
COMMENT ON TABLE tmp IS 'table comment';
COMMENT ON TABLE tmp IS NULL;

ALTER TABLE tmp ADD COLUMN a int4 default 3;

ALTER TABLE tmp ADD COLUMN b name default 'Alan Turing';

ALTER TABLE tmp ADD COLUMN c text default 'Pivotal';

ALTER TABLE tmp ADD COLUMN d float8 default 0;

ALTER TABLE tmp ADD COLUMN e float4 default 0;

ALTER TABLE tmp ADD COLUMN f int2 default 0;

ALTER TABLE tmp ADD COLUMN g polygon default '(1,1),(1,2),(2,2)'::polygon;

ALTER TABLE tmp ADD COLUMN h abstime default null;

ALTER TABLE tmp ADD COLUMN i char default 'P';

ALTER TABLE tmp ADD COLUMN j abstime[] default ARRAY['2/2/2013 4:05:06'::abstime, '2/2/2013 5:05:06'::abstime];

ALTER TABLE tmp ADD COLUMN k int4 default 0;

ALTER TABLE tmp ADD COLUMN l tid default '(0,1)'::tid;

ALTER TABLE tmp ADD COLUMN m xid default '0'::xid;

ALTER TABLE tmp ADD COLUMN n oidvector default '0 0 0 0'::oidvector;

--ALTER TABLE tmp ADD COLUMN o lock;
ALTER TABLE tmp ADD COLUMN p smgr default 'magnetic disk'::smgr;

ALTER TABLE tmp ADD COLUMN q point default '(0,0)'::point;

ALTER TABLE tmp ADD COLUMN r lseg default '(0,0),(1,1)'::lseg;

ALTER TABLE tmp ADD COLUMN s path default '(1,1),(1,2),(2,2)'::path;

ALTER TABLE tmp ADD COLUMN t box default box(circle '((0,0), 2.0)');

ALTER TABLE tmp ADD COLUMN u tinterval default tinterval('2/2/2013 4:05:06', '2/2/2013 5:05:06');

ALTER TABLE tmp ADD COLUMN v timestamp default '2/2/2013 4:05:06'::timestamp;

ALTER TABLE tmp ADD COLUMN w interval default '3 4:05:06'::interval;

ALTER TABLE tmp ADD COLUMN x float8[] default ARRAY[0, 0, 0];

ALTER TABLE tmp ADD COLUMN y float4[] default ARRAY[0, 0, 0];

ALTER TABLE tmp ADD COLUMN z int2[] default ARRAY[0, 0, 0];

INSERT INTO tmp (a, b, c, d, e, f, g, h, i, j, k, l, m, n, p, q, r, s, t, u,
	v, w, x, y, z)
   VALUES (4, 'name', 'text', 4.1, 4.1, 2, '(4.1,4.1,3.1,3.1)', 
        'Mon May  1 00:30:30 1995', 'c', '{Mon May  1 00:30:30 1995, Monday Aug 24 14:43:07 1992, epoch}', 
	314159, '(1,1)', '512',
	'1 2 3 4 5 6 7 8', 'magnetic disk', '(1.1,1.1)', '(4.1,4.1,3.1,3.1)',
	'(0,2,4.1,4.1,3.1,3.1)', '(4.1,4.1,3.1,3.1)', '["epoch" "infinity"]',
	'epoch', '01:00:10', '{1.0,2.0,3.0,4.0}', '{1.0,2.0,3.0,4.0}', '{1,2,3,4}');

SELECT * FROM tmp;

DROP TABLE tmp;

-- add a column of user defined type
create type udt1
       AS(base integer, incbase integer, ctime timestamptz);
create table ao1 (a integer, b integer) distributed by(a);
insert into ao1 select i, 10*i from generate_series(1,10)i;
alter table ao1 add column c udt1 default null;
insert into ao1
       select i, -i,
       	      (-i*2, 10*i, '12/1/14 22:22:01')::udt1
	       from generate_series(1,10)i;
select * from ao1 order by a,b;
drop table ao1;

--
-- MPP-19664 
-- Test ALTER TABLE ADD COLUMN WITH NULL DEFAULT on CO TABLES
--
--- 
--- basic support for alter add column with NULL default to CO tables
--- 
drop table if exists aoco1;
create table aoco1(col1 varchar(2), col2 int)
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

insert into aoco1 values('aa', 1);
insert into aoco1 values('bb', 2);

-- following should be OK.
alter table aoco1 add column col3 char(1) default 5;

-- the following should be supported now
alter table aoco1 add column col4 char(1) default NULL;

select * from aoco1 order by col1;
insert into aoco1 values('cc', 3);
select * from aoco1 order by col1;

alter table aoco1 alter column col4 drop default; 
select * from aoco1 order by col1;
insert into aoco1 values('dd', 4);
select * from aoco1 order by col1;

---
--- check catalog contents after alter table on AO/CO tables 
---
drop table if exists aoco1;
create table aoco1(col1 varchar(2), col2 int)
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

-- relnatts is 2
select relname, relnatts from pg_class where relname = 'aoco1';

alter table aoco1 add column col3 char(1) default NULL;

-- relnatts in pg_class should be 3
select relname, relnatts from pg_class where relname = 'aoco1';

-- check col details in pg_attribute
select  pg_class.relname, attname, typname from pg_attribute, pg_class, pg_type where attrelid = pg_class.oid and pg_class.relname = 'aoco1' and atttypid = pg_type.oid and attname = 'col3';

-- no explicit entry in pg_attrdef for NULL default
select relname, attname, adsrc from pg_class, pg_attribute, pg_attrdef where attrelid = pg_class.oid and adrelid = pg_class.oid and adnum = pg_attribute.attnum and pg_class.relname = 'aoco1';

--- 
--- check with IS NOT NULL constraint
--- 
drop table if exists aoco1;
create table aoco1(col1 varchar(2), col2 int)
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

insert into aoco1 values('a', 1); 

-- should fail (rewrite needs to do null checking) 
alter table aoco1 add column col3 char(1) not null default NULL; 
alter table aoco1 add column c5 int check (c5 IS NOT NULL) default NULL;

-- should fail (rewrite needs to do constraint checking) 
insert into aoco1(col1, col2) values('a', NULL);
alter table aoco1 alter column col2 set not null; 

-- should pass (rewrite needs to do constraint checking) 
alter table aoco1 alter column col2 type int; 

drop table if exists aoco1;
create table aoco1(col1 varchar(2), col2 int)
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

-- should pass
alter table aoco1 add column col3 char(1) not null default NULL; 

-- this should fail (same behavior as heap tables)
insert into aoco1 (col1, col2) values('a', 10);

drop table if exists aoco1;
create table aoco1(col1 varchar(2), col2 int not null)
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

insert into aoco1 values('aa', 1);
alter table aoco1 add column col3 char(1) default NULL;
insert into aoco1 values('bb', 2);
select * from aoco1 order by col1;
alter table aoco1 add column col4 char(1) not NULL default NULL;
select * from aoco1 order by col1;

---
--- alter add with no default should continue to fail
---
drop table if exists aoco1;
create table aoco1(col1 varchar(1))
WITH (APPENDONLY=TRUE, ORIENTATION=column) distributed randomly;

insert into aoco1 values('1');
insert into aoco1 values('1');
insert into aoco1 values('1');
insert into aoco1 values('1');

alter table aoco1 add column col2 char(1);
select * from aoco1 order by col1;

drop table aoco1;

-- add a column of user defined type
create table aoco1 (a integer, b integer)
       with (appendonly=true, orientation=column)
       distributed by(a);
insert into aoco1 select i, -10*i from generate_series(1,10)i;
alter table aoco1 add column c udt1 default null;
insert into aoco1
       select i, -i,
       	      (i, 10*i, '12/1/14 22:22:01')::udt1
	       from generate_series(1,10)i;
select * from aoco1 order by a,b;
drop table aoco1;
drop type udt1;
