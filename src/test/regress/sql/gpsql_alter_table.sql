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
CREATE TABLE altablec(a int, b text, c int) WITH (appendonly=true, orientation=column); -- should error: deprecated

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
drop type udt1;
