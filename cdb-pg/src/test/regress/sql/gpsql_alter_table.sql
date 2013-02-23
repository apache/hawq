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

ALTER TABLE altable SET DISTRIBUTED BY c;

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
