set optimizer_disable_missing_stats_collection = on;
--
-- VACUUM
--

CREATE TABLE vactst (j INT DEFAULT 1,i INT);
INSERT INTO vactst(i) VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst(i) VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT i FROM vactst ORDER BY 1;
VACUUM FULL vactst;
UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst(i) VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM FULL vactst;
DELETE FROM vactst;
SELECT i FROM vactst ORDER BY 1;

DROP TABLE vactst;

-- vacuum/analyze a table with indexes
create table vactst (i int, b bool, c char, d date);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;

create index vactst_c on vactst (c);
vacuum vactst;
analyze vactst;

create index vactst_b on vactst using bitmap(b);
vacuum vactst;
analyze vactst;

vacuum analyze vactst;

drop table vactst;

create table vactst (i int, b bool, c char, d date);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;

create index vactst_c on vactst (c);
create index vactst_b on vactst using bitmap(b);
vacuum analyze vactst;

drop table vactst;

-- vacuum analyze a table that has dropped a column
create table vactst (i int, b bool, c char, d date);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;
alter table vactst drop column b;
vacuum analyze vactst;

alter table vactst drop column i;
vacuum analyze vactst;
drop table vactst;

-- vacuum analyze a table whose index has pg_statistic stats
create table vactst (i int, b bool, c char, d date);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;
create index vactst_c on vactst (upper(c));
vacuum analyze vactst;
drop table vactst;

-- vacuum analyze an AO table
create table vactst (i int, b bool, c char, d date) with (appendonly=true);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;
vacuum analyze vactst;
drop table vactst;

-- vacuum analyze a partition table
create table vactst (i int, b bool, c char, d date)
distributed by (i)
partition by list (b)
(partition b1 values ('f'),
 partition b2 values ('t')
);
insert into vactst select i, (i%123 > 50), (i/11) || '', '2008/10/12'::date + (i || ' days')::interval
from generate_series(0, 99) i;
vacuum analyze vactst;
drop table vactst;
