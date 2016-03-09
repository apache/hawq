---
--- Test for CREATE TABLE distribution policy
---

CREATE TABLE t1(c1 int);

CREATE TABLE t1_1(c2 int) INHERITS(t1);

-- should error out messages with different bucketnum
CREATE TABLE t1_1_w(c2 int) INHERITS(t1) WITH (bucketnum = 3);

CREATE TABLE t1_1_w(c2 int) INHERITS(t1) WITH (bucketnum = 8);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_w');

CREATE TABLE t1_1_1(c2 int) INHERITS (t1) DISTRIBUTED BY(c1);

CREATE TABLE t1_1_2(c2 int) INHERITS (t1) DISTRIBUTED BY(c2);

CREATE TABLE t1_1_3(c2 int) INHERITS (t1) DISTRIBUTED RANDOMLY;

-- should error out messages with different bucketnum
CREATE TABLE t1_1_4(c2 int) INHERITS (t1) WITH (bucketnum = 3) DISTRIBUTED BY(c1) ;

CREATE TABLE t1_1_5(c2 int) INHERITS (t1) WITH (bucketnum = 5) DISTRIBUTED BY(c2);

CREATE TABLE t1_1_6(c2 int) INHERITS (t1) WITH (bucketnum = 7) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_2');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_3');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_4');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_5');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_1_6');

CREATE TABLE t1_2(LIKE t1);        

-- should error out messages with different bucketnum
CREATE TABLE t1_2_w(LIKE t1) WITH (bucketnum = 4);   

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2_w');

CREATE TABLE t1_2_1(LIKE t1) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2_1');

CREATE TABLE t1_2_2(LIKE t1) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2_2');

-- should error out messages with different bucketnum
CREATE TABLE t1_2_3(LIKE t1) WITH (bucketnum = 4) DISTRIBUTED BY (c1);

CREATE TABLE t1_2_4(LIKE t1) WITH (bucketnum = 4) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2_3');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_2_4');

CREATE TABLE t1_3 AS (SELECT * FROM t1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3');

CREATE TABLE t1_3_w WITH (bucketnum = 4) AS (SELECT * FROM t1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3_w');

CREATE TABLE t1_3_1 AS (SELECT * FROM  t1) DISTRIBUTED BY (c1);                   

CREATE TABLE t1_3_2 AS (SELECT * FROM  t1) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3_1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3_2');

CREATE TABLE t1_3_3 WITH (bucketnum = 6) AS (SELECT * FROM  t1) DISTRIBUTED BY (c1);                   

CREATE TABLE t1_3_4 WITH (bucketnum = 7) AS (SELECT * FROM  t1) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3_3');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't1_3_4');

DROP TABLE t1_3_4, t1_3_3, t1_3_2, t1_3_1, t1_3_w, t1_3, t1_2_4, t1_2_3, t1_2_2, t1_2_1, t1_2_w, t1_2, t1_1_3, t1_1_w, t1_1, t1;

CREATE TABLE t2(c1 int) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2');

CREATE TABLE t2_1(c2 int) INHERITS (t2);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1');

CREATE TABLE t2_1_w(c2 int) INHERITS (t2) WITH (bucketnum = 3);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_w');

CREATE TABLE t2_1_1(c2 int) INHERITS (t2) DISTRIBUTED BY (c1);

CREATE TABLE t2_1_2(c2 int) INHERITS (t2) DISTRIBUTED BY (c2);

CREATE TABLE t2_1_3(c2 int) INHERITS (t2) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_2');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_3');

CREATE TABLE t2_1_4(c2 int) INHERITS (t2) WITH (bucketnum = 3) DISTRIBUTED BY (c1);

CREATE TABLE t2_1_5(c2 int) INHERITS (t2) WITH (bucketnum = 5) DISTRIBUTED BY (c2);

CREATE TABLE t2_1_6(c2 int) INHERITS (t2) WITH (bucketnum = 7) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_4');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_5');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_1_6');

CREATE TABLE t2_2(LIKE t2);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2');

CREATE TABLE t2_2_w(LIKE t2) WITH (bucketnum = 4);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_w');

CREATE TABLE t2_2_1(LIKE t2) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_1');

CREATE TABLE t2_2_2(LIKE t2) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_2');

CREATE TABLE t2_2_3(LIKE t2) WITH (bucketnum = 5) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_3');

CREATE TABLE t2_2_4(LIKE t2) WITH (bucketnum = 6) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_2_4');

CREATE TABLE t2_3 AS (SELECT * FROM  t2);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3');

CREATE TABLE t2_3_w WITH (bucketnum = 4) AS (SELECT * FROM  t2);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_w');
                                                                                                                                    ;
CREATE TABLE t2_3_1 AS (SELECT * FROM  t2) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_1');

CREATE TABLE t2_3_2 AS (SELECT * FROM  t2) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_2');

CREATE TABLE t2_3_3 WITH (bucketnum = 5) AS (SELECT * FROM  t2) DISTRIBUTED BY (c1);

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_3');

CREATE TABLE t2_3_4 WITH (bucketnum = 6) AS (SELECT * FROM  t2) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't2_3_4');

DROP TABLE t2_3_4, t2_3_3, t2_3_2, t2_3_1, t2_3_w, t2_3, t2_2_4, t2_2_3, t2_2_2, t2_2_1, t2_2_w, t2_2, t2_1_1, t2_1_w, t2_1, t2;

CREATE TABLE t3 (c1 int) WITH (bucketnum = 4);

CREATE TABLE t3_1 (c1 int) WITH (bucketnum = 5) DISTRIBUTED BY(c1);

CREATE TABLE t3_2 (c1 int) WITH (bucketnum = 6) DISTRIBUTED RANDOMLY;

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3_1');

SELECT bucketnum, attrnums FROM gp_distribution_policy WHERE localoid = (SELECT oid FROM pg_class WHERE relname = 't3_2');

DROP TABLE t3_2, t3_1, t3;

CREATE TABLE t4 (id int, date date, amt decimal(10,2))
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (date)
( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE WITH (bucketnum = 9), 
 PARTITION Feb08 START (date '2008-02-01') INCLUSIVE END (date '2008-03-01') EXCLUSIVE WITH (bucketnum = 8));

-- expected error out
select bucketnum, attrnums from gp_distribution_policy where localoid='t4'::regclass;

CREATE TABLE t4 (id int, date date, amt decimal(10,2))
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (date)
( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE WITH (bucketnum = 8), 
 PARTITION Feb08 START (date '2008-02-01') INCLUSIVE END (date '2008-03-01') EXCLUSIVE WITH (bucketnum = 8));

select bucketnum, attrnums from gp_distribution_policy where localoid='t4'::regclass;

ALTER TABLE t4 ADD PARTITION 
START (date '2008-03-01') INCLUSIVE 
END (date '2008-04-01') EXCLUSIVE WITH (bucketnum = 6, tablename='t4_new_part');

-- expected error out
select bucketnum, attrnums from gp_distribution_policy where localoid='t4_new_part'::regclass;

ALTER TABLE t4 ADD PARTITION 
START (date '2008-03-01') INCLUSIVE 
END (date '2008-04-01') EXCLUSIVE WITH (bucketnum = 8, tablename='t4_new_part');

select bucketnum, attrnums from gp_distribution_policy where localoid='t4_new_part'::regclass;

DROP TABLE t4 CASCADE;
