set enable_partition_rules = false;
set gp_enable_hash_partitioned_tables = true;

-- missing subpartition by
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

-- missing subpartition spec
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
(
partition aa ,
partition bb 
);

-- subpart spec conflict
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b) 
subpartition by hash (d) subpartition template (subpartition jjj)
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

-- missing subpartition by
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
subpartition by hash (d)
(
partition aa (subpartition cc, subpartition dd (subpartition iii)),
partition bb (subpartition cc, subpartition dd)
);

-- Test column lookup works
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash(doesnotexist)
partitions 3;

create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash(b)
partitions 3
subpartition by list(alsodoesntexist)
subpartition template (
subpartition aa values(1)
);

-- will not work since sql has no subpartition templates
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

drop table if exists ggg cascade;

-- disable hash partitions
set gp_enable_hash_partitioned_tables = false;

create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

drop table ggg cascade;

set gp_enable_hash_partitioned_tables = true;

-- should work
create table ggg (a char(1), b char(2), d char(3), e int)
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
subpartition template ( 
subpartition cc,
subpartition dd
), 
subpartition by hash (e) 
subpartition template ( 
subpartition ee,
subpartition ff
) 
(
partition aa,
partition bb
);

drop table ggg cascade;

-- should not work since the first-level subpartitioning has no template
create table ggg (a char(1), b char(2), d char(3), e int)
distributed by (a)
partition by hash (b)
subpartition by hash (d),
subpartition by hash (e)
subpartition template ( 
subpartition ee,
subpartition ff
) 
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

drop table if exists ggg cascade;

-- doesn't work because cannot have nested declaration in template
create table ggg (a char(1), b char(2), d char(3), e int)
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
subpartition template ( 
subpartition cc (subpartition ee, subpartition ff),
subpartition dd (subpartition ee, subpartition ff)
), 
subpartition by hash (e) 
(
partition aa,
partition bb
);

drop table ggg cascade;

--ERROR: Missing boundary specification in partition 'aa' of type LIST 
create table fff (a char(1), b char(2), d char(3)) distributed by
(a) partition by list (b) (partition aa ); 


-- ERROR: Invalid use of RANGE boundary specification in partition
--   number 1 of type LIST
create table fff (a char(1), b char(2), d char(3)) distributed by (a)
partition by list (b) (start ('a') );


-- should work
create table fff (a char(1), b char(2), d char(3)) distributed by (a)
partition by list (b) (partition aa values ('2'));

drop table fff cascade;

-- Invalid use of RANGE boundary specification in partition "cc" of 
-- type HASH (at depth 2) & subpartition at one level has no template
create table ggg (a char(1), b char(2), d char(3), e int) distributed by (a)
partition by hash (b) subpartition by hash (d),
subpartition by hash (e) 
subpartition template ( subpartition ee, subpartition ff ) (
partition aa (subpartition cc, subpartition dd), partition bb
(subpartition cc start ('a') , subpartition dd) );

-- this is subtly wrong -- it defines 4 partitions
-- the problem is the comma before "end", which causes us to
-- generate 2 anonymous partitions.
-- This is an error: 
-- ERROR:  invalid use of mixed named and unnamed RANGE boundary specifications
create table ggg (a char(1), b int, d char(3))
distributed by (a)
partition by range (b)
(
partition aa start ('2007'), end ('2008'),
partition bb start ('2008'), end ('2009')
);

create table ggg (a char(1), b int)
distributed by (a)
partition by range(b)
(
partition aa start ('2007'), end ('2008')
);

drop table ggg cascade;

create table ggg (a char(1), b date, d char(3))
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01'),
partition bb start (date '2008-01-01') end (date '2009-01-01')
);


drop table ggg cascade;

-- don't allow nonconstant expressions, even simple ones...
create table ggg (a char(1), b numeric, d numeric)
distributed by (a)
partition by range (b,d)
(
partition aa start (2007,1) end (2008,2+2),
partition bb start (2008,2) end (2009,3)
);

-- composite key
create table ggg (a char(1), b numeric, d numeric)
distributed by (a)
partition by range (b,d)
(
partition aa start (2007,1) end (2008,2),
partition bb start (2008,2) end (2009,3)
);

-- demo starts here

-- nested subpartitions
create table ggg
 (a char(1),   b date,
  d char(3),  e numeric,
  f numeric,  g numeric,
  h numeric)
distributed by (a)
partition by hash(b)
partitions 2
subpartition by hash(d)
subpartitions 2,
subpartition by hash(e) subpartitions 2,
subpartition by hash(f) subpartitions 2,
subpartition by hash(g) subpartitions 2,
subpartition by hash(h) subpartitions 2;

drop table ggg cascade;


-- named, inline subpartitions
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

drop table ggg cascade;


-- subpartitions with templates
create table ggg (a char(1), b char(2), d char(3), e numeric)
distributed by (a)
partition by hash (b)
subpartition by hash (d) 
subpartition template ( 
subpartition cc,
subpartition dd
), 
subpartition by hash (e) 
subpartition template ( 
subpartition ee,
subpartition ff
) 
(
partition aa,
partition bb
);

drop table ggg cascade;


-- mixed inline subpartition declarations with templates
create table ggg (a char(1), b char(2), d char(3), e numeric)
distributed by (a)
partition by hash (b)
subpartition by hash (d) , 
subpartition by hash (e) 
subpartition template ( 
subpartition ee,
subpartition ff
) 
(
partition aa (subpartition cc, subpartition dd),
partition bb (subpartition cc, subpartition dd)
);

drop table ggg cascade;


-- basic list partition
create table ggg (a char(1), b char(2), d char(3))
distributed by (a)
partition by LIST (b)
(
partition aa values ('a', 'b', 'c', 'd'),
partition bb values ('e', 'f', 'g')
);

insert into ggg values ('x', 'a');
insert into ggg values ('x', 'b');
insert into ggg values ('x', 'c');
insert into ggg values ('x', 'd');
insert into ggg values ('x', 'e');
insert into ggg values ('x', 'f');
insert into ggg values ('x', 'g');
insert into ggg values ('x', 'a');
insert into ggg values ('x', 'b');
insert into ggg values ('x', 'c');
insert into ggg values ('x', 'd');
insert into ggg values ('x', 'e');
insert into ggg values ('x', 'f');
insert into ggg values ('x', 'g');

select * from ggg order by 1, 2;

-- ok
select * from ggg_1_prt_aa order by 1, 2;
select * from ggg_1_prt_bb order by 1, 2;

drop table ggg cascade;

-- documentation example - partition by list and range
CREATE TABLE rank (id int, rank int, year date, gender 
char(1)) DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01'),
start (date '2002-01-01'),
start (date '2003-01-01'),
start (date '2004-01-01'),
start (date '2005-01-01')
)
(
  partition boys values ('M'),
  partition girls values ('F')
);

insert into rank values (1, 1, date '2001-01-15', 'M');
insert into rank values (2, 1, date '2002-02-15', 'M');
insert into rank values (3, 1, date '2003-03-15', 'M');
insert into rank values (4, 1, date '2004-04-15', 'M');
insert into rank values (5, 1, date '2005-05-15', 'M');
insert into rank values (6, 1, date '2001-01-15', 'F');
insert into rank values (7, 1, date '2002-02-15', 'F');
insert into rank values (8, 1, date '2003-03-15', 'F');
insert into rank values (9, 1, date '2004-04-15', 'F');
insert into rank values (10, 1, date '2005-05-15', 'F');

select * from rank order by 1, 2, 3, 4;
select * from rank_1_prt_boys order by 1, 2, 3, 4;
select * from rank_1_prt_girls order by 1, 2, 3, 4;
select * from rank_1_prt_girls_2_prt_1 order by 1, 2, 3, 4;
select * from rank_1_prt_girls_2_prt_2 order by 1, 2, 3, 4;


drop table rank cascade;



-- range list hash combo, but subpartitions has no templates
create table ggg (a char(1), b date, d char(3), e numeric)
distributed by (a)
partition by range (b)
subpartition by list(d),
subpartition by hash(e) subpartitions 3
(
partition aa 
start  (date '2007-01-01') 
end (date '2008-01-01') 
       (subpartition dd values ('1', '2', '3'), 
	    subpartition ee values ('4', '5', '6')),
partition bb
start  (date '2008-01-01') 
end (date '2009-01-01') 
       (subpartition dd values ('1', '2', '3'),
	    subpartition ee values ('4', '5', '6'))
);

drop table ggg cascade;


-- demo ends here


-- LIST validation

-- duplicate partition name
CREATE TABLE rank (id int, rank int, year date, gender
char(1)) DISTRIBUTED BY (id, gender, year)
partition by list (gender)
(
  partition boys values ('M'),
  partition girls values ('a'),
  partition girls values ('b'),
  partition girls values ('c'),
  partition girls values ('d'),
  partition girls values ('e'),
  partition bob values ('M')
);

-- RANGE validation

-- legal if end of aa not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01'),
partition bb start (date '2008-01-01') end (date '2009-01-01') 
every (interval '10 days'));

drop table ggg cascade;


-- bad - legal if end of aa not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01') inclusive,
partition bb start (date '2008-01-01') end (date '2009-01-01') 
every (interval '10 days'));

drop table ggg cascade;

-- legal because start of bb not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01') inclusive,
partition bb start (date '2008-01-01') exclusive end (date '2009-01-01') 
every (interval '10 days'));

drop table ggg cascade;

-- legal if end of aa not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition bb start (date '2008-01-01') end (date '2009-01-01'),
partition aa start (date '2007-01-01') end (date '2008-01-01')
);

drop table ggg cascade;

-- bad - legal if end of aa not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition bb start (date '2008-01-01') end (date '2009-01-01'),
partition aa start (date '2007-01-01') end (date '2008-01-01') inclusive
);

drop table ggg cascade;

-- legal because start of bb not inclusive
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition bb start (date '2008-01-01') exclusive end (date '2009-01-01'),
partition aa start (date '2007-01-01') end (date '2008-01-01') inclusive
);

drop table ggg cascade;

-- validate aa - start greater than end
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition bb start (date '2008-01-01') end (date '2009-01-01'),
partition aa start (date '2007-01-01') end (date '2006-01-01')
);

drop table ggg cascade;

-- formerly we could not set end of first partition because next is before
-- but we can sort them now so this is legal.
create table ggg (a char(1), b date, d char(3)) 
distributed by (a)
partition by range (b)
(
partition bb start (date '2008-01-01') ,
partition aa start (date '2007-01-01') 
);

drop table ggg cascade;

-- test cross type coercion
-- int -> char(N)
create table ggg (i int, a char(1))
distributed by (i)
partition by list(a)
(partition aa values(1, 2));

drop table ggg cascade;

-- int -> numeric
create table ggg (i int, n numeric(20, 2))
distributed by (i)
partition by list(n)
(partition aa values(1.22, 4.1));
drop table ggg cascade;

-- EVERY

--  the documentation example, rewritten with EVERY in a template
CREATE TABLE rank (id int,
rank int, year date, gender char(1))
DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01')
end (date '2006-01-01') every (interval '1 year')) (
partition boys values ('M'),
partition girls values ('F')
);


insert into rank values (1, 1, date '2001-01-15', 'M');
insert into rank values (2, 1, date '2002-02-15', 'M');
insert into rank values (3, 1, date '2003-03-15', 'M');
insert into rank values (4, 1, date '2004-04-15', 'M');
insert into rank values (5, 1, date '2005-05-15', 'M');
insert into rank values (6, 1, date '2001-01-15', 'F');
insert into rank values (7, 1, date '2002-02-15', 'F');
insert into rank values (8, 1, date '2003-03-15', 'F');
insert into rank values (9, 1, date '2004-04-15', 'F');
insert into rank values (10, 1, date '2005-05-15', 'F');


select * from rank order by 1, 2, 3, 4;
select * from rank_1_prt_boys order by 1, 2, 3, 4;
select * from rank_1_prt_girls order by 1, 2, 3, 4;
select * from rank_1_prt_girls_2_prt_1 order by 1, 2, 3, 4;
select * from rank_1_prt_girls_2_prt_2 order by 1, 2, 3, 4;

drop table rank cascade;

-- integer ranges work too
create table ggg (id integer, a integer)
distributed by (id)
partition by range (a)
(start (1) end (10) every (1));

insert into ggg values (1, 1);
insert into ggg values (2, 2);
insert into ggg values (3, 3);
insert into ggg values (4, 4);
insert into ggg values (5, 5);
insert into ggg values (6, 6);
insert into ggg values (7, 7);
insert into ggg values (8, 8);
insert into ggg values (9, 9);
insert into ggg values (10, 10);

select * from ggg order by 1, 2;

select * from ggg_1_prt_1 order by 1, 2;
select * from ggg_1_prt_2 order by 1, 2;
select * from ggg_1_prt_3 order by 1, 2;
select * from ggg_1_prt_4 order by 1, 2;

drop table ggg cascade;

-- hash tests

create table ggg (a char(1), b varchar(2), d varchar(2))
distributed by (a)
partition by hash(b)
partitions 3
(partition a, partition b, partition c);

insert into ggg values (1,1,1);
insert into ggg values (2,2,1);
insert into ggg values (1,3,1);
insert into ggg values (2,2,3);
insert into ggg values (1,4,5);
insert into ggg values (2,2,4);
insert into ggg values (1,5,6);
insert into ggg values (2,7,3);
insert into ggg values (1,'a','b');
insert into ggg values (2,'c','c');

select * from ggg order by 1, 2, 3;

--select * from ggg_1_prt_a order by 1, 2, 3;
--select * from ggg_1_prt_b order by 1, 2, 3;
--select * from ggg_1_prt_c order by 1, 2, 3;

drop table ggg cascade;

-- use multiple cols
create table ggg (a char(1), b varchar(2), d varchar(2))
distributed by (a)
partition by hash(b,d)
partitions 3
(partition a, partition b, partition c);

-- append only tests
create table foz (i int, d date) with (appendonly = true) distributed by (i)
partition by range (d) (start (date '2001-01-01') end (date '2005-01-01')
every(interval '1 year'));
insert into foz select i, '2001-01-01'::date + ('1 day'::interval * i) from
generate_series(1, 1000) i;
select count(*) from foz;
select count(*) from foz_1_prt_1;

select min(d), max(d) from foz;
select min(d), max(d) from foz_1_prt_1;
select min(d), max(d) from foz_1_prt_2;
select min(d), max(d) from foz_1_prt_3;
select min(d), max(d) from foz_1_prt_4;


drop table foz cascade;


-- complain if create table as select (CTAS)

CREATE TABLE rank1 (id int,
rank int, year date, gender char(1));

create table rank2 as select * from rank1
DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01')
end (date '2006-01-01') every (interval '1 year')) (
partition boys values ('M'),
partition girls values ('F')
);

-- like is ok

create table rank2 (like rank1)
DISTRIBUTED BY (id, gender, year)
partition by list (gender)
subpartition by range (year)
subpartition template (
start (date '2001-01-01')
end (date '2006-01-01') every (interval '1 year')) (
partition boys values ('M'),
partition girls values ('F')
);

drop table rank1 cascade;
drop table rank2 cascade;


-- alter table testing

create table hhh (a char(1), b date, d char(3))
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01') 
    with (appendonly=true),
partition bb start (date '2008-01-01') end (date '2009-01-01')
    with (appendonly=true)
);

-- already exists
alter table hhh add partition aa;

-- no partition spec
alter table hhh add partition cc;

-- overlaps
alter table hhh add partition cc start ('2008-01-01') end ('2010-01-01');
alter table hhh add partition cc end ('2008-01-01');

-- reversed (start > end)
alter table hhh add partition cc start ('2010-01-01') end ('2009-01-01');

-- works
--alter table hhh add partition cc start ('2009-01-01') end ('2010-01-01');
alter table hhh add partition cc end ('2010-01-01');

-- works - anonymous partition MPP-3350
alter table hhh add partition end ('2010-02-01');

-- MPP-3607 - ADD PARTITION with open intervals
create table no_end1 (aa int, bb int) partition by range (bb)
(partition foo start(3));

-- fail overlap
alter table no_end1 add partition baz end (4);

-- fail overlap (because prior partition has no end)
alter table no_end1 add partition baz start (5);

-- ok (terminates on foo start)
alter table no_end1 add partition baz start (2);

-- ok (because ends before baz start)
alter table no_end1 add partition baz2 end (1);

create table no_start1 (aa int, bb int) partition by range (bb)
(partition foo end(3));

-- fail overlap (because next partition has no start)
alter table no_start1 add partition baz start (2);

-- fail overlap (because next partition has no start)
alter table no_start1 add partition baz end (1);

-- ok (starts on foo end)
alter table no_start1 add partition baz end (4);

-- ok (because starts after baz end)
alter table no_start1 add partition baz2 start (5);

select tablename, partitionlevel, parentpartitiontablename,
partitionname, partitionrank, partitionboundary from pg_partitions
where tablename = 'no_start1' or tablename = 'no_end1' 
order by tablename, partitionrank;

drop table no_end1;
drop table no_start1;

-- hash partitions cannot have default partitions
create table jjj (aa int, bb int) 
partition by hash(bb) 
(partition j1, partition j2);

alter table jjj add default partition;

drop table jjj cascade;

-- default partitions cannot have boundary specifications
create table jjj (aa int, bb date) 
partition by range(bb) 
(partition j1 end (date '2008-01-01'), 
partition j2 end (date '2009-01-01'));

-- must have a name
alter table jjj add default partition;
alter table jjj add default partition for (rank(1));
-- cannot have boundary spec
alter table jjj add default partition j3 end (date '2010-01-01');

drop table jjj cascade;

-- only one default partition
create table jjj (aa int, bb date) 
partition by range(bb) 
(partition j1 end (date '2008-01-01'), 
partition j2 end (date '2009-01-01'),
default partition j3);

alter table jjj add default partition j3 ;
alter table jjj add default partition j4 ;

-- cannot add if have default, must split
alter table jjj add partition j5 end (date '2010-01-01');

drop table jjj cascade;

alter table hhh alter partition cc set tablespace foo_p;

alter table hhh alter partition aa set tablespace foo_p;

alter table hhh coalesce partition cc;

alter table hhh coalesce partition aa;

alter table hhh drop partition cc;

alter table hhh drop partition cc cascade;

alter table hhh drop partition cc restrict;

alter table hhh drop partition if exists cc;

-- fail (mpp-3265)
alter table hhh drop partition for (rank(0));
alter table hhh drop partition for (rank(-55));
alter table hhh drop partition for ('2001-01-01');


create table hhh_r1 (a char(1), b date, d char(3))
distributed by (a)
partition by range (b)
(
partition aa start (date '2007-01-01') end (date '2008-01-01') 
             every (interval '1 month')
);

create table hhh_l1 (a char(1), b date, d char(3))
distributed by (a)
partition by list (b)
(
partition aa values ('2007-01-01'),
partition bb values ('2008-01-01'),
partition cc values ('2009-01-01') 
);

-- must have name or value for list partition
alter table hhh_l1 drop partition;
alter table hhh_l1 drop partition aa;
alter table hhh_l1 drop partition for ('2008-01-01');

-- if not specified, drop first range partition...
alter table hhh_r1 drop partition for ('2007-04-01');
alter table hhh_r1 drop partition;
alter table hhh_r1 drop partition;
alter table hhh_r1 drop partition;
alter table hhh_r1 drop partition;
alter table hhh_r1 drop partition;

-- more add partition tests

-- start before first partition (fail because start equal end)
alter table hhh_r1 add partition zaa start ('2007-07-01');
-- start before first partition (ok)
alter table hhh_r1 add partition zaa start ('2007-06-01');
-- start > last (fail because start equal end)
alter table hhh_r1 add partition bb start ('2008-01-01') end ('2008-01-01') ;
-- start > last (ok)
alter table hhh_r1 add partition bb start ('2008-01-01') 
end ('2008-02-01') inclusive;
-- start > last (fail because start == last end inclusive)
alter table hhh_r1 add partition cc start ('2008-02-01') end ('2008-03-01') ;
-- start > last (ok [and leave a gap])
alter table hhh_r1 add partition cc start ('2008-04-01') end ('2008-05-01') ;
-- overlap (fail)
alter table hhh_r1 add partition dd start ('2008-01-01') end ('2008-05-01') ;
-- new partition in "gap" (ok)
alter table hhh_r1 add partition dd start ('2008-03-01') end ('2008-04-01') ;
-- overlap all partitions (fail)
alter table hhh_r1 add partition ee start ('2006-01-01') end ('2009-01-01') ;
-- start before first partition (fail because end in "gap" [and overlaps])
alter table hhh_r1 add partition yaa start ('2007-05-01') end ('2007-07-01');
-- start before first partition (fail )
alter table hhh_r1 add partition yaa start ('2007-05-01') 
end ('2007-10-01') inclusive;
-- start before first partition (fail because end overlaps)
alter table hhh_r1 add partition yaa start ('2007-05-01') 
end ('2007-10-01') exclusive;

drop table hhh_r1 cascade;
drop table hhh_l1 cascade;

-- SPLIT tests
-- basic sanity tests. All should pass.
create table k (i int) partition by range(i) (start(1) end(10) every(2), 
default partition mydef);
insert into k select i from generate_series(1, 100) i;
alter table k split partition mydef at (20) into (partition mydef, 
partition foo);
drop table k;

create table j (i int) partition by list(i) (partition a values(1, 2, 3, 4),
partition b values(5, 6, 7, 8));
insert into j select i from generate_series(1, 8) i;
alter table j split partition for(1) at (2, 3) into (partition fa, partition
fb);
select * from j_1_prt_fa;
select * from j_1_prt_fb;
alter table j split partition for(5) at (6);
select * from j;
-- should fail
alter table j split partition for (1) at (100);
drop table j;
create table k (i int) partition by range(i) (start(1) end(10) every(2), 
default partition mydef);
-- should fail
alter table k split default partition start(30) end (300) into (partition mydef, partition mydef);
alter table k split partition for(3) at (20);
drop table k;
-- should work
create table k (i int) partition by range(i) (start(1) end(10) every(2), 
default partition mydef);
insert into k select i from generate_series(1, 30) i;
alter table k split default partition start(15) end(20) into
(partition mydef, partition foo);
select * from k_1_prt_foo;
alter table k split default partition start(22) exclusive end(25) inclusive
into (partition bar, partition mydef);
select * from k_1_prt_bar;
alter table k split partition bar at (23) into (partition baz, partition foz);
select partitiontablename,partitionposition,partitionrangestart,
       partitionrangeend from pg_partitions where tablename = 'k'
	   order by partitionposition;
drop table k;
-- Test errors for default handling
create table k (i int) partition by range(i) (start(1) end(2), 
default partition mydef);
alter table k split partition mydef at (25) into (partition foo, partition
mydef);
drop table k;
-- check that when we split a default, the INTO clause must named the default
create table k (i date) partition by range(i) (start('2008-01-01')
end('2009-01-01') every(interval '1 month'), default partition default_part);
alter table k split default partition start ('2009-01-01') end ('2009-02-01')
into (partition aa, partition nodate);
alter table k split default partition start ('2009-01-01') end ('2009-02-01')
into (partition aa, partition default_part);
-- check that it works without INTO
alter table k split default partition start ('2009-02-01') end ('2009-03-01');
drop table k;
-- List too
create table k (i int) partition by list(i) (partition a values(1, 2),
partition b values(3, 4), default partition mydef);
alter table k split partition mydef at (5) into (partition foo, partition bar);
alter table k split partition mydef at (5) into (partition foo, partition mydef);
alter table k split partition mydef at (10);
drop table k;

-- For LIST, make sure that we reject AT() clauses which match all parameters
create table j (i int) partition by list(i) (partition a values(1, 2, 3, 4),
 partition b values(5, 6, 7, 8));
alter table j split partition for(1) at (1,2) into (partition fa, partition fb);
alter table j split partition for(1) at (1,2) 
into (partition f1a, partition f1b); -- This has partition rules that overlaps
drop table j;

-- Check that we can split LIST partitions that have a default partition
create table j (i int) partition by list(i) (partition a values(1, 2, 3, 4),
partition b values(5, 6, 7, 8), default partition default_part);
alter table j split partition for(1) at (1,2) into (partition f1a, partition
f1b);
drop table j;
-- Make sure range can too
create table j (i int) partition by range(i) (partition a start(1) end(10),
default partition default_part);
alter table j split partition for(1) at (5) into (partition f1a, partition f1b);
drop table j;

-- GPSQL-285 -- GPSQL-277
create table pt_table (a int, b int, c int, d int) distributed by (a) partition by range(b) (default partition others, start(1) end(5) every(1));
select partitionname, partitiontablename from pg_partitions where tablename='pt_table' order by partitionname;
insert into pt_table values(1,1,1,1);
insert into pt_table values(1,2,1,1);
select * from pt_table_1_prt_2;
select * from pt_table_1_prt_3;
insert into pt_table_1_prt_2 values(1,1,2,2);
select * from pt_table_1_prt_2 order by d;
insert into pt_table_1_prt_others values(1,1,1,1);
select * from pt_table order by b,d;
select * from pt_table_1_prt_2 order by b,d;
select * from pt_table_1_prt_others order by b,d;

drop table pt_table;

-- GPSQL-278
-- GPSQL-278 - sanity
drop table if exists pt_check;
create table pt_check
(
distcol int,
ptcol date,
col1 text,
CONSTRAINT distcol_chk CHECK (distcol > 0)
)
distributed by (distcol)
partition by range (ptcol)
(
default partition defpt,
start (date '2010-01-01') inclusive
end (date '2010-12-31') inclusive
every (interval '1 month')
);
--Insert 2 records to partitioned table.
INSERT INTO pt_check values (1, '2010-01-10'::date, 'part 1');
INSERT INTO pt_check values (2, '2010-01-21'::date, 'part 2');
select * from pt_check order by col1;
--Split partition '2010-01-10' into 2 parts (Jan 1-15 and Jan 16-31).
ALTER TABLE pt_check SPLIT PARTITION FOR ('2010-01-01')
AT ('2010-01-16')
INTO (PARTITION jan1thru15, PARTITION jan16thru31);
-- Verify split result.
Select * from pt_check_1_prt_jan1thru15 order by col1;
Select * from pt_check_1_prt_jan16thru31 order by col1;

-- GPSQL-278 - default partitions
drop table if exists pt_check;
create table pt_check
(
distcol int,
ptcol date,
col1 text,
CONSTRAINT distcol_chk CHECK (distcol > 0)
)
distributed by (distcol)
partition by range (ptcol)
(
default partition defpt,
start (date '2010-01-01') inclusive
end (date '2010-12-31') inclusive
every (interval '1 month')
);
--Insert 2 records to partitioned table.
INSERT INTO pt_check values (1, '2011-01-10'::date, 'part 1');
INSERT INTO pt_check values (2, '2011-02-21'::date, 'part 2');
select * from pt_check order by col1;
--Split default partition into 2 parts (Jan 2011 and default).
ALTER TABLE pt_check SPLIT DEFAULT PARTITION
	START ('2011-01-01') INCLUSIVE END ('2011-02-01') EXCLUSIVE
	INTO (PARTITION jan2011, DEFAULT PARTITION);
-- Verify split result.
select * from pt_check_1_prt_jan2011 order by col1;
select * from pt_check_1_prt_defpt order by col1;

-- GPSQL-278 - default partitions
drop table if exists pt_check;
create table pt_check
(
distcol int,
ptcol date,
col1 text,
CONSTRAINT distcol_chk CHECK (distcol > 0)
)
distributed by (distcol)
partition by range (ptcol)
(
default partition defpt,
start (date '2010-01-01') inclusive
end (date '2010-12-31') inclusive
every (interval '1 month')
);
--Insert 2 records to partitioned table.
INSERT INTO pt_check values (1, '2011-01-10'::date, 'part 1');
INSERT INTO pt_check values (2, '2011-02-21'::date, 'part 2');
select * from pt_check order by col1;
--Split default partition into 2 parts (Jan 2011 and default).
ALTER TABLE pt_check SPLIT DEFAULT PARTITION
	START ('2011-01-01') INCLUSIVE END ('2011-02-01') EXCLUSIVE
	INTO (DEFAULT PARTITION, PARTITION jan2011);
-- Verify split result.
select * from pt_check_1_prt_jan2011 order by col1;
select * from pt_check_1_prt_defpt order by col1;
--- allow the creation of multi-level partition tables with templates
CREATE TABLE sales (id int, date date, amt decimal(10,2), region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
              SUBPARTITION BY LIST (region)
                SUBPARTITION TEMPLATE (
                  SUBPARTITION usa VALUES ('usa'),
                  SUBPARTITION europe VALUES ('europe'),
                  SUBPARTITION asia VALUES ('asia'))
( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE , 
  PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
  PARTITION Mar08 START (date '2008-03-01') INCLUSIVE ,
  PARTITION Apr08 START (date '2008-04-01') INCLUSIVE ,
  PARTITION May08 START (date '2008-05-01') INCLUSIVE ,
  PARTITION Jun08 START (date '2008-06-01') INCLUSIVE ,
  PARTITION Jul08 START (date '2008-07-01') INCLUSIVE ,
  PARTITION Aug08 START (date '2008-08-01') INCLUSIVE ,
  PARTITION Sep08 START (date '2008-09-01') INCLUSIVE ,
  PARTITION Oct08 START (date '2008-10-01') INCLUSIVE ,
  PARTITION Nov08 START (date '2008-11-01') INCLUSIVE ,
  PARTITION Dec08 START (date '2008-12-01') INCLUSIVE 
                  END (date '2009-01-01') EXCLUSIVE )
;
drop table sales;
--- allow the creation of multi-level partition tables with templates
CREATE TABLE MPP10223pk
(
rnc VARCHAR(100),
wbts VARCHAR(100),
axc VARCHAR(100),
vptt VARCHAR(100),
vcct VARCHAR(100),
agg_level CHAR(5),
period_start_time TIMESTAMP WITH TIME ZONE,
load_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
interval INTEGER,
totcellsegress double precision,
totcellsingress double precision
)
 
DISTRIBUTED BY (rnc,wbts,axc,vptt,vcct)
 
PARTITION BY LIST (AGG_LEVEL)
  SUBPARTITION BY RANGE (PERIOD_START_TIME)
  SUBPARTITION TEMPLATE
    (
       SUBPARTITION P_FUTURE  START (date '2001-01-01') INCLUSIVE,
       SUBPARTITION P_ENDPART START (date '2999-12-30') INCLUSIVE
                              END (date '2999-12-31') EXCLUSIVE
    )
(
  PARTITION min15part  VALUES ('15min'),
  PARTITION hourpart   VALUES ('hour'),
  PARTITION daypart    VALUES ('day')
);

drop table MPP10223pk;

--- disallow the creation of multi-level partition tables without templates
CREATE TABLE MPP10223pk
(
rnc VARCHAR(100),
wbts VARCHAR(100),
axc VARCHAR(100),
vptt VARCHAR(100),
vcct VARCHAR(100),
agg_level CHAR(5),
period_start_time TIMESTAMP WITH TIME ZONE,
load_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
interval INTEGER,
totcellsegress double precision,
totcellsingress double precision
)
 
DISTRIBUTED BY (rnc,wbts,axc,vptt,vcct)
 
PARTITION BY LIST (AGG_LEVEL)
  SUBPARTITION BY RANGE (PERIOD_START_TIME)
(
  PARTITION min15part  VALUES ('15min')
    (
       SUBPARTITION P_FUTURE  START (date '2001-01-01') INCLUSIVE,
       SUBPARTITION P_ENDPART START (date '2999-12-30') INCLUSIVE
                              END (date '2999-12-31') EXCLUSIVE
    ),
  PARTITION hourpart   VALUES ('hour')
    (
               SUBPARTITION P20100622 START (date '2010-06-22') INCLUSIVE,
               SUBPARTITION P20100623 START (date '2010-06-23') INCLUSIVE,
               SUBPARTITION P20100624 START (date '2010-06-24') INCLUSIVE,
               SUBPARTITION P20100625 START (date '2010-06-25') INCLUSIVE,
               SUBPARTITION P20100626 START (date '2010-06-26') INCLUSIVE,
               SUBPARTITION P_FUTURE  START (date '2001-01-01') INCLUSIVE,
               SUBPARTITION P_ENDPART START (date '2999-12-30') INCLUSIVE
                                      END (date '2999-12-31') EXCLUSIVE
    ),
  PARTITION daypart    VALUES ('day')
    (
               SUBPARTITION P20100622 START (date '2010-06-22') INCLUSIVE,
               SUBPARTITION P20100623 START (date '2010-06-23') INCLUSIVE,
               SUBPARTITION P20100624 START (date '2010-06-24') INCLUSIVE,
               SUBPARTITION P20100625 START (date '2010-06-25') INCLUSIVE,
               SUBPARTITION P20100626 START (date '2010-06-26') INCLUSIVE,
               SUBPARTITION P_FUTURE  START (date '2001-01-01') INCLUSIVE,
               SUBPARTITION P_ENDPART START (date '2999-12-30') INCLUSIVE
                                      END (date '2999-12-31') EXCLUSIVE
    )
);


--- disallow the creation of multi-level partition tables without templates
CREATE TABLE rank3 (id int, rank int,
year date, gender char(1),
usstate char(2))
DISTRIBUTED BY (id, gender, year, usstate)
partition by list (gender)
subpartition by range (year),
subpartition by list (usstate)
(
  partition boys values ('M') 
(
subpartition jan01 start (date '2001-01-01') 
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan02 start (date '2002-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan03 start (date '2003-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan04 start (date '2004-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan05 start (date '2005-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
)
)
,
  partition girls values ('F')
(
subpartition jan01 start (date '2001-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan02 start (date '2002-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan03 start (date '2003-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan04 start (date '2004-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan05 start (date '2005-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
)
)
);

-- Tests for sort operator before insert with AO and PARQUET tables (HAWQ-404)
-- A GUC's value is set to less than the number of partitions in the example table, so that sort is activated.

DROP TABLE IF EXISTS ch_sort_src, ch_sort_aodest, ch_sort_pqdest, ch_sort_aopqdest, ch_sort__pq_table;

SET optimizer_parts_to_force_sort_on_insert = 5;

CREATE TABLE ch_sort_src (id int, year int, month int, day int, region text)
DISTRIBUTED BY (month); 
INSERT INTO ch_sort_src select i, 2000 + i, i % 12, (2*i) % 30, i::text from generate_series(0, 99) i; 

-- AO partitioned table
CREATE TABLE ch_sort_aodest (id int, year int, month int, day int, region text)
WITH (APPENDONLY=TRUE)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( 
    START (2002) END (2010) EVERY (1),
    DEFAULT PARTITION outlying_years
);

-- PARQUET partitioned table
CREATE TABLE ch_sort_pqdest (id int, year int, month int, day int, region text)
WITH (APPENDONLY=TRUE, ORIENTATION = PARQUET)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( 
    START (2002) END (2010) EVERY (1),
    DEFAULT PARTITION outlying_years
);

-- AO/PARQUET mixed table
CREATE TABLE ch_sort_aopqdest (id int, year int, month int, day int, region text)
WITH (APPENDONLY=TRUE)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
( 
    START (2002) END (2010) EVERY (1),
    DEFAULT PARTITION outlying_years
);

CREATE TABLE ch_sort__pq_table (id int, year int, month int, day int, region text)
WITH (APPENDONLY=TRUE, ORIENTATION = PARQUET)
DISTRIBUTED BY (id);

ALTER TABLE ch_sort_aopqdest
EXCHANGE PARTITION FOR(2006)
WITH TABLE ch_sort__pq_table;


-- Test that inserts work
INSERT INTO ch_sort_aodest SELECT * FROM ch_sort_src;
SELECT COUNT(*) FROM ch_sort_aodest;
SELECT COUNT(*) FROM ch_sort_aodest_1_prt_6;
SELECT COUNT(*) FROM ch_sort_aodest_1_prt_outlying_years;

INSERT INTO ch_sort_pqdest SELECT * FROM ch_sort_src;
SELECT COUNT(*) FROM ch_sort_pqdest;
SELECT COUNT(*) FROM ch_sort_pqdest_1_prt_6;
SELECT COUNT(*) FROM ch_sort_pqdest_1_prt_outlying_years;

INSERT INTO ch_sort_aopqdest SELECT * FROM ch_sort_src;
SELECT COUNT(*) FROM ch_sort_aopqdest;
SELECT COUNT(*) FROM ch_sort_aopqdest_1_prt_6;
SELECT COUNT(*) FROM ch_sort_aopqdest_1_prt_outlying_years;

RESET optimizer_parts_to_force_sort_on_insert;

drop table if exists sales2;
CREATE TABLE sales2 (id int, date date, amt decimal(10,2))    
DISTRIBUTED BY (id) 
PARTITION BY RANGE (date) 
( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE ,
PARTITION Feb08 START (date '2008-02-01') INCLUSIVE ,
PARTITION Mar08 START (date '2008-03-01') INCLUSIVE , 
PARTITION Apr08 START (date '2008-04-01') INCLUSIVE 
END (date '2008-05-01') EXCLUSIVE );


insert into sales2 values(1,'2008-01-03',1.2);
select count(*) from sales2;

alter table sales2 add partition START (date '2008-05-01') INCLUSIVE END (date '2008-06-01') EXCLUSIVE;
insert into sales2 values(1,'2008-05-03',1.2);
select count(*) from sales2;

set default_hash_table_bucket_number = 99;

alter table sales2 add partition START (date '2008-06-01') INCLUSIVE END (date '2008-07-01') EXCLUSIVE;
insert into sales2 values(1,'2008-06-03',1.2);
select count(*) from sales2;

drop table sales2;


