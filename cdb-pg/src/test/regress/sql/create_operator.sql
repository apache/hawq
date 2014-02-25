--
-- CREATE_OPERATOR
--

drop table if exists operator_test;
create table operator_test(a int, b int);
insert into operator_test select i, i from generate_series(1, 10)i;

drop function if exists int_add(int, int);
create or replace function int_add(a int, b int) returns int as $$
declare
begin
  return a + b;
end;
$$ language plpgsql immutable strict;

create operator +% (
  leftarg = int, rightarg = int, procedure = int_add
);

select array_agg(b order by b +% a) from operator_test;

drop operator if exists public.=(int4, int4);
create operator public.= (
  leftarg = int4, rightarg = int4, procedure = int4eq
  ,commutator = operator(public.=)
  ,negator = <>
  ,restrict = eqsel
  ,join = eqjoinsel
  ,hashes
  ,merges
  ,sort1 = <, sort2 = <
  ,ltcmp = <, gtcmp = >
);

select t1.a, t2.b from operator_test t1
 inner join operator_test t2 on t1.b operator(public.=) t2.a;


-- Test comments
COMMENT ON OPERATOR ###### (int4, NONE) IS 'bad right unary';


--
-- CREATE_CAST
--

create type atype as (a int, b int);
create type btype as (b int, a int);
create cast (atype as btype) without function;
drop table if exists cast_table;
create table cast_table(a int, at atype);
insert into cast_table select i, row(i, i+i)::atype from generate_series(1, 10)i;
select a, at::btype from cast_table;

create type ctype as (a int, b int);
create or replace function int8_ctype(a int8) returns ctype as $$
declare
  ret ctype;
begin
  ret.a := ((a >> 32) & x'ffffffff'::int8)::int4;
  ret.b := (a & x'ffffffff'::int8)::int4;
  return ret;
end;
$$ language plpgsql immutable strict;
drop cast if exists (int8 as ctype);
create cast (int8 as ctype) with function int8_ctype(int8);
select a::int8::ctype, a from cast_table;

comment on cast (int8 as ctype) is 'decode int8 into two ints';

create or replace function my_int4gt(a int, b int) returns bool as $$
declare
begin
  return a > b;
end;
$$ language plpgsql immutable strict;

--
-- CREATE OPERATOR CLASS
--
drop operator if exists public.>(int4, int4);
create operator public.>(
  leftarg = int4, rightarg = int4, procedure = my_int4gt
);
create operator class my_int_opclass for type int using btree as
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 public.>,
  function 1 btint4cmp(int, int)
;

select a, b from operator_test order by b using operator(public.>);
