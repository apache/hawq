-- Objective: test aggregate derived window functions in HAWQ.
-- Aggregate derived window functions are nothing but user defined
-- aggregates when used with "OVER()" clause.  Refer to GPSQL-1418.

-- Begin EMA (copied from attachment to MPP-14845)
--
-- Definition of ema -- exponential moving average
-- 
-- Given a sequence of numbers 
--     V = (V_0, V_1, ... , V_n-1)
-- and a real number smoothing factor 
--     X such that 0 < X <= 1
-- then 
--     ema(V) = E =  (E_0, E_1, ... , E_n-1)
-- and
--     E_0 = V_0
--     
--     E_i = E_i-1 * (1-X) + V_i * X
--         = E_i-1 + (V_i - E_i-1) * X
--  
-- Here the sequence V is represented by table ema_test ordered by k.

drop type if exists ema_type cascade;
drop table if exists ema_test cascade;

create type ema_type as (x float, e float);

create function ema_adv(t ema_type, v float, x float) 
    returns ema_type 
    as $$
        begin
            if t.e is null then
                t.e = v;
                t.x = x;
            else
                if t.x != x then
                    raise exception 'ema smoothing x may not vary';
                end if;
                t.e = t.e + (v - t.e) * t.x;
            end if;
            return t;
        end;
    $$ language plpgsql;

create function ema_fin(t ema_type)
    returns float
    as $$
       begin
           return t.e;
       end;
    $$ language plpgsql;

-- Work around for MPP-14845: define a placebo prefunc.  This should
-- never be called.
create function ema_pre(s1 ema_type, s2 ema_type)
    returns ema_type
    as $$
       select '(,)'::ema_type;
    $$ language sql;

create aggregate ema(float, float) (
    sfunc = ema_adv,
    stype = ema_type,
    finalfunc = ema_fin,
    prefunc = ema_pre,
    initcond = '(,)'
    );

create table ema_test 
    ( k int, v float )
    distributed by (k);

insert into ema_test
    select i, 4*(22/7::float) + 10.0*(1+cos(radians(i*5)))
    from generate_series(0,19) i(i);

select 
    k, v, 
    ema(v, 0.9) over (order by k rows between unbounded preceding and current row) 
from ema_test
order by k;

select 
    k, v, 
    ema(v, 0.9) over (order by k) 
from ema_test
order by k;

-- End EMA (MPP-14845)

--
-- Aggregate derived equivalent of "lag()" window function.
--
create function mylag_transfn(st int[], val int, lag int)
    returns int[]
    as $$
       declare
           local_st int[] := st;
           local_lag int := lag;
       begin
           if local_st is null then
               local_st := '{}'::int[];
               while local_lag >= 0
               loop
                   select array_append(local_st, null::int) into local_st;
                   local_lag := local_lag - 1;
               end loop;
           end if;
           return array_append(local_st[2:lag+1], val);
       end;
    $$ language plpgsql;

create function mylag_finalfn(st int[])
    returns int
    as $$
       begin
           return st[1];
       end;
    $$ language plpgsql;

create function mylag_prefn(st1 int[], st2 int[])
    returns int[]
    as $$
       select '{}'::int[];
    $$ language sql;

create aggregate mylag(int, int) (
    sfunc = mylag_transfn,
    stype = int[],
    finalfunc = mylag_finalfn,
    prefunc = mylag_prefn,
    initcond = '{null,null}'
    );

-- This will be executed only on master, not on segments.
select i, mylag(i, 2) over (order by i) from generate_series(1,10)i;

create table t1 (a int, b int) distributed by (a);

insert into t1 select i%3, 22*i/7 from generate_series(0,10)i;

select a,b,mylag(b,1) over (order by b) from t1;

--
-- Misc tests - cover different ways of defining a window.
--
CREATE AGGREGATE mysum (int) (
  STYPE = bigint,
  SFUNC = int4_sum,
  prefunc = int8pl
);
SELECT a,b,mysum(b) over (order by b) FROM t1;
SELECT a,b,mysum(b) over (w) FROM t1 WINDOW w as (); -- mvd 1,2->3
SELECT a,b,mysum(b) over (w) FROM t1 WINDOW w as (PARTITION BY a); -- mvd 1,2->3
SELECT a,b,mysum(b) over (w) FROM t1 WINDOW w as (ORDER BY b); -- mvd 1,2->3
SELECT a,b,mysum(b) over (w) FROM t1 WINDOW w as (PARTITION BY a ORDER BY b); -- mvd 1,2->3
SELECT a,b,mysum(b) over (w) FROM t1 WINDOW w as
  (PARTITION BY a ORDER BY b ROWS BETWEEN 1 preceding and current row) order by a,b; -- mvd 1,2->3

select a,b,mylag(b, 1) over (partition by a order by b) from t1 order by a,b; -- mvd 1,2->3

