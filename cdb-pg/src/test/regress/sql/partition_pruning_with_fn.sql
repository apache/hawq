--
-- See MPP-6861
--
CREATE TABLE ds_4
(
  month_id character varying(6),
  cust_group_acc numeric(10),
  mobile_no character varying(10),
  source character varying(12),
  vas_group numeric(10),
  vas_type numeric(10),
  count_vas integer,
  amt_vas numeric(10,2),
  network_type character varying(3),
  execution_id integer
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (cust_group_acc, mobile_no)
PARTITION BY LIST(month_id)
          (
          PARTITION p200800 VALUES('200800'),
          PARTITION p200801 VALUES('200801'),
          PARTITION p200802 VALUES('200802'),
          PARTITION p200803 VALUES('200803')
);

-- this is the case that worked before MPP-6861
explain select * from ds_4 where month_id = '200800';


-- now we can evaluate this function at planning/prune time
explain select * from ds_4 where month_id::int = 200800;

-- this will be satisfied by 200800
explain select * from ds_4 where month_id::int - 801 < 200000;

-- test OR case -- should NOT get pruning
explain select * from ds_4 where month_id::int - 801 < 200000 OR count_vas > 10;

-- test AND case -- should still get pruning
explain select * from ds_4 where month_id::int - 801 < 200000 AND count_vas > 10;

-- test expression case : should get pruning
explain select * from ds_4 where case when month_id = '200800' then 100 else 2 end = 100;

-- test expression case : should get pruning
explain select * from ds_4 where case when month_id = '200800' then NULL else 2 end IS NULL;

-- should still get pruning here -- count_vas is only used in the path for month id = 200800
explain select * from ds_4 where case when month_id::int = 200800 then count_vas else 2 end IS NULL;

-- do one that matches a couple partitions
explain select * from ds_4 where month_id::int in (200801, 1,55,6,6,6,6,66,565,65,65,200803);

-- cleanup
drop table ds_4;
