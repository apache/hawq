-- start_ignore
drop table if exists testAnalyze;
drop table if exists testA;
drop table if exists tmpa;
drop table if exists tmpb;
drop function test();
-- end_ignore

-- test analyze debug
create table testAnalyze(id int);
insert into testAnalyze select generate_series(1, 10);
set debug_udf_plan = true;
analyze testAnalyze;
set debug_udf_plan = false;

-- test udf debug
-- start_ignore
create table tmpa(cif_cust_id text, etl_date date)
    with (appendonly=true, orientation=row, compresstype=snappy,oids=false)
    distributed randomly partition by range(etl_date)
    (partition p20150101 start ('2015-01-01'::date)end ('2015-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20160101 start ('2016-01-01'::date)end ('2016-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20170101 start ('2017-01-01'::date)end ('2017-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20180101 start ('2018-01-01'::date)end ('2018-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20190101 start ('2019-01-01'::date)end ('2019-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20090101 start ('2005-01-01'::date)end ('2005-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20110101 start ('2009-01-01'::date)end ('2009-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20120101 start ('2008-01-01'::date)end ('2008-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy));

create table tmpb(cif_cust_id text, etl_date date)
    with (appendonly=true, orientation=row, compresstype=snappy,oids=false)
    distributed randomly partition by range(etl_date)
    (partition p20350101 start ('2015-01-01'::date)end ('2015-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20150501 start ('2015-05-01'::date)end ('2015-05-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20160101 start ('2016-01-01'::date)end ('2016-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20170101 start ('2017-01-01'::date)end ('2017-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20180101 start ('2018-01-01'::date)end ('2018-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20190101 start ('2019-01-01'::date)end ('2019-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20090101 start ('2005-01-01'::date)end ('2005-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p20110101 start ('2009-01-01'::date)end ('2009-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p12340101 start ('1015-02-01'::date)end ('1015-02-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10150101 start ('1015-01-01'::date)end ('1015-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10160101 start ('1016-01-01'::date)end ('1016-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10170101 start ('1017-01-01'::date)end ('1017-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10180101 start ('1018-01-01'::date)end ('1018-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10190101 start ('1019-01-01'::date)end ('1019-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10090101 start ('1005-01-01'::date)end ('1005-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p10110101 start ('1009-01-01'::date)end ('1009-01-02'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p30350101 start ('1009-01-03'::date)end ('1009-01-04'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p30150101 start ('1009-01-05'::date)end ('1009-01-06'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p30160101 start ('1009-01-07'::date)end ('1009-01-08'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p30170101 start ('1009-01-09'::date)end ('1009-01-10'::date)with (appendonly=true, orientation=row, compresstype=snappy),
    partition p30120101 start ('1009-01-11'::date)end ('1009-01-12'::date)with (appendonly=true, orientation=row, compresstype=snappy));
-- start_ignore
create table testA(id int);

create function test() returns void as $$
declare
    i int;
    pval int;
    maml_date date;
begin
    maml_date :=to_date('2020-01-01'::date, 'YYYY-MM-DD');
    insert into testA select count(1) from tmpa A1 left join tmpb A2
        on A1.cif_cust_id=A2.cif_cust_id where trim(A1.cif_cust_id) !=''
        and A2.etl_date=maml_date::date;
end;
$$ language plpgsql;

set debug_udf_plan = true;
select test();
set debug_udf_plan = false;
