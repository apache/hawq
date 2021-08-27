-- start_ignore
drop table if exists testUdfWithAnalyze;
drop function test();
-- end_ignore

-- prepare table an test udf
create table testUdfWithAnalyze(i int, c text);

create function test() returns void as $$
begin
  insert into testUdfWithAnalyze select generate_series(1,100),'abc';
  analyze testUdfWithAnalyze;
end;
$$ language plpgsql;

-- run test
alter resource queue pg_default with (active_statements=1);
select test();  -- should complete without error raised
alter resource queue pg_default with (active_statements=20);