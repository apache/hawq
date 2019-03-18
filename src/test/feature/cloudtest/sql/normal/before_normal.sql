drop function f4();

drop language plpythonu CASCADE;

drop OPERATOR CLASS sva_special_ops USING btree;

drop OPERATOR <# (text,text) CASCADE;

drop FUNCTION si_same(text, text);

drop FUNCTION si_lt(text, text);

drop FUNCTION normalize_si(text);

DROP RESOURCE QUEUE myqueue;

drop table foo;

drop tablespace mytblspace;

drop EXTERNAL TABLE ext_t;

drop EXTERNAL TABLE ext_t2;

DROP AGGREGATE scube2(numeric);

DROP FUNCTION scube_accum(numeric, numeric);

drop type mytype cascade;

drop SEQUENCE myseq;

drop view av;

drop table aaa;

drop table aa;

drop table a;

drop schema sa CASCADE;