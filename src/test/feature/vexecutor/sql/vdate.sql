SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt - '1 1 1'::vint4;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt + '1 1 1'::vint4;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt - '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt = '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt > '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt >= '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt < '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt <= '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

SELECT '1998-08-01 1998-08-02 1998-08-03'::vdateadt <> '1998-07-30 1998-08-20 1998-08-03'::vdateadt;

select a + i from test1;

select count(a) from test1 where a = '1998-04-25';
select count(a) from test1 where a > '1998-04-25';
select count(a) from test1 where a < '1998-04-25';
select count(a) from test1 where a >= '1998-04-25';
select count(a) from test1 where a <= '1998-04-25';
select count(a) from test1 where a <> '1998-04-25';
