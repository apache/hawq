SET gp_enable_column_oriented_table=on;
CREATE TABLE co_disabled1 ( p int) with (appendonly=true, orientation=column);

SET gp_enable_column_oriented_table=off;

ALTER TABLE co_disabled1 add column q int default 1;

INSERT into co_disabled1 values(100);

COPY co_disabled1 from stdin;

CREATE TABLE co_disabled2 ( q varchar) with (appendonly=true, orientation=column);

CREATE TABLE co_disabled3
    (id SERIAL,a1 int,a2 char(5),a3 numeric,a4 boolean DEFAULT false ,a5 char DEFAULT 'd',a6 text,a7 timestamp,a8 character varying(705),a9 bigint,a10 date,a11 varchar(600),a12 text,a13 decimal,a14 real,a15 bigint,a16 int4 ,a17 bytea,a18 timestamp with time zone,a19 timetz,a20 path,a21 box,a22 macaddr,a23 interval,a24 character varying(800),a25 lseg,a26 point,a27 double precision,a28 circle,a29 int4,a30 numeric(8),a31 polygon,a32 date,a33 real,a34 money,a35 cidr,a36 inet,a37 time,a38 text,a39 bit,a40 bit varying(5),a41 smallint,a42 int )
     WITH (appendonly=true, orientation=parquet) distributed randomly  Partition by list(a2) Subpartition by range(a1) subpartition template (default subpartition df_sp, start(1)  end(5000) every(1000)
         WITH (appendonly=true, orientation=parquet,compresstype=snappy)) (partition p1 values ('M'), partition p2 values ('F'));
ALTER table co_disabled3 add partition new_p values('C')  WITH (appendonly=true, orientation=column);
