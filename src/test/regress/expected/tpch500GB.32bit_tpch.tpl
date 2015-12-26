-- ensure legacy default settings;
-- new installations have different settings due to modified conf files;
set enable_mergejoin = on;
set enable_nestloop = on;
set gp_enable_agg_distinct = off;
set gp_selectivity_damping_for_scans = off;
set gp_selectivity_damping_for_joins = off;
set allow_system_table_mods = dml;
-- 
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
-- 
-- Database: tpch500
-- Date:     2007-10-05
-- Time:     12:07:47.570184-07
-- CmdLine:    
-- Version:  PostgreSQL 8.2.4 (Greenplum Database Release-3_1_0_0-alpha1-branch build 1896) on i386-pc-solaris2.10, compiled by GCC gcc.exe (GCC) 4.1.1 compiled on Sep 25 2007 10:48:21
-- 
--
-- Greenplum Database database dump
--
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
--
-- Name: SCHEMA tpch500gb; Type: COMMENT; Schema: -; Owner: -
--
COMMENT ON SCHEMA public IS 'Standard public schema';
create schema tpch500gb;
SET search_path = tpch500gb, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;
--
-- Name: customer; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE customer (
    c_custkey integer NOT NULL,
    c_name character varying(25) NOT NULL,
    c_address character varying(40) NOT NULL,
    c_nationkey integer NOT NULL,
    c_phone character(15) NOT NULL,
    c_acctbal numeric(15,2) NOT NULL,
    c_mktsegment character(10) NOT NULL,
    c_comment character varying(117) NOT NULL
) distributed by (c_custkey);
--
-- Name: lineitem; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE lineitem (
    l_orderkey bigint NOT NULL,
    l_partkey integer NOT NULL,
    l_suppkey integer NOT NULL,
    l_linenumber integer NOT NULL,
    l_quantity numeric(15,2) NOT NULL,
    l_extendedprice numeric(15,2) NOT NULL,
    l_discount numeric(15,2) NOT NULL,
    l_tax numeric(15,2) NOT NULL,
    l_returnflag character(1) NOT NULL,
    l_linestatus character(1) NOT NULL,
    l_shipdate date NOT NULL,
    l_commitdate date NOT NULL,
    l_receiptdate date NOT NULL,
    l_shipinstruct character(25) NOT NULL,
    l_shipmode character(10) NOT NULL,
    l_comment character varying(44) NOT NULL
) distributed by (l_orderkey);
--
-- Name: nation; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE nation (
    n_nationkey integer NOT NULL,
    n_name character(25) NOT NULL,
    n_regionkey integer NOT NULL,
    n_comment character varying(152)
) distributed by (n_nationkey);
--
-- Name: orders; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE orders (
    o_orderkey bigint NOT NULL,
    o_custkey integer NOT NULL,
    o_orderstatus character(1) NOT NULL,
    o_totalprice numeric(15,2) NOT NULL,
    o_orderdate date NOT NULL,
    o_orderpriority character(15) NOT NULL,
    o_clerk character(15) NOT NULL,
    o_shippriority integer NOT NULL,
    o_comment character varying(79) NOT NULL
) distributed by (o_orderkey);
--
-- Name: part; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE part (
    p_partkey integer NOT NULL,
    p_name character varying(55) NOT NULL,
    p_mfgr character(25) NOT NULL,
    p_brand character(10) NOT NULL,
    p_type character varying(25) NOT NULL,
    p_size integer NOT NULL,
    p_container character(10) NOT NULL,
    p_retailprice numeric(15,2) NOT NULL,
    p_comment character varying(23) NOT NULL
) distributed by (p_partkey);
--
-- Name: partsupp; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE partsupp (
    ps_partkey integer NOT NULL,
    ps_suppkey integer NOT NULL,
    ps_availqty integer NOT NULL,
    ps_supplycost numeric(15,2) NOT NULL,
    ps_comment character varying(199) NOT NULL
) distributed by (ps_partkey);
--
-- Name: region; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE region (
    r_regionkey integer NOT NULL,
    r_name character(25) NOT NULL,
    r_comment character varying(152)
) distributed by (r_regionkey);
--
-- Name: revenue0; Type: VIEW; Schema: tpch500gb; Owner: -
--
CREATE VIEW revenue0 AS
    SELECT lineitem.l_suppkey AS supplier_no, sum((lineitem.l_extendedprice * ((1)::numeric - lineitem.l_discount))) AS total_revenue FROM lineitem WHERE ((lineitem.l_shipdate >= '1993-01-01'::date) AND (lineitem.l_shipdate < ('1993-01-01'::date + '3 mons'::interval))) GROUP BY lineitem.l_suppkey;
--
-- Name: supplier; Type: TABLE; Schema: tpch500gb; Owner: -; Tablespace: 
--
CREATE TABLE supplier (
    s_suppkey integer NOT NULL,
    s_name character(25) NOT NULL,
    s_address character varying(40) NOT NULL,
    s_nationkey integer NOT NULL,
    s_phone character(15) NOT NULL,
    s_acctbal numeric(15,2) NOT NULL,
    s_comment character varying(101) NOT NULL
) distributed by (s_suppkey);
-- 
-- Table: customer, Attribute: c_custkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	97::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::int4[],
	'{804,2921948,5863959,8787979,11802789,14784434,17816642,20845138,23890922,26778640,29746505,32749801,35795334,38741386,41761423,44846847,47954678,51000547,53971082,56912203,59868521,62836009,65897832,68906877,72003218,74983822,74997699}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: customer, Attribute: c_name
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	2::smallint,
	0::real,
	19::integer,
	-1::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: customer, Attribute: c_address
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	3::smallint,
	0::real,
	26::integer,
	-1::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: customer, Attribute: c_nationkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	4::smallint,
	0::real,
	4::integer,
	25::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.0428491,0.0427156,0.0423152,0.0421283,0.0414876,0.0410337,0.0409002,0.0406066,0.0406066,0.0398857,0.0396989,0.0396722,0.0396722,0.0396188,0.0393251,0.0393251,0.0391916,0.0391916,0.0390314,0.0388712,0.0386844,0.038631,0.0382038,0.0382038,0.0381504}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{23,2,15,19,11,8,9,18,13,24,22,1,12,4,3,7,21,16,10,5,17,14,6,0,20}'::int4[],
	'{0,1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22,23,24}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: customer, Attribute: c_phone
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	5::smallint,
	0::real,
	16::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	1058::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::bpchar[],
	'{10-100-191-7266,11-141-431-6620,12-146-228-8482,12-989-498-7126,14-102-488-8228,15-110-736-8802,16-134-382-8692,17-180-990-4687,18-179-696-6251,19-168-563-7001,20-156-810-2884,21-169-585-7571,22-134-156-5368,23-142-180-9482,24-124-668-5127,25-146-190-8013,26-105-145-1566,27-125-452-1979,28-158-715-8691,29-138-317-9725,29-986-797-4558,31-131-791-7114,32-156-414-9083,33-147-732-1957,33-991-436-9035,34-994-434-4065,34-997-979-7892}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: customer, Attribute: c_acctbal
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	6::smallint,
	0::real,
	8::integer,
	1.02349e+06::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.000106789,8.00918e-05,8.00918e-05,8.00918e-05,8.00918e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05,5.33946e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1326.23,6331.71,7152.66,8601.76,6948.19,8351.64,5258.77,8026.44,3748.38,3272.33,9381.39,6373.51,6483.41,7139.38,8439.24,2742.33,7603.02,24.43,3497.27,6354.67,2552.85,592.58,9742.41,8182.96,9651.53}'::numeric[],
	'{-999.66,-528.82,-82.74,363.12,801.53,1249.85,1674.72,2111.78,2532.23,2966.11,3412.64,3850.63,4281.29,4720.86,5170.36,5625.04,6066.04,6511.02,6960.76,7389.45,7820.73,8247.92,8669.04,9108.46,9560.62,9998.53,9999.71}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: customer, Attribute: c_mktsegment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	7::smallint,
	0::real,
	11::integer,
	5::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.201458,0.200203,0.199776,0.199455,0.199108}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"HOUSEHOLD ","BUILDING  ","FURNITURE ","MACHINERY ",AUTOMOBILE}'::bpchar[],
	'{AUTOMOBILE,"BUILDING  ","FURNITURE ","HOUSEHOLD ","MACHINERY "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: customer, Attribute: c_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.customer'::regclass,
	8::smallint,
	0::real,
	73::integer,
	-0.760944::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{5.33946e-05,5.33946e-05,5.33946e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05,2.66973e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"uests. carefully ironic asymptotes wake carefully across the courts. special, furious requests sleep. si"," ironic orbits. slyly express requests haggle across the final dependencies! unusual,","ets haggle slyly carefully bold requests. ironic theodolites wake quickly. carefully final pi"," requests cajole against the ironic pinto beans. slyly regular packages cajole furiously. blithely e"," use furiously according to the ","nst the sly, silent deposits boost ironic, bold requests. quickly regular theodolites integrate blithely quickly fi","lly special instructions. ironic, final requests nag alongside","ently regular accounts cajole carefully silent theodolites. final, regular foxes nag blithely regular request","s cajole blithely carefully regular warthogs. furio","ackages. pending courts wake. furiously u","ly final deposits wake slyly among th"," silent grouches. instructions sleep? furiously special deposits sleep b"," pinto beans lose fluffily. slyly silent theodolites cajole fluffily according to ","regular accounts eat blithely. theodolites according to the express pinto","r accounts are furiously. quickly enticing braids are about th"," escapades. blithely regular pinto beans detect carefully carefully permanent tithes. fluffi","ing packages are slyly across the blithely ironic accounts. regular pl","ackages. slyly special deposits sleep slyly alongside of the quickly e","ourts haggle quickly blithely express packages. carefully final requests haggle quickly blithely ironic ideas."," accounts. unusual accounts nag fluffi","lent theodolites kindle slyly ","ites use! blithely special packa","quickly across the packages. final ins","the regular deposits. fluffily special accoun","eposits. slyly quiet deposits use blithely pending, unusual ideas. furiously bold excu"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: lineitem, Attribute: l_orderkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	1::smallint,
	0::real,
	8::integer,
	2.83108e+08::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	410::oid,
	412::oid,
	0::oid,
	0::oid,
	'{4.61276e-05,4.61276e-05,4.61276e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{684660003,2498724164,2601169125,665000674,966822981,70399204,2970597478,2001386055,291876741,1402120835,2416267206,1493869799,873810726,247996930,2143385415,1485096743,2140300835,2048646853,639228836,1250075495,210992546,1999492995,678628385,2045559744,395294948}'::int8[],
	'{63270,122175974,245661766,368423714,481598659,603820005,715990439,832983360,955418464,1078067687,1197146500,1315714816,1434740995,1556093057,1675966246,1797661605,1918969792,2041767175,2162361312,2279143781,2398070019,2510123811,2635543239,2757323586,2881136101,2999710886,2999977856}'::int8[],
	NULL::int8[],
	NULL::int8[]);
-- 
-- Table: lineitem, Attribute: l_partkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	2::smallint,
	0::real,
	4::integer,
	5.42498e+07::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05,2.30638e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{74816973,99986397,35934058,83893509,24494308,38058554,64215895,71106066,94717702,2166665,33238873,76663389,89934431,41370087,38358229,62637654,79148976,44613095,88856810,94549934,29688330,14757870,81756903,81285545,52654208}'::int4[],
	'{3514,3778791,7623228,11814393,15941479,20062466,24081414,28064591,32197294,36158321,39989279,43964528,48004896,52096756,56105946,60253290,64325207,68313587,72237369,76164467,80046212,83991569,87905531,91954431,95975224,99985038,99996317}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: lineitem, Attribute: l_suppkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	3::smallint,
	0::real,
	4::integer,
	5.07803e+06::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{571170,342194,4232105,2317002,1649152,449904,790709,2556241,460723,2261648,225307,2522688,3140446,29637,1236417,796525,4361999,1871637,4087599,4459098,2658631,2278367,3121922,57515,2428966}'::int4[],
	'{254,199905,397439,595963,789527,987880,1192416,1393289,1594098,1792035,1997828,2198858,2397260,2595017,2789066,2991306,3190760,3387653,3581070,3778384,3982147,4182247,4389723,4592954,4799542,4999295,4999864}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: lineitem, Attribute: l_linenumber
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	4::smallint,
	0::real,
	4::integer,
	7::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.250081,0.213156,0.178814,0.14258,0.1075,0.0717515,0.0361179}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1,2,3,4,5,6,7}'::int4[],
	'{1,2,3,4,5,6,7}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: lineitem, Attribute: l_quantity
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	5::smallint,
	0::real,
	7::integer,
	50::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.0212418,0.0211726,0.0210342,0.0210111,0.0209881,0.0209419,0.0208266,0.0207574,0.0207344,0.0206421,0.020596,0.0205729,0.0204345,0.0203884,0.0203423,0.0203192,0.0202961,0.0202961,0.0202269,0.0202039,0.0202039,0.0202039,0.0201808,0.0201578,0.0200886}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1.00,5.00,6.00,4.00,23.00,11.00,31.00,36.00,28.00,12.00,42.00,2.00,50.00,7.00,29.00,32.00,17.00,22.00,45.00,43.00,15.00,48.00,41.00,44.00,47.00}'::numeric[],
	'{1.00,2.00,4.00,6.00,8.00,10.00,12.00,14.00,16.00,18.00,20.00,22.00,24.00,26.00,28.00,30.00,32.00,34.00,36.00,38.00,41.00,42.00,44.00,46.00,48.00,50.00}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: lineitem, Attribute: l_extendedprice
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	6::smallint,
	0::real,
	10::integer,
	2.69251e+06::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05,4.61276e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1585.12,54070.70,36389.10,29623.92,70271.12,28700.28,19146.40,55276.36,53105.76,8911.28,50629.60,23917.60,66085.44,34047.43,45696.96,22102.08,38978.10,25021.44,39818.16,72407.16,60931.20,44936.00,36705.20,46454.52,55112.40}'::numeric[],
	'{910.92,3461.73,6301.72,9103.08,11906.10,14773.44,17681.70,20503.04,23425.87,26367.39,29258.19,32097.88,35049.00,37955.84,40728.45,43636.62,46445.00,49474.18,52516.82,55719.30,59404.64,63507.36,68282.28,73785.77,82066.50,102182.00,103239.50}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: lineitem, Attribute: l_discount
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	7::smallint,
	0::real,
	6::integer,
	11::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.0941695,0.0934314,0.0923936,0.0914249,0.0913557,0.0910328,0.0905254,0.0901794,0.0892338,0.0886342,0.0876194}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{0.03,0.10,0.08,0.09,0.01,0.04,0.00,0.06,0.05,0.07,0.02}'::numeric[],
	'{0.00,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.10}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: lineitem, Attribute: l_tax
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	8::smallint,
	0::real,
	6::integer,
	9::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.113197,0.112597,0.112367,0.112021,0.11179,0.111191,0.109853,0.108515,0.108469}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{0.01,0.06,0.07,0.08,0.05,0.02,0.04,0.03,0.00}'::numeric[],
	'{0.00,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: lineitem, Attribute: l_returnflag
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	9::smallint,
	0::real,
	2::integer,
	3::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.508072,0.247082,0.244845}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{N,A,R}'::bpchar[],
	'{A,N,R}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: lineitem, Attribute: l_linestatus
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	10::smallint,
	0::real,
	2::integer,
	2::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.501707,0.498293}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{O,F}'::bpchar[],
	'{F,O}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: lineitem, Attribute: l_shipdate
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	11::smallint,
	0::real,
	4::integer,
	2515.75::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1093::oid,
	1095::oid,
	0::oid,
	0::oid,
	'{0.000761105,0.000761105,0.000738041,0.000738041,0.000714978,0.000714978,0.000714978,0.000714978,0.000714978,0.000691914,0.000691914,0.000691914,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.000645786,0.000645786,0.000645786,0.000645786}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1992-04-25,1998-07-20,1994-12-05,1998-07-10,1996-01-06,1997-06-18,1996-02-25,1996-07-10,1998-07-09,1996-11-25,1995-09-01,1994-02-12,1997-07-26,1998-06-22,1996-05-30,1997-12-22,1998-05-07,1992-08-29,1993-09-08,1996-09-24,1994-06-22,1995-09-10,1995-05-05,1995-03-04,1996-01-23}'::date[],
	'{1992-01-05,1992-06-03,1992-09-04,1992-12-13,1993-03-20,1993-06-28,1993-10-05,1994-01-09,1994-04-15,1994-07-19,1994-10-23,1995-01-27,1995-05-04,1995-08-11,1995-11-16,1996-02-21,1996-05-29,1996-08-31,1996-12-04,1997-03-12,1997-06-18,1997-09-21,1997-12-25,1998-03-29,1998-06-30,1998-11-23,1998-11-30}'::date[],
	NULL::date[],
	NULL::date[]);
-- 
-- Table: lineitem, Attribute: l_commitdate
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	12::smallint,
	0::real,
	4::integer,
	2462.34::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1093::oid,
	1095::oid,
	0::oid,
	0::oid,
	'{0.000738041,0.000738041,0.000738041,0.000714978,0.000714978,0.000714978,0.000714978,0.000691914,0.000691914,0.000691914,0.000691914,0.000691914,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.00066885,0.000645786,0.000645786}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1995-05-14,1992-06-23,1998-01-31,1992-08-03,1993-01-12,1993-04-05,1996-05-08,1995-09-10,1995-01-25,1996-01-26,1994-07-17,1992-08-14,1994-10-30,1996-01-22,1995-10-12,1998-01-16,1992-05-04,1994-12-27,1996-08-27,1998-05-10,1994-06-02,1997-07-26,1993-03-16,1996-03-23,1993-11-24}'::date[],
	'{1992-02-02,1992-06-04,1992-09-03,1992-12-12,1993-03-20,1993-06-25,1993-10-04,1994-01-09,1994-04-14,1994-07-19,1994-10-22,1995-01-27,1995-05-03,1995-08-10,1995-11-13,1996-02-19,1996-05-29,1996-08-28,1996-12-03,1997-03-11,1997-06-17,1997-09-20,1997-12-26,1998-03-29,1998-06-30,1998-10-25,1998-10-31}'::date[],
	NULL::date[],
	NULL::date[]);
-- 
-- Table: lineitem, Attribute: l_receiptdate
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	13::smallint,
	0::real,
	4::integer,
	2521.81::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1093::oid,
	1095::oid,
	0::oid,
	0::oid,
	'{0.000830297,0.000784169,0.000784169,0.000761105,0.000761105,0.000738041,0.000738041,0.000738041,0.000738041,0.000738041,0.000738041,0.000738041,0.000714978,0.000714978,0.000714978,0.000691914,0.000691914,0.000691914,0.000691914,0.000691914,0.000691914,0.000691914,0.00066885,0.00066885,0.00066885}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1996-06-27,1995-04-20,1997-10-21,1997-04-18,1996-12-24,1994-07-15,1992-08-19,1995-06-21,1998-05-27,1994-06-16,1994-10-15,1997-12-24,1994-06-01,1992-09-09,1997-06-17,1992-05-30,1997-07-24,1998-05-14,1995-01-09,1998-08-21,1994-03-24,1994-09-30,1998-01-13,1997-02-17,1998-01-31}'::date[],
	'{1992-01-18,1992-06-18,1992-09-20,1992-12-28,1993-04-05,1993-07-13,1993-10-21,1994-01-25,1994-05-01,1994-08-03,1994-11-07,1995-02-13,1995-05-20,1995-08-27,1995-12-01,1996-03-07,1996-06-14,1996-09-15,1996-12-19,1997-03-27,1997-07-04,1997-10-05,1998-01-10,1998-04-13,1998-07-15,1998-12-15,1998-12-18}'::date[],
	NULL::date[],
	NULL::date[]);
-- 
-- Table: lineitem, Attribute: l_shipinstruct
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	14::smallint,
	0::real,
	26::integer,
	4::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.252848,0.252687,0.248097,0.246367}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"TAKE BACK RETURN         ","NONE                     ","COLLECT COD              ","DELIVER IN PERSON        "}'::bpchar[],
	'{"COLLECT COD              ","DELIVER IN PERSON        ","NONE                     ","TAKE BACK RETURN         "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: lineitem, Attribute: l_shipmode
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	15::smallint,
	0::real,
	11::integer,
	7::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.146086,0.145256,0.143734,0.142165,0.141704,0.141358,0.139697}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"FOB       ","AIR       ","MAIL      ","TRUCK     ","REG AIR   ","SHIP      ","RAIL      "}'::bpchar[],
	'{"AIR       ","FOB       ","MAIL      ","RAIL      ","REG AIR   ","SHIP      ","TRUCK     "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: lineitem, Attribute: l_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.lineitem'::regclass,
	16::smallint,
	0::real,
	27::integer,
	1.23854e+06::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.000230638,0.00018451,0.000161447,0.000161447,0.000161447,0.000161447,0.000138383,0.000138383,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,0.000115319,9.22552e-05,9.22552e-05,9.22552e-05,9.22552e-05,9.22552e-05,9.22552e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"furiously "," carefully ","deposits. ","y regular "," furiously"," theodolit",structions," instructio"," furiously ","ly regular ","ly regular","e carefull","s. regular","e furiousl"," instructi",". furiousl"," packages ","carefully ","ly ironic "," regular request",dependencie," final packages",". furiously"," carefully"," deposits. "}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: nation, Attribute: n_nationkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.nation'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{7,17,12,3,8,19,14,4,11,2,13,6,9,0,24,5,16,21,22,23,20,18,15,1,10}'::int4[],
	'{}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: nation, Attribute: n_name
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.nation'::regclass,
	2::smallint,
	0::real,
	26::integer,
	-1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"GERMANY                  ","FRANCE                   ","RUSSIA                   ","KENYA                    ","UNITED KINGDOM           ","ARGENTINA                ","INDONESIA                ","IRAN                     ","BRAZIL                   ","MOROCCO                  ","CHINA                    ","INDIA                    ","VIETNAM                  ","JAPAN                    ","JORDAN                   ","PERU                     ","ROMANIA                  ","MOZAMBIQUE               ","CANADA                   ","IRAQ                     ","UNITED STATES            ","EGYPT                    ","ETHIOPIA                 ","ALGERIA                  ","SAUDI ARABIA             "}'::bpchar[],
	'{}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: nation, Attribute: n_regionkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.nation'::regclass,
	3::smallint,
	0::real,
	4::integer,
	-0.2::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.2,0.2,0.2,0.2,0.2}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{3,4,2,0,1}'::int4[],
	'{}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: nation, Attribute: n_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.nation'::regclass,
	4::smallint,
	0::real,
	75::integer,
	-1::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04,0.04}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{" pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t"," haggle. carefully final deposits detect slyly agai","platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun","y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ","rns. blithely bold courts among the closely regular packages use furiously bold platelets?","refully final requests. regular, ironi","eans boost carefully special requests. accounts are. carefull","y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be","ic deposits are blithely about the carefully regular pa","ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account","al foxes promise slyly according to the regular accounts. bold requests alon","efully alongside of the slyly final dependencies. ","ss excuses cajole slyly across the packages. deposits print aroun","ts. silent requests haggle. closely express packages sleep across the blithely"," slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull"," requests against the platelets use never according to the quickly regular pint","s. ironic, unusual asymptotes wake blithely r","y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d","eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold","hely enticingly express accounts. even, final ","l platelets. regular accounts x-ray: unusual, regular acco","nic deposits boost atop the quickly final requests? quickly regula","ously. final, express gifts cajole a","c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos","ven packages wake quickly. regu"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: orders, Attribute: o_orderkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	1::smallint,
	0::real,
	8::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	412::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::int8[],
	'{61892,119652261,243055783,365757188,486140609,604643847,727829185,844894951,963745156,1080925607,1204339687,1320041447,1435639654,1554755745,1684670886,1808222915,1926850179,2047859874,2168544097,2289931588,2405096417,2522349095,2641814116,2761700166,2880131844,2998909952,2999943650}'::int8[],
	NULL::int8[],
	NULL::int8[]);
-- 
-- Table: orders, Attribute: o_custkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	2::smallint,
	0::real,
	4::integer,
	5.58477e+07::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{39105769,63807202,72410635,18846655,62315263,70525174,14436991,23197640,59424196,57096607,43924048,5033527,59821054,35895631,66929311,38935051,67681264,1100941,74896732,10194644,5643805,23958109,32067908,5807269,11897753}'::int4[],
	'{1453,2973184,5965483,9029530,12087334,15028252,18069238,21116027,24112660,27070826,30064510,32947372,36016186,39153244,42008777,44991388,47992295,51063148,54016762,56970046,59948365,62894089,65982722,68987845,72089479,74983168,74999722}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: orders, Attribute: o_orderstatus
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	3::smallint,
	0::real,
	2::integer,
	3::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.487203,0.486596,0.0262013}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{F,O,P}'::bpchar[],
	'{F,O,P}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: orders, Attribute: o_totalprice
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	4::smallint,
	0::real,
	10::integer,
	4.19851e+07::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{51720.84,336372.40,253417.01,153220.96,33107.69,117940.45,198176.51,183294.55,74513.29,82571.35,259080.20,170610.02,260342.50,130267.63,187183.44,58940.35,176680.48,84023.62,59491.86,249518.04,159130.57,262426.61,186030.55,108733.56,236015.03}'::numeric[],
	'{887.99,18823.19,32959.30,45056.42,55345.18,65221.12,75531.97,85832.71,96300.81,107620.82,118015.57,128845.57,139675.31,150230.96,160829.22,172131.44,183414.41,194926.94,207069.97,219616.63,233419.72,247929.48,265036.75,285932.07,315923.62,437564.16,517486.23}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: orders, Attribute: o_orderdate
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	5::smallint,
	0::real,
	4::integer,
	2406::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1093::oid,
	1095::oid,
	0::oid,
	0::oid,
	'{0.000874997,0.000802081,0.000777775,0.000777775,0.00075347,0.00075347,0.00075347,0.00075347,0.000729164,0.000729164,0.000704859,0.000704859,0.000704859,0.000704859,0.000680553,0.000680553,0.000680553,0.000680553,0.000656248,0.000656248,0.000656248,0.000656248,0.000656248,0.000656248,0.000656248}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1997-09-22,1995-02-19,1996-08-13,1997-06-19,1996-12-31,1995-05-09,1993-10-18,1996-05-06,1994-12-08,1992-05-06,1995-09-08,1992-11-10,1995-10-20,1996-02-10,1996-03-26,1993-05-02,1997-08-31,1992-10-24,1992-09-25,1992-07-21,1993-02-04,1993-02-12,1995-08-20,1992-10-15,1995-04-02}'::date[],
	'{1992-01-01,1992-04-07,1992-07-09,1992-10-11,1993-01-13,1993-04-20,1993-07-25,1993-10-29,1994-02-04,1994-05-11,1994-08-21,1994-11-25,1995-02-28,1995-06-04,1995-09-08,1995-12-15,1996-03-22,1996-06-25,1996-10-02,1997-01-06,1997-04-13,1997-07-18,1997-10-20,1998-01-21,1998-04-25,1998-08-02}'::date[],
	NULL::date[],
	NULL::date[]);
-- 
-- Table: orders, Attribute: o_orderpriority
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	6::smallint,
	0::real,
	16::integer,
	5::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.200787,0.200472,0.200253,0.199281,0.199208}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"4-NOT SPECIFIED","2-HIGH         ","1-URGENT       ","3-MEDIUM       ","5-LOW          "}'::bpchar[],
	'{"1-URGENT       ","2-HIGH         ","3-MEDIUM       ","4-NOT SPECIFIED","5-LOW          "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: orders, Attribute: o_clerk
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	7::smallint,
	0::real,
	16::integer,
	488990::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{9.72219e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05,7.29164e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{Clerk#000208581,Clerk#000292245,Clerk#000088870,Clerk#000097719,Clerk#000105522,Clerk#000387151,Clerk#000275998,Clerk#000018085,Clerk#000126515,Clerk#000161705,Clerk#000172216,Clerk#000235515,Clerk#000489479,Clerk#000279130,Clerk#000045535,Clerk#000150794,Clerk#000395144,Clerk#000420523,Clerk#000486717,Clerk#000197312,Clerk#000402389,Clerk#000304725,Clerk#000294622,Clerk#000002206,Clerk#000468131}'::bpchar[],
	'{Clerk#000000011,Clerk#000020130,Clerk#000040407,Clerk#000060196,Clerk#000080283,Clerk#000100917,Clerk#000120280,Clerk#000140140,Clerk#000159663,Clerk#000180207,Clerk#000199184,Clerk#000219792,Clerk#000239685,Clerk#000259956,Clerk#000279872,Clerk#000300064,Clerk#000319923,Clerk#000339438,Clerk#000359262,Clerk#000379550,Clerk#000399768,Clerk#000420258,Clerk#000439369,Clerk#000459093,Clerk#000479016,Clerk#000499839,Clerk#000499998}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: orders, Attribute: o_shippriority
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	8::smallint,
	0::real,
	4::integer,
	1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{1}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{0}'::int4[],
	'{0}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: orders, Attribute: o_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.orders'::regclass,
	9::smallint,
	0::real,
	49::integer,
	3.81924e+07::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,4.86109e-05,2.43055e-05,2.43055e-05,2.43055e-05,2.43055e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"ly final accounts. ","carefully regular d"," ironic deposits cajole again","regular instructions","e furiously regular reque","ously regular accounts","nding courts use slyly after the even, final asy","instructions haggle","ironic pinto beans. ","y regular pinto bean","ts haggle furiously","ing theodolites. sl"," the furiously even","above the fluffily regular","express deposits. e"," furiously regular ","unts. furiously pending ","the quickly regular requests","equests. slyly iron"," regular pinto bean"," regular requests across the fluffily unusual asymptotes shall have to sl","packages haggle among the pending deposits. special, f"," packages. slyly ironic deposits grow furiously. regularly ironic","usly blithely bold pearls. carefully pending accounts detect car","side of the final requests cajole quickly after the i"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: part, Attribute: p_partkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	97::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::int4[],
	'{2184,4034646,8033425,12028701,15957541,19878421,23863338,27932589,31819643,35915302,39819845,43873091,47925930,51821011,55820315,59832091,63933931,67992462,72090902,76191246,80167793,84131791,88015771,91885736,95986573,99955210,99999134}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: part, Attribute: p_name
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	2::smallint,
	0::real,
	34::integer,
	-1::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: part, Attribute: p_mfgr
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	3::smallint,
	0::real,
	26::integer,
	5::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.202716,0.20029,0.199842,0.199604,0.197547}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"Manufacturer#4           ","Manufacturer#5           ","Manufacturer#2           ","Manufacturer#1           ","Manufacturer#3           "}'::bpchar[],
	'{"Manufacturer#1           ","Manufacturer#2           ","Manufacturer#3           ","Manufacturer#4           ","Manufacturer#5           "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: part, Attribute: p_brand
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	4::smallint,
	0::real,
	11::integer,
	25::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.0433281,0.0431698,0.0417985,0.041192,0.0410338,0.0408228,0.0407964,0.0407173,0.0403217,0.0401899,0.0399789,0.0399789,0.0399262,0.0398998,0.0397416,0.0396888,0.0394515,0.0394251,0.0392141,0.0389504,0.0388449,0.038634,0.0384494,0.0375264,0.0369198}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"Brand#42  ","Brand#15  ","Brand#25  ","Brand#43  ","Brand#13  ","Brand#21  ","Brand#55  ","Brand#44  ","Brand#22  ","Brand#32  ","Brand#51  ","Brand#23  ","Brand#54  ","Brand#52  ","Brand#31  ","Brand#53  ","Brand#33  ","Brand#12  ","Brand#35  ","Brand#34  ","Brand#45  ","Brand#41  ","Brand#14  ","Brand#11  ","Brand#24  "}'::bpchar[],
	'{"Brand#11  ","Brand#12  ","Brand#13  ","Brand#14  ","Brand#15  ","Brand#21  ","Brand#22  ","Brand#23  ","Brand#25  ","Brand#31  ","Brand#32  ","Brand#33  ","Brand#34  ","Brand#35  ","Brand#41  ","Brand#42  ","Brand#43  ","Brand#44  ","Brand#45  ","Brand#51  ","Brand#52  ","Brand#54  ","Brand#55  "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: part, Attribute: p_type
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	5::smallint,
	0::real,
	21::integer,
	150::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.00785865,0.00783228,0.00767405,0.00751582,0.00743671,0.00738397,0.0073576,0.00733122,0.00733122,0.00733122,0.00727848,0.00725211,0.00725211,0.00722574,0.00722574,0.007173,0.007173,0.007173,0.007173,0.00714662,0.00714662,0.00712025,0.00709388,0.00709388,0.00706751}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"ECONOMY PLATED STEEL","ECONOMY POLISHED BRASS","ECONOMY BRUSHED TIN","ECONOMY PLATED COPPER","STANDARD POLISHED BRASS","SMALL PLATED COPPER","ECONOMY ANODIZED STEEL","ECONOMY BRUSHED NICKEL","ECONOMY PLATED TIN","ECONOMY BURNISHED NICKEL","LARGE BURNISHED BRASS","LARGE PLATED BRASS","MEDIUM ANODIZED COPPER","SMALL ANODIZED TIN","LARGE PLATED STEEL","PROMO POLISHED TIN","MEDIUM POLISHED NICKEL","MEDIUM BURNISHED BRASS","SMALL BURNISHED STEEL","ECONOMY POLISHED STEEL","MEDIUM ANODIZED STEEL","PROMO PLATED NICKEL","LARGE BURNISHED NICKEL","MEDIUM BRUSHED STEEL","ECONOMY BURNISHED BRASS"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: part, Attribute: p_size
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	6::smallint,
	0::real,
	4::integer,
	50::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.0216245,0.0212816,0.0212553,0.0212289,0.0210443,0.0210443,0.0210179,0.0209388,0.0207542,0.0207015,0.0207015,0.0206487,0.020596,0.0205432,0.0204641,0.0203059,0.0202532,0.0202004,0.0202004,0.0201477,0.0201477,0.0200949,0.0200422,0.0200158,0.0199631}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{27,40,34,32,50,21,45,26,48,46,25,44,42,11,4,8,9,36,31,13,5,3,29,15,20}'::int4[],
	'{1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,46,48,50}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: part, Attribute: p_container
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	7::smallint,
	0::real,
	11::integer,
	40::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.0262395,0.0262131,0.0261076,0.0260285,0.025923,0.0258966,0.0258703,0.0258175,0.025712,0.0256593,0.0256593,0.0256329,0.0254483,0.0253692,0.0253428,0.0252901,0.0252901,0.0251846,0.0250791,0.0249736,0.0248945,0.0248154,0.024789,0.0247363,0.0247363}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"WRAP BAG  ","MED PKG   ","WRAP PKG  ","JUMBO BOX ","JUMBO CASE","WRAP DRUM ","LG CAN    ","JUMBO JAR ","JUMBO PACK","JUMBO BAG ","SM CAN    ","SM DRUM   ","SM JAR    ","LG PACK   ","MED CAN   ","LG DRUM   ","WRAP CASE ","SM CASE   ","JUMBO DRUM","LG BAG    ","SM BAG    ","LG JAR    ","WRAP JAR  ","LG CASE   ","SM PACK   "}'::bpchar[],
	'{"JUMBO BAG ","JUMBO BOX ","JUMBO CASE","JUMBO DRUM","JUMBO PACK","JUMBO PKG ","LG BOX    ","LG CASE   ","LG DRUM   ","LG PACK   ","LG PKG    ","MED BOX   ","MED CASE  ","MED DRUM  ","MED PACK  ","SM BAG    ","SM BOX    ","SM CASE   ","SM DRUM   ","SM PACK   ","WRAP BAG  ","WRAP BOX  ","WRAP CASE ","WRAP DRUM ","WRAP PACK ","WRAP PKG  "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: part, Attribute: p_retailprice
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	8::smallint,
	0::real,
	8::integer,
	107651::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.000131857,0.000131857,0.000131857,0.000131857,0.000131857,0.000131857,0.000131857,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485,0.000105485}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{1701.00,1135.47,1184.41,1383.89,1865.31,1202.43,1243.40,1757.91,1911.54,1630.79,1438.70,1571.20,1738.70,1348.58,1671.72,1516.98,1823.17,1961.74,1305.92,1091.32,1620.74,1941.17,1661.08,1358.44,1753.02}'::numeric[],
	'{900.03,1020.96,1074.29,1116.61,1156.85,1197.79,1238.08,1277.78,1317.59,1358.91,1397.27,1437.00,1477.94,1519.24,1560.33,1599.55,1640.29,1681.02,1719.61,1759.89,1800.78,1841.23,1881.69,1922.11,1973.22,2083.39,2094.18}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: part, Attribute: p_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.part'::regclass,
	9::smallint,
	0::real,
	14::integer,
	121849::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.000501055,0.000474684,0.000448312,0.000448312,0.000421941,0.000421941,0.000421941,0.000421941,0.000421941,0.000421941,0.00039557,0.00039557,0.00039557,0.00039557,0.00039557,0.000369198,0.000369198,0.000369198,0.000369198,0.000369198,0.000369198,0.000369198,0.000369198,0.000342827,0.000342827}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{" care",uriou,iously,"lyly ",egula,furio,arefully,refully," packa"," final "," slyl",fully,"ronic "," deposi",furiousl," regu",lithely,"arefully ",arefu,packages,caref,riousl,regular," acco",uriously}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: partsupp, Attribute: ps_partkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.partsupp'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-0.20436::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{45103944,90376746,29576586,98082786,69829133,28227832,41329757,10852676,72493567,9766328,87820714,30995304,21438586,58827416,80241615,4814718,8752986,39556982,75633063,30374373,40349610,23971462,80473032,59725828,13992782}'::int4[],
	'{1353,3810511,7883465,11871349,15839722,19831885,23772438,27894006,31807139,35802350,39601411,43485366,47544279,51433702,55739695,59790643,63788168,67761612,71789861,75848464,79869092,83944519,87947251,91927837,95998197,99980787,99996634}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: partsupp, Attribute: ps_suppkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.partsupp'::regclass,
	2::smallint,
	0::real,
	4::integer,
	5.53321e+06::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05,4.98318e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{2504243,2698290,515527,1189017,4069774,3745950,2087453,779459,4989047,2487265,2182961,2779653,189788,2811164,2243101,627303,3788784,4549789,4583609,3855610,3086600,1658041,3585719,1500550,1288256}'::int4[],
	'{124,201570,404602,600433,790524,991895,1202777,1393680,1587079,1781735,1984692,2193781,2405959,2607920,2794278,2988335,3180103,3379907,3590409,3793786,3992046,4189129,4388420,4587739,4794408,4998460,4999773}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: partsupp, Attribute: ps_availqty
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.partsupp'::regclass,
	3::smallint,
	0::real,
	4::integer,
	10000.4::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.000323907,0.000323907,0.000323907,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000298991,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075,0.000274075}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{9228,6888,7938,4581,9064,2417,4233,5575,307,2588,8331,9577,3091,3163,8735,6016,1107,7725,3552,7206,8645,4454,3845,4806,2399}'::int4[],
	'{1,397,794,1196,1601,1993,2399,2821,3208,3621,4014,4409,4812,5214,5609,6014,6395,6800,7205,7574,7979,8378,8777,9177,9583,9997,9999}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: partsupp, Attribute: ps_supplycost
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.partsupp'::regclass,
	4::smallint,
	0::real,
	8::integer,
	98854.1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{0.00012458,0.00012458,0.00012458,0.00012458,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05,9.96636e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{949.57,39.48,684.21,595.27,409.50,655.77,398.88,797.76,357.96,689.40,35.87,170.59,528.30,650.23,650.71,33.73,321.64,292.88,228.39,185.92,38.38,648.17,661.42,229.34,630.37}'::numeric[],
	'{1.01,39.05,79.11,118.88,160.40,200.41,241.42,281.84,321.02,359.21,399.82,437.80,479.63,517.55,558.10,600.56,641.25,680.98,720.20,759.01,800.10,840.53,878.79,918.81,959.67,999.71,999.93}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: partsupp, Attribute: ps_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.partsupp'::regclass,
	5::smallint,
	0::real,
	126::integer,
	-0.506794::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{4.98318e-05,4.98318e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05,2.49159e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"the fluffily ironic deposits. carefully ironic dependencies are slyly according to the slyly final depths. bold accounts wake silent, express asymptotes. ","es across the slyly express instructions poach carefully ironic packages. furiously regular notornis sleep quickly regular platelets. regular deco","yly final requests integrate furiously. express, ironic ideas boost slyly regular pinto beans. pending packages sleep quickly after the quickly ironic dinos; blithely speci","cajole carefully. carefully regular deposits wake carefully. blithely bold packages cajole furiously. carefully ironic packages boost fluffily final accounts: qu","ng the special platelets. carefully careful sauternes nag furiously even requests. blithely final ideas haggle above the special packages. ironic"," blithely fluffily regular packages. quickly final asymptotes cajole carefully special deposits. slyly ironic forges wake slyly according to the slyly bold requests. even, ironic ","aggle during the furiously regular theodolites. dependencies cajole carefully. ironic, ironic foxes use furiously ironic platelets. fluffily regular packages affix blit","ctions boost pending dolphins. slowly slow pinto beans use among the carefully unusual pains. bold accounts across the f","ions wake furiously across the regular, final foxes. regular packages doze fluffily pending, regular she","ual requests wake. even asymptotes wake carefully ironic accounts. deposits hinder blithely permanently regular packages. express, pending packages against the quickly bol","ets sleep against the even theodolites. even theodolites cajole ironic sheaves. final, silent instructions integrate slyl","iously ironic requests. quickly final requests cajole quickly along the regular notornis. even deposits among the b","ely silent pinto beans; slyly even packages among the carefully final deposits wake furiously regular ideas. slyly final deposits wake slyly across the ",". blithely fluffy requests sleep carefully. pinto beans haggle carefully ironic ideas. brave accounts play. slyly even request","leep at the quickly even packages. quickly even packages are. careful deposits are carefully after the requests. carefully special ins","ess theodolites hang carefully. fluffily idle platelets according to the quickly unusual patterns detect slyly unusual tithes! accounts k","e carefully silent escapades cajole furiously after the slyly careful pinto beans. furiously regular packages print blithely even pinto beans. pending, regular packages promise carefully dogged","rding to the pinto beans. regular packages engage carefully finally special packages. furiously bold deposits haggle. even escapades nag fluffily. special, unusual foxes are ironic, bold pac","ithely furiously express packages. carefully regular ideas nag. quickly stealthy foxes wake. regular dolphins cajole slyly fluffily unusual deposits. furiously special dependencies haggle","en instructions. even excuses around the ironic courts thrash blithely blithe accounts. blithely special foxes bo","s. furiously bold requests wake quickly at the slyly regular foxes. carefully final accounts affix. ironic deposits maintain special multipliers. regular depths x-ray fl",". quickly final gifts wake furiously. carefully special accounts about the enticingly regular theodolit","ngage alongside of the carefully ironic deposits. slyly even accounts sleep blith","ix quickly final, even instructions. blithely unusual requests sleep according to the slyly even deposits. slyly express requests are across the re"," special theodolites. furious, stealthy deposits are carefully. carefully regular pinto beans haggle furiously along the final, bold theodolit"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: region, Attribute: r_regionkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.region'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.2,0.2,0.2,0.2,0.2}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{3,4,2,0,1}'::int4[],
	'{}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: region, Attribute: r_name
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.region'::regclass,
	2::smallint,
	0::real,
	26::integer,
	-1::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1054::oid,
	1058::oid,
	0::oid,
	0::oid,
	'{0.2,0.2,0.2,0.2,0.2}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"AFRICA                   ","ASIA                     ","MIDDLE EAST              ","EUROPE                   ","AMERICA                  "}'::bpchar[],
	'{}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: region, Attribute: r_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.region'::regclass,
	3::smallint,
	0::real,
	67::integer,
	-1::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{0.2,0.2,0.2,0.2,0.2}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl","hs use ironic, even requests. s","ly final courts cajole furiously final excuse","lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ","ges. thinly even pinto beans ca"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: supplier, Attribute: s_suppkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	1::smallint,
	0::real,
	4::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	97::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::int4[],
	'{75,198009,403469,595318,795815,995854,1187745,1392906,1588223,1785531,1976849,2182915,2386482,2575972,2776861,2979720,3190512,3390881,3591012,3782111,3980788,4185809,4395823,4597173,4796910,4998851,4999799}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: supplier, Attribute: s_name
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	2::smallint,
	0::real,
	26::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	1058::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::bpchar[],
	'{"Supplier#000000075       ","Supplier#000198009       ","Supplier#000403469       ","Supplier#000595318       ","Supplier#000795815       ","Supplier#000995854       ","Supplier#001187745       ","Supplier#001392906       ","Supplier#001588223       ","Supplier#001785531       ","Supplier#001976849       ","Supplier#002182915       ","Supplier#002386482       ","Supplier#002575972       ","Supplier#002776861       ","Supplier#002979720       ","Supplier#003190512       ","Supplier#003390881       ","Supplier#003591012       ","Supplier#003782111       ","Supplier#003980788       ","Supplier#004185809       ","Supplier#004395823       ","Supplier#004597173       ","Supplier#004796910       ","Supplier#004998851       ","Supplier#004999799       "}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: supplier, Attribute: s_address
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	3::smallint,
	0::real,
	25::integer,
	-1::real,
	0::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	0::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Table: supplier, Attribute: s_nationkey
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	4::smallint,
	0::real,
	4::integer,
	25::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	96::oid,
	97::oid,
	0::oid,
	0::oid,
	'{0.0433444,0.0422578,0.041805,0.0415032,0.0413522,0.0413221,0.041141,0.0407184,0.0405976,0.0404769,0.0400241,0.0398732,0.039843,0.0396921,0.0396318,0.0395714,0.0395714,0.0390884,0.0389677,0.0389073,0.0388772,0.0385753,0.0378509,0.0377603,0.0372472}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{4,0,8,1,10,5,19,2,22,3,16,9,18,12,6,17,7,14,11,24,20,23,21,13,15}'::int4[],
	'{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24}'::int4[],
	NULL::int4[],
	NULL::int4[]);
-- 
-- Table: supplier, Attribute: s_phone
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	5::smallint,
	0::real,
	16::integer,
	-1::real,
	0::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	0::oid,
	1058::oid,
	0::oid,
	0::oid,
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	NULL::bpchar[],
	'{10-100-736-4380,10-950-210-5611,11-921-425-5389,12-907-335-2223,13-887-266-8935,14-814-568-2290,15-794-243-4488,16-783-625-5753,17-799-138-1484,18-758-245-4627,19-756-483-8004,20-726-611-7907,21-763-871-1204,22-757-408-4955,23-797-612-2644,24-820-694-4723,25-881-963-2940,26-896-992-4679,27-907-181-9303,28-910-466-3208,29-877-770-4908,30-894-451-5759,31-951-589-8870,32-940-205-3055,33-968-179-8702,34-994-363-7909,34-999-182-8076}'::bpchar[],
	NULL::bpchar[],
	NULL::bpchar[]);
-- 
-- Table: supplier, Attribute: s_acctbal
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	6::smallint,
	0::real,
	8::integer,
	-0.18404::real,
	1::smallint,
	2::smallint,
	0::smallint,
	0::smallint,
	1752::oid,
	1754::oid,
	0::oid,
	0::oid,
	'{9.05524e-05,9.05524e-05,9.05524e-05,9.05524e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05,6.03682e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{9440.09,7515.32,-58.99,4065.42,4946.64,3195.76,3934.64,7389.49,-491.31,3994.96,9371.25,8072.80,8289.40,3524.95,8447.05,4552.66,1924.19,5767.39,8679.12,1076.73,-431.52,7108.51,7372.22,2718.32,2864.92}'::numeric[],
	'{-999.49,-571.36,-115.69,306.52,727.36,1186.43,1609.38,2045.63,2493.49,2914.18,3360.65,3810.07,4243.84,4670.28,5112.98,5561.66,5978.06,6430.82,6873.89,7320.80,7758.15,8217.33,8659.01,9125.49,9556.99,9998.20,9999.51}'::numeric[],
	NULL::numeric[],
	NULL::numeric[]);
-- 
-- Table: supplier, Attribute: s_comment
-- 
INSERT INTO pg_statistic VALUES (
	'tpch500gb.supplier'::regclass,
	7::smallint,
	0::real,
	63::integer,
	-0.982448::real,
	1::smallint,
	0::smallint,
	0::smallint,
	0::smallint,
	98::oid,
	0::oid,
	0::oid,
	0::oid,
	'{6.03682e-05,6.03682e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05,3.01841e-05}'::real[],
	NULL::real[],
	NULL::real[],
	NULL::real[],
	'{"arefully regular accounts","inal warthogs wake carefully above the ideas. e","the regular, regular deposits. regular deposits ","e platelets. quickly bold sheav","ithely silent courts. silent ideas engage carefully slyly ironic foxes. blithel","ously unusual excuses haggle carefully alongside of the quickly bold ","uests. theodolites integrate fluffily bold ideas. slyly even foxes nag! fluffily ","ess asymptotes. sometimes special instructions are around the u","es. fluffily even dolphins against the blithely daring packages haggle carefully silent, ironic pac","uses integrate. quickly dogge","e the special excuses. slyly express dolphins kindle caref","silent packages. express accounts sleep. final, ironic ideas am","tructions haggle slyly. even requests nag? slyly pending requests are."," the furiously express dugouts. fluffily ironic ","ily careful accounts. pending instruction","ctions wake slyly carefully regular dependenci","ironic, regular deposits nag about the carefully express accounts. carefully final","lithely bold theodolites haggle fluff","l foxes. final theodolites engage blithely after the excuses. blithely final deposits a","ey players are furiously according to ","ts about the furiously even accounts sleep abou","ly bold requests. furiously ironic frays integrate furiously. regular, regular request","endencies sleep. slyly regular realms are quickly final deposits. silent dolphins haggle carefull","sly even packages. blithely final instructions boost. furiously ironic request","ould boost. thin dolphins affix. furiously fina"}'::varchar[],
	NULL::varchar[],
	NULL::varchar[],
	NULL::varchar[]);
-- 
-- Greenplum Database Statistics Dump complete
-- 
-- set pages and tuple counts
-- 
-- Table: nation
-- 
UPDATE pg_class
SET
	relpages = 24::int,
	reltuples = 25::int
WHERE relname = 'nation' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: region
-- 
UPDATE pg_class
SET
	relpages = 5::int,
	reltuples = 5::int
WHERE relname = 'region' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: part
-- 
UPDATE pg_class
SET
	relpages = 514920::int,
	reltuples = 9.81335e+07::int
WHERE relname = 'part' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: supplier
-- 
UPDATE pg_class
SET
	relpages = 27809::int,
	reltuples = 4.91852e+06::int
WHERE relname = 'supplier' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: partsupp
-- 
UPDATE pg_class
SET
	relpages = 2203275::int,
	reltuples = 3.91908e+08::int
WHERE relname = 'partsupp' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: customer
-- 
UPDATE pg_class
SET
	relpages = 448938::int,
	reltuples = 7.34745e+07::int
WHERE relname = 'customer' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: orders
-- 
UPDATE pg_class
SET
	relpages = 3418818::int,
	reltuples = 7.35569e+08::int
WHERE relname = 'orders' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
-- 
-- Table: lineitem
-- 
UPDATE pg_class
SET
	relpages = 15044639::int,
	reltuples = 2.93783e+09::real
WHERE relname = 'lineitem' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tpch500gb');
set gp_segments_for_planner = 16;
--
--
-- 01.txt
--
--
-- using 2133894592 as a seed to the RNG
select 'query 01' as current_query;
 current_query 
---------------
 query 01
(1 row)

explain select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '106 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice2; segments: 2)  (cost=120573745.63..120573745.64 rows=3 width=248)
   Merge Key: l_returnflag, l_linestatus
   ->  Sort  (cost=120573745.63..120573745.64 rows=3 width=248)
         Sort Key: partial_aggregation.l_returnflag, partial_aggregation.l_linestatus
         ->  HashAggregate  (cost=120573745.37..120573745.55 rows=3 width=248)
               Group By: lineitem.l_returnflag, lineitem.l_linestatus
               ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=120573744.95..120573745.16 rows=3 width=248)
                     Hash Key: lineitem.l_returnflag, lineitem.l_linestatus
                     ->  HashAggregate  (cost=120573744.95..120573745.04 rows=3 width=248)
                           Group By: lineitem.l_returnflag, lineitem.l_linestatus
                           ->  Seq Scan on lineitem  (cost=0.00..51767512.60 rows=1376124647 width=33)
                                 Filter: l_shipdate <= 'Mon Aug 17 00:00:00 1998'::timestamp without time zone
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(13 rows)

--
--
-- 02.txt
--
--
-- using 2133894592 as a seed to the RNG
select 'query 02' as current_query;
 current_query 
---------------
 query 02
(1 row)

explain select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 36
        and p_type like '%COPPER'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
LIMIT 100;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=23224951.08..23224951.20 rows=6 width=194)
   ->  Gather Motion 2:1  (slice7; segments: 2)  (cost=23224951.08..23224951.20 rows=3 width=194)
         Merge Key: tpch500gb.supplier.s_acctbal, tpch500gb.nation.n_name, tpch500gb.supplier.s_name, part.p_partkey
         ->  Limit  (cost=23224951.08..23224951.10 rows=3 width=194)
               ->  Sort  (cost=23224951.08..23224951.10 rows=3 width=194)
                     Sort Key (Limit): tpch500gb.supplier.s_acctbal, tpch500gb.nation.n_name, tpch500gb.supplier.s_name, part.p_partkey
                     ->  Hash Join  (cost=21035046.61..23224951.02 rows=3 width=194)
                           Hash Cond: tpch500gb.partsupp.ps_suppkey = tpch500gb.supplier.s_suppkey
                           ->  Redistribute Motion 2:2  (slice4; segments: 2)  (cost=20884242.76..23074147.05 rows=15 width=34)
                                 Hash Key: tpch500gb.partsupp.ps_suppkey
                                 ->  Hash Join  (cost=20884242.76..23074146.48 rows=15 width=34)
                                       Hash Cond: "Expr_SUBQUERY".csq_c0 = part.p_partkey AND "Expr_SUBQUERY".csq_c1 = tpch500gb.partsupp.ps_supplycost
                                       ->  HashAggregate  (cost=11710544.23..12855336.01 rows=34837052 width=36)
                                             Group By: tpch500gb.partsupp.ps_partkey
                                             ->  Hash Join  (cost=502478.00..11088308.22 rows=34837052 width=12)
                                                   Hash Cond: tpch500gb.partsupp.ps_suppkey = tpch500gb.supplier.s_suppkey
                                                   ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=16)
                                                   ->  Hash  (cost=305737.21..305737.21 rows=7869632 width=4)
                                                         ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=31.85..305737.21 rows=7869632 width=4)
                                                               ->  Hash Join  (cost=31.85..138507.54 rows=491852 width=4)
                                                                     Hash Cond: tpch500gb.supplier.s_nationkey = tpch500gb.nation.n_nationkey
                                                                     ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                                                     ->  Hash  (cost=30.85..30.85 rows=41 width=4)
                                                                           ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=5.43..30.85 rows=41 width=4)
                                                                                 ->  Hash Join  (cost=5.43..30.00 rows=3 width=4)
                                                                                       Hash Cond: tpch500gb.nation.n_regionkey = tpch500gb.region.r_regionkey
                                                                                       ->  Seq Scan on nation  (cost=0.00..24.25 rows=13 width=8)
                                                                                       ->  Hash  (cost=5.23..5.23 rows=9 width=4)
                                                                                             ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..5.23 rows=9 width=4)
                                                                                                   ->  Seq Scan on region  (cost=0.00..5.06 rows=1 width=4)
                                                                                                         Filter: r_name = 'EUROPE'::bpchar
                                       ->  Hash  (cost=9132243.60..9132243.60 rows=1381832 width=46)
                                             ->  Hash Join  (cost=1995572.80..9132243.60 rows=1381832 width=46)
                                                   Hash Cond: tpch500gb.partsupp.ps_partkey = part.p_partkey
                                                   ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=16)
                                                   ->  Hash  (cost=1986922.56..1986922.56 rows=346010 width=30)
                                                         ->  Seq Scan on part  (cost=0.00..1986922.56 rows=346010 width=30)
                                                               Filter: p_size = 36 AND p_type::text ~~ '%COPPER'::text
                           ->  Hash  (cost=138507.54..138507.54 rows=491852 width=168)
                                 ->  Hash Join  (cost=31.85..138507.54 rows=491852 width=168)
                                       Hash Cond: tpch500gb.supplier.s_nationkey = tpch500gb.nation.n_nationkey
                                       ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=146)
                                       ->  Hash  (cost=30.85..30.85 rows=41 width=30)
                                             ->  Broadcast Motion 2:2  (slice6; segments: 2)  (cost=5.43..30.85 rows=41 width=30)
                                                   ->  Hash Join  (cost=5.43..30.00 rows=3 width=30)
                                                         Hash Cond: tpch500gb.nation.n_regionkey = tpch500gb.region.r_regionkey
                                                         ->  Seq Scan on nation  (cost=0.00..24.25 rows=13 width=34)
                                                         ->  Hash  (cost=5.23..5.23 rows=9 width=4)
                                                               ->  Broadcast Motion 2:2  (slice5; segments: 2)  (cost=0.00..5.23 rows=9 width=4)
                                                                     ->  Seq Scan on region  (cost=0.00..5.06 rows=1 width=4)
                                                                           Filter: r_name = 'EUROPE'::bpchar
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(52 rows)

--
--
-- 03.txt
--
--
select 'query 03' as current_query;
 current_query 
---------------
 query 03
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-22'
	and l_shipdate > date '1995-03-22'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=136238292.45..136238292.68 rows=10 width=48)
   ->  Gather Motion 2:1  (slice2; segments: 2)  (cost=136238292.45..136238292.68 rows=5 width=48)
         Merge Key: revenue, orders.o_orderdate
         ->  Limit  (cost=136238292.45..136238292.48 rows=5 width=48)
               ->  Sort  (cost=136238292.45..136637393.84 rows=79820278 width=48)
                     Sort Key (Limit): revenue, orders.o_orderdate
                     ->  HashAggregate  (cost=92100736.51..96422256.22 rows=79820278 width=48)
                           Group By: lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority
                           ->  Hash Join  (cost=26219811.80..88976520.96 rows=79820278 width=32)
                                 Hash Cond: lineitem.l_orderkey = orders.o_orderkey
                                 ->  Seq Scan on lineitem  (cost=0.00..51767512.60 rows=820148525 width=24)
                                       Filter: l_shipdate > '03-22-1995'::date
                                 ->  Hash  (cost=25237564.70..25237564.70 rows=35794324 width=16)
                                       ->  Hash Join  (cost=6980243.51..25237564.70 rows=35794324 width=16)
                                             Hash Cond: orders.o_custkey = customer.c_custkey
                                             ->  Seq Scan on orders  (cost=0.00..12613430.80 rows=179773405 width=20)
                                                   Filter: o_orderdate < '03-22-1995'::date
                                             ->  Hash  (cost=3854360.45..3854360.45 rows=117034883 width=4)
                                                   ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..3854360.45 rows=117034883 width=4)
                                                         ->  Seq Scan on customer  (cost=0.00..1367369.20 rows=7314681 width=4)
                                                               Filter: c_mktsegment = 'AUTOMOBILE'::bpchar
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(22 rows)

--
--
-- 04.txt
--
--
select 'query 04' as current_query;
 current_query 
---------------
 query 04
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1996-07-01'
        and o_orderdate < date '1996-07-01' + interval '3 month'
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice2; segments: 2)  (cost=79166496.69..79166496.69 rows=1 width=72)
   Merge Key: o_orderpriority
   ->  Sort  (cost=79166496.69..79166496.69 rows=1 width=72)
         Sort Key: partial_aggregation.o_orderpriority
         ->  HashAggregate  (cost=79166496.66..79166496.68 rows=1 width=72)
               Group By: orders.o_orderpriority
               ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=79166496.63..79166496.65 rows=1 width=72)
                     Hash Key: orders.o_orderpriority
                     ->  HashAggregate  (cost=79166496.63..79166496.63 rows=1 width=72)
                           Group By: orders.o_orderpriority
                           ->  Result  (cost=78801360.21..78983928.42 rows=18256821 width=22)
                                 ->  Unique  (cost=78801360.21..78983928.42 rows=18256821 width=22)
                                       Group By: orders.ctid
                                       ->  Sort  (cost=78801360.21..78892644.32 rows=18256821 width=22)
                                             Sort Key (Distinct): orders.ctid
                                             ->  Hash Join  (cost=14835362.82..71460313.52 rows=18256821 width=22)
                                                   Hash Cond: lineitem.l_orderkey = orders.o_orderkey
                                                   ->  Seq Scan on lineitem  (cost=0.00..51767512.60 rows=489638315 width=8)
                                                         Filter: l_commitdate < l_receiptdate
                                                   ->  Hash  (cost=14452353.36..14452353.36 rows=13713339 width=30)
                                                         ->  Seq Scan on orders  (cost=0.00..14452353.36 rows=13713339 width=30)
                                                               Filter: o_orderdate >= '07-01-1996'::date AND o_orderdate < 'Tue Oct 01 00:00:00 1996'::timestamp without time zone
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(23 rows)

--
--
-- 05.txt
--
--
select 'query 05' as current_query;
 current_query 
---------------
 query 05
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
count(*)
--	n_name,
--	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= date '1993-01-01'
	and o_orderdate < date '1993-01-01' + interval '1 year'
--group by
--	n_name
--order by
--	revenue desc
;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=82836950.11..82836950.12 rows=1 width=8)
   ->  Gather Motion 2:1  (slice6; segments: 2)  (cost=82836949.91..82836950.09 rows=1 width=8)
         ->  Aggregate  (cost=82836949.91..82836949.92 rows=1 width=8)
               ->  Hash Join  (cost=22440953.76..82828342.64 rows=1721454 width=0)
                     Hash Cond: customer.c_nationkey = supplier.s_nationkey AND lineitem.l_suppkey = supplier.s_suppkey
                     ->  Hash Join  (cost=20270514.36..79631456.28 rows=44442733 width=12)
                           Hash Cond: lineitem.l_orderkey = orders.o_orderkey
                           ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=12)
                           ->  Hash  (cost=19965159.90..19965159.90 rows=11127499 width=16)
                                 ->  Redistribute Motion 2:2  (slice4; segments: 2)  (cost=2285832.40..19965159.90 rows=11127499 width=16)
                                       Hash Key: orders.o_orderkey
                                       ->  Hash Join  (cost=2285832.40..19520059.96 rows=11127499 width=16)
                                             Hash Cond: orders.o_custkey = customer.c_custkey
                                             ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=0.00..16677852.68 rows=55637484 width=12)
                                                   Hash Key: orders.o_custkey
                                                   ->  Seq Scan on orders  (cost=0.00..14452353.36 rows=55637484 width=12)
                                                         Filter: o_orderdate >= '01-01-1993'::date AND o_orderdate < 'Sat Jan 01 00:00:00 1994'::timestamp without time zone
                                             ->  Hash  (cost=2102146.13..2102146.13 rows=7347451 width=12)
                                                   ->  Hash Join  (cost=31.85..2102146.13 rows=7347451 width=12)
                                                         Hash Cond: customer.c_nationkey = nation.n_nationkey
                                                         ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=8)
                                                         ->  Hash  (cost=30.85..30.85 rows=41 width=4)
                                                               ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=5.43..30.85 rows=41 width=4)
                                                                     ->  Hash Join  (cost=5.43..30.00 rows=3 width=4)
                                                                           Hash Cond: nation.n_regionkey = region.r_regionkey
                                                                           ->  Seq Scan on nation  (cost=0.00..24.25 rows=13 width=8)
                                                                           ->  Hash  (cost=5.23..5.23 rows=9 width=4)
                                                                                 ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=0.00..5.23 rows=9 width=4)
                                                                                       ->  Seq Scan on region  (cost=0.00..5.06 rows=1 width=4)
                                                                                             Filter: r_name = 'ASIA'::bpchar
                     ->  Hash  (cost=913142.60..913142.60 rows=39348160 width=8)
                           ->  Broadcast Motion 2:2  (slice5; segments: 2)  (cost=0.00..913142.60 rows=39348160 width=8)
                                 ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(34 rows)

--
--
-- 06.txt
--
--
select 'query 06' as current_query;
 current_query 
---------------
 query 06
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1993-01-01'
	and l_shipdate < date '1993-01-01' + interval '1 year'
	and l_discount between 0.02 - 0.01 and 0.02 + 0.01
	and l_quantity < 25;
                                                                                                    QUERY PLAN                                                                                                    
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=81285244.19..81285244.20 rows=1 width=32)
   ->  Gather Motion 2:1  (slice1; segments: 2)  (cost=81285243.99..81285244.18 rows=1 width=32)
         ->  Aggregate  (cost=81285243.99..81285244.01 rows=1 width=32)
               ->  Seq Scan on lineitem  (cost=0.00..81145811.48 rows=27886503 width=16)
                     Filter: l_shipdate >= '01-01-1993'::date AND l_shipdate < 'Sat Jan 01 00:00:00 1994'::timestamp without time zone AND l_discount >= 0.01 AND l_discount <= 0.03 AND l_quantity < 25::numeric
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(6 rows)

--
--
-- 07.txt
--
--
select 'query 07' as current_query;
 current_query 
---------------
 query 07
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'ALGERIA' and n2.n_name = 'ROMANIA')
				or (n1.n_name = 'ROMANIA' and n2.n_name = 'ALGERIA')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
                                                                                                                   QUERY PLAN                                                                                                                    
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice6; segments: 2)  (cost=85867195.23..85868345.74 rows=230102 width=248)
   Merge Key: supp_nation, cust_nation, l_year
   ->  Sort  (cost=85867195.23..85868345.74 rows=230102 width=248)
         Sort Key: partial_aggregation.supp_nation, partial_aggregation.cust_nation, partial_aggregation.unnamed_attr_3
         ->  HashAggregate  (cost=85818156.09..85823908.64 rows=230102 width=248)
               Group By: n1.n_name, n2.n_name, "?column3?"
               ->  Redistribute Motion 2:2  (slice5; segments: 2)  (cost=85795145.89..85808952.01 rows=230102 width=248)
                     Hash Key: n1.n_name, n2.n_name, unnamed_attr_3
                     ->  HashAggregate  (cost=85795145.89..85799747.93 rows=230102 width=248)
                           Group By: n1.n_name, n2.n_name, date_part('year'::text, lineitem.l_shipdate::timestamp without time zone)
                           ->  Hash Join  (cost=68275249.57..85768515.92 rows=1331499 width=72)
                                 Hash Cond: n2.n_nationkey = customer.c_nationkey AND orders.o_custkey = customer.c_custkey
                                 ->  Redistribute Motion 2:2  (slice4; segments: 2)  (cost=65917696.17..82530478.96 rows=33287462 width=80)
                                       Hash Key: orders.o_custkey
                                       ->  Hash Join  (cost=65917696.17..81198980.50 rows=33287462 width=80)
                                             Hash Cond: orders.o_orderkey = lineitem.l_orderkey
                                             ->  Seq Scan on orders  (cost=0.00..10774508.24 rows=367784512 width=12)
                                             ->  Hash  (cost=64866084.63..64866084.63 rows=33287462 width=84)
                                                   ->  Hash Join  (cost=275594.17..64866084.63 rows=33287462 width=84)
                                                         Hash Cond: lineitem.l_suppkey = supplier.s_suppkey
                                                         ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=429931399 width=32)
                                                               Filter: l_shipdate >= '01-01-1995'::date AND l_shipdate <= '12-31-1996'::date
                                                         ->  Hash  (cost=196960.82..196960.82 rows=3145334 width=60)
                                                               ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=354.87..196960.82 rows=3145334 width=60)
                                                                     ->  Hash Join  (cost=354.87..130122.47 rows=196584 width=60)
                                                                           Hash Cond: supplier.s_nationkey = n1.n_nationkey
                                                                           ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                                                           ->  Hash  (cost=353.94..353.94 rows=38 width=60)
                                                                                 ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=28.90..353.94 rows=38 width=60)
                                                                                       ->  Nested Loop  (cost=28.90..353.15 rows=3 width=60)
                                                                                             Join Filter: (n1.n_name = 'ALGERIA'::bpchar AND n2.n_name = 'ROMANIA'::bpchar) OR (n1.n_name = 'ROMANIA'::bpchar AND n2.n_name = 'ALGERIA'::bpchar)
                                                                                             ->  Seq Scan on nation n1  (cost=0.00..24.25 rows=13 width=30)
                                                                                             ->  Materialize  (cost=28.90..32.90 rows=200 width=30)
                                                                                                   ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..28.50 rows=200 width=30)
                                                                                                         ->  Seq Scan on nation n2  (cost=0.00..24.25 rows=13 width=30)
                                 ->  Hash  (cost=1183682.96..1183682.96 rows=36737248 width=8)
                                       ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=8)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(38 rows)

--
--
-- 08.txt
--
--
select 'query 08' as current_query;
 current_query 
---------------
 query 08
(1 row)

-- using 2133894592 as a seed to the RNG
-- set enable_nestloop=off;
explain select
	o_year,
	sum(case
		when nation = 'ROMANIA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'EUROPE'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY BRUSHED COPPER'
	) as all_nations
group by
	o_year
order by
	o_year;
                                                                                        QUERY PLAN                                                                                         
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice8; segments: 2)  (cost=72596753.00..72596754.83 rows=367 width=40)
   Merge Key: o_year
   ->  Sort  (cost=72596753.00..72596754.83 rows=367 width=40)
         Sort Key: partial_aggregation.unnamed_attr_1
         ->  HashAggregate  (cost=72596705.29..72596718.11 rows=367 width=40)
               Group By: "?column1?"
               ->  Redistribute Motion 2:2  (slice7; segments: 2)  (cost=72596664.97..72596692.46 rows=367 width=72)
                     Hash Key: unnamed_attr_1
                     ->  HashAggregate  (cost=72596664.97..72596677.80 rows=367 width=72)
                           Group By: date_part('year'::text, orders.o_orderdate::timestamp without time zone)
                           ->  Hash Join  (cost=70968934.45..72588166.93 rows=566537 width=46)
                                 Hash Cond: supplier.s_nationkey = n2.n_nationkey
                                 ->  Hash Join  (cost=70968900.95..72522981.77 rows=566537 width=24)
                                       Hash Cond: lineitem.l_suppkey = supplier.s_suppkey
                                       ->  Redistribute Motion 2:2  (slice5; segments: 2)  (cost=70830425.25..72367418.12 rows=584910 width=24)
                                             Hash Key: lineitem.l_suppkey
                                             ->  Hash Join  (cost=70830425.25..72344021.75 rows=584910 width=24)
                                                   Hash Cond: customer.c_nationkey = n1.n_nationkey
                                                   ->  Hash Join  (cost=70830393.41..72270876.25 rows=2924546 width=28)
                                                         Hash Cond: customer.c_custkey = orders.o_custkey
                                                         ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=8)
                                                         ->  Hash  (cost=70757279.76..70757279.76 rows=2924546 width=28)
                                                               ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=54977627.99..70757279.76 rows=2924546 width=28)
                                                                     Hash Key: orders.o_custkey
                                                                     ->  Hash Join  (cost=54977627.99..70640297.93 rows=2924546 width=28)
                                                                           Hash Cond: orders.o_orderkey = lineitem.l_orderkey
                                                                           ->  Seq Scan on orders  (cost=0.00..14452353.36 rows=111983987 width=16)
                                                                                 Filter: o_orderdate >= '01-01-1995'::date AND o_orderdate <= '12-31-1996'::date
                                                                           ->  Hash  (cost=54707018.79..54707018.79 rows=9604969 width=28)
                                                                                 ->  Hash Join  (cost=1979009.36..54707018.79 rows=9604969 width=28)
                                                                                       Hash Cond: lineitem.l_partkey = part.p_partkey
                                                                                       ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=32)
                                                                                       ->  Hash  (cost=1850673.92..1850673.92 rows=5133418 width=4)
                                                                                             ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..1850673.92 rows=5133418 width=4)
                                                                                                   ->  Seq Scan on part  (cost=0.00..1741588.80 rows=320839 width=4)
                                                                                                         Filter: p_type::text = 'ECONOMY BRUSHED COPPER'::text
                                                   ->  Hash  (cost=30.85..30.85 rows=41 width=4)
                                                         ->  Broadcast Motion 2:2  (slice4; segments: 2)  (cost=5.43..30.85 rows=41 width=4)
                                                               ->  Hash Join  (cost=5.43..30.00 rows=3 width=4)
                                                                     Hash Cond: n1.n_regionkey = region.r_regionkey
                                                                     ->  Seq Scan on nation n1  (cost=0.00..24.25 rows=13 width=8)
                                                                     ->  Hash  (cost=5.23..5.23 rows=9 width=4)
                                                                           ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=0.00..5.23 rows=9 width=4)
                                                                                 ->  Seq Scan on region  (cost=0.00..5.06 rows=1 width=4)
                                                                                       Filter: r_name = 'EUROPE'::bpchar
                                       ->  Hash  (cost=76994.20..76994.20 rows=2459260 width=8)
                                             ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                 ->  Hash  (cost=28.50..28.50 rows=200 width=30)
                                       ->  Broadcast Motion 2:2  (slice6; segments: 2)  (cost=0.00..28.50 rows=200 width=30)
                                             ->  Seq Scan on nation n2  (cost=0.00..24.25 rows=13 width=30)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(51 rows)

--set enable_nestloop=on;
--
--
-- 09.txt
--
--
select 'query 09' as current_query;
 current_query 
---------------
 query 09
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%seashell%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
                                                                                              QUERY PLAN                                                                                               
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice7; segments: 2)  (cost=74955700.97..74955701.85 rows=14 width=144)
   Merge Key: nation, o_year
   ->  GroupAggregate  (cost=74955700.97..74955701.85 rows=14 width=144)
         Group By: nation.n_name, "?column2?"
         ->  Sort  (cost=74955700.97..74955701.04 rows=14 width=144)
               Sort Key: nation.n_name, unnamed_attr_2
               ->  Redistribute Motion 2:2  (slice6; segments: 2)  (cost=74955698.77..74955700.33 rows=14 width=144)
                     Hash Key: nation.n_name, "?column2?"
                     ->  GroupAggregate  (cost=74955698.77..74955699.79 rows=14 width=144)
                           Group By: nation.n_name, "?column7?"
                           ->  Sort  (cost=74955698.77..74955698.84 rows=14 width=61)
                                 Sort Key: nation.n_name, "?column7?"
                                 ->  Hash Join  (cost=62342266.81..74955698.10 rows=14 width=61)
                                       Hash Cond: orders.o_orderkey = lineitem.l_orderkey
                                       ->  Seq Scan on orders  (cost=0.00..10774508.24 rows=367784512 width=12)
                                       ->  Hash  (cost=62342266.46..62342266.46 rows=14 width=65)
                                             ->  Redistribute Motion 2:2  (slice5; segments: 2)  (cost=54260368.51..62342266.46 rows=14 width=65)
                                                   Hash Key: lineitem.l_orderkey
                                                   ->  Hash Join  (cost=54260368.51..62342265.90 rows=14 width=65)
                                                         Hash Cond: supplier.s_nationkey = nation.n_nationkey
                                                         ->  Redistribute Motion 2:2  (slice4; segments: 2)  (cost=54260343.95..62342240.92 rows=14 width=43)
                                                               Hash Key: supplier.s_nationkey
                                                               ->  Hash Join  (cost=54260343.95..62342240.36 rows=14 width=43)
                                                                     Hash Cond: partsupp.ps_suppkey = supplier.s_suppkey
                                                                     ->  Redistribute Motion 2:2  (slice3; segments: 2)  (cost=54121868.25..62203764.23 rows=16 width=47)
                                                                           Hash Key: lineitem.l_suppkey
                                                                           ->  Hash Join  (cost=54121868.25..62203763.60 rows=16 width=47)
                                                                                 Hash Cond: partsupp.ps_partkey = part.p_partkey AND partsupp.ps_suppkey = lineitem.l_suppkey
                                                                                 ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=16)
                                                                                 ->  Hash  (cost=54027460.15..54027460.15 rows=3146937 width=43)
                                                                                       ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=1819376.40..54027460.15 rows=3146937 width=43)
                                                                                             Hash Key: lineitem.l_partkey
                                                                                             ->  Hash Join  (cost=1819376.40..53901582.68 rows=3146937 width=43)
                                                                                                   Hash Cond: lineitem.l_partkey = part.p_partkey
                                                                                                   ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=39)
                                                                                                   ->  Hash  (cost=1777329.05..1777329.05 rows=1681895 width=4)
                                                                                                         ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..1777329.05 rows=1681895 width=4)
                                                                                                               ->  Seq Scan on part  (cost=0.00..1741588.80 rows=105119 width=4)
                                                                                                                     Filter: p_name::text ~~ '%seashell%'::text
                                                                     ->  Hash  (cost=76994.20..76994.20 rows=2459260 width=8)
                                                                           ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                                         ->  Hash  (cost=24.25..24.25 rows=13 width=30)
                                                               ->  Seq Scan on nation  (cost=0.00..24.25 rows=13 width=30)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(44 rows)

--
--
-- 10.txt
--
--
select 'query 10' as current_query;
 current_query 
---------------
 query 10
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-06-01'
	and o_orderdate < date '1993-06-01' + interval '3 month'
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=111294577.28..111294577.73 rows=20 width=638)
   ->  Gather Motion 2:1  (slice3; segments: 2)  (cost=111294577.28..111294577.73 rows=10 width=638)
         Merge Key: revenue
         ->  Limit  (cost=111294577.28..111294577.33 rows=10 width=638)
               ->  Sort  (cost=111294577.28..111362281.64 rows=13540873 width=638)
                     Sort Key (Limit): revenue
                     ->  HashAggregate  (cost=77980916.12..79689261.18 rows=13540873 width=638)
                           Group By: customer.c_custkey, customer.c_name, customer.c_acctbal, customer.c_phone, nation.n_name, customer.c_address, customer.c_comment
                           ->  Hash Join  (cost=17325738.21..76204866.69 rows=13540873 width=188)
                                 Hash Cond: customer.c_nationkey = nation.n_nationkey
                                 ->  Hash Join  (cost=17325704.71..74783041.55 rows=13540872 width=166)
                                       Hash Cond: orders.o_custkey = customer.c_custkey
                                       ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=14828951.55..71412693.23 rows=13540872 width=20)
                                             Hash Key: orders.o_custkey
                                             ->  Hash Join  (cost=14828951.55..70871058.36 rows=13540872 width=20)
                                                   Hash Cond: lineitem.l_orderkey = orders.o_orderkey
                                                   ->  Seq Scan on lineitem  (cost=0.00..51767512.60 rows=359656484 width=24)
                                                         Filter: l_returnflag = 'R'::bpchar
                                                   ->  Hash  (cost=14452353.36..14452353.36 rows=13846888 width=12)
                                                         ->  Seq Scan on orders  (cost=0.00..14452353.36 rows=13846888 width=12)
                                                               Filter: o_orderdate >= '06-01-1993'::date AND o_orderdate < 'Wed Sep 01 00:00:00 1993'::timestamp without time zone
                                       ->  Hash  (cost=1183682.96..1183682.96 rows=36737248 width=150)
                                             ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=150)
                                 ->  Hash  (cost=28.50..28.50 rows=200 width=30)
                                       ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=0.00..28.50 rows=200 width=30)
                                             ->  Seq Scan on nation  (cost=0.00..24.25 rows=13 width=30)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(27 rows)

--
--
-- 11.txt
--
--
select 'query 11' as current_query;
 current_query 
---------------
 query 11
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDIA'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0000001000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'INDIA'
		)
order by
	value desc;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice6; segments: 2)  (cost=19599154.59..19633991.64 rows=6967410 width=36)
   Merge Key: value
   ->  Sort  (cost=19599154.59..19633991.64 rows=6967410 width=36)
         Sort Key: value
         InitPlan  (slice7)
           ->  Aggregate  (cost=8005649.55..8005649.56 rows=1 width=32)
                 ->  Gather Motion 2:1  (slice5; segments: 2)  (cost=8005649.35..8005649.54 rows=1 width=32)
                       ->  Aggregate  (cost=8005649.35..8005649.37 rows=1 width=32)
                             ->  Hash Join  (cost=171946.31..7970812.30 rows=6967410 width=12)
                                   Hash Cond: tpch500gb.partsupp.ps_suppkey = tpch500gb.supplier.s_suppkey
                                   ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=16)
                                   ->  Hash  (cost=132598.15..132598.15 rows=1573927 width=4)
                                         ->  Broadcast Motion 2:2  (slice4; segments: 2)  (cost=24.68..132598.15 rows=1573927 width=4)
                                               ->  Hash Join  (cost=24.68..99152.22 rows=98371 width=4)
                                                     Hash Cond: tpch500gb.supplier.s_nationkey = tpch500gb.nation.n_nationkey
                                                     ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                                     ->  Hash  (cost=24.48..24.48 rows=8 width=4)
                                                           ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=0.00..24.48 rows=8 width=4)
                                                                 ->  Seq Scan on nation  (cost=0.00..24.31 rows=1 width=4)
                                                                       Filter: n_name = 'INDIA'::bpchar
         ->  HashAggregate  (cost=8159014.01..8625912.10 rows=6967410 width=36)
               Filter: sum(tpch500gb.partsupp.ps_supplycost * tpch500gb.partsupp.ps_availqty::numeric) > $0
               Group By: tpch500gb.partsupp.ps_partkey
               ->  Hash Join  (cost=171946.31..7970812.30 rows=6967410 width=16)
                     Hash Cond: tpch500gb.partsupp.ps_suppkey = tpch500gb.supplier.s_suppkey
                     ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=20)
                     ->  Hash  (cost=132598.15..132598.15 rows=1573927 width=4)
                           ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=24.68..132598.15 rows=1573927 width=4)
                                 ->  Hash Join  (cost=24.68..99152.22 rows=98371 width=4)
                                       Hash Cond: tpch500gb.supplier.s_nationkey = tpch500gb.nation.n_nationkey
                                       ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=8)
                                       ->  Hash  (cost=24.48..24.48 rows=8 width=4)
                                             ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..24.48 rows=8 width=4)
                                                   ->  Seq Scan on nation  (cost=0.00..24.31 rows=1 width=4)
                                                         Filter: n_name = 'INDIA'::bpchar
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(36 rows)

--
--
-- 12.txt
--
--
select 'query 12' as current_query;
 current_query 
---------------
 query 12
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('AIR', 'MAIL')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1997-01-01'
	and l_receiptdate < date '1997-01-01' + interval '1 year'
group by
	l_shipmode
order by
	l_shipmode;
                                                                                                                                     QUERY PLAN                                                                                                                                     
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice2; segments: 2)  (cost=94177104.73..94177104.74 rows=1 width=60)
   Merge Key: l_shipmode
   ->  Sort  (cost=94177104.73..94177104.74 rows=1 width=60)
         Sort Key: partial_aggregation.l_shipmode
         ->  HashAggregate  (cost=94177104.71..94177104.72 rows=1 width=60)
               Group By: lineitem.l_shipmode
               ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=94177104.66..94177104.69 rows=1 width=60)
                     Hash Key: lineitem.l_shipmode
                     ->  HashAggregate  (cost=94177104.66..94177104.67 rows=1 width=60)
                           Group By: lineitem.l_shipmode
                           ->  Hash Join  (cost=81306527.78..94080674.88 rows=6428653 width=27)
                                 Hash Cond: orders.o_orderkey = lineitem.l_orderkey
                                 ->  Seq Scan on orders  (cost=0.00..10774508.24 rows=367784512 width=24)
                                 ->  Hash  (cost=81145811.48..81145811.48 rows=6428653 width=19)
                                       ->  Seq Scan on lineitem  (cost=0.00..81145811.48 rows=6428653 width=19)
                                             Filter: (l_shipmode = ANY ('{AIR,MAIL}'::bpchar[])) AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '01-01-1997'::date AND l_receiptdate < 'Thu Jan 01 00:00:00 1998'::timestamp without time zone
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(17 rows)

--
--
-- 13.txt
--
--
select 'query 13' as current_query;
 current_query 
---------------
 query 13
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%unusual%deposits%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice4; segments: 2)  (cost=62485181.31..62485183.81 rows=500 width=16)
   Merge Key: custdist, c_count
   ->  Sort  (cost=62485181.31..62485183.81 rows=500 width=16)
         Sort Key: custdist, partial_aggregation.c_count
         ->  HashAggregate  (cost=62485118.98..62485131.48 rows=500 width=16)
               Group By: c_orders.c_count
               ->  Redistribute Motion 2:2  (slice3; segments: 2)  (cost=62485083.98..62485103.98 rows=500 width=16)
                     Hash Key: c_orders.c_count
                     ->  HashAggregate  (cost=62485083.98..62485083.98 rows=500 width=16)
                           Group By: c_orders.c_count
                           ->  Subquery Scan c_orders  (cost=60283360.43..62117711.50 rows=36737248 width=8)
                                 ->  HashAggregate  (cost=60283360.43..61382966.54 rows=36737248 width=12)
                                       Group By: customer.c_custkey
                                       ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=52476943.84..59000068.09 rows=36737248 width=12)
                                             Hash Key: customer.c_custkey
                                             ->  HashAggregate  (cost=52476943.84..57530578.17 rows=36737248 width=12)
                                                   Group By: customer.c_custkey
                                                   ->  Hash Left Join  (cost=37312021.19..48801408.85 rows=367553499 width=12)
                                                         Hash Cond: customer.c_custkey = orders.o_custkey
                                                         ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=4)
                                                         ->  Hash  (cost=27315570.74..27315570.74 rows=367553499 width=12)
                                                               ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=0.00..27315570.74 rows=367553499 width=12)
                                                                     Hash Key: orders.o_custkey
                                                                     ->  Seq Scan on orders  (cost=0.00..12613430.80 rows=367553499 width=12)
                                                                           Filter: o_comment::text !~~ '%unusual%deposits%'::text
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(26 rows)

--
--
-- 14.txt
--
--
select 'query 14' as current_query;
 current_query 
---------------
 query 14
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1997-08-01'
	and l_shipdate < date '1997-08-01' + interval '1 month';
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=63693203.44..63693203.46 rows=1 width=32)
   ->  Gather Motion 2:1  (slice2; segments: 2)  (cost=63693203.23..63693203.42 rows=1 width=64)
         ->  Aggregate  (cost=63693203.23..63693203.25 rows=1 width=64)
               ->  Hash Join  (cost=2878653.84..63512025.97 rows=18117726 width=37)
                     Hash Cond: lineitem.l_partkey = part.p_partkey
                     ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=0.00..59836796.35 rows=18117726 width=20)
                           Hash Key: lineitem.l_partkey
                           ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=18117726 width=20)
                                 Filter: l_shipdate >= '08-01-1997'::date AND l_shipdate < 'Mon Sep 01 00:00:00 1997'::timestamp without time zone
                     ->  Hash  (cost=1496255.04..1496255.04 rows=49066752 width=25)
                           ->  Seq Scan on part  (cost=0.00..1496255.04 rows=49066752 width=25)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(12 rows)

--
--
-- 15.txt
--
--
select 'query 15' as current_query;
 current_query 
---------------
 query 15
(1 row)

-- using 2133894592 as a seed to the RNG
create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1997-08-01'
		and l_shipdate < date '1997-08-01' + interval '3 month'
	group by
		l_suppkey;
ERROR:  relation "revenue0" already exists
explain select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	s_suppkey;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice4; segments: 2)  (cost=119640040.92..119640484.36 rows=88688 width=103)
   Merge Key: s_suppkey
   ->  Sort  (cost=119640040.92..119640484.36 rows=88688 width=103)
         Sort Key: supplier.s_suppkey
         InitPlan  (slice5)
           ->  Aggregate  (cost=59636709.51..59636709.52 rows=1 width=32)
                 ->  Gather Motion 2:1  (slice3; segments: 2)  (cost=59636709.31..59636709.49 rows=1 width=32)
                       ->  Aggregate  (cost=59636709.31..59636709.32 rows=1 width=32)
                             ->  Subquery Scan revenue0  (cost=59632274.91..59636265.87 rows=88688 width=32)
                                   ->  HashAggregate  (cost=59632274.91..59634492.11 rows=88688 width=36)
                                         Group By: tpch500gb.lineitem.l_suppkey
                                         ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=59625179.87..59629614.27 rows=88688 width=36)
                                               Hash Key: tpch500gb.lineitem.l_suppkey
                                               ->  HashAggregate  (cost=59625179.87..59626066.75 rows=88688 width=36)
                                                     Group By: tpch500gb.lineitem.l_suppkey
                                                     ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=51309255 width=20)
                                                           Filter: l_shipdate >= '01-01-1993'::date AND l_shipdate < 'Thu Apr 01 00:00:00 1993'::timestamp without time zone
         ->  Hash Join  (cost=59896359.66..59987867.36 rows=88688 width=103)
               Hash Cond: supplier.s_suppkey = revenue0.supplier_no
               ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=71)
               ->  Hash  (cost=59894142.46..59894142.46 rows=88688 width=36)
                     ->  Subquery Scan revenue0  (cost=59889264.62..59894142.46 rows=88688 width=36)
                           ->  HashAggregate  (cost=59889264.62..59892368.70 rows=88688 width=36)
                                 Filter: sum(partial_aggregation.unnamed_attr_2) = $0
                                 Group By: tpch500gb.lineitem.l_suppkey
                                 ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=59881726.14..59886160.54 rows=88688 width=36)
                                       Hash Key: tpch500gb.lineitem.l_suppkey
                                       ->  HashAggregate  (cost=59881726.14..59882613.02 rows=88688 width=36)
                                             Group By: tpch500gb.lineitem.l_suppkey
                                             ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=51309255 width=20)
                                                   Filter: l_shipdate >= '01-01-1993'::date AND l_shipdate < 'Thu Apr 01 00:00:00 1993'::timestamp without time zone
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(32 rows)

drop view revenue0;
--
--
-- 16.txt
--
--
select 'query 16' as current_query;
 current_query 
---------------
 query 16
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#15'
	and p_type not like 'LARGE BRUSHED%'
	and p_size in (4, 41, 17, 9, 15, 2, 25, 34)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
                                                                                             QUERY PLAN                                                                                             
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice3; segments: 2)  (cost=18321628.07..18321690.94 rows=12575 width=124)
   Merge Key: supplier_cnt, p_brand, p_type, p_size
   ->  Sort  (cost=18321628.07..18321690.94 rows=12575 width=124)
         Sort Key: supplier_cnt, partial_aggregation.p_brand, partial_aggregation.p_type, partial_aggregation.p_size
         ->  HashAggregate  (cost=18319475.44..18319789.82 rows=12575 width=124)
               Group By: partial_aggregation.p_brand, partial_aggregation.p_type, partial_aggregation.p_size
               ->  HashAggregate  (cost=16308198.92..17268149.68 rows=26283144 width=120)
                     Group By: part.p_brand, part.p_type, part.p_size, partsupp.ps_suppkey
                     ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=13114180.91..14822585.27 rows=26283144 width=120)
                           Hash Key: part.p_brand, part.p_type, part.p_size
                           ->  HashAggregate  (cost=13114180.91..13771259.51 rows=26283144 width=120)
                                 Group By: part.p_brand, part.p_type, part.p_size, partsupp.ps_suppkey
                                 ->  Hash Left Anti Semi Join (Not-In)  (cost=3248727.04..12457102.30 rows=26283145 width=40)
                                       Hash Cond: partsupp.ps_suppkey = "NotIn_SUBQUERY".s_suppkey
                                       ->  Hash Join  (cost=3158498.68..11708857.42 rows=26283150 width=40)
                                             Hash Cond: partsupp.ps_partkey = part.p_partkey
                                             ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=8)
                                                   Filter: ps_suppkey IS NOT NULL
                                             ->  Hash  (cost=2968257.60..2968257.60 rows=6581284 width=40)
                                                   ->  Seq Scan on part  (cost=0.00..2968257.60 rows=6581284 width=40)
                                                         Filter: p_brand <> 'Brand#15'::bpchar AND p_type::text !~~ 'LARGE BRUSHED%'::text AND (p_size = ANY ('{4,41,17,9,15,2,25,34}'::integer[]))
                                       ->  Hash  (cost=89734.75..89734.75 rows=19745 width=4)
                                             ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..89734.75 rows=19745 width=4)
                                                   ->  Subquery Scan "NotIn_SUBQUERY"  (cost=0.00..89315.18 rows=1235 width=4)
                                                         ->  Seq Scan on supplier  (cost=0.00..89290.50 rows=1235 width=4)
                                                               Filter: s_comment::text ~~ '%Customer%Complaints%'::text
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(27 rows)

--
--
-- 17.txt
--
--
select 'query 17' as current_query;
 current_query 
---------------
 query 17
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#54'
	and p_container = 'SM CAN'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=135605249.61..135605249.63 rows=1 width=32)
   ->  Gather Motion 2:1  (slice3; segments: 2)  (cost=135605249.42..135605249.60 rows=1 width=32)
         ->  Aggregate  (cost=135605249.42..135605249.43 rows=1 width=32)
               ->  Hash Join  (cost=83692089.41..135602741.30 rows=501624 width=10)
                     Hash Cond: tpch500gb.lineitem.l_partkey = part.p_partkey
                     Join Filter: tpch500gb.lineitem.l_quantity < "Expr_SUBQUERY".csq_c1
                     ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=21)
                     ->  Hash  (cost=83680973.86..83680973.86 rows=444622 width=40)
                           ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=81939234.53..83680973.86 rows=444622 width=40)
                                 ->  Hash Join  (cost=81939234.53..83671525.65 rows=27789 width=40)
                                       Hash Cond: "Expr_SUBQUERY".csq_c0 = part.p_partkey
                                       ->  HashAggregate  (cost=79951055.27..81004529.17 rows=27124900 width=36)
                                             Group By: tpch500gb.lineitem.l_partkey
                                             ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=59112087.32..78897581.37 rows=27124900 width=36)
                                                   Hash Key: tpch500gb.lineitem.l_partkey
                                                   ->  HashAggregate  (cost=59112087.32..77812585.37 rows=27124900 width=36)
                                                         Group By: tpch500gb.lineitem.l_partkey
                                                         ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=11)
                                       ->  Hash  (cost=1986922.56..1986922.56 rows=50268 width=4)
                                             ->  Seq Scan on part  (cost=0.00..1986922.56 rows=50268 width=4)
                                                   Filter: p_brand = 'Brand#54'::bpchar AND p_container = 'SM CAN'::bpchar
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(22 rows)

--
--
-- 18.txt
--
--
select 'query 18' as current_query;
 current_query 
---------------
 query 18
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
LIMIT 100;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=510594737.04..510594742.04 rows=100 width=132)
   ->  Gather Motion 2:1  (slice3; segments: 2)  (cost=510594737.04..510594742.04 rows=50 width=132)
         Merge Key: orders.o_totalprice, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
         ->  Limit  (cost=510594737.04..510594740.04 rows=50 width=132)
               ->  GroupAggregate  (cost=510594737.04..544516358.94 rows=565360365 width=132)
                     Group By: orders.o_totalprice, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
                     ->  Sort  (cost=510594737.04..513421538.87 rows=565360366 width=52)
                           Sort Key: orders.o_totalprice, orders.o_orderdate, customer.c_name, customer.c_custkey, orders.o_orderkey
                           ->  Hash Join  (cost=131739869.36..205505011.09 rows=565360366 width=52)
                                 Hash Cond: tpch500gb.lineitem.l_orderkey = orders.o_orderkey
                                 ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=15)
                                 ->  Hash  (cost=127509837.36..127509837.36 rows=141554000 width=53)
                                       ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=92031170.56..127509837.36 rows=141554000 width=53)
                                             Hash Key: orders.o_orderkey
                                             ->  Hash Join  (cost=92031170.56..121847677.36 rows=141554000 width=53)
                                                   Hash Cond: orders.o_custkey = customer.c_custkey
                                                   ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=89821427.40..114246911.20 rows=141554000 width=34)
                                                         Hash Key: orders.o_custkey
                                                         ->  Hash Join  (cost=89821427.40..108584751.20 rows=141554000 width=34)
                                                               Hash Cond: orders.o_orderkey = "IN_subquery".l_orderkey
                                                               ->  Seq Scan on orders  (cost=0.00..10774508.24 rows=367784512 width=26)
                                                               ->  Hash  (cost=86006104.40..86006104.40 rows=141554000 width=8)
                                                                     ->  HashAggregate  (cost=69020245.86..83175024.40 rows=141554000 width=8)
                                                                           Filter: sum(tpch500gb.lineitem.l_quantity) > 314::numeric
                                                                           Group By: tpch500gb.lineitem.l_orderkey
                                                                           ->  Seq Scan on lineitem  (cost=0.00..44422937.88 rows=1468914944 width=15)
                                                   ->  Hash  (cost=1183682.96..1183682.96 rows=36737248 width=23)
                                                         ->  Seq Scan on customer  (cost=0.00..1183682.96 rows=36737248 width=23)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(29 rows)

--
--
-- 19.txt
--
--
select 'query 19' as current_query;
 current_query 
---------------
 query 19
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#11'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 9 and l_quantity <= 9 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#51'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 10 and l_quantity <= 10 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#23'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 22 and l_quantity <= 22 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
                                                                                                                                                                                                                                                                                                                                                        QUERY PLAN                                                                                                                                                                                                                                                                                                                                                        
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Aggregate  (cost=70109084.82..70109084.83 rows=1 width=32)
   ->  Gather Motion 2:1  (slice2; segments: 2)  (cost=70109084.62..70109084.81 rows=1 width=32)
         ->  Aggregate  (cost=70109084.62..70109084.64 rows=1 width=32)
               ->  Hash Join  (cost=3135899.13..70108968.54 rows=23215 width=16)
                     Hash Cond: lineitem.l_partkey = part.p_partkey
                     Join Filter: (part.p_brand = 'Brand#11'::bpchar AND (part.p_container = ANY ('{"SM CASE","SM BOX","SM PACK","SM PKG"}'::bpchar[])) AND lineitem.l_quantity >= 9::numeric AND lineitem.l_quantity <= 19::numeric AND part.p_size <= 5) OR (part.p_brand = 'Brand#51'::bpchar AND (part.p_container = ANY ('{"MED BAG","MED BOX","MED PKG","MED PACK"}'::bpchar[])) AND lineitem.l_quantity >= 10::numeric AND lineitem.l_quantity <= 20::numeric AND part.p_size <= 10) OR (part.p_brand = 'Brand#23'::bpchar AND (part.p_container = ANY ('{"LG CASE","LG BOX","LG PACK","LG PKG"}'::bpchar[])) AND lineitem.l_quantity >= 22::numeric AND lineitem.l_quantity <= 32::numeric AND part.p_size <= 15)
                     ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=0.00..61214767.57 rows=52567007 width=27)
                           Hash Key: lineitem.l_partkey
                           ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=52567007 width=27)
                                 Filter: (l_shipmode = ANY ('{AIR,"AIR REG"}'::bpchar[])) AND l_shipinstruct = 'DELIVER IN PERSON'::bpchar
                     ->  Hash  (cost=1741588.80..1741588.80 rows=49064374 width=30)
                           ->  Seq Scan on part  (cost=0.00..1741588.80 rows=49064374 width=30)
                                 Filter: p_size >= 1
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(14 rows)

--
--
-- 20.txt
--
--
select 'query 20' as current_query;
 current_query 
---------------
 query 20
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'burnished%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= date '1997-01-01'
					and l_shipdate < date '1997-01-01' + interval '1 year'
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'ALGERIA'
order by
	s_name;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice6; segments: 2)  (cost=77572610.05..77572610.06 rows=3 width=222)
   Merge Key: s_name
   ->  Sort  (cost=77572610.05..77572610.06 rows=3 width=222)
         Sort Key: supplier.s_name
         ->  HashAggregate  (cost=77572609.95..77572609.99 rows=3 width=222)
               Group By: supplier.ctid::bigint, nation.ctid::bigint, supplier.gp_segment_id, nation.gp_segment_id
               ->  Result  (cost=70397001.99..77572609.91 rows=3 width=71)
                     ->  Redistribute Motion 2:2  (slice5; segments: 2)  (cost=70397001.99..77572609.91 rows=3 width=71)
                           Hash Key: supplier.ctid, nation.ctid
                           ->  Hash Join  (cost=70397001.99..77572609.81 rows=3 width=71)
                                 Hash Cond: partsupp.ps_partkey = "Expr_SUBQUERY".csq_c0 AND partsupp.ps_suppkey = "Expr_SUBQUERY".csq_c1
                                 Join Filter: partsupp.ps_availqty::numeric > "Expr_SUBQUERY".csq_c2
                                 ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=1747722.14..8913532.21 rows=979771 width=16)
                                       Hash Key: partsupp.ps_partkey, partsupp.ps_suppkey
                                       ->  Hash Join  (cost=1747722.14..8874341.41 rows=979771 width=16)
                                             Hash Cond: partsupp.ps_partkey = part.p_partkey
                                             ->  Seq Scan on partsupp  (cost=0.00..6122355.01 rows=195954001 width=12)
                                             ->  Hash  (cost=1741588.80..1741588.80 rows=245334 width=4)
                                                   ->  Seq Scan on part  (cost=0.00..1741588.80 rows=245334 width=4)
                                                         Filter: p_name::text ~~ 'burnished%'::text
                                 ->  Hash  (cost=68623674.89..68623674.89 rows=853499 width=115)
                                       ->  Hash Join  (cost=67191814.38..68623674.89 rows=853499 width=115)
                                             Hash Cond: "Expr_SUBQUERY".csq_c1 = supplier.s_suppkey
                                             ->  HashAggregate  (cost=67019868.07..67832942.14 rows=21337464 width=40)
                                                   Group By: lineitem.l_partkey, lineitem.l_suppkey
                                                   ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=62312706.87..66100106.68 rows=21337464 width=40)
                                                         Hash Key: lineitem.l_partkey, lineitem.l_suppkey
                                                         ->  HashAggregate  (cost=62312706.87..65246608.12 rows=21337464 width=40)
                                                               Group By: lineitem.l_partkey, lineitem.l_suppkey
                                                               ->  Seq Scan on lineitem  (cost=0.00..59112087.32 rows=213374637 width=15)
                                                                     Filter: l_shipdate >= '01-01-1997'::date AND l_shipdate < 'Thu Jan 01 00:00:00 1998'::timestamp without time zone
                                             ->  Hash  (cost=132598.15..132598.15 rows=1573927 width=75)
                                                   ->  Broadcast Motion 2:2  (slice4; segments: 2)  (cost=24.68..132598.15 rows=1573927 width=75)
                                                         ->  Hash Join  (cost=24.68..99152.22 rows=98371 width=75)
                                                               Hash Cond: supplier.s_nationkey = nation.n_nationkey
                                                               ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=69)
                                                               ->  Hash  (cost=24.48..24.48 rows=8 width=14)
                                                                     ->  Broadcast Motion 2:2  (slice3; segments: 2)  (cost=0.00..24.48 rows=8 width=14)
                                                                           ->  Seq Scan on nation  (cost=0.00..24.31 rows=1 width=14)
                                                                                 Filter: n_name = 'ALGERIA'::bpchar
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(41 rows)

--
--
-- 21.txt
--
--
select 'query 21' as current_query;
 current_query 
---------------
 query 21
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA'
group by
	s_name
order by
	numwait desc,
	s_name
LIMIT 100;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=255819983.00..255819985.25 rows=100 width=112)
   ->  Gather Motion 2:1  (slice4; segments: 2)  (cost=255819983.00..255819985.25 rows=50 width=112)
         Merge Key: numwait, partial_aggregation.s_name
         ->  Limit  (cost=255819983.00..255819983.25 rows=50 width=112)
               ->  Sort  (cost=255819983.00..255832279.30 rows=2459260 width=112)
                     Sort Key (Limit): numwait, partial_aggregation.s_name
                     ->  HashAggregate  (cost=254133353.78..254221973.59 rows=2459260 width=112)
                           Group By: supplier.s_name
                           ->  Redistribute Motion 2:2  (slice3; segments: 2)  (cost=246567523.24..254032437.66 rows=2459260 width=112)
                                 Hash Key: supplier.s_name
                                 ->  HashAggregate  (cost=246567523.24..253934067.26 rows=2459260 width=112)
                                       Group By: supplier.s_name
                                       ->  Hash EXISTS Join  (cost=206295419.60..240698880.63 rows=586864262 width=26)
                                             Hash Cond: l1.l_orderkey = l2.l_orderkey
                                             Join Filter: l2.l_suppkey <> supplier.s_suppkey AND l2.l_suppkey <> l1.l_suppkey
                                             ->  Hash Left Anti Semi Join  (cost=121922011.12..139789998.31 rows=9242394 width=50)
                                                   Hash Cond: l1.l_orderkey = l3.l_orderkey
                                                   Join Filter: l3.l_suppkey <> l1.l_suppkey
                                                   ->  Hash Join  (cost=56837674.66..71356774.89 rows=9242394 width=50)
                                                         Hash Cond: orders.o_orderkey = l1.l_orderkey
                                                         ->  Seq Scan on orders  (cost=0.00..12613430.80 rows=179185719 width=8)
                                                               Filter: o_orderstatus = 'F'::bpchar
                                                         ->  Hash  (cost=56284681.82..56284681.82 rows=18970314 width=42)
                                                               ->  Hash Join  (cost=171946.31..56284681.82 rows=18970314 width=42)
                                                                     Hash Cond: l1.l_suppkey = supplier.s_suppkey
                                                                     ->  Seq Scan on lineitem l1  (cost=0.00..51767512.60 rows=489638315 width=12)
                                                                           Filter: l_receiptdate > l_commitdate
                                                                     ->  Hash  (cost=132598.15..132598.15 rows=1573927 width=30)
                                                                           ->  Broadcast Motion 2:2  (slice2; segments: 2)  (cost=24.68..132598.15 rows=1573927 width=30)
                                                                                 ->  Hash Join  (cost=24.68..99152.22 rows=98371 width=30)
                                                                                       Hash Cond: supplier.s_nationkey = nation.n_nationkey
                                                                                       ->  Seq Scan on supplier  (cost=0.00..76994.20 rows=2459260 width=34)
                                                                                       ->  Hash  (cost=24.48..24.48 rows=8 width=4)
                                                                                             ->  Broadcast Motion 2:2  (slice1; segments: 2)  (cost=0.00..24.48 rows=8 width=4)
                                                                                                   ->  Seq Scan on nation  (cost=0.00..24.31 rows=1 width=4)
                                                                                                         Filter: n_name = 'SAUDI ARABIA'::bpchar
                                                   ->  Hash  (cost=51767512.60..51767512.60 rows=489638315 width=12)
                                                         ->  Seq Scan on lineitem l3  (cost=0.00..51767512.60 rows=489638315 width=12)
                                                               Filter: l_receiptdate > l_commitdate AND l_orderkey IS NOT NULL AND l_suppkey IS NOT NULL
                                             ->  Hash  (cost=44422937.88..44422937.88 rows=1468914944 width=12)
                                                   ->  Seq Scan on lineitem l2  (cost=0.00..44422937.88 rows=1468914944 width=12)
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(42 rows)

--
--
-- 22.txt
--
--
select 'query 22' as current_query;
 current_query 
---------------
 query 22
(1 row)

-- using 2133894592 as a seed to the RNG
explain select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('10', '28', '11', '15', '18', '16', '19')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('10', '28', '11', '15', '18', '16', '19')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
                                                                                       QUERY PLAN                                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Gather Motion 2:1  (slice4; segments: 2)  (cost=40740110.94..40746093.38 rows=85464 width=72)
   Merge Key: cntrycode
   ->  GroupAggregate  (cost=40740110.94..40746093.38 rows=85464 width=72)
         Group By: "?column1?"
         InitPlan  (slice5)
           ->  Aggregate  (cost=2378818.07..2378818.08 rows=1 width=32)
                 ->  Gather Motion 2:1  (slice3; segments: 2)  (cost=2378817.87..2378818.05 rows=1 width=32)
                       ->  Aggregate  (cost=2378817.87..2378817.88 rows=1 width=32)
                             ->  Seq Scan on customer  (cost=0.00..2377643.52 rows=234871 width=8)
                                   Filter: c_acctbal > 0.00 AND ("substring"(c_phone::text, 1, 2) = ANY ('{10,28,11,15,18,16,19}'::text[]))
         ->  Sort  (cost=38361292.86..38361720.18 rows=85464 width=72)
               Sort Key: unnamed_attr_1
               ->  Redistribute Motion 2:2  (slice2; segments: 2)  (cost=38337890.37..38346436.72 rows=85464 width=72)
                     Hash Key: "?column1?"
                     ->  GroupAggregate  (cost=38337890.37..38343018.18 rows=85464 width=72)
                           Group By: "?column3?"
                           ->  Sort  (cost=38337890.37..38338317.69 rows=85464 width=24)
                                 Sort Key: "?column3?"
                                 ->  Hash Left Anti Semi Join  (cost=35309039.52..38323034.23 rows=85464 width=24)
                                       Hash Cond: tpch500gb.customer.c_custkey = orders.o_custkey
                                       ->  Seq Scan on customer  (cost=0.00..2377643.52 rows=85464 width=28)
                                             Filter: ("substring"(c_phone::text, 1, 2) = ANY ('{10,28,11,15,18,16,19}'::text[])) AND c_acctbal > $0
                                       ->  Hash  (cost=25485888.72..25485888.72 rows=367784512 width=4)
                                             ->  Redistribute Motion 2:2  (slice1; segments: 2)  (cost=0.00..25485888.72 rows=367784512 width=4)
                                                   Hash Key: orders.o_custkey
                                                   ->  Seq Scan on orders  (cost=0.00..10774508.24 rows=367784512 width=4)
                                                         Filter: o_custkey IS NOT NULL
 Settings:  enable_mergejoin=on; enable_nestloop=on; gp_enable_agg_distinct=off; gp_segments_for_planner=16; gp_selectivity_damping_for_joins=off; gp_selectivity_damping_for_scans=off
(28 rows)

-- EOF
