

-- These declarations are for the functions in chunkfuncs.c

DROP TYPE __getserializedrows CASCADE;
CREATE TYPE __getserializedrows AS (reportedsize integer, serialized bytea);

CREATE OR REPLACE FUNCTION getserializedrows(text) RETURNS SETOF __getserializedrows
    AS 'libcdbtest.so', 'GetSerializedRows__text1'
    LANGUAGE C IMMUTABLE STRICT;

DROP TYPE __getchunkedrows CASCADE;
CREATE TYPE __getchunkedrows AS (rownum integer, chunknum integer, chunksize integer, chunkdata bytea);

CREATE OR REPLACE FUNCTION getchunkedrows(text) RETURNS SETOF __getchunkedrows
    AS 'libcdbtest.so', 'GetChunkedRows__text1'
    LANGUAGE C IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION getdeserializedrows(text, regtype) RETURNS SETOF RECORD
    AS 'libcdbtest.so', 'GetDeserializedRows__text1_regtype1'
    LANGUAGE C IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION getdechunkedrows(text, regtype) RETURNS SETOF RECORD
    AS 'libcdbtest.so', 'GetDechunkedRows__text1_regtype1'
    LANGUAGE C IMMUTABLE STRICT;

-- Plan and Query Serialization functions
CREATE OR REPLACE FUNCTION cdb_exec_plan(text, text, text) RETURNS boolean
    AS 'libcdbtest.so', 'cdb_exec_plan'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdb_serialize_plan(text, boolean) RETURNS text
    AS 'libcdbtest.so', 'cdb_serialize_plan'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdb_serialize_query(text, boolean) RETURNS text
    AS 'libcdbtest.so', 'cdb_serialize_query'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdb_exec_indirect(text) RETURNS SETOF RECORD
    AS 'libcdbtest.so', 'cdb_exec_indirect'
    LANGUAGE C IMMUTABLE STRICT;
    
CREATE OR REPLACE FUNCTION cdb_test_cdbexec(text) RETURNS boolean
    AS 'libcdbtest.so', 'cdb_test_cdbexec'
    LANGUAGE C IMMUTABLE STRICT;

-- Alias information function
CREATE OR REPLACE FUNCTION cdb_show_alias(text) RETURNS text
    AS 'libcdbtest.so', 'cdb_show_alias'
    LANGUAGE C IMMUTABLE STRICT;

-- ipc test function
CREATE OR REPLACE FUNCTION ml_ipc_getMsg(text) RETURNS text
    AS 'libcdbtest.so', 'ml_ipc_getMsg__text1'
    LANGUAGE C IMMUTABLE STRICT;

-- ml_ipc benchmark functions
DROP TYPE __ml_ipc_bench_results CASCADE;
CREATE TYPE __ml_ipc_bench_results AS ( testTimeInMsec   int8,
									    totalMBytes      float8,
										bw_MB_per_sec    float8	 );

CREATE OR REPLACE FUNCTION ml_ipc_bench(int, int) RETURNS SETOF __ml_ipc_bench_results 
    AS 'libcdbtest.so', 'ml_ipc_bench'
    LANGUAGE C IMMUTABLE STRICT;

 
-- Functions for access to the Motion Layer API (what motion nodes use)

-- Initialize a motion layer node.
CREATE OR REPLACE FUNCTION mlapi_init_node(integer, bool, regtype) RETURNS integer
    AS 'libcdbtest.so', 'MLAPI_InitMLNode__int1_bool1_regtype1'
    LANGUAGE C IMMUTABLE STRICT;

-- Clean up a motion layer node.
CREATE OR REPLACE FUNCTION mlapi_end_node(integer) RETURNS integer
    AS 'libcdbtest.so', 'MLAPI_EndMLNode__int1'
    LANGUAGE C IMMUTABLE STRICT;

-- Begin a send-operation, and hopefully complete it.  Return-value indicates
-- whether the send completed or not.
CREATE OR REPLACE FUNCTION mlapi_sendtuple(integer, text) RETURNS integer
    AS 'libcdbtest.so', 'MLAPI_SendTuple__int1_text1'
    LANGUAGE C IMMUTABLE STRICT;

-- Attempt to complete an existing send operation.  Return-value indicates
-- whether the send completed or not.
CREATE OR REPLACE FUNCTION mlapi_finish_sendtuple(integer) RETURNS integer
    AS 'libcdbtest.so', 'MLAPI_FinishSendTuple__int1'
    LANGUAGE C IMMUTABLE STRICT;

-- Attempt to receive a tuple.  NULL is returned if a row is not ready.
CREATE OR REPLACE FUNCTION mlapi_recvtuple(integer) RETURNS RECORD
    AS 'libcdbtest.so', 'MLAPI_RecvTuple__int1'
    LANGUAGE C IMMUTABLE STRICT;

-- Find out the last receive return-code.  Obviously, mlapi_recvtuple should
-- be called before this function is used, otherwise the result is invalid.
CREATE OR REPLACE FUNCTION mlapi_getlastrecvcode() RETURNS INTEGER
    AS 'libcdbtest.so', 'MLAPI_GetLastRecvCode'
    LANGUAGE C IMMUTABLE STRICT;

DROP TYPE __mlapi_sendrecv_perf CASCADE;
CREATE TYPE __mlapi_sendrecv_perf AS (
    total_tups integer,
    total_bytes bigint,
    tuple_bytes bigint,
    send_start bigint,
    send_end bigint,
    send_ops bigint,
    recv_start bigint,
    recv_end bigint,
    recv_ops bigint
);

CREATE OR REPLACE FUNCTION mlapi_sendrecv_perf(integer, text) RETURNS __mlapi_sendrecv_perf
    AS 'libcdbtest.so', 'MLAPI_SendRecvPerf__int1_text1'
    LANGUAGE C IMMUTABLE STRICT;

-- Functions for access to the Cdb Hash API (used by motion node and PG-COPY)
-- In each of those the first 2 arguments are integers. The first represents the 
-- number of segments we want to hash to, and the second represents a choice of
-- a hashing algorithm. Currently the 2 options are 1 for FNV1 and 2 for FNV1A.
-- the third argument is the hash key.
 
CREATE OR REPLACE FUNCTION cdbhash_1_bigint(integer, integer, bigint) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_BigInt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_int(integer, integer, integer) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_Int'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_smallint(integer, integer, integer) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_SmallInt'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_2_int_int(integer, integer, integer, integer) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_2_Int_Int'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_bpchar(integer, integer, BpChar) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_BpChar'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_text(integer, integer, text) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_Text'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_varchar(integer, integer, VarChar) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_Varchar'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_bytea(integer, integer, bytea) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_Bytea'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_2_text_text(integer, integer, text, text) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_2_Text_Text'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_float8(integer, integer, float8) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_float8'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_float4(integer, integer, float4) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_float4'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_null(integer, integer) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_null'
    LANGUAGE C IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION cdbhash_1_timestamp(integer, integer, timestamp) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_timestamp'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_timestamptz(integer, integer, timestamp with time zone) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_timestamptz'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_date(integer, integer, date) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_date'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_time(integer, integer, time) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_time'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_timetz(integer, integer, time with time zone) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_timetz'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_numeric(integer, integer, numeric) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_numeric'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdbhash_1_bool(integer, integer, boolean) RETURNS integer
    AS 'libcdbtest.so', 'HASHAPI_Hash_1_Bool'
    LANGUAGE C IMMUTABLE STRICT;
	


CREATE OR REPLACE FUNCTION cdb_heap_test(integer, integer, integer) RETURNS boolean
    AS 'libcdbtest.so', 'cdb_heap_test'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdb_get_oid() RETURNS int4
    AS 'libcdbtest.so', 'cdb_get_oid'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION cdb_set_oid(int4) RETURNS boolean
    AS 'libcdbtest.so', 'cdb_set_oid'
    LANGUAGE C IMMUTABLE STRICT;
