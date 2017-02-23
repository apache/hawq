/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
*
* hashapi_access.c
*     Test functions for accessing the Cdb Hash API.
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*-------------------------------------------------------------------------
*/
#include "postgres.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "cdb/cdbhash.h"
#include "cdbtest.h"

#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/*
 * Pointer to the hash session characteristics. 
 */
static CdbHash *h;

/*==============================================
 *
 * SINGLE VALUE HASHING
 *
 *==============================================
 */

/* 
 * HASHAPI_Hash_1_BigInt
 * Perform a simple 1-value bigint hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_BigInt);
Datum HASHAPI_Hash_1_BigInt(PG_FUNCTION_ARGS)
{
    int32 num_segs;    /* number of segments  */
	int16 algorithm;  /* hashing algorithm   */
	int64 val1;        /* big int input value */
    unsigned int targetbucket; /* 0-based  */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
	
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
    
	/* Get the value to hash */
	val1 = PG_GETARG_INT64(2);
	
	d1 = Int64GetDatum(val1);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = INT8OID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
* HASHAPI_Hash_1_BigInt
 * Perform a simple 1-value bigint hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Bool);
Datum HASHAPI_Hash_1_Bool(PG_FUNCTION_ARGS)
{
    int32 num_segs;    /* number of segments  */
	Datum d1;
	Oid oid;
	int16 algorithm;  /* hashing algorithm   */
	bool val1;        /* boolean input value */
    unsigned int targetbucket; /* 0-based  */
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
	
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
    
	/* Get the value to hash */
	val1 = PG_GETARG_BOOL(2);
	
	d1 = BoolGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = BOOLOID;
	
	
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}


/* 
 * HASHAPI_Hash_1_Int
 * Perform a simple 1-value int4 hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Int);
Datum HASHAPI_Hash_1_Int(PG_FUNCTION_ARGS)
{
    int32 num_segs; /* number of segments  */
	int16 algorithm;/* hashing algorithm   */
	int32 val1;     /* int input value */
    unsigned int targetbucket; /* 0-based  */
	Datum d1;
	Oid oid;
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
 
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_INT32(2);
	
	d1 = Int32GetDatum(val1);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = INT4OID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
* HASHAPI_Hash_1_SmallInt
 * Perform a simple 1-value int2 hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_SmallInt);
Datum HASHAPI_Hash_1_SmallInt(PG_FUNCTION_ARGS)
{
    int32 num_segs; /* number of segments  */
	int16 algorithm;/* hashing algorithm   */
	int32 value;	/* int input value 
					   will be cast to int16 */
	int16 val1;
    unsigned int targetbucket; /* 0-based   */
	Datum d1;
	Oid oid;
	 
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
	
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	value = PG_GETARG_INT32(2);
	
	val1 = (int16)value;
	
	d1 = Int16GetDatum(val1);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = INT2OID;
	
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_Char
 * Perform a simple 1 character hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_BpChar);
Datum HASHAPI_Hash_1_BpChar(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
    int16 algorithm;  /* hashing algorithm   */
	BpChar *val1;				   /* char value          */
    unsigned int targetbucket; /* 0-based             */
	Datum d1;
	Oid oid;
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);

	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_BPCHAR_P(2);	
				
	d1 = PointerGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = BPCHAROID;
	
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(val1, 2);

    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_Text
 * Perform a simple 1 string hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Text);
Datum HASHAPI_Hash_1_Text(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	text *val1;	        	   /* char value          */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);

	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_TEXT_P(2);
	
			
	d1 = PointerGetDatum(val1);
			
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
			
	/* init cdb hash */
	cdbhashinit(h);
			
	oid = TEXTOID;

	cdbhash(h, d1, oid);
			
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(val1, 2);
			
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_Varchar
 * Perform a simple 1 string (of type VARCHAR) hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Varchar);
Datum HASHAPI_Hash_1_Varchar(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	VarChar    *val1;        	   /* varchar value		  */
    unsigned int targetbucket; /* 0-based             */
    int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);

	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_VARCHAR_P(2);
	
	
	d1 = PointerGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = VARCHAROID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	
	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(val1, 2);
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_Bytea
 * Perform a simple 1 string (of type BYTEA) hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_Bytea);
Datum HASHAPI_Hash_1_Bytea(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	bytea    *val1;        	   /* bytea value		  */
    unsigned int targetbucket; /* 0-based             */
    int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
	
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_BYTEA_P(2);
	
	d1 = PointerGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	cdbhashinit(h);
	oid = BYTEAOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(val1, 2);
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}


/* 
 * HASHAPI_Hash_1_float8
 * Perform a single float8 value (double) hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_float8);
Datum HASHAPI_Hash_1_float8(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	float8    val1;        	   /* varchar value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;
	
	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);

	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_FLOAT8(2);
	
	d1 = Float8GetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = FLOAT8OID;
	
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_float4
 * Perform a single float4 value (double) hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_float4);
Datum HASHAPI_Hash_1_float4(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	float4    val1;        	   /* varchar value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_FLOAT4(2);
	
	d1 = Float4GetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = FLOAT4OID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
* HASHAPI_Hash_1_timestamp
 * Perform a single timestamp value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timestamp);
Datum HASHAPI_Hash_1_timestamp(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	Timestamp val1;        	   /* varchar value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_TIMESTAMP(2);
	
	d1 = TimestampGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = TIMESTAMPOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_timestamptz
 * Perform a single timestamp with time zone value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timestamptz);
Datum HASHAPI_Hash_1_timestamptz(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	TimestampTz val1;        	   /* varchar value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_TIMESTAMPTZ(2);
	
	d1 = TimestampTzGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = TIMESTAMPTZOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_date
 * Perform a single date value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_date);
Datum HASHAPI_Hash_1_date(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	DateADT val1;        	   /* date value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_DATEADT(2);
	
	d1 = DateADTGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = DATEOID;
	
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
* HASHAPI_Hash_1_time
 * Perform a single time value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_time);
Datum HASHAPI_Hash_1_time(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	TimeADT val1;        	   /* time value		  */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_TIMEADT(2);
	
	d1 = TimeADTGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = TIMEOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
* HASHAPI_Hash_1_timetz
 * Perform a single time with time zone value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_timetz);
Datum HASHAPI_Hash_1_timetz(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	TimeTzADT *val1;		   /* time w/timezone value */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_TIMETZADT_P(2);
	
	d1 = TimeTzADTPGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = TIMETZOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_numeric
 * Perform a single NUMERIC value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_numeric);
Datum HASHAPI_Hash_1_numeric(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	Numeric val1;	    	   /* NUMERIC value */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);
	
	/* Get the value to hash */
	val1 = PG_GETARG_NUMERIC(2);
	
	d1 = NumericGetDatum(val1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = NUMERICOID;
	cdbhash(h, d1, oid);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_1_null
 * Perform a single null value hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_1_null);
Datum HASHAPI_Hash_1_null(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	unsigned int targetbucket; /* 0-based             */
	int algorithm;           /* hashing algorithm   */

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT32(1);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	cdbhashnull(h);
	
	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/*==============================================
 *
 * MULTI VALUE HASHING
 *
 *==============================================
 */

/* 
 * HASHAPI_Hash_2_Int_BigInt
 * Perform a simple 1-value int4 hash 
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_2_Int_Int);
Datum HASHAPI_Hash_2_Int_Int(PG_FUNCTION_ARGS)
{
    int32 num_segs; /* number of segments  */
	int32 val1;     /* int input value     */
	int32 val2;     /* bigint input value  */
    unsigned int targetbucket; /* 0-based  */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1,d2;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the values to hash */
	val1 = PG_GETARG_INT32(2);
	val2 = PG_GETARG_INT32(3);
	
	d1 = Int32GetDatum(val1);
	d2 = Int32GetDatum(val2);
	
	/* create a CdbHash for this hash test. */
    h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	
	oid = INT4OID;
	
	cdbhash(h, d1, oid);
	cdbhash(h, d2, oid);

	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
    PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}

/* 
 * HASHAPI_Hash_2_Text_Text
 * Perform a simple 1 string hash.
 */ 
PG_FUNCTION_INFO_V1(HASHAPI_Hash_2_Text_Text);
Datum HASHAPI_Hash_2_Text_Text(PG_FUNCTION_ARGS)
{
    int32 num_segs;            /* number of segments  */
	text *val1;	        	   /* text value1         */
	text *val2;	        	   /* text value2         */
    unsigned int targetbucket; /* 0-based             */
	int16 algorithm;  /* hashing algorithm   */
	Datum d1,d2;
	Oid oid;

	/* Get number of segments */
    num_segs = PG_GETARG_INT32(0);
    
	/* Get hashing algoriithm */
	algorithm = PG_GETARG_INT16(1);

	/* Get the value to hash */
	val1 = PG_GETARG_TEXT_P(2);
	val2 = PG_GETARG_TEXT_P(3);

	
	d1 = PointerGetDatum(val1);
	d2 = PointerGetDatum(val2);
	
	/* create a CdbHash for this hash test. */
	h = makeCdbHash(num_segs, algorithm);
	
	/* init cdb hash */
	cdbhashinit(h);
	oid = TEXTOID;
	cdbhash(h, d1, oid);
	cdbhash(h, d2, oid);

	/* reduce the result hash value */
	targetbucket = cdbhashreduce(h);	
	
	/* Avoid leaking memory for toasted inputs */
	PG_FREE_IF_COPY(val1, 1);
	PG_FREE_IF_COPY(val2, 2);

	PG_RETURN_INT32(targetbucket); /* return target bucket (segID) */
}




