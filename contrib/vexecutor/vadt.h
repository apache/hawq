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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef __VADT_H__
#define __VADT_H__

#include "postgres.h"
#include "fmgr.h"

#define MAX_VECTOR_SIZE 1024

/* 
 * Vectorized abstract data types headers
 */
#define VADT_HEADER() \
	typedef struct vheader \
	{ \
		int _vl_len; \
		Oid elemtype; \
		int dim; \
		bool *isnull; \
	}vheader;

/* 
 * Vectorized abstract data types,
 */
#define VADT_STRUCT(type) \
	typedef struct v##type \
	{ \
		vheader header; \
		type values[0]; \
	}v##type;

/* MACRO "HEADER" is used to the functions declare */

/* Get the size of vectorized data */
#define FUNCTION_VTYPESIZE_HEADER(type) \
extern size_t v##type##Size(vheader *vh); 

/* Build the vectorized data */
#define FUNCTION_BUILD_HEADER(type, typeoid) \
extern vheader* buildv##type(int n);

/* Destroy the vectorized data */
#define FUNCTION_DESTORY_HEADER(type, typeoid) \
extern void destoryv##type(vheader **header);

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define FUNCTION_IN_HEADER(type, typeoid) \
extern Datum v##type##in(PG_FUNCTION_ARGS);

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define FUNCTION_OUT_HEADER(type, typeoid) \
extern Datum v##type##out(PG_FUNCTION_ARGS);

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_HEADER(type1, type2, opsym, opstr) \
extern Datum v##type1##v##type2##opstr(PG_FUNCTION_ARGS);

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
extern Datum v##type##const_type##opstr(PG_FUNCTION_ARGS);

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_HEADER(type1, type2, cmpsym, cmpstr) \
extern Datum v##type1##v##type2##cmpstr(PG_FUNCTION_ARGS);

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
extern Datum v##type##const_type##cmpstr(PG_FUNCTION_ARGS);

/*
 * Serialize functions and deserialize functions for the abstract data types
 */
#define FUNCTION_SERIALIZATION_HEADER(type,typeoid) \
extern size_t v##type##serialization(vheader* vh,unsigned char *buf); \
extern Datum v##type##deserialization(unsigned char* buf,size_t* len); \



/* Implement the functions for the all data types by the upper BASE MARCO.*/
#define _FUNCTION_OP_HEADER(type1, type2) \
    __FUNCTION_OP_HEADER(type1, type2, +, pl)  \
    __FUNCTION_OP_HEADER(type1, type2, -, mi)  \
    __FUNCTION_OP_HEADER(type1, type2, *, mul) \
    __FUNCTION_OP_HEADER(type1, type2, /, div)

#define FUNCTION_OP_HEADER(type) \
    _FUNCTION_OP_HEADER(type, int2) \
    _FUNCTION_OP_HEADER(type, int4) \
    _FUNCTION_OP_HEADER(type, int8) \
    _FUNCTION_OP_HEADER(type, float4) \
    _FUNCTION_OP_HEADER(type, float8)


#define _FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, /, div)


#define FUNCTION_OP_RCONST_HEADER(type) \
    _FUNCTION_OP_RCONST_HEADER(type, int2, PG_GETARG_INT16) \
    _FUNCTION_OP_RCONST_HEADER(type, int4, PG_GETARG_INT32) \
    _FUNCTION_OP_RCONST_HEADER(type, int8, PG_GETARG_INT64) \
    _FUNCTION_OP_RCONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_OP_RCONST_HEADER(type, float8, PG_GETARG_FLOAT8)


#define _FUNCTION_CMP_HEADER(type1, type2) \
    __FUNCTION_CMP_HEADER(type1, type2, ==, eq) \
    __FUNCTION_CMP_HEADER(type1, type2, !=, ne) \
    __FUNCTION_CMP_HEADER(type1, type2, >, gt) \
    __FUNCTION_CMP_HEADER(type1, type2, >=, ge) \
    __FUNCTION_CMP_HEADER(type1, type2, <, lt) \
    __FUNCTION_CMP_HEADER(type1, type2, <=, le)

#define FUNCTION_CMP_HEADER(type1) \
    _FUNCTION_CMP_HEADER(type1, int2) \
    _FUNCTION_CMP_HEADER(type1, int4) \
    _FUNCTION_CMP_HEADER(type1, int8) \
    _FUNCTION_CMP_HEADER(type1, float4) \
    _FUNCTION_CMP_HEADER(type1, float8)


#define _FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, ==, eq)  \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, !=, ne)  \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  >, gt) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, >=, ge) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  <, lt) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, <=, le) 

#define FUNCTION_CMP_RCONST_HEADER(type) \
    _FUNCTION_CMP_RCONST_HEADER(type, int2, PG_GETARG_INT16) \
    _FUNCTION_CMP_RCONST_HEADER(type, int4, PG_GETARG_INT32) \
    _FUNCTION_CMP_RCONST_HEADER(type, int8, PG_GETARG_INT64) \
    _FUNCTION_CMP_RCONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_CMP_RCONST_HEADER(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_OP_ALL_HEADER(type) \
    FUNCTION_OP_HEADER(type) \
    FUNCTION_OP_RCONST_HEADER(type) \
    FUNCTION_CMP_HEADER(type) \
    FUNCTION_CMP_RCONST_HEADER(type) \
    FUNCTION_VTYPESIZE_HEADER(type) 


#define TYPE_HEADER(type, typeoid) \
    VADT_BUILD(type) \
    FUNCTION_OP_ALL_HEADER(type) \
    FUNCTION_IN_HEADER(type, typeoid) \
    FUNCTION_OUT_HEADER(type, typeoid) \
    FUNCTION_BUILD_HEADER(type, typeoid) \
    FUNCTION_DESTORY_HEADER(type, typeoid) \
    FUNCTION_SERIALIZATION_HEADER(type,typeoid) \

#define VADT_BUILD(type) \
    VADT_STRUCT(type) \
    FUNCTION_VTYPESIZE_HEADER(type)

/* These MACRO will be expanded when the code is compiled. */
VADT_HEADER()
TYPE_HEADER(int2, INT2OID)
TYPE_HEADER(int4, INT4OID)
TYPE_HEADER(int8, INT8OID)
TYPE_HEADER(float4, FLOAT4OID)
TYPE_HEADER(float8, FLOAT8OID)
TYPE_HEADER(bool, BOOLOID)

#endif
