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

#ifndef __VTYPE_H___
#define __VTYPE_H___
#include "postgres.h"
#include "fmgr.h"
typedef struct vtype {
    int     _vl_len;
    Oid     elemtype;
    int     dim;
    bool    *isnull;
    bool    *skipref;
    Datum   values[0];
}vtype;


#define CANARYSIZE  sizeof(char)
#define VTYPEHEADERSZ (sizeof(vtype))
#define VDATUMSZ(dim) (sizeof(Datum) * dim)
#define ISNULLSZ(dim) (sizeof(bool) * dim)
#define VTYPESIZE(dim) (VTYPEHEADERSZ + VDATUMSZ(dim) + CANARYSIZE + ISNULLSZ(dim))
#define CANARYOFFSET(vtype) ((char*)((unsigned char*)vtype + VTYPEHEADERSZ + VDATUMSZ(dim)))
#define ISNULLOFFSET(vtype) ((bool*)((unsigned char*)vtype + VTYPEHEADERSZ + VDATUMSZ(vtype->dim) + CANARYSIZE))
#define VTYPE_STURCTURE(type) typedef struct vtype v##type;

#define FUNCTION_BUILD_HEADER(type) \
v##type* buildv##type(int dim, bool *skip);

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
#define __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
extern Datum \
const_type##v##type##opstr(PG_FUNCTION_ARGS);
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

#define _FUNCTION_OP_HEADER(type1, type2) \
    __FUNCTION_OP_HEADER(type1, type2, +, pl)  \
    __FUNCTION_OP_HEADER(type1, type2, -, mi)  \
    __FUNCTION_OP_HEADER(type1, type2, *, mul) \
    __FUNCTION_OP_HEADER(type1, type2, /, div)

#define _FUNCTION_OP_CONST_HEADER(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, /, div) \
    __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_LCONST_HEADER(type, const_type, CONST_ARG_MACRO, /, div)

#define _FUNCTION_CMP_HEADER(type1, type2) \
    __FUNCTION_CMP_HEADER(type1, type2, ==, eq) \
    __FUNCTION_CMP_HEADER(type1, type2, !=, ne) \
    __FUNCTION_CMP_HEADER(type1, type2, >, gt) \
    __FUNCTION_CMP_HEADER(type1, type2, >=, ge) \
    __FUNCTION_CMP_HEADER(type1, type2, <, lt) \
    __FUNCTION_CMP_HEADER(type1, type2, <=, le)

#define _FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, ==, eq)  \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, !=, ne)  \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  >, gt) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, >=, ge) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO,  <, lt) \
    __FUNCTION_CMP_RCONST_HEADER(type, const_type, CONST_ARG_MACRO, <=, le) 

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

#define FUNCTION_OP_HEADER(type) \
    _FUNCTION_OP_HEADER(type, int2) \
    _FUNCTION_OP_HEADER(type, int4) \
    _FUNCTION_OP_HEADER(type, int8) \
    _FUNCTION_OP_HEADER(type, float4) \
    _FUNCTION_OP_HEADER(type, float8)

#define FUNCTION_OP_CONST_HEADER(type) \
    _FUNCTION_OP_CONST_HEADER(type, int2, PG_GETARG_INT16) \
    _FUNCTION_OP_CONST_HEADER(type, int4, PG_GETARG_INT32) \
    _FUNCTION_OP_CONST_HEADER(type, int8, PG_GETARG_INT64) \
    _FUNCTION_OP_CONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_OP_CONST_HEADER(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_CMP_HEADER(type1) \
    _FUNCTION_CMP_HEADER(type1, int2) \
    _FUNCTION_CMP_HEADER(type1, int4) \
    _FUNCTION_CMP_HEADER(type1, int8) \
    _FUNCTION_CMP_HEADER(type1, float4) \
    _FUNCTION_CMP_HEADER(type1, float8)

#define FUNCTION_CMP_RCONST_HEADER(type) \
    _FUNCTION_CMP_RCONST_HEADER(type, int2, PG_GETARG_INT16) \
    _FUNCTION_CMP_RCONST_HEADER(type, int4, PG_GETARG_INT32) \
    _FUNCTION_CMP_RCONST_HEADER(type, int8, PG_GETARG_INT64) \
    _FUNCTION_CMP_RCONST_HEADER(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_CMP_RCONST_HEADER(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_OP_ALL_HEADER(type) \
    FUNCTION_OP_HEADER(type) \
    FUNCTION_OP_CONST_HEADER(type) \
    FUNCTION_CMP_HEADER(type) \
    FUNCTION_CMP_RCONST_HEADER(type) \

#define TYPE_HEADER(type,oid) \
    VTYPE_STURCTURE(type) \
	FUNCTION_BUILD_HEADER(type) \
    FUNCTION_OP_ALL_HEADER(type) \
    FUNCTION_IN_HEADER(type, typeoid) \
    FUNCTION_OUT_HEADER(type, typeoid) \

TYPE_HEADER(int2, INT2OID)
TYPE_HEADER(int4, INT4OID)
TYPE_HEADER(int8, INT8OID)
TYPE_HEADER(float4, FLOAT4OID)
TYPE_HEADER(float8, FLOAT8OID)
TYPE_HEADER(bool, BOOLOID)

extern vtype* buildvtype(Oid elemtype,int dim,bool *skip);
extern void destroyvtype(vtype** vt);
#endif
