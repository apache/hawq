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

#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "vadt.h"


/* the maximum length of numeric */
#define MAX_NUM_LEN 32

/* Get the size of vectorized data */
#define FUNCTION_VTYPESIZE(type) \
size_t v##type##Size(vheader *vh) \
{\
	size_t len = offsetof(vheader,isnull);\
	return  len + vh->dim * sizeof(bool) + vh->dim * sizeof(type) + sizeof(Datum);\
}

/* Build the vectorized data */
#define FUNCTION_BUILD(type, typeoid) \
vheader* buildv##type(int n) \
{ \
	vheader* result; \
	result = (vheader*) palloc0(offsetof(v##type,values) + n * sizeof(type) + sizeof(Datum)) ; \
	result->dim = n; \
	result->elemtype = typeoid; \
	result->isnull = palloc0(sizeof(bool) * n); \
	SET_VARSIZE(result,v##type##Size(result)); \
	return result; \
}

/* Destroy the vectorized data */
#define FUNCTION_DESTORY(type, typeoid) \
void destroyv##type(vheader **header) \
{ \
	v##type** ptr = (v##type**) header; \
	pfree((*header)->isnull); \
	pfree(*ptr); \
	*ptr = NULL; \
}

/*
 * Serialize functions and deserialize functions for the abstract data types
 */
#define FUNCTION_SERIALIZATION(type,typeoid) \
size_t v##type##serialization(vheader* vh,unsigned char *buf) \
{ \
	size_t len = 0; \
	v##type *vt = (v##type *)vh; \
 \
	memcpy(buf + len,vt,offsetof(vheader,isnull)); \
	len += offsetof(vheader,isnull); \
 \
	memcpy(buf + len,vh->isnull,sizeof(bool) * vh->dim); \
	len += sizeof(bool) * vh->dim; \
 \
	memcpy(buf + len,vt->values,sizeof(type) * vh->dim); \
	len += sizeof(type) * vh->dim; \
 \
	return len; \
} \
\
\
Datum v##type##deserialization(unsigned char* buf,size_t* len) \
{\
	vheader* vh = (vheader *) buf;\
	*len = 0;\
	\
	v##type* vt = palloc(sizeof(v##type) + sizeof(type) * vh->dim);\
	memcpy(vt,buf,offsetof(vheader,isnull));\
	*len += offsetof(vheader,isnull);\
	\
	vt->header.isnull = palloc(vh->dim * sizeof(bool));\
	memcpy(vt->header.isnull, buf + *len,vh->dim * sizeof(bool));\
	*len += vh->dim * sizeof(bool);\
	\
	memcpy(vt->values,buf + *len,vh->dim * sizeof(type)); \
	*len += vh->dim * sizeof(type); \
	\
	return PointerGetDatum(vt); \
}

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define FUNCTION_IN(type, typeoid, MACRO_DATUM) \
PG_FUNCTION_INFO_V1(v##type##in); \
Datum \
v##type##in(PG_FUNCTION_ARGS) \
{ \
    char *intString = PG_GETARG_CSTRING(0); \
    v##type *res = NULL; \
    char tempstr[MAX_NUM_LEN] = {0}; \
    int n = 0; \
    res = palloc0(offsetof(v##type, values) + (MAX_VECTOR_SIZE) * sizeof(type)); \
    for (n = 0; *intString && n < MAX_VECTOR_SIZE; n++) \
    { \
    	    char *start = NULL;\
        while (*intString && isspace((unsigned char) *intString)) \
            intString++; \
        if (*intString == '\0') \
            break; \
        start = intString; \
        while ((*intString && !isspace((unsigned char) *intString)) && *intString != '\0') \
            intString++; \
        Assert(intString - start < MAX_NUM_LEN); \
        strncpy(tempstr, start, intString - start); \
        tempstr[intString - start] = 0; \
        res->values[n] =  MACRO_DATUM(DirectFunctionCall1(type##in, CStringGetDatum(tempstr))); \
        while (*intString && !isspace((unsigned char) *intString)) \
            intString++; \
    } \
    while (*intString && isspace((unsigned char) *intString)) \
        intString++; \
    if (*intString) \
        ereport(ERROR, \
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                errmsg("int2vector has too many elements"))); \
    SET_VARSIZE(res, (offsetof(v##type, values) + n * sizeof(type))); \
    res->header.elemtype = typeoid; \
    res->header.dim = n; \
    PG_RETURN_POINTER(res); \
}

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define FUNCTION_OUT(type, typeoid, MACRO_DATUM) \
PG_FUNCTION_INFO_V1(v##type##out); \
Datum \
v##type##out(PG_FUNCTION_ARGS) \
{ \
	v##type * arg1 = (v##type *) PG_GETARG_POINTER(0); \
	int len = arg1->header.dim; \
    int i = 0; \
	char *rp; \
	char *result; \
	rp = result = (char *) palloc0(len * MAX_NUM_LEN + 1); \
	for (i = 0; i < len; i++) \
	{ \
		if (i != 0) \
			*rp++ = ' '; \
		strcat(rp, DatumGetCString(DirectFunctionCall1(type##out, MACRO_DATUM(arg1->values[i]))));\
		while (*++rp != '\0'); \
	} \
	*rp = '\0'; \
	PG_RETURN_CSTRING(result); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP(type1, type2, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##opstr); \
Datum \
v##type1##v##type2##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    v##type1 *res = NULL; \
    Assert((arg1)->header.dim == (arg2)->header.dim); \
    size = (arg1)->header.dim; \
    if(sizeof(type1) > sizeof(type2)) \
    { \
        res = palloc0(offsetof(v##type1, values) + (size) * sizeof(type1)); \
        SET_VARSIZE(res, (offsetof(v##type1, values) + (size) * sizeof(type1))); \
        res->header.dim = size;  \
        res->header.elemtype = arg1->header.elemtype; \
    } \
    else \
    { \
        res = palloc0(offsetof(v##type2, values) + (size) * sizeof(type2)); \
        SET_VARSIZE(res, (offsetof(v##type2, values) + (size) * sizeof(type2))); \
        res->header.dim = size;  \
        res->header.elemtype = arg2->header.elemtype; \
    } \
    while(i < size) \
    { \
        res->values[i] = arg1->values[i] opsym arg2->values[i]; \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##opstr); \
Datum \
v##type##const_type##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    v##type *res = NULL; \
    size = (arg1)->header.dim; \
    res = palloc0(offsetof(v##type, values) + (size) * sizeof(type)); \
    SET_VARSIZE(res, (offsetof(v##type, values) + (size) * sizeof(type)));  \
    res->header.elemtype = arg1->header.elemtype; \
    res->header.dim = arg1->header.dim; \
    while(i < size) \
    { \
        res->values[i] = arg1->values[i] opsym arg2; \
        i ++ ;\
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP(type1, type2, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##cmpstr); \
Datum \
v##type1##v##type2##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    vbool *res = NULL; \
    size = (arg1)->header.dim; \
    res = palloc0(offsetof(vbool, values) + (size) * sizeof(bool)); \
    SET_VARSIZE(res, (offsetof(vbool, values) + (size) * sizeof(bool)));  \
    res->header.elemtype = BOOLOID; \
    res->header.dim = arg1->header.dim; \
    while(i < size) \
    { \
        res->values[i] = arg1->values[i] cmpsym arg2->values[i]; \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##cmpstr); \
Datum \
v##type##const_type##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    vbool *res = NULL; \
    size = (arg1)->header.dim; \
    res = palloc0(offsetof(vbool, values) + (size) * sizeof(bool)); \
    SET_VARSIZE(res, (offsetof(vbool, values) + (size) * sizeof(bool)));  \
    res->header.elemtype = BOOLOID; \
    res->header.dim = size; \
    while(i < size) \
    { \
        res->values[i] = arg1->values[i] cmpsym arg2; \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

/* Implement the functions for the all data types by the upper BASE MARCO.*/
#define _FUNCTION_OP(type1, type2) \
    __FUNCTION_OP(type1, type2, +, pl)  \
    __FUNCTION_OP(type1, type2, -, mi)  \
    __FUNCTION_OP(type1, type2, *, mul) \
    __FUNCTION_OP(type1, type2, /, div)

#define FUNCTION_OP(type) \
    _FUNCTION_OP(type, int2) \
    _FUNCTION_OP(type, int4) \
    _FUNCTION_OP(type, int8) \
    _FUNCTION_OP(type, float4) \
    _FUNCTION_OP(type, float8)


#define _FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, /, div)


#define FUNCTION_OP_RCONST(type) \
    _FUNCTION_OP_RCONST(type, int2, PG_GETARG_INT16) \
    _FUNCTION_OP_RCONST(type, int4, PG_GETARG_INT32) \
    _FUNCTION_OP_RCONST(type, int8, PG_GETARG_INT64) \
    _FUNCTION_OP_RCONST(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_OP_RCONST(type, float8, PG_GETARG_FLOAT8)


#define _FUNCTION_CMP(type1, type2) \
    __FUNCTION_CMP(type1, type2, ==, eq) \
    __FUNCTION_CMP(type1, type2, !=, ne) \
    __FUNCTION_CMP(type1, type2, >, gt) \
    __FUNCTION_CMP(type1, type2, >=, ge) \
    __FUNCTION_CMP(type1, type2, <, lt) \
    __FUNCTION_CMP(type1, type2, <=, le)

#define FUNCTION_CMP(type1) \
    _FUNCTION_CMP(type1, int2) \
    _FUNCTION_CMP(type1, int4) \
    _FUNCTION_CMP(type1, int8) \
    _FUNCTION_CMP(type1, float4) \
    _FUNCTION_CMP(type1, float8)


#define _FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, ==, eq)  \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, !=, ne)  \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO,  >, gt) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, >=, ge) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO,  <, lt) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, <=, le) \

#define FUNCTION_CMP_RCONST(type) \
    _FUNCTION_CMP_RCONST(type, int2, PG_GETARG_INT16) \
    _FUNCTION_CMP_RCONST(type, int4, PG_GETARG_INT32) \
    _FUNCTION_CMP_RCONST(type, int8, PG_GETARG_INT64) \
    _FUNCTION_CMP_RCONST(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_CMP_RCONST(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_OP_ALL(type) \
    FUNCTION_OP(type) \
    FUNCTION_OP_RCONST(type) \
    FUNCTION_CMP(type) \
    FUNCTION_CMP_RCONST(type) 

#define FUNCTION_GETPTR(type) \
type* getptrv##type(vheader *header,int n) \
{\
	if(n < 0 || n > header->dim) return NULL; \
	v##type* ptr = (v##type *) header; \
	return ptr->values + n; \
}

#define FUNCTION_GETVALUE(type) \
void getvaluev##type(vheader *header,int n,Datum *ptr) \
{ \
	if(n < 0 || n > header->dim) return;\
	*ptr = ((v##type*)header)->values[n];\
}

#define TYPE_DEFINE(type, typeoid) \
    FUNCTION_VTYPESIZE(type) \
    FUNCTION_OP_ALL(type) \
    FUNCTION_BUILD(type, typeoid) \
    FUNCTION_DESTORY(type, typeoid) \
    FUNCTION_GETPTR(type) \
    FUNCTION_GETVALUE(type) \
    FUNCTION_SERIALIZATION(type,typeoid) \

/* These MACRO will be expanded when the code is compiled. */
TYPE_DEFINE(int2, INT2OID)
TYPE_DEFINE(int4, INT4OID)
TYPE_DEFINE(int8, INT8OID)
TYPE_DEFINE(float4, FLOAT4OID)
TYPE_DEFINE(float8, FLOAT8OID)
TYPE_DEFINE(bool, BOOLOID)

FUNCTION_IN(int2, INT2OID, DatumGetInt16)
FUNCTION_IN(int4, INT4OID, DatumGetInt32)
FUNCTION_IN(int8, INT8OID, DatumGetInt64)
FUNCTION_IN(float4, FLOAT4OID, DatumGetFloat4)
FUNCTION_IN(float8, FLOAT8OID, DatumGetFloat8)
FUNCTION_IN(bool, BOOLOID, DatumGetBool)

FUNCTION_OUT(int2, INT2OID, Int16GetDatum)
FUNCTION_OUT(int4, INT4OID, Int32GetDatum)
FUNCTION_OUT(int8, INT8OID, Int64GetDatum)
FUNCTION_OUT(float4, FLOAT4OID, Float4GetDatum)
FUNCTION_OUT(float8, FLOAT8OID, Float8GetDatum)
FUNCTION_OUT(bool, BOOLOID, BoolGetDatum)
