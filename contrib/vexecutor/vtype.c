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
#include "vtype.h"

#define MAX_NUM_LEN 64
extern int BATCHSIZE;
const char canary = 0xe7;

vtype* buildvtype(Oid elemtype,int dim,bool *skip)
{
    vtype *res;
    res = palloc0(VTYPESIZE(dim));
    res->dim = dim;
    res->elemtype = elemtype;

    *CANARYOFFSET(res) = canary;
    res->isnull = ISNULLOFFSET(res);
    res->skipref = skip;

    SET_VARSIZE(res, VTYPESIZE(dim));

    return res;
}

void destroyvtype(vtype** vt)
{
    pfree((*vt));
    *vt = NULL;
}

#define _FUNCTION_BUILD(type, typeoid) \
v##type* buildv##type(int dim, bool *skip) \
{ \
    return buildvtype(typeoid, dim, skip); \
}

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define _FUNCTION_IN(type, typeoid) \
PG_FUNCTION_INFO_V1(v##type##in); \
Datum \
v##type##in(PG_FUNCTION_ARGS) \
{ \
    char *intString = PG_GETARG_CSTRING(0); \
    vtype *res = NULL; \
    char tempstr[MAX_NUM_LEN] = {0}; \
    int n = 0; \
    res = buildvtype(typeoid,BATCHSIZE,NULL);\
    for (n = 0; *intString && n < BATCHSIZE; n++) \
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
        res->values[n] = DirectFunctionCall1(type##in, CStringGetDatum(tempstr)); \
        while (*intString && !isspace((unsigned char) *intString)) \
            intString++; \
    } \
    while (*intString && isspace((unsigned char) *intString)) \
        intString++; \
    if (*intString) \
        ereport(ERROR, \
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                errmsg("int2vector has too many elements"))); \
    res->elemtype = typeoid; \
    res->dim = n; \
    SET_VARSIZE(res, VTYPESIZE(n)); \
    PG_RETURN_POINTER(res); \
}

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define _FUNCTION_OUT(type, typeoid) \
PG_FUNCTION_INFO_V1(v##type##out); \
Datum \
v##type##out(PG_FUNCTION_ARGS) \
{ \
	vtype * arg1 = (v##type *) PG_GETARG_POINTER(0); \
	int len = arg1->dim; \
    int i = 0; \
	char *rp; \
	char *result; \
	rp = result = (char *) palloc0(len * MAX_NUM_LEN + 1); \
	for (i = 0; i < len; i++) \
	{ \
		if (i != 0) \
			*rp++ = ' '; \
		strcat(rp, DatumGetCString(DirectFunctionCall1(type##out, arg1->values[i])));\
		while (*++rp != '\0'); \
	} \
	*rp = '\0'; \
	PG_RETURN_CSTRING(result); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2pl(PG_FUNCTION_ARGS);
 * NOTE:we assum that return type is same with the type of arg1,
 * we have not processed the overflow so far.
 */
#define __FUNCTION_OP(type1, XTYPE1, type2, XTYPE2, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##opstr); \
Datum \
v##type1##v##type2##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    v##type1 *res = buildv##type1(BATCHSIZE, NULL); \
    Assert(arg1->dim == arg2->dim); \
    size = arg1->dim; \
    while(i < size) \
    { \
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = XTYPE1##GetDatum((DatumGet##XTYPE1(arg1->values[i])) opsym (DatumGet##XTYPE2(arg2->values[i]))); \
        i++; \
    } \
    res->dim = arg1->dim; \
    PG_RETURN_POINTER(res); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##opstr); \
Datum \
v##type##const_type##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    v##type *res = buildv##type(BATCHSIZE, NULL); \
    size = arg1->dim;\
    while(i < size) \
    { \
        res->isnull[i] = arg1->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = XTYPE##GetDatum((DatumGet##XTYPE(arg1->values[i])) opsym ((type)arg2)); \
        i ++ ;\
    } \
    res->dim = arg1->dim; \
    PG_RETURN_POINTER(res); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the
 * Consts OP V-types.
 * e.g. extern Datum int2vint2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_LCONST(type, XTYPE, const_type, CONST_ARG_MACRO, opsym, opstr) \
PG_FUNCTION_INFO_V1(const_type##v##type##opstr); \
Datum \
const_type##v##type##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    const_type arg1 = CONST_ARG_MACRO(0); \
    v##type *arg2 = PG_GETARG_POINTER(1); \
    v##type *res = buildv##type(BATCHSIZE, NULL); \
    size = arg2->dim;\
    while(i < size) \
    { \
        res->isnull[i] = arg2->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = XTYPE##GetDatum(((type)arg1) opsym (DatumGet##XTYPE(arg2->values[i]))); \
        i ++ ;\
    } \
    res->dim = arg2->dim; \
    PG_RETURN_POINTER(res); \
}


/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##cmpstr); \
Datum \
v##type1##v##type2##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    Assert(arg1->dim == arg2->dim); \
    vbool *res = buildvtype(BOOLOID, BATCHSIZE, NULL); \
    size = arg1->dim; \
    while(i < size) \
    { \
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = BoolGetDatum(DatumGet##XTYPE1(arg1->values[i]) cmpsym (DatumGet##XTYPE2(arg2->values[i]))); \
        i++; \
    } \
    res->dim = arg1->dim; \
    PG_RETURN_POINTER(res); \
}

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##cmpstr); \
Datum \
v##type##const_type##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    vbool *res = buildvtype(BOOLOID, BATCHSIZE, NULL); \
    size = arg1->dim; \
    while(i < size) \
    { \
        res->isnull[i] = arg1->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = BoolGetDatum((DatumGet##XTYPE(arg1->values[i])) cmpsym arg2); \
        i++; \
    } \
    res->dim = arg1->dim; \
    PG_RETURN_POINTER(res); \
}

//Macro Level 3
/* These MACRO will be expanded when the code is compiled. */
#define _FUNCTION_OP(type1, XTYPE1, type2, XTYPE2) \
    __FUNCTION_OP(type1, XTYPE1, type2, XTYPE2, +, pl)  \
    __FUNCTION_OP(type1, XTYPE1, type2, XTYPE2, -, mi)  \
    __FUNCTION_OP(type1, XTYPE1, type2, XTYPE2, *, mul) \
    __FUNCTION_OP(type1, XTYPE1, type2, XTYPE2, /, div)

#define _FUNCTION_OP_CONST(type, XTYPE, const_type, CONST_ARG_MACRO) \
    __FUNCTION_OP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, /, div) \
    __FUNCTION_OP_LCONST(type, XTYPE, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_LCONST(type, XTYPE, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_LCONST(type, XTYPE, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_LCONST(type, XTYPE, const_type, CONST_ARG_MACRO, /, div)

#define _FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, ==, eq) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, !=, ne) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, >, gt) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, >=, ge) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, <, lt) \
    __FUNCTION_CMP(type1, XTYPE1, type2, XTYPE2, <=, le)

#define _FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO) \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, ==, eq)  \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, !=, ne)  \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO,  >, gt) \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, >=, ge) \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO,  <, lt) \
    __FUNCTION_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, <=, le) \

//Macro Level 2
#define FUNCTION_OP(type, XTYPE1) \
    _FUNCTION_OP(type, XTYPE1, int2, Int16) \
    _FUNCTION_OP(type, XTYPE1, int4, Int32) \
    _FUNCTION_OP(type, XTYPE1, int8, Int64) \
    _FUNCTION_OP(type, XTYPE1, float4, Float4) \
    _FUNCTION_OP(type, XTYPE1, float8, Float8)

#define FUNCTION_OP_RCONST(type, XTYPE) \
    _FUNCTION_OP_CONST(type, XTYPE, int2, PG_GETARG_INT16) \
    _FUNCTION_OP_CONST(type, XTYPE, int4, PG_GETARG_INT32) \
    _FUNCTION_OP_CONST(type, XTYPE, int8, PG_GETARG_INT64) \
    _FUNCTION_OP_CONST(type, XTYPE, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_OP_CONST(type, XTYPE, float8, PG_GETARG_FLOAT8)

#define FUNCTION_CMP(type1, XTYPE1) \
    _FUNCTION_CMP(type1, XTYPE1, int2, Int16) \
    _FUNCTION_CMP(type1, XTYPE1, int4, Int32) \
    _FUNCTION_CMP(type1, XTYPE1, int8, Int64) \
    _FUNCTION_CMP(type1, XTYPE1, float4, Float4) \
    _FUNCTION_CMP(type1, XTYPE1, float8, Float8)

#define FUNCTION_CMP_RCONST(type, XTYPE) \
    _FUNCTION_CMP_RCONST(type, XTYPE, int2, PG_GETARG_INT16) \
    _FUNCTION_CMP_RCONST(type, XTYPE, int4, PG_GETARG_INT32) \
    _FUNCTION_CMP_RCONST(type, XTYPE, int8, PG_GETARG_INT64) \
    _FUNCTION_CMP_RCONST(type, XTYPE, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_CMP_RCONST(type, XTYPE, float8, PG_GETARG_FLOAT8)

//Macro Level 1
#define FUNCTION_OP_ALL(type, XTYPE1) \
    FUNCTION_OP(type, XTYPE1) \
    FUNCTION_OP_RCONST(type, XTYPE1) \
    FUNCTION_CMP(type, XTYPE1) \
    FUNCTION_CMP_RCONST(type, XTYPE1)

#define FUNCTION_BUILD(type, typeoid) \
    _FUNCTION_BUILD(type, typeoid) \
    _FUNCTION_IN(type, typeoid) \
    _FUNCTION_OUT(type, typeoid)

//Macro Level 0
FUNCTION_BUILD(int2, INT2OID)
FUNCTION_BUILD(int4, INT4OID)
FUNCTION_BUILD(int8, INT8OID)
FUNCTION_BUILD(float4, FLOAT4OID)
FUNCTION_BUILD(float8, FLOAT8OID)
FUNCTION_BUILD(bool, BOOLOID)

FUNCTION_OP_ALL(int2, Int16)
FUNCTION_OP_ALL(int4, Int32)
FUNCTION_OP_ALL(int8, Int64)
FUNCTION_OP_ALL(float4, Float4)
FUNCTION_OP_ALL(float8, Float8)
FUNCTION_OP_ALL(bool, Bool)

