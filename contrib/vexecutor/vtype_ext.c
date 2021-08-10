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
#include "vtype_ext.h"
#include "utils/builtins.h"

#define MAX_NUM_LEN 64
extern int BATCHSIZE;

/* vdateadtin()
 * Given text string, convert to internal format date.
 */
PG_FUNCTION_INFO_V1(vdateadtin);
Datum
vdateadtin(PG_FUNCTION_ARGS)
{
    char *intString = PG_GETARG_CSTRING(0);
    vtype *res = NULL;
    char tempstr[MAX_NUM_LEN] = {0};
    int n = 0;
    res = buildvtype(DATEOID,BATCHSIZE,NULL);
    for (n = 0; *intString && n < BATCHSIZE; n++)
    {
        char *start = NULL;
        while (*intString && isspace((unsigned char) *intString))
            intString++;
        if (*intString == '\0')
            break;
        start = intString;
        while ((*intString && !isspace((unsigned char) *intString)) && *intString != '\0')
            intString++;
        Assert(intString - start < MAX_NUM_LEN);
        strncpy(tempstr, start, intString - start);
        tempstr[intString - start] = 0;
        res->values[n] = DirectFunctionCall1(date_in, CStringGetDatum(tempstr));
        while (*intString && !isspace((unsigned char) *intString))
            intString++;
    }
    while (*intString && isspace((unsigned char) *intString))
        intString++;
    if (*intString)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("int2vector has too many elements")));
    res->elemtype = DATEOID;
    res->dim = n;
    SET_VARSIZE(res, VTYPESIZE(n));
    PG_RETURN_POINTER(res);
}

/* vdateadtout()
 * Given internal format date, convert to text string.
 */
PG_FUNCTION_INFO_V1(vdateadtout);
Datum
vdateadtout(PG_FUNCTION_ARGS)
{
    vtype * arg1 = (vtype *) PG_GETARG_POINTER(0);
    int len = arg1->dim;
    int i = 0;
    char *rp;
    char *result;
    rp = result = (char *) palloc0(len * MAX_NUM_LEN + 1);
    for (i = 0; i < len; i++)
    {
        if (i != 0)
            *rp++ = ' ';
        strcat(rp, DatumGetCString(DirectFunctionCall1(date_out, arg1->values[i])));
        while (*++rp != '\0');
    }
    *rp = '\0';
    PG_RETURN_CSTRING(result);
}

#define EXT_TYPE_CMP(TYPE,TYPEEXPR,cmpsym,cmpstr) \
PG_FUNCTION_INFO_V1(v##TYPE##_##cmpstr); \
Datum v##TYPE##_##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##TYPE* arg1 = PG_GETARG_POINTER(0); \
    v##TYPE* arg2 = PG_GETARG_POINTER(1); \
    Assert(arg1->dim == arg2->dim); \
    vbool *res = buildvtype(BOOLOID, BATCHSIZE, NULL); \
    size = arg1->dim; \
    while(i < size) \
    { \
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; \
        if(!res->isnull[i]) \
            res->values[i] = BoolGetDatum(DatumGet##TYPEEXPR(arg1->values[i]) cmpsym (DatumGet##TYPEEXPR(arg2->values[i]))); \
        i++; \
    } \
    res->dim = arg1->dim; \
    PG_RETURN_POINTER(res);\
}; \

#define EXT_TYPE_CMP_RCONST(type, XTYPE, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type##_##cmpstr##_##type); \
Datum \
v##type##_##cmpstr##_##type(PG_FUNCTION_ARGS) \
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

EXT_TYPE_CMP(dateadt,DateADT,==,eq)
EXT_TYPE_CMP(dateadt,DateADT,!=,ne)
EXT_TYPE_CMP(dateadt,DateADT,<,lt)
EXT_TYPE_CMP(dateadt,DateADT,<=,le)
EXT_TYPE_CMP(dateadt,DateADT,>,gt)
EXT_TYPE_CMP(dateadt,DateADT,>=,ge)

EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT , PG_GETARG_DATEADT, ==, eq)
EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT, PG_GETARG_DATEADT, !=, ne)
EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT, PG_GETARG_DATEADT, <, lt)
EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT, PG_GETARG_DATEADT, <=, le)
EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT, PG_GETARG_DATEADT, >, gt)
EXT_TYPE_CMP_RCONST(dateadt, DateADT, DateADT, PG_GETARG_DATEADT, >=, ge)

PG_FUNCTION_INFO_V1(vdateadt_mi_dateadt);
Datum vdateadt_mi_dateadt(PG_FUNCTION_ARGS)
{
    int size = 0;
    int i = 0;
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    DateADT arg2 = PG_GETARG_INT32(1);
    vint4 *res = buildvint4(BATCHSIZE, NULL);
    size = arg1->dim;
    while(i < size)
    {
        res->isnull[i] = arg1->isnull[i];
        if(!res->isnull[i])
            res->values[i] = Int32GetDatum((DatumGetDateADT(arg1->values[i])) - (DatumGetDateADT(arg2)));
        i++;
    }
    res->dim = arg1->dim;
    PG_RETURN_POINTER(res);
}

/* vdateadt_mii_int4
 * vdateadt - int4
 * */
PG_FUNCTION_INFO_V1(vdateadt_mii_int4);
Datum vdateadt_mii_int4(PG_FUNCTION_ARGS)
{

    int size = 0;
    int i = 0;
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    int arg2 = PG_GETARG_INT32(1);
    vdateadt* res = buildvint4(BATCHSIZE, NULL);
    size = arg1->dim;
    while(i < size)
    {
        res->isnull[i] = arg1->isnull[i] ;
        if(!res->isnull[i])
            res->values[i] = DateADTGetDatum((DatumGetDateADT(arg1->values[i])) - arg2);
        i++;
    }
    res->dim = arg1->dim;
    PG_RETURN_POINTER(res);
}

/* vdateadt_pli_int4
 * vdateadt + int4
 * */
PG_FUNCTION_INFO_V1(vdateadt_pli_int4);
Datum vdateadt_pli_int4(PG_FUNCTION_ARGS)
{
    int size = 0;
    int i = 0;
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    int arg2 = PG_GETARG_INT32(1);
    vdateadt* res = buildvint4(BATCHSIZE, NULL);
    size = arg1->dim;
    while(i < size)
    {
        res->isnull[i] = arg1->isnull[i] ;
        if(!res->isnull[i])
            res->values[i] = Int32GetDatum((DatumGetDateADT(arg1->values[i])) + arg2);
        i++;
    }
    res->dim = arg1->dim;
    PG_RETURN_POINTER(res);
}

/* vdateadt_mi
 * vdateadt - vdateadt
 * */
PG_FUNCTION_INFO_V1(vdateadt_mi);
Datum vdateadt_mi(PG_FUNCTION_ARGS)
{
    int size = 0; 
    int i = 0;
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    vdateadt* arg2 = PG_GETARG_POINTER(1);
    vint4 *res = buildvint4(BATCHSIZE, NULL); 
    Assert(arg1->dim == arg2->dim); 
    size = arg1->dim; 
    while(i < size) 
    { 
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; 
        if(!res->isnull[i]) 
            res->values[i] = Int32GetDatum((DatumGetDateADT(arg1->values[i])) - (DatumGetDateADT(arg2->values[i]))); 
        i++; 
    } 
    res->dim = arg1->dim; 
    PG_RETURN_POINTER(res); 
}

/* vdateadt_mii
 * vdateadt - vint4
 * */
PG_FUNCTION_INFO_V1(vdateadt_mii);
Datum vdateadt_mii(PG_FUNCTION_ARGS)
{
    int size = 0; 
    int i = 0; 
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    vint4* arg2 = PG_GETARG_POINTER(1); 
    vdateadt* res = buildvint4(BATCHSIZE, NULL);
    Assert(arg1->dim == arg2->dim); 
    size = arg1->dim; 
    while(i < size) 
    {
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; 
        if(!res->isnull[i]) 
            res->values[i] = DateADTGetDatum((DatumGetDateADT(arg1->values[i])) - (DatumGetInt32(arg2->values[i]))); 
        i++; 
    } 
    res->dim = arg1->dim; 
    PG_RETURN_POINTER(res); 
}

/* vdateadt_pli
 * vdateadt + vint4
 * */
PG_FUNCTION_INFO_V1(vdateadt_pli);
Datum vdateadt_pli(PG_FUNCTION_ARGS)
{
    int size = 0; 
    int i = 0; 
    vdateadt* arg1 = PG_GETARG_POINTER(0);
    vint4* arg2 = PG_GETARG_POINTER(1); 
    vdateadt* res = buildvint4(BATCHSIZE, NULL);
    Assert(arg1->dim == arg2->dim); 
    size = arg1->dim; 
    while(i < size) 
    { 
        res->isnull[i] = arg1->isnull[i] || arg2->isnull[i]; 
        if(!res->isnull[i]) 
            res->values[i] = Int32GetDatum((DatumGetDateADT(arg1->values[i])) + (DatumGetDateADT(arg2->values[i]))); 
        i++; 
    } 
    res->dim = arg1->dim; 
    PG_RETURN_POINTER(res); 
}
