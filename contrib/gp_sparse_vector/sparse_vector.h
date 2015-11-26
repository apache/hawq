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

/**
 * @file
 * \brief Persistent storage for the Sparse Vector Datatype
 *
 */

#ifndef SPARSEVECTOR_H
#define SPARSEVECTOR_H

#include "SparseData.h"
#include "float_specials.h"

/*!
 * \internal
 * Consists of the dimension of the vector (how many elements) and a SparseData
 * structure that stores the data in a compressed format.
 * \endinternal
 */
typedef struct {
	int32 vl_len_;   /**< This is unused at the moment */
	int32 dimension; /**< Number of elements in this vector, special case is -1 indicates a scalar */
	char data[1];   /**< The serialized SparseData representing the vector here */
} SvecType;

#define DatumGetSvecTypeP(X)           ((SvecType *) PG_DETOAST_DATUM(X))
#define DatumGetSvecTypePCopy(X)       ((SvecType *) PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_SVECTYPE_P(n)        DatumGetSvecTypeP(PG_GETARG_DATUM(n))
#define PG_GETARG_SVECTYPE_P_COPY(n)   DatumGetSvecTypePCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_SVECTYPE_P(x)        PG_RETURN_POINTER(x)

/* Below are the locations of the SparseData values within the serialized
 * inline SparseData below the Svec header
 *
 * All macros take an (SvecType *) as argument
 */
#define SVECHDRSIZE	(VARHDRSZ + sizeof(int32))
/* Beginning of the serialized SparseData */
#define SVEC_SDATAPTR(x)	((char *)(x)+SVECHDRSIZE)
#define SVEC_SIZEOFSERIAL(x)	(SVECHDRSIZE+SIZEOF_SPARSEDATASERIAL((SparseData)SVEC_SDATAPTR(x)))
#define SVEC_UNIQUE_VALCNT(x)	(SDATA_UNIQUE_VALCNT(SVEC_SDATAPTR(x)))
#define SVEC_TOTAL_VALCNT(x)	(SDATA_TOTAL_VALCNT(SVEC_SDATAPTR(x)))
#define SVEC_DATA_SIZE(x) 	(SDATA_DATA_SIZE(SVEC_SDATAPTR(x)))
#define SVEC_VALS_PTR(x)	(SDATA_VALS_PTR(SVEC_SDATAPTR(x)))
/* The size of the index is variable unlike the values, so in the serialized
 * SparseData, we include an int32 that indicates the size of the index.
 */
#define SVEC_INDEX_SIZE(x) 	(SDATA_INDEX_SIZE(SVEC_SDATAPTR(x)))
#define SVEC_INDEX_PTR(x) 	(SDATA_INDEX_PTR(SVEC_SDATAPTR(x)))

/** @return True if input is a scalar */
#define IS_SCALAR(x)	(((x)->dimension) < 0 ? 1 : 0 )

/** @return True if input is a NULL, represented internally as a NVP */
#define IS_NVP(x)  (memcmp(&(x),&(NVP),sizeof(double)) == 0)

static inline int check_scalar(int i1, int i2)
{
	if ((!i1) && (!i2)) return(0);
	else if (i1 && i2) return(3);
	else if (i1)  return(1);
	else if (i2) return(2);
	return(0);
}

/*
 * This routine supplies a pointer to a SparseData derived from an SvecType.
 * The SvecType is a serialized structure with fixed memory allocations, so
 * care must be taken not to append to the embedded StringInfo structs
 * without re-serializing the SparseData into the SvecType.
 */
static inline SparseData sdata_from_svec(SvecType *svec)
{
	char *sdataptr   = SVEC_SDATAPTR(svec);
	SparseData sdata = (SparseData)sdataptr;
	sdata->vals  = (StringInfo)SDATA_DATA_SINFO(sdataptr);
	sdata->index = (StringInfo)SDATA_INDEX_SINFO(sdataptr);
	sdata->vals->data   = SVEC_VALS_PTR(svec);
	if (sdata->index->maxlen == 0)
	{
		sdata->index->data = NULL;
	} else
	{
		sdata->index->data  = SVEC_INDEX_PTR(svec);
	}
	return(sdata);
}

static inline void printout_svec(SvecType *svec, char *msg, int stop);
static inline void printout_svec(SvecType *svec, char *msg, int stop)
{
	printout_sdata((SparseData)SVEC_SDATAPTR(svec), msg, stop);
	elog(NOTICE,"len,dimension=%d,%d",VARSIZE(svec),svec->dimension);
}

char *svec_out_internal(SvecType *svec);
SvecType *svec_from_sparsedata(SparseData sdata,bool trim);
ArrayType *svec_return_array_internal(SvecType *svec);
char *svec_out_internal(SvecType *svec);
SvecType *svec_make_scalar(float8 value);
SvecType *svec_from_float8arr(float8 *array, int dimension);
SvecType *op_svec_by_svec_internal(enum operation_t operation, SvecType *svec1, SvecType *svec2);
SvecType *svec_operate_on_sdata_pair(int scalar_args,enum operation_t operation,SparseData left,SparseData right);
SvecType *makeEmptySvec(int allocation);
SvecType *reallocSvec(SvecType *source);

Datum svec_in(PG_FUNCTION_ARGS);
Datum svec_out(PG_FUNCTION_ARGS);
Datum svec_return_vector(PG_FUNCTION_ARGS);
Datum svec_return_array(PG_FUNCTION_ARGS);
Datum svec_send(PG_FUNCTION_ARGS);
Datum svec_recv(PG_FUNCTION_ARGS);

// Operators
Datum svec_pow(PG_FUNCTION_ARGS);
Datum svec_equals(PG_FUNCTION_ARGS);
Datum svec_minus(PG_FUNCTION_ARGS);
Datum svec_plus(PG_FUNCTION_ARGS);
Datum svec_div(PG_FUNCTION_ARGS);
Datum svec_dot(PG_FUNCTION_ARGS);
Datum svec_mult(PG_FUNCTION_ARGS);
Datum svec_summate(PG_FUNCTION_ARGS);

Datum float8arr_minus_float8arr(PG_FUNCTION_ARGS);
Datum svec_minus_float8arr(PG_FUNCTION_ARGS);
Datum float8arr_minus_svec(PG_FUNCTION_ARGS);
Datum float8arr_plus_float8arr(PG_FUNCTION_ARGS);
Datum svec_plus_float8arr(PG_FUNCTION_ARGS);
Datum float8arr_plus_svec(PG_FUNCTION_ARGS);
Datum float8arr_mult_float8arr(PG_FUNCTION_ARGS);
Datum svec_mult_float8arr(PG_FUNCTION_ARGS);
Datum float8arr_mult_svec(PG_FUNCTION_ARGS);
Datum float8arr_div_float8arr(PG_FUNCTION_ARGS);
Datum svec_div_float8arr(PG_FUNCTION_ARGS);
Datum float8arr_div_svec(PG_FUNCTION_ARGS);
Datum svec_dot_float8arr(PG_FUNCTION_ARGS);
Datum float8arr_dot_svec(PG_FUNCTION_ARGS);

// Casts
Datum svec_cast_int2(PG_FUNCTION_ARGS);
Datum svec_cast_int4(PG_FUNCTION_ARGS);
Datum svec_cast_int8(PG_FUNCTION_ARGS);
Datum svec_cast_float4(PG_FUNCTION_ARGS);
Datum svec_cast_float8(PG_FUNCTION_ARGS);
Datum svec_cast_numeric(PG_FUNCTION_ARGS);

Datum float8arr_cast_int2(PG_FUNCTION_ARGS);
Datum float8arr_cast_int4(PG_FUNCTION_ARGS);
Datum float8arr_cast_int8(PG_FUNCTION_ARGS);
Datum float8arr_cast_float4(PG_FUNCTION_ARGS);
Datum float8arr_cast_float8(PG_FUNCTION_ARGS);
Datum float8arr_cast_numeric(PG_FUNCTION_ARGS);

Datum svec_cast_float8arr(PG_FUNCTION_ARGS);
Datum svec_cast_positions_float8arr(PG_FUNCTION_ARGS);

#endif  /* SPARSEVECTOR_H */
