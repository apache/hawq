/**
 * @file
 * This module defines a collection of operators for svecs. The functions
 * are usually wrappers that call the corresponding operators defined for
 * SparseData.
 */

#include <postgres.h>

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "utils/array.h"
#include "catalog/pg_type.h"
#include "utils/numeric.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "access/hash.h"

#include "sparse_vector.h"

#ifndef NO_PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/**
 * For many functions defined in this module, the operation has no meaning
 * if the array dimensions aren't the same, unless one of the inputs is a
 * scalar. This routine checks that condition.
 */
void check_dimension(SvecType *svec1, SvecType *svec2, char *msg) {
	if ((!IS_SCALAR(svec1)) &&
	    (!IS_SCALAR(svec2)) &&
	    (svec1->dimension != svec2->dimension)) {
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("%s: array dimension of inputs are not the same: dim1=%d, dim2=%d\n",
				msg, svec1->dimension, svec2->dimension)));
	}
}

/**
 * Dot Product of two svec types
 */
double svec_svec_dot_product(SvecType *svec1, SvecType *svec2) {
	SparseData left  = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);

	check_dimension(svec1,svec2,"svec_svec_dot_product");
	return sum_sdata_values_double( op_sdata_by_sdata(multiply,left,right));
}

/**
 *  svec_concat_replicate - replicates an svec multiple times
 */
Datum svec_concat_replicate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_concat_replicate);

Datum svec_concat_replicate(PG_FUNCTION_ARGS)
{
	int multiplier = PG_GETARG_INT32(0);
	if (multiplier < 0)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("multiplier cannot be negative")));

	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData rep  = sdata_from_svec(svec);
	SparseData sdata = concat_replicate(rep, multiplier);

	PG_RETURN_SVECTYPE_P(svec_from_sparsedata(sdata,true));
}

/**
 *  svec_concat - concatenates two svecs
 */
Datum svec_concat(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_concat );
Datum svec_concat(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) && (!PG_ARGISNULL(1)))
                PG_RETURN_SVECTYPE_P(PG_GETARG_SVECTYPE_P(1));
        else if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
                PG_RETURN_NULL();
        else if (PG_ARGISNULL(1))
                PG_RETURN_SVECTYPE_P(PG_GETARG_SVECTYPE_P(0));

	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);
	SparseData sdata = concat(left, right);

	PG_RETURN_SVECTYPE_P(svec_from_sparsedata(sdata,true));
}

/**
 *  svec_eq - returns the equality of two svecs
 */
Datum svec_eq(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_eq );
Datum svec_eq(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	SparseData left  = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);
	PG_RETURN_BOOL(sparsedata_eq(left,right));
}

/*
 * Svec comparison functions based on the l2 norm
 */
static int32_t svec_l2_cmp_internal(SvecType *svec1, SvecType *svec2)
{
	SparseData left  = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);
	double magleft  = l2norm_sdata_values_double(left);
	double magright = l2norm_sdata_values_double(right);
	int result;

	if (IS_NVP(magleft) || IS_NVP(magright)) {
		result = -5;
		PG_RETURN_INT32(result);
	}

	if (magleft < magright) result = -1;
	else if (magleft > magright) result = 1;
	else result = 0;

	PG_RETURN_INT32(result);
}
Datum svec_l2_cmp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_cmp );
Datum svec_l2_cmp(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_INT32(result);
}
Datum svec_l2_lt(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_lt );
Datum svec_l2_lt(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL((result == -1) ? 1 : 0);
}
Datum svec_l2_le(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_le );
Datum svec_l2_le(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL(((result == -1)||(result == 0)) ? 1 : 0);
}
Datum svec_l2_eq(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_eq );
Datum svec_l2_eq(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL((result == 0) ? 1 : 0);
}
Datum svec_l2_ne(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_ne );
Datum svec_l2_ne(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL((result != 0) ? 1 : 0);
}
Datum svec_l2_gt(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_gt );
Datum svec_l2_gt(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL((result == 1) ? 1 : 0);
}
Datum svec_l2_ge(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( svec_l2_ge );
Datum svec_l2_ge(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	int result = svec_l2_cmp_internal(svec1,svec2);

	if (result == -5) PG_RETURN_NULL();

	PG_RETURN_BOOL(((result == 0) || (result == 1)) ? 1 : 0);
}

/**
 * Performs one of subtract, add, multiply, or divide depending on value
 * of operation.
 */
SvecType * svec_operate_on_sdata_pair(int scalar_args, enum operation_t op,
				      SparseData left, SparseData right)
{
	SparseData sdata = NULL;
	float8 *left_vals = (float8 *)(left->vals->data);
	float8 *right_vals = (float8 *)(right->vals->data);
	float8 data_result;

	switch (scalar_args) {
	case 0: 		//neither arg is scalar
		sdata = op_sdata_by_sdata(op,left,right);
		break;
	case 1:			//left arg is scalar
		sdata=op_sdata_by_scalar_copy(op,(char *)left_vals,right,false);
		break;
	case 2:			//right arg is scalar
		sdata=op_sdata_by_scalar_copy(op,(char *)right_vals,left,true);
		break;
	case 3:			//both args are scalar
		switch (op) {
		case subtract:
			data_result = left_vals[0] - right_vals[0];
			break;
		case add:
		default:
			data_result = left_vals[0] + right_vals[0];
			break;
		case multiply:
			data_result = left_vals[0] * right_vals[0];
			break;
		case divide:
			data_result = left_vals[0] / right_vals[0];
			break;
		}
		return svec_make_scalar(data_result);
		break;
	}
	return svec_from_sparsedata(sdata,true);
}


SvecType * op_svec_by_svec_internal(enum operation_t op, SvecType *svec1, SvecType *svec2)
{
	SparseData left  = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);

	int scalar_args = check_scalar(IS_SCALAR(svec1),IS_SCALAR(svec2));

	return svec_operate_on_sdata_pair(scalar_args,op,left,right);
}

/*
 * Do exponentiation, only makes sense if the left is a vector and the right
 * is a scalar or if both are scalar
 */
static SvecType *
pow_svec_by_scalar_internal(SvecType *svec1, SvecType *svec2)
{
	SparseData left  = sdata_from_svec(svec1);
	SparseData right = sdata_from_svec(svec2);
	SparseData sdata = NULL;
	double *left_vals=(double *)(left->vals->data);
	double *right_vals=(double *)(right->vals->data);
	double data_result;

	int scalar_args = check_scalar(IS_SCALAR(svec1),IS_SCALAR(svec2));

	switch(scalar_args) {
	case 0: 		//neither arg is scalar
	case 1:			//left arg is scalar
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Svec exponentiation is undefined when the right argument is a vector")));
		break;
	case 2:			//right arg is scalar
		if (right_vals[0] == 2.) // the squared case
		{
			sdata = square_sdata(left);
		} else if (right_vals[0] == 3.) // the cubed case
		{
			sdata = cube_sdata(left);
		}  else if (right_vals[0] == 4.) // the quad case
		{
			sdata = quad_sdata(left);
		} else {
			sdata = pow_sdata_by_scalar(left,(char *)right_vals);
		}
		break;
	case 3:			//both args are scalar
		data_result = pow(left_vals[0],right_vals[0]);
		return svec_make_scalar(data_result);
		break;
	}
	return svec_from_sparsedata(sdata,true);
}

PG_FUNCTION_INFO_V1( svec_pow );
Datum svec_pow(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	check_dimension(svec1,svec2,"svec_pow");
	SvecType *result = pow_svec_by_scalar_internal(svec1,svec2);
	PG_RETURN_SVECTYPE_P(result);
}

PG_FUNCTION_INFO_V1( svec_minus );
Datum svec_minus(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	check_dimension(svec1,svec2,"svec_minus");
	SvecType *result = op_svec_by_svec_internal(subtract,svec1,svec2);
	PG_RETURN_SVECTYPE_P(result);
}

PG_FUNCTION_INFO_V1( svec_plus );
Datum svec_plus(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	check_dimension(svec1,svec2,"svec_plus");
	SvecType *result = op_svec_by_svec_internal(add,svec1,svec2);
	PG_RETURN_SVECTYPE_P(result);
}

PG_FUNCTION_INFO_V1( svec_mult );
Datum svec_mult(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	check_dimension(svec1,svec2,"svec_mult");
	SvecType *result = op_svec_by_svec_internal(multiply,svec1,svec2);
	PG_RETURN_SVECTYPE_P(result);
}

PG_FUNCTION_INFO_V1( svec_div );
Datum svec_div(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);
	check_dimension(svec1,svec2,"svec_div");
	SvecType *result = op_svec_by_svec_internal(divide,svec1,svec2);
	PG_RETURN_SVECTYPE_P(result);
}

PG_FUNCTION_INFO_V1( svec_dot );
/**
 *  svec_dot - computes the dot product of two svecs
 */
Datum svec_dot(PG_FUNCTION_ARGS)
{
	SvecType *svec1 = PG_GETARG_SVECTYPE_P(0);
	SvecType *svec2 = PG_GETARG_SVECTYPE_P(1);

	double accum = svec_svec_dot_product( svec1, svec2);

	if (IS_NVP(accum)) PG_RETURN_NULL();

	PG_RETURN_FLOAT8(accum);
}

/*
 * Cast from int2,int4,int8,float4,float8 scalar to SvecType
 */
PG_FUNCTION_INFO_V1( svec_cast_int2 );
Datum svec_cast_int2(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT16(0);
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}
PG_FUNCTION_INFO_V1( svec_cast_int4 );
Datum svec_cast_int4(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT32(0);
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}
PG_FUNCTION_INFO_V1( svec_cast_int8 );
Datum svec_cast_int8(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT64(0);
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}
PG_FUNCTION_INFO_V1( svec_cast_float4 );
Datum svec_cast_float4(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_FLOAT4(0);
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}
PG_FUNCTION_INFO_V1( svec_cast_float8 );
Datum svec_cast_float8(PG_FUNCTION_ARGS) {
	float8 value=PG_GETARG_FLOAT8(0);
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}
PG_FUNCTION_INFO_V1( svec_cast_numeric );
Datum svec_cast_numeric(PG_FUNCTION_ARGS) {
	Datum num=PG_GETARG_DATUM(0);
	float8 value;
	value = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow,num));
	PG_RETURN_SVECTYPE_P(svec_make_scalar(value));
}

/*
 * Cast from int2,int4,int8,float4,float8 scalar to float8[]
 */
PG_FUNCTION_INFO_V1( float8arr_cast_int2 );
Datum float8arr_cast_int2(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT16(0);
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}
PG_FUNCTION_INFO_V1( float8arr_cast_int4 );
Datum float8arr_cast_int4(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT32(0);
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}
PG_FUNCTION_INFO_V1( float8arr_cast_int8 );
Datum float8arr_cast_int8(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_INT64(0);
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}
PG_FUNCTION_INFO_V1( float8arr_cast_float4 );
Datum float8arr_cast_float4(PG_FUNCTION_ARGS) {
	float8 value=(float8 )PG_GETARG_FLOAT4(0);
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}
PG_FUNCTION_INFO_V1( float8arr_cast_float8 );
Datum float8arr_cast_float8(PG_FUNCTION_ARGS) {
	float8 value=PG_GETARG_FLOAT8(0);
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}
PG_FUNCTION_INFO_V1( float8arr_cast_numeric );
Datum float8arr_cast_numeric(PG_FUNCTION_ARGS) {
	Datum num=PG_GETARG_DATUM(0);
	float8 value;
	value = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow,num));
	PG_RETURN_ARRAYTYPE_P(svec_return_array_internal(svec_make_scalar(value)));
}

/** Constructs an 1-dimensional svec given a float8 */
SvecType *svec_make_scalar(float8 value) {
	SparseData sdata = float8arr_to_sdata(&value,1);
	SvecType *result = svec_from_sparsedata(sdata,true);
	result->dimension = -1;
	return result;
}


PG_FUNCTION_INFO_V1( svec_cast_float8arr );
/**
 *  svec_cast_float8arr - turns a float8 array into an svec
 */
Datum svec_cast_float8arr(PG_FUNCTION_ARGS) {
	ArrayType *A_PG = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *output_svec;
	float8 *array_temp;
	bits8 *bitmap;
	int bitmask;
	int i,j;

	if (ARR_ELEMTYPE(A_PG) != FLOAT8OID)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_float8arr only defined over float8[]")));
	if (ARR_NDIM(A_PG) != 1)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_float8arr only defined over 1 dimensional arrays")));

	/* Extract array */
	int dimension = ARR_DIMS(A_PG)[0];
	float8 *array = (float8 *)ARR_DATA_PTR(A_PG);

	/* If the data array has NULLs, then we need to create an array to
	 * store the NULL values as NVP values defined in float_specials.h.
	 */
	if (ARR_HASNULL(A_PG)) {
		array_temp = array;
		array = (double *)palloc(sizeof(float8) * dimension);
		bitmap = ARR_NULLBITMAP(A_PG);
		bitmask = 1;
		j = 0;
		for (i=0; i<dimension; i++) {
			if (bitmap && (*bitmap & bitmask) == 0) // if NULL
				array[i] = NVP;
			else {
				array[i] = array_temp[j];
				j++;
			}
			if (bitmap) { // advance bitmap pointer
				bitmask <<= 1;
				if (bitmask == 0x100) {
					bitmap++;
					bitmask = 1;
				}
			}
		}
	 }

	/* Create the output SVEC */
	SparseData sdata = float8arr_to_sdata(array,dimension);
	output_svec = svec_from_sparsedata(sdata,true);

	if (ARR_HASNULL(A_PG))
		pfree(array);

	PG_RETURN_SVECTYPE_P(output_svec);
}

PG_FUNCTION_INFO_V1( svec_cast_positions_float8arr );
/**
 *  svec_cast_positions_float8arr - turns a pair of arrays, the first an int4[]
 *    denoting positions and the second a float8[] denoting values, into an
 *    svec of a given size with a given default value everywhere else.
 */
Datum svec_cast_positions_float8arr(PG_FUNCTION_ARGS) {
	ArrayType *B_PG = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *A_PG = PG_GETARG_ARRAYTYPE_P(1);
	int64 size = PG_GETARG_INT64(2);
	float8 base_value = PG_GETARG_FLOAT8(3);
	SvecType *output_svec;
	int i = 0;

	if (ARR_ELEMTYPE(A_PG) != FLOAT8OID)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr valeus only defined over float8[]")));
	if (ARR_NDIM(A_PG) != 1)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr only defined over 1 dimensional arrays")));

	if (ARR_NULLBITMAP(A_PG))
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr does not allow null bitmaps on arrays")));

	if (ARR_ELEMTYPE(B_PG) != INT8OID)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr positions only defined over int[]")));
	if (ARR_NDIM(B_PG) != 1)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr only defined over 1 dimensional arrays")));

	if (ARR_NULLBITMAP(B_PG))
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr does not allow null bitmaps on arrays")));

	/* Extract array */
	int dimension = ARR_DIMS(A_PG)[0];
	int dimension2 = ARR_DIMS(B_PG)[0];

	if (dimension != dimension2)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr position and value vectors must be of the same size")));

	float8 *array = (float8 *)ARR_DATA_PTR(A_PG);
	int64 *array_pos =  (int64 *)ARR_DATA_PTR(B_PG);

	if (array_pos[dimension-1] > size)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("svec_cast_positions_float8arr some of the position values are larger than maximum array size declared")));

	for(i=0;i < dimension;++i){
		if(array_pos[i] <= 0){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("svec_cast_positions_float8arr only accepts position that are positive integers (x > 0)")));
		}
	}

	/* Create the output SVEC */
	SparseData sdata = position_to_sdata(array,array_pos,FLOAT8OID,dimension,size,base_value);
	output_svec = svec_from_sparsedata(sdata,true);

	PG_RETURN_SVECTYPE_P(output_svec);
}

/*
 * Provide some operators for Postgres FLOAT8OID arrays
 */
/*
 * Equality
 */
static bool float8arr_equals_internal(ArrayType *left, ArrayType *right)
{
	/*
	 * Note that we are only defined for FLOAT8OID
	 */
        int dimleft = ARR_NDIM(left), dimright = ARR_NDIM(right);
        int *dimsleft = ARR_DIMS(left), *dimsright = ARR_DIMS(right);
	int numleft = ArrayGetNItems(dimleft,dimsleft);
	int numright = ArrayGetNItems(dimright,dimsright);
        double *vals_left = (double *)ARR_DATA_PTR(left);
	double *vals_right = (double *)ARR_DATA_PTR(right);
        bits8 *bitmap_left = ARR_NULLBITMAP(left);
	bits8 *bitmap_right = ARR_NULLBITMAP(right);
        int bitmask = 1;

        if ((dimsleft!=dimsright) || (numleft!=numright))
		return false;

	/*
	 * First we'll check to see if the null bitmaps are equivalent
	 */
	if (bitmap_left)
		if (! bitmap_right) return false;
	if (bitmap_right)
		if (! bitmap_left) return false;

	if (bitmap_left)
	{
        	for (int i=0; i<numleft; i++)
		{
                	if ((*bitmap_left & bitmask) == 0)
                		if ((*bitmap_left & bitmask) != 0)
			  		return false;
                        bitmask <<= 1;
                        if (bitmask == 0x100)
                        {
                                bitmap_left++;
                                bitmask = 1;
                        }
		}
	}

	/*
	 * Now we check for equality of all array values
	 */
       	for (int i=0; i<numleft; i++)
		if (vals_left[i] != vals_right[i]) return false;

        return true;
}

/**
 *  float8arr_equals - checks whether two float8 arrays are identical
 */
Datum float8arr_equals(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( float8arr_equals);
Datum float8arr_equals(PG_FUNCTION_ARGS) {
	ArrayType *left  = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *right = PG_GETARG_ARRAYTYPE_P(1);

	PG_RETURN_BOOL(float8arr_equals_internal(left,right));
}

/*
 * Returns a SparseData formed from a dense float8[] in uncompressed format.
 * This is useful for creating a SparseData without processing that can be
 * used by the SparseData processing routines.
 */
SparseData sdata_uncompressed_from_float8arr_internal(ArrayType *array)
{
        int dim = ARR_NDIM(array);
        int *dims = ARR_DIMS(array);
	int num = ArrayGetNItems(dim,dims);
        double *vals =(double *)ARR_DATA_PTR(array);
        bits8 *bitmap = ARR_NULLBITMAP(array);
        int   bitmask=1;

	/* Convert null items into NVPs */
	if (bitmap)
	{
		int j = 0;
		double *vals_temp = vals;
		vals = (double *)palloc(sizeof(float8) * num);
        	for (int i=0; i<num; i++)
		{
                	if ((*bitmap & bitmask) == 0) // if NULL
				vals[i] = NVP;
			else {
				vals[i] = vals_temp[j];
				j++;
			}
                        bitmask <<= 1;
                        if (bitmask == 0x100)
                        {
                                bitmap++;
                                bitmask = 1;
                        }
		}
	}
	/* Makes the SparseData; this relies on using NULL to represent a
	 * count array of ones, as described in SparseData.h, after definition
	 * of SparseDataStruct.
	 */
	SparseData result = makeInplaceSparseData(
				 (char *)vals,NULL,
				 num*sizeof(float8),0,FLOAT8OID,num,num);

	return(result);
}

/**
 *  float8arr_dot - computes the dot product of two float8 arrays
 */
Datum float8arr_dot(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1( float8arr_dot);
Datum float8arr_dot(PG_FUNCTION_ARGS) {
	ArrayType *arr_left   = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr_right  = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left  = sdata_uncompressed_from_float8arr_internal(arr_left);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr_right);
	SparseData mult_result;
	double accum;

	mult_result = op_sdata_by_sdata(multiply,left,right);
	accum = sum_sdata_values_double(mult_result);
	freeSparseData(left);
	freeSparseData(right);
	freeSparseDataAndData(mult_result);

	if (IS_NVP(accum)) PG_RETURN_NULL();

	PG_RETURN_FLOAT8(accum);
}

/*
 * Permute the basic operators (minus,plus,mult,div) between SparseData
 * and float8[]
 *
 * For each function, make a version that takes the left and right args as
 * each type (without copies)
 */
PG_FUNCTION_INFO_V1( float8arr_minus_float8arr );
Datum
float8arr_minus_float8arr(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left  = sdata_uncompressed_from_float8arr_internal(arr1);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr2);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,subtract,left,right));
}
PG_FUNCTION_INFO_V1( svec_minus_float8arr );
Datum
svec_minus_float8arr(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left = sdata_from_svec(svec);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,subtract,left,right));
}
PG_FUNCTION_INFO_V1( float8arr_minus_svec );
Datum
float8arr_minus_svec(PG_FUNCTION_ARGS)
{
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData right = sdata_from_svec(svec);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,subtract,left,right));
}

PG_FUNCTION_INFO_V1( float8arr_plus_float8arr );
Datum
float8arr_plus_float8arr(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left  = sdata_uncompressed_from_float8arr_internal(arr1);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr2);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,add,left,right));
}
PG_FUNCTION_INFO_V1( svec_plus_float8arr );
Datum
svec_plus_float8arr(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left = sdata_from_svec(svec);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,add,left,right));
}
PG_FUNCTION_INFO_V1( float8arr_plus_svec );
Datum
float8arr_plus_svec(PG_FUNCTION_ARGS)
{
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData right = sdata_from_svec(svec);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,add,left,right));
}
PG_FUNCTION_INFO_V1( float8arr_mult_float8arr );
Datum
float8arr_mult_float8arr(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left  = sdata_uncompressed_from_float8arr_internal(arr1);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr2);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	SvecType *svec = svec_operate_on_sdata_pair(scalar_args,multiply,left,right);
	PG_RETURN_SVECTYPE_P(svec);
}
PG_FUNCTION_INFO_V1( svec_mult_float8arr );
Datum
svec_mult_float8arr(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left = sdata_from_svec(svec);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	SvecType *result = svec_operate_on_sdata_pair(scalar_args,multiply,left,right);
	PG_RETURN_SVECTYPE_P(result);
}
PG_FUNCTION_INFO_V1( float8arr_mult_svec );
Datum
float8arr_mult_svec(PG_FUNCTION_ARGS)
{
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData right = sdata_from_svec(svec);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,multiply,left,right));
}
PG_FUNCTION_INFO_V1( float8arr_div_float8arr );
Datum
float8arr_div_float8arr(PG_FUNCTION_ARGS)
{
	ArrayType *arr1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType *arr2 = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left  = sdata_uncompressed_from_float8arr_internal(arr1);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr2);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,divide,left,right));
}
PG_FUNCTION_INFO_V1( svec_div_float8arr );
Datum
svec_div_float8arr(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
	SparseData left = sdata_from_svec(svec);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,divide,left,right));
}
PG_FUNCTION_INFO_V1( float8arr_div_svec );
Datum
float8arr_div_svec(PG_FUNCTION_ARGS)
{
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData right = sdata_from_svec(svec);
	int scalar_args = check_scalar(SDATA_IS_SCALAR(left),SDATA_IS_SCALAR(right));
	PG_RETURN_SVECTYPE_P(svec_operate_on_sdata_pair(scalar_args,divide,left,right));
}
PG_FUNCTION_INFO_V1( svec_dot_float8arr );
Datum
svec_dot_float8arr(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
	SparseData right = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData left = sdata_from_svec(svec);
	SparseData mult_result;
	double accum;
	mult_result = op_sdata_by_sdata(multiply,left,right);
	accum = sum_sdata_values_double(mult_result);
	freeSparseData(right);
	freeSparseDataAndData(mult_result);

	if (IS_NVP(accum)) PG_RETURN_NULL();

	PG_RETURN_FLOAT8(accum);
}
PG_FUNCTION_INFO_V1( float8arr_dot_svec);
Datum
float8arr_dot_svec(PG_FUNCTION_ARGS)
{
	ArrayType *arr = PG_GETARG_ARRAYTYPE_P(0);
	SvecType *svec = PG_GETARG_SVECTYPE_P(1);
	SparseData left = sdata_uncompressed_from_float8arr_internal(arr);
	SparseData right = sdata_from_svec(svec);
	SparseData mult_result;
	double accum;
	mult_result = op_sdata_by_sdata(multiply,left,right);
	accum = sum_sdata_values_double(mult_result);
	freeSparseData(left);
	freeSparseDataAndData(mult_result);

	if (IS_NVP(accum)) PG_RETURN_NULL();

	PG_RETURN_FLOAT8(accum);
}
