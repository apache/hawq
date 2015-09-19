/**
 * @file
 * \brief This file implements different operations on the SparseData structure.
 *
 * Most functions on sparse vectors are first defined on SparseData, and
 * then packaged up as functions on sparse vectors using wrappers.
 *
 * This is the general procedure for adding functionalities to sparse vectors:
 *  1. Write the function for SparseData.
 *  2. Wrap it up for SvecType in operators.c or sparse_vector.c.
 *  3. Make the function available in gp_svec.sql.
 */

#include <postgres.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include "SparseData.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "parser/parse_func.h"
#include "access/htup.h"
#include "catalog/pg_proc.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif

void* array_pos_ref = NULL;


/* -----------------------------------------------------------------------------
 *
 * The following functions used to be defined in SparseData.h. It is unclear to
 * me why (Florian Schoppmann, June 4, 2012).
 *
 * -------------------------------------------------------------------------- */

/* BEGIN Previously in SparseData.h */

/* Returns the size of each basic type
 */
size_t
size_of_type(Oid type)
{
	switch (type)
	{
		case FLOAT4OID: return(4);
		case FLOAT8OID: return(8);
		case CHAROID: return(1);
		case INT2OID: return(2);
		case INT4OID: return(4);
		case INT8OID: return(8);
	}
	return(1);
}

/* Appends a count to the count array
 * The function appendBinaryStringInfo always make sure to attach a trailing '\0'
 * to the data array of the index StringInfo.
 */
void append_to_rle_index(StringInfo index, int64 run_len)
{
	char bytes[]={0,0,0,0,0,0,0,0,0}; /* 9 bytes into which the compressed
					     int8 value is written */
	int8_to_compword(run_len,bytes); /* create compressed version of
					    int8 value */
	appendBinaryStringInfo(index,bytes,int8compstoragesize(bytes));
}

/* Adds a new block to a SparseData
 * The function appendBinaryStringInfo always make sure to attach a trailing '\0'
 * to the data array of the vals StringInfo.
 */
void add_run_to_sdata(char *run_val, int64 run_len, size_t width,
		SparseData sdata)
{
	StringInfo index = sdata->index;
	StringInfo vals  = sdata->vals;

	appendBinaryStringInfo(vals,run_val,width);
	append_to_rle_index(index, run_len);
	sdata->unique_value_count++;
	sdata->total_value_count+=run_len;
}

/*------------------------------------------------------------------------------
 * Each integer count in the RLE index is stored in a number of bytes determined
 * by its size.  The larger the integer count, the larger the size of storage.
 * Following is the map of count maximums to storage size:
 * 	Range			Storage
 * 	---------		-----------------------------------------
 * 	0     - 127		signed char stores the negative count
 *
 * 		All higher than 127 have two parts, the description byte
 * 		and the count word
 *
 * 	description byte	signed char stores the number of bytes in the
 * 				count word one of 1,2,4 or 8
 *
 * 	128   - 32767		count word is 2 bytes, signed int16_t
 * 	32768 - 2147483648	count word is 4 bytes, signed int32_t
 * 	2147483648 - max	count word is 8 bytes, signed int64
 *------------------------------------------------------------------------------
 */
/* Transforms an int64 value to an RLE entry.  The entry is placed in the
 * provided entry[] array and will have a variable size depending on the value.
 */
void int8_to_compword(int64 num, char entry[9])
{
	if (num < 128) {
		/* The reason this is negative is because entry[0] is
	           used to record sizes in the other cases. */
		entry[0] = -(char)num;
		return;
	}

	entry[1] = (char)(num & 0xFF);
	entry[2] = (char)((num & 0xFF00) >> 8);

	if (num < 32768) { entry[0] = 2; return; }

	entry[3] = (char)((num & 0xFF0000L) >> 16);
	entry[4] = (char)((num & 0xFF000000L) >> 24);

	if (num < 2147483648LL) { entry[0] = 4; return; }

	entry[5] = (char)((num & 0xFF00000000LL) >> 32);
	entry[6] = (char)((num & 0xFF0000000000LL) >> 40);
	entry[7] = (char)((num & 0xFF000000000000LL) >> 48);
	entry[8] = (char)((num & 0xFF00000000000000LL) >> 56);

	entry[0] = 8;
}

/* Transforms a count entry into an int64 value when provided with a pointer
 * to an entry.
 */
int64 compword_to_int8(const char *entry)
{
	char size = int8compstoragesize(entry);
	int16_t num_2;
	char *numptr2 = (char *)(&num_2);
	int32_t num_4;
	char *numptr4 = (char *)(&num_4);
	int64 num = 0;
	char *numptr8 = (char *)(&num);

	switch(size) {
	        case 0: /* entry == NULL represents an array of ones; see
			 * comment after definition of SparseDataStruct above
			 */
			return 1;
		case 1:
			num = -(entry[0]);
			break;
		case 3:
			numptr2[0] = entry[1];
			numptr2[1] = entry[2];
			num = num_2;
			break;
		case 5:
			numptr4[0] = entry[1];
			numptr4[1] = entry[2];
			numptr4[2] = entry[3];
			numptr4[3] = entry[4];
			num = num_4;
			break;
		case 9:
			numptr8[0] = entry[1];
			numptr8[1] = entry[2];
			numptr8[2] = entry[3];
			numptr8[3] = entry[4];
			numptr8[4] = entry[5];
			numptr8[5] = entry[6];
			numptr8[6] = entry[7];
			numptr8[7] = entry[8];
			break;
	}
	return num;
}

void printout_double(double *vals, int num_values, int stop)
{
	(void) stop; /* avoid warning about unused parameter */
	char *output_str = (char *)palloc(sizeof(char)*(num_values*(6+18+2))+1);
	char *str = output_str;
	int numout;
	for (int i=0; i<num_values; i++) {
		numout = snprintf(str,26,"%6.2f,%#llX,",vals[i],
				  *((long long unsigned int *)(&(vals[i]))));
		str += numout-1;
	}
	*str = '\0';
	elog(NOTICE,"doubles:%s",output_str);
}
void printout_index(char *ix, int num_values, int stop)
{
	(void) stop; /* avoid warning about unused parameter */
	char *output_str = (char *)palloc(sizeof(char)*((num_values*7)+1));
	char *str = output_str;
	int numout;
	elog(NOTICE,"num_values=%d",num_values);
	for (int i=0; i<num_values; i++,ix+=int8compstoragesize(ix)) {
		numout=snprintf(str,7,"%lld,",(long long int)compword_to_int8(ix));
		str+=numout;
	}
	*str = '\0';
	elog(NOTICE,"index:%s",output_str);
}
void printout_sdata(SparseData sdata, char *msg, int stop)
{
	elog(NOTICE,"%s ==> unvct,tvct,ilen,dlen,datatype=%d,%d,%d,%d,%d",
			msg,
			sdata->unique_value_count,sdata->total_value_count,
			sdata->index->len,sdata->vals->len,
			sdata->type_of_data);
	{
		char *ix=sdata->index->data;
		double *ar=(double *)(sdata->vals->data);
		printout_double(ar,sdata->unique_value_count,0);
		printout_index(ix,sdata->unique_value_count,0);
	}

	if (stop)
	  ereport(ERROR,
			  (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			   errmsg("LAL STOP")));
}

/*------------------------------------------------------------------------------
 * Multiplication, Addition, Division by scalars
 *------------------------------------------------------------------------------
 */
#define typref(type,ptr) (*((type *)(ptr)))
#define valref(type,val,i) (((type *)(val)->vals->data)[(i)])

#define valsquare(val)	(val*val)
#define valcube(val)	(val*valsquare(val))
#define valquad(val)	(valsquare(valsquare(val)))

#define apply_const_to_sdata(sdata,i,op,scalar) \
	switch ((sdata)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,sdata,i) op typref(float,scalar); \
		break; \
	case FLOAT8OID: \
		valref(float8,sdata,i) op typref(float8,scalar); \
		break; \
	case CHAROID: \
		valref(char,sdata,i) op typref(char,scalar); \
		break; \
	case INT2OID: \
		valref(int16,sdata,i) op typref(int16,scalar); \
		break; \
	case INT4OID: \
		valref(int32,sdata,i) op typref(int32,scalar); \
		break; \
	case INT8OID: \
		valref(int64,sdata,i) op typref(int64,scalar); \
		break; \
}

#define apply_scalar_left_to_sdata(sdata,i,op,scalar) \
	switch ((sdata)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,sdata,i) = \
		typref(float,scalar) op valref(float,sdata,i); \
		break; \
	case FLOAT8OID: \
		valref(float8,sdata,i) = \
		typref(float8,scalar) op valref(float8,sdata,i); \
		break; \
	case CHAROID: \
		valref(char,sdata,i) = \
		typref(char,scalar) op valref(char,sdata,i); \
		break; \
	case INT2OID: \
		valref(int16,sdata,i) = \
		typref(int16,scalar) op valref(int16_t,sdata,i); \
		break; \
	case INT4OID: \
		valref(int32,sdata,i) = \
		typref(int32,scalar) op valref(int32_t,sdata,i); \
		break; \
	case INT8OID: \
		valref(int64,sdata,i) = \
		typref(int64,scalar) op valref(int64,sdata,i); \
		break; \
}

#define accum_sdata_result(result,left,i,op,right,j) \
	switch ((left)->type_of_data) \
{ \
	case FLOAT4OID: \
		typref(float,result) = \
		valref(float,left,i) op \
		valref(float,right,j); \
		break; \
	case FLOAT8OID: \
		typref(float8,result) = \
		valref(float8,left,i) op \
		valref(float8,right,j); \
		break; \
	case CHAROID: \
		typref(char,result) = \
		valref(char,left,i) op \
		valref(char,right,j); \
		break; \
	case INT2OID: \
		typref(int16,result) = \
		valref(int16,left,i) op \
		valref(int16,right,j); \
		break; \
	case INT4OID: \
		typref(int32,result) = \
		valref(int32,left,i) op \
		valref(int32,right,j); \
		break; \
	case INT8OID: \
		typref(int64,result) = \
		valref(int64,left,i) op \
		valref(int64,right,j); \
		break; \
}

#define apply_function_sdata_scalar(result,func,left,i,scalar) \
	switch ((left)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,result,i) =\
		(float) func(valref(float,left,i),typref(float,scalar)); \
		break; \
	case FLOAT8OID: \
		valref(float8,result,i) =\
		func(valref(float8,left,i),typref(float8,scalar)); \
		break; \
	case CHAROID: \
		valref(char,result,i) =\
		(char) func(valref(char,left,i),typref(char,scalar)); \
		break; \
	case INT2OID: \
		valref(int16,result,i) =\
		(int16) func(valref(int16,left,i),typref(int16,scalar)); \
		break; \
	case INT4OID: \
		valref(int32,result,i) =\
		(int32) func(valref(int32,left,i),typref(int32,scalar)); \
		break; \
	case INT8OID: \
		valref(int64,result,i) =\
		(int64) func(valref(int64,left,i),typref(int64,scalar)); \
		break; \
}

#define apply_square_sdata(result,left,i) \
	switch ((left)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,result,i) = \
		valsquare(valref(float,left,i)); \
		break; \
	case FLOAT8OID: \
		valref(float8,result,i) = \
		valsquare(valref(float8,left,i));\
		break; \
	case CHAROID: \
		valref(char,result,i) = \
		valsquare(valref(char,left,i));\
		break; \
	case INT2OID: \
		valref(int16_t,result,i) = \
		valsquare(valref(int16_t,left,i));\
		break; \
	case INT4OID: \
		valref(int32_t,result,i) = \
		valsquare(valref(int32_t,left,i));\
		break; \
	case INT8OID: \
		valref(int64,result,i) = \
		valsquare(valref(int64,left,i));\
		break; \
}

#define apply_cube_sdata(result,left,i) \
	switch ((left)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,result,i) = \
		valcube(valref(float,left,i)); \
		break; \
	case FLOAT8OID: \
		valref(float8,result,i) = \
		valcube(valref(float8,left,i));\
		break; \
	case CHAROID: \
		valref(char,result,i) = \
		valcube(valref(char,left,i));\
		break; \
	case INT2OID: \
		valref(int16_t,result,i) = \
		valcube(valref(int16_t,left,i));\
		break; \
	case INT4OID: \
		valref(int32_t,result,i) = \
		valcube(valref(int32_t,left,i));\
		break; \
	case INT8OID: \
		valref(int64,result,i) = \
		valcube(valref(int64,left,i));\
		break; \
}
#define apply_quad_sdata(result,left,i) \
	switch ((left)->type_of_data) \
{ \
	case FLOAT4OID: \
		valref(float,result,i) = \
		valquad(valref(float,left,i)); \
		break; \
	case FLOAT8OID: \
		valref(float8,result,i) = \
		valquad(valref(float8,left,i));\
		break; \
	case CHAROID: \
		valref(char,result,i) = \
		valquad(valref(char,left,i));\
		break; \
	case INT2OID: \
		valref(int16_t,result,i) = \
		valquad(valref(int16_t,left,i));\
		break; \
	case INT4OID: \
		valref(int32_t,result,i) = \
		valquad(valref(int32_t,left,i));\
		break; \
	case INT8OID: \
		valref(int64,result,i) = \
		valquad(valref(int64,left,i));\
		break; \
}

/* Checks that two SparseData have the same dimension
 */
void
check_sdata_dimensions(SparseData left, SparseData right)
{
	if (left->total_value_count != right->total_value_count)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("dimensions of vectors must be the same")));
	}
}

/* Do one of subtract, add, multiply, or divide depending on
 * the value of operation.
 */
void op_sdata_by_scalar_inplace(enum operation_t operation,
		char *scalar, SparseData sdata, bool scalar_is_right)
{
	if (scalar_is_right) //scalar is on the right
	{
		for(int i=0; i<sdata->unique_value_count; i++)
		{
			switch(operation)
			{
			case subtract:
				apply_const_to_sdata(sdata,i,-=,scalar)
				break;
			case add:
				apply_const_to_sdata(sdata,i,+=,scalar)
				break;
			case multiply:
				apply_const_to_sdata(sdata,i,*=,scalar)
				break;
			case divide:
				apply_const_to_sdata(sdata,i,/=,scalar)
				break;
			}
		}
	} else { //scalar is on the left
		for(int i=0; i<sdata->unique_value_count; i++)
		{
			switch(operation)
			{
			case subtract:
				apply_scalar_left_to_sdata(sdata,i,-,scalar)
				break;
			case add:
				apply_scalar_left_to_sdata(sdata,i,+,scalar)
				break;
			case multiply:
				apply_scalar_left_to_sdata(sdata,i,*,scalar)
				break;
			case divide:
				apply_scalar_left_to_sdata(sdata,i,/,scalar)
				break;
			}
		}
	}

}
SparseData op_sdata_by_scalar_copy(enum operation_t operation,
		char *scalar, SparseData source_sdata, bool scalar_is_right)
{
	SparseData sdata = makeSparseDataCopy(source_sdata);
	op_sdata_by_scalar_inplace(operation,scalar,sdata,scalar_is_right);
	return sdata;
}

/* Exponentiates an sdata left argument with a right scalar
 */
SparseData pow_sdata_by_scalar(SparseData sdata,
		char *scalar)
{
	SparseData result = makeSparseDataCopy(sdata);
	for(int i=0; i<sdata->unique_value_count; i++)
		apply_function_sdata_scalar(result,pow,sdata,i,scalar)

	return(result);
}
SparseData square_sdata(SparseData sdata)
{
	SparseData result = makeSparseDataCopy(sdata);
	for(int i=0; i<sdata->unique_value_count; i++)
		apply_square_sdata(result,sdata,i)

	return(result);
}
SparseData cube_sdata(SparseData sdata)
{
	SparseData result = makeSparseDataCopy(sdata);
	for(int i=0; i<sdata->unique_value_count; i++)
		apply_cube_sdata(result,sdata,i)

	return(result);
}
SparseData quad_sdata(SparseData sdata)
{
	SparseData result = makeSparseDataCopy(sdata);
	for(int i=0; i<sdata->unique_value_count; i++)
		apply_quad_sdata(result,sdata,i)

	return(result);
}

/* Checks the equality of two SparseData. We can't assume that two
 * SparseData are in canonical form.
 *
 * The algorithm is simple: we traverse the left SparseData element by
 * element, and for each such element x, we traverse all the elements of
 * the right SparseData that overlaps with x and check that they are equal.
 *
 * Note: This function only works on SparseData of float8s at present.
 */
bool sparsedata_eq(SparseData left, SparseData right)
{
	if (left->total_value_count != right->total_value_count)
		return false;

	char * ix = left->index->data;
	double * vals = (double *)left->vals->data;

	char * rix = right->index->data;
	double * rvals = (double *)right->vals->data;

	int read = 0, rread = 0;
	int rvid = 0;
	int rrun_length, i;

	for (i=0; i<left->unique_value_count; i++,ix+=int8compstoragesize(ix)) {
		read += compword_to_int8(ix);

		while (true) {
			/*
			 * We need to use memcmp to handle NULLs (represented
			 * as NaNs) properly
			 */
			if (memcmp(&(vals[i]),&(rvals[rvid]),sizeof(float8))!=0)
				return false;

			/*
			 * We never move the right element pointer beyond
			 * the current left element
			 */
			rrun_length = compword_to_int8(rix);
			if (rread + rrun_length > read) break;

			/*
			 * Increase counters if there are more elements in
			 * the right SparseData that overlaps with current
			 * left element
			 */
			rread += rrun_length;
			if (rvid < right->unique_value_count) {
				rix += int8compstoragesize(rix);
				rvid++;
			}
			if (rread == read) break;
		}
	}
	Assert(rread == read);
	return true;
}

/* Checks the equality of two SparseData. We can't assume that two
 * SparseData are in canonical form.
 *
 * The algorithm is simple: we traverse the left SparseData element by
 * element, and for each such element x, we traverse all the elements of
 * the right SparseData that overlaps with x and check that they are equal.
 *
 * Unlike sparsedata_eq, this function assumes that any zero represents a
 * missing data and hence still implies equality
 *
 * Note: This function only works on SparseData of float8s at present.
 */
bool sparsedata_eq_zero_is_equal(SparseData left, SparseData right)
{
	char * ix = left->index->data;
	double* vals = (double *)left->vals->data;

	char * rix = right->index->data;
	double* rvals = (double *)right->vals->data;

	int read = 0, rread = 0;
	int i=-1, j=-1, minimum = 0;
	minimum = (left->total_value_count > right->total_value_count) ?
		  right->total_value_count : left->total_value_count;

	for (;(read < minimum)||(rread < minimum);) {
		if (read < rread) {
			read += (int)compword_to_int8(ix);
			ix +=int8compstoragesize(ix);
			i++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0) &&
			    (vals[i]!=0.0)&&(rvals[j]!=0.0)) {
				return false;
			}
		} else if (read > rread){
			rread += (int)compword_to_int8(rix);
			rix+=int8compstoragesize(rix);
			j++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0) &&
			    (vals[i]!=0.0)&&(rvals[j]!=0.0)) {
				return false;
			}
		} else {
			read += (int)compword_to_int8(ix);
			rread += (int)compword_to_int8(rix);
			ix +=int8compstoragesize(ix);
			rix+=int8compstoragesize(rix);
			i++;
			j++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0) &&
			    (vals[i]!=0.0)&&(rvals[j]!=0.0)) {
				return false;
			}
		}
	}
	/*sprintf(result, "result after %d %f", j, rvals[j]);
	 ereport(NOTICE,
	 (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	 errmsg(result)));*/
	return true;
}

/* Checks if one SparseData object contained in another
 *
 * First vector is said to contain second if all non-zero elements
 * of the second data object equal those of the first one
 *
 * Note: This function only works on SparseData of float8s at present.
 */
bool sparsedata_contains(SparseData left, SparseData right)
{
	char * ix = left->index->data;
	double* vals = (double *)left->vals->data;

	char * rix = right->index->data;
	double* rvals = (double *)right->vals->data;

	int read = 0, rread = 0;
	int i=-1, j=-1, minimum = 0;
	int lsize, rsize;
	lsize = left->total_value_count;
	rsize = right->total_value_count;
	if((rsize > lsize)&&(rvals[right->unique_value_count-1]!=0.0)){
		return false;
	}

	minimum = (lsize > rsize)?rsize:lsize;

	for (;(read < minimum)||(rread < minimum);) {
		if(read < rread){
			read += (int)compword_to_int8(ix);
			ix +=int8compstoragesize(ix);
			i++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0)&&(rvals[j]!=0.0)){
				return false;
			}
		}else if(read > rread){
			rread += (int)compword_to_int8(rix);
			rix+=int8compstoragesize(rix);
			j++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0)&&(rvals[j]!=0.0)){
				return false;
			}
		}else{
			read += (int)compword_to_int8(ix);
			rread += (int)compword_to_int8(rix);
			ix +=int8compstoragesize(ix);
			rix+=int8compstoragesize(rix);
			i++;
			j++;
			if ((memcmp(&(vals[i]),&(rvals[j]),sizeof(float8))!=0)&&(rvals[j]!=0.0)){
				return false;
			}
		}
	}
	return true;
}


static inline double id(double x) { return x; }
static inline double square(double x) { return x*x; }
static inline double myabs(double x) { return (x < 0) ? -(x) : x ; }

/* This function is introduced to capture a common routine for
 * traversing a SparseData, transforming each element as we go along and
 * summing up the transformed elements. The method is non-destructive to
 * the input SparseData.
 */
double
accum_sdata_values_double(SparseData sdata, double (*func)(double))
{
	double accum=0.;
	char *ix = sdata->index->data;
	double *vals = (double *)sdata->vals->data;
	int64 run_length;

	for (int i=0;i<sdata->unique_value_count;i++)
	{
		run_length = compword_to_int8(ix);
		accum += func(vals[i])*run_length;
		ix+=int8compstoragesize(ix);
	}
	return (accum);
}

/* Computes the running sum of the elements of a SparseData */
double sum_sdata_values_double(SparseData sdata) {
	return accum_sdata_values_double(sdata, id);
}

/* Computes the l2 norm of a SparseData */
double l2norm_sdata_values_double(SparseData sdata) {
	return sqrt(accum_sdata_values_double(sdata, square));
}

/* Computes the l1 norm of a SparseData */
double l1norm_sdata_values_double(SparseData sdata) {
	return accum_sdata_values_double(sdata, myabs);
}

/*
 * Addition, Scalar Product, Division between SparseData arrays
 *
 * There are a few factors to consider:
 * - The dimension of the left and right arguments must be the same
 * - We employ an algorithm that does the computation on the compressed contents
 *   which creates a new SparseData array
 *------------------------------------------------------------------------------
 */
SparseData op_sdata_by_sdata(enum operation_t operation,
					   SparseData left, SparseData right)
{
	SparseData sdata = makeSparseData();

	/*
	 * Loop over the contents of the left array, operating on elements
	 * of the right array and append a new value to the sdata when a
	 * new unique value is generated.
	 *
	 * We will manage two cursors, one for each of left and right arrays
	 */
	char *liptr=left->index->data;
	char *riptr=right->index->data;
	int left_run_length, right_run_length;
	char *new_value,*last_new_value;
	int tot_run_length=-1;
	left_run_length = compword_to_int8(liptr);
	right_run_length = compword_to_int8(riptr);
	int left_lst=0,right_lst=0;
	int left_nxt=left_run_length,right_nxt=right_run_length;
	int nextpos = Min(left_nxt,right_nxt),lastpos=0;
	int i=0,j=0;
	size_t width = size_of_type(left->type_of_data);

	check_sdata_dimensions(left,right);

	new_value      = (char *)palloc(width);
	last_new_value = (char *)palloc(width);

	while (1)
	{
		switch (operation)
		{
			case subtract:
				accum_sdata_result(new_value,left,i,-,right,j)
				break;
			case add:
			default:
				accum_sdata_result(new_value,left,i,+,right,j)
				break;
			case multiply:
				accum_sdata_result(new_value,left,i,*,right,j)
				break;
			case divide:
				accum_sdata_result(new_value,left,i,/,right,j)
				break;
		}
		/*
		 * Potentially add a new run, depending on whether this is a
		 * different value from the previous calculation. It may be
		 * that this calculation has produced an identical value to
		 * the previous, in which case we store it up, waiting for a
		 * new value to happen.
		 */
		if (tot_run_length==-1)
		{
			memcpy(last_new_value,new_value,width);
			tot_run_length=0;
		}
		if (memcmp(new_value,last_new_value,width))
		{
			add_run_to_sdata(last_new_value,tot_run_length,sizeof(float8),sdata);
			tot_run_length = 0;
			memcpy(last_new_value,new_value,width);
		}
		tot_run_length += (nextpos-lastpos);

		if ((left_nxt==right_nxt)&&(left_nxt==(left->total_value_count))) {
			break;
		} else if (left_nxt==right_nxt) {
			i++;j++;
			left_lst=left_nxt;right_lst=right_nxt;
			liptr+=int8compstoragesize(liptr);
			riptr+=int8compstoragesize(riptr);
		} else if (nextpos==left_nxt) {
			i++;
			left_lst=left_nxt;
			liptr+=int8compstoragesize(liptr);
		} else if (nextpos==right_nxt) {
			j++;
			right_lst=right_nxt;
			riptr+=int8compstoragesize(riptr);
		}
		left_run_length = compword_to_int8(liptr);
		right_run_length = compword_to_int8(riptr);
		left_nxt=left_run_length+left_lst;
		right_nxt=right_run_length+right_lst;
		lastpos=nextpos;
		nextpos = Min(left_nxt,right_nxt);
	}

	/*
	 * Add the last run if we ended with a repeating value
	 */
	if (tot_run_length!=0)
		add_run_to_sdata(new_value,tot_run_length,sizeof(float8),sdata);

	/*
	 * Set the return data type
	 */
	sdata->type_of_data = left->type_of_data;

	pfree(new_value);
	pfree(last_new_value);

	return sdata;
}

/* END Previously in SparseData.h */



/**
 * @return A SparseData structure with allocated empty dynamic
 * StringInfo of unknown initial sizes.
 */
SparseData makeSparseData(void) {
	/* Allocate the struct */
	SparseData sdata = (SparseData)palloc(sizeof(SparseDataStruct));

	/* Allocate the included elements */
	sdata->vals  = makeStringInfo();
	sdata->index = makeStringInfo();

	sdata->unique_value_count=0;
	sdata->total_value_count=0;
	sdata->type_of_data = FLOAT8OID;
	return sdata;

	// makeStringInfo ensures vals and index each has a trailing '\0'
}

/**
 * @return A SparseData with zero storage in its StringInfos.
 */
SparseData makeEmptySparseData(void) {
	/* Allocate the struct */
	SparseData sdata = (SparseData)palloc(sizeof(SparseDataStruct));

	/* Set up the data area */
	sdata->vals = (StringInfo)palloc(sizeof(StringInfoData));
	sdata->vals->data = (char *)palloc(1);
	sdata->vals->maxlen = 1;
	sdata->vals->data[0] = '\0';
	sdata->vals->len = 0;
	sdata->vals->cursor = 0;

	sdata->index = (StringInfo)palloc(sizeof(StringInfoData));
	sdata->index->data = (char *)palloc(1);
	sdata->index->maxlen = 1;
	sdata->index->data[0] = '\0';
	sdata->index->len = 0;
	sdata->index->cursor = 0;

	sdata->unique_value_count = 0;
	sdata->total_value_count = 0;
	sdata->type_of_data = FLOAT8OID;

	return sdata;
}

/**
 * @param vals An array of data values
 * @param index An array of run-lengths
 * @param datasize The length of the vals array
 * @param indexsize The length of the index array
 * @param datatype The object ID of the elements represented in the vals array
 * @param unique_value_count The number of RLE blocks in the vals array
 * @param total_value_count The total number of individual elements represented in the vals and index arrays
 * @return A SparseData created in place using pointers to the vals and index data
 */
SparseData makeInplaceSparseData(char *vals, char *index,
		int datasize, int indexsize, Oid datatype,
		int unique_value_count, int total_value_count) {
	SparseData sdata = makeEmptySparseData();
	sdata->unique_value_count = unique_value_count;
	sdata->total_value_count  = total_value_count;

	/*
	 * To be safe, we obey the constraint demanded in lib/stringinfo.h that
	 * the data field of StringInfoData is always terminated with a null.
	 */
	if (vals != NULL && vals[datasize] != '\0') {
		char * vals_temp = (char *)palloc(datasize+1);
		memcpy(vals_temp, vals, datasize);
		vals_temp[datasize] = '\0';
		vals = vals_temp;
	}

	if (index != NULL && index[indexsize] != '\0') {
		char * index_temp = (char *)palloc(indexsize+1);
		memcpy(index_temp, index, indexsize);
		index_temp[indexsize] = '\0';
		index = index_temp;
	}

	sdata->vals->data = vals;
	sdata->vals->len = datasize;
	sdata->vals->maxlen = datasize+1;
	sdata->index->data = index;

	sdata->index->len = indexsize;
	sdata->index->maxlen = index == NULL ? 0 : indexsize+1;
	   // here we are taking care of the special case of null index, which
	   // represents uncompressed sparsedata; see SparseDataStruct defn
	sdata->type_of_data = datatype;

	return sdata;
}

/**
 * @return A copy of an existing SparseData.
 */
SparseData makeSparseDataCopy(SparseData source_sdata) {
	/* Allocate the struct */
	SparseData sdata = (SparseData)palloc(sizeof(SparseDataStruct));
	/* Allocate the included elements */
	sdata->vals = copyStringInfo(source_sdata->vals);
	sdata->index = copyStringInfo(source_sdata->index);
	sdata->type_of_data       = source_sdata->type_of_data;
	sdata->unique_value_count = source_sdata->unique_value_count;
	sdata->total_value_count  = source_sdata->total_value_count;
	return sdata;
}
/**
 * @param constant The value of an RLE block
 * @param dimension The size of an RLE block
 * @return A SparseData with a single RLE block of size dimension having value constant
 */
SparseData makeSparseDataFromDouble(double constant,int64 dimension) {
	char *bytestore=(char *)palloc(sizeof(char)*9);
	SparseData sdata = float8arr_to_sdata(&constant,1);
	int8_to_compword(dimension,bytestore); /* create compressed version of
					          int8 value */

	int newlen = int8compstoragesize(bytestore);
	sdata->index->len = 0; // write over existing value
	appendBinaryStringInfo(sdata->index, bytestore, newlen);

	sdata->total_value_count = dimension;

	return sdata;
}

/**
 * Frees up the memory occupied by sdata
 */
void freeSparseData(SparseData sdata) {
	pfree(sdata->vals);
	pfree(sdata->index);
	pfree(sdata);
}

/**
 * Frees up the memory occupied by sdata, including the data elements of vals and index.
 */
void freeSparseDataAndData(SparseData sdata) {
	pfree(sdata->vals->data);
	pfree(sdata->index->data);
	freeSparseData(sdata);
}

/**
 * @param sinfo The StringInfo structure to be copied
 * @return A copy of sinfo
 */
StringInfo copyStringInfo(StringInfo sinfo) {
	StringInfo result;
	char *data;
	if (sinfo->data == NULL) {
		data = NULL;
	} else {
		data   = (char *)palloc(sizeof(char)*(sinfo->len+1));
		memcpy(data,sinfo->data,sinfo->len);
		data[sinfo->len] = '\0';
	}
	result = makeStringInfoFromData(data,sinfo->len);
	return result;
}

/**
 * @param data Pointer to an array of elements
 * @param len The size of the data array
 * @return A StringInfo from a data pointer and length
 */
StringInfo makeStringInfoFromData(char *data,int len) {
	StringInfo sinfo;
	sinfo = (StringInfo)palloc(sizeof(StringInfoData));

	if (data != NULL && data[len] != '\0') {
		char * data_temp = (char *)palloc(len+1);
		memcpy(data_temp, data, len);
		data_temp[len] = '\0';
		data = data_temp;
	}

	sinfo->data   = data;
	sinfo->len    = len;
	sinfo->maxlen = len+1;
	sinfo->cursor = 0;
	return sinfo;
}

/**
 * @param array The array of doubles to be converted to a SparseData
 * @param count The size of array
 * @return A SparseData representation of an input array of doubles
 */
SparseData float8arr_to_sdata(double *array, int count) {
	return arr_to_sdata((char *)array, sizeof(float8), FLOAT8OID, count);
}

/**
 * @param array The array of elements to be converted to a SparseData
 * @param width The size of the elements in array
 * @param type_of_data The object ID of the elements in array
 * @param count The size of array
 * @return A SparseData representation of an input array
 */
SparseData arr_to_sdata(char *array, size_t width, Oid type_of_data, int count){
	char *run_val=array;
	int64 run_len=1;
	SparseData sdata = makeSparseData();

	sdata->type_of_data=type_of_data;

	for (int i=1; i<count; i++) {
		char *curr_val=array+ (i*size_of_type(type_of_data));

		/*
		 * Note that special double values like denormalized numbers and exceptions
		 * like NaN are treated like any other value - if there are duplicates, the
		 * value of the special number is preserved and they are counted.
		 */
		if (memcmp(curr_val,run_val,width))
		{       /*run is interrupted, initiate new run */
			/* package up the finished run */
			add_run_to_sdata(run_val,run_len,width,sdata);
			/* mark beginning of new run */
			run_val=curr_val;
			run_len=1;
		} else
		{ /* we're still in the same run */
			run_len++;
		}
	}
	add_run_to_sdata(run_val, run_len, width, sdata); /* package up the last run */

	/* Add the final tallies */
	sdata->unique_value_count = sdata->vals->len/width;
	sdata->total_value_count = count;

	return sdata;
}

int compar(const void *i, const void *j){
	return (int)((int64*)array_pos_ref)[*(int*)i] - ((int64*)array_pos_ref)[*(int*)j];
}

/**
 * @param array_val The array of values to be converted to values in SparseData
 * @param array_pos The array of positions to be converted to runs in SparseData
 * @param type_of_data type of the value element
 * @param count The (common) size of array and array_pos
 * @param end The size of the desired SparseData
 * @param default_val The default value for positions unspecified in array_pos
 * @return A SparseData representation of an input array of doubles
 */
SparseData position_to_sdata(double *array_val, int64 *array_pos,
			     Oid type_of_data,
			     int count, int64 end, double default_val) {

	char * array = (char *)array_val;
	char * base_val = (char *)&default_val;
	size_t width = size_of_type(type_of_data);
	char *run_val=array;
	int64 run_len;
	SparseData sdata = makeSparseData();

	int *index = (int*)palloc(count*sizeof(int));
	for(int i = 0; i < count; ++i)
		index[i] = i;

	/* this is a temp solution that will be in place only until qsort_r
	 * is a standard library function */
	array_pos_ref = array_pos;
	qsort(index, count, sizeof(int), compar);

	sdata->type_of_data=type_of_data;
	if(array_pos[index[0]] > 1){
		run_len = array_pos[index[0]]-1;
		add_run_to_sdata(base_val,run_len,width,sdata);
	}

	for (int i = 0; i<count; i++) {
		run_len=1;
		/*
		 * Note that special double values like denormalized numbers and exceptions
		 * like NaN are treated like any other value - if there are duplicates, the
		 * value of the special number is preserved and they are counted.
		 */
		while ((i < count-1) &&
		       ((array_pos[index[i+1]] - array_pos[index[i]])==1) &&
		       (memcmp((array+index[i]*size_of_type(type_of_data)),
			       (array+index[i+1]*size_of_type(type_of_data)),width)==0)) {
			run_len++;
			i++;
		}
		while ((i < count-1)&&((array_pos[index[i+1]] - array_pos[index[i]])==0)) {
			if ((memcmp((array+index[i]*size_of_type(type_of_data)),
				    (array+index[i+1]*size_of_type(type_of_data)),width)==0)) {
				i++;
			} else {
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("posit_to_sdata conflicting values for the same position")));
			}
		}
		run_val = array+index[i]*size_of_type(type_of_data);
		add_run_to_sdata(run_val,run_len,width,sdata);

		if(i < count-1){
			run_len = array_pos[index[i+1]]-array_pos[index[i]]-1;
			if (run_len > 0){
				add_run_to_sdata(base_val,run_len,width,sdata);
			}
		}else if(array_pos[index[i]] < end){
			run_len = end-array_pos[index[i]];
			if (run_len > 0){
				add_run_to_sdata(base_val,run_len,width,sdata);
			}
		}
	}

	/* Add the final tallies */
	sdata->unique_value_count = sdata->vals->len/width;
	sdata->total_value_count = end;
	pfree(index);
	return sdata;
}


/**
 * @param sdata The SparseData to be converted to an array of float8s
 * @return A float8[] representation of a SparseData
 */
double *sdata_to_float8arr(SparseData sdata) {
	double *array;
	int j, aptr;
	char *iptr;

	if (sdata->type_of_data != FLOAT8OID) {
		ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("Data type of SparseData is not FLOAT64\n")));
	}

	if ((array=(double *)palloc(sizeof(double)*(sdata->total_value_count)))
	      == NULL)
	{
		ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			       errmsg("Error allocating memory for array\n")));
	}

	iptr = sdata->index->data;
	aptr = 0;
	for (int i=0; i<sdata->unique_value_count; i++) {
		for (j=0;j<compword_to_int8(iptr);j++,aptr++) {
			array[aptr] = ((double *)(sdata->vals->data))[i];
		}
		iptr+=int8compstoragesize(iptr);
	}

	if ((aptr) != sdata->total_value_count)
	{
		ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		  errmsg("Array size is incorrect, is: %d and should be %d\n",
				aptr,sdata->total_value_count)));

		pfree(array);
		return NULL;
	}

	return array;
}

/**
 * @return An array of integers given the (compressed) count array of a SparseData
 */
int64 *sdata_index_to_int64arr(SparseData sdata) {
	char *iptr;
	int64 *array_ix = (int64 *)palloc(
			sizeof(int64)*(sdata->unique_value_count));

	iptr = sdata->index->data;
	for (int i=0; i<sdata->unique_value_count; i++,
			iptr+=int8compstoragesize(iptr)) {
		array_ix[i] = compword_to_int8(iptr);
	}
	return(array_ix);
}

/**
 * @param target The memory area to store the serialised SparseData
 * @para source The SparseData to be serialised
 * @return The serialisation of a SparseData structure
 */
void serializeSparseData(char *target, SparseData source)
{
	/* SparseDataStruct header */
	memcpy(target,source,SIZEOF_SPARSEDATAHDR);
	/* Two StringInfo structures describing the data and index */
	memcpy(SDATA_DATA_SINFO(target), source->vals,sizeof(StringInfoData));
	memcpy(SDATA_INDEX_SINFO(target),source->index,sizeof(StringInfoData));
	/* The unique data values */
	memcpy(SDATA_VALS_PTR(target),source->vals->data,source->vals->maxlen);
	/* The index values */
	memcpy(SDATA_INDEX_PTR(target),source->index->data,source->index->maxlen);

	/*
	 * Set pointers to the data areas of the serialized structure
	 * First the two StringInfo structures contained in the SparseData,
	 * then the data areas inside each of the two StringInfos.
	 */
	((SparseData)target)->vals = (StringInfo)SDATA_DATA_SINFO(target);
	((SparseData)target)->index = (StringInfo)SDATA_INDEX_SINFO(target);
	((StringInfo)(SDATA_DATA_SINFO(target)))->data = SDATA_VALS_PTR(target);
	if (source->index->data != NULL)
	{
		((StringInfo)(SDATA_INDEX_SINFO(target)))->data = SDATA_INDEX_PTR(target);
	} else {
		((StringInfo)(SDATA_INDEX_SINFO(target)))->data = NULL;
	}
}

/**
 * Prints a SparseData
 */
void printSparseData(SparseData sdata) {
	int value_count = sdata->unique_value_count;
	{
		char *indexdata = sdata->index->data;
		double *values  = (double *)(sdata->vals->data);
		for (int i=0; i<value_count; i++) {
			printf("run_length[%d] = %lld, ",i,(long long int)compword_to_int8(indexdata));
			printf("value[%d] = %f\n",i,values[i]);
			indexdata+=int8compstoragesize(indexdata);
		}
	}
}

/**
 * @param sdata The SparseData to be projected on
 * @param idx The index to be projected
 * @return The element of a SparseData at location idx.
 */
double sd_proj(SparseData sdata, int idx) {
	char * ix = sdata->index->data;
	double * vals = (double *)sdata->vals->data;
	int read, i;

	/* error checking */
	if (0 >= idx || idx > sdata->total_value_count)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Index out of bounds.")));

	/* find desired block; as is normal in SQL, we start counting from one */
	read = compword_to_int8(ix);
	i = 0;
	while (read < idx) {
		ix += int8compstoragesize(ix);
		read += compword_to_int8(ix);
		i++;
	}
	return vals[i];
}

/**
 * @param sdata The SparseData from which to extract a subarray
 * @param start The start index of the desired subarray
 * @param end The end index of the desired subarray
 * @return The sub-array, indexed by start and end, of a SparseData.
 */
SparseData subarr(SparseData sdata, int start, int end) {
	char * ix = sdata->index->data;
	double * vals = (double *)sdata->vals->data;
	SparseData ret = makeSparseData();
	size_t wf8 = sizeof(float8);

	if (start > end)
		return reverse(subarr(sdata,end,start));

	/* error checking */
	if (0 >= start || start > end || end > sdata->total_value_count)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Array index out of bounds.")));

	/* find start block */
	int read = compword_to_int8(ix);
	int i = 0;
	while (read < start) {
		ix += int8compstoragesize(ix);
		read += compword_to_int8(ix);
		i++;
	}
	if (end <= read) {
		/* the whole subarray is in the first block, we are done */
		add_run_to_sdata((char *)(&vals[i]), end-start+1, wf8, ret);
		return ret;
	}
	/* else start building subarray */
	add_run_to_sdata((char *)(&vals[i]), read-start+1, wf8, ret);

	for (int j=i+1; j<sdata->unique_value_count; j++) {
		ix += int8compstoragesize(ix);
		int esize = compword_to_int8(ix);
		if (read + esize > end) {
			add_run_to_sdata((char *)(&vals[j]), end-read, wf8,ret);
			break;
		}
		add_run_to_sdata((char *)(&vals[j]), esize, wf8, ret);
		read += esize;
		if (read == end) break;
	}
	return ret;
}

/**
 * @param sdata The SparseData to be reversed
 * @return A copy of the input SparseData, with the order of the elements reversed.
 */
SparseData reverse(SparseData sdata) {
	char * ix = sdata->index->data;
	double * vals = (double *)sdata->vals->data;
	SparseData ret = makeSparseData();
	size_t w = sizeof(float8);

	/* move to the last count array element */
	for (int j=0; j<sdata->unique_value_count-1; j++)
		ix += int8compstoragesize(ix);

	/* copy from right to left */
	for (int j=sdata->unique_value_count-1; j!=-1; j--) {
		add_run_to_sdata((char *)(&vals[j]),compword_to_int8(ix),w,ret);
		ix -= int8compstoragesize(ix);
	}
	return ret;
}

/**
 * @param left The SparseData that comes first in the resulting concatenation
 * @param right The SparseData that comes second in the resulting concatenation
 * @return The concatenation of two input SparseData.
 */
SparseData concat(SparseData left, SparseData right) {
	if (left == NULL && right == NULL) {
		return NULL;
	} else if (left == NULL && right != NULL) {
		return makeSparseDataCopy(right);
	} else if (left != NULL && right == NULL) {
		return makeSparseDataCopy(left);
	}
	SparseData sdata = makeEmptySparseData();
	char *vals,*index;
	int l_val_len = left->vals->len;
	int r_val_len = right->vals->len;
	int l_ind_len = left->index->len;
	int r_ind_len = right->index->len;
	int val_len = l_val_len + r_val_len;
	int ind_len = l_ind_len + r_ind_len;

	vals = (char *)palloc(sizeof(char)*val_len + 1);
	index = (char *)palloc(sizeof(char)*ind_len + 1);

	memcpy(vals, left->vals->data,l_val_len);
	memcpy(vals+l_val_len,right->vals->data,r_val_len);
	vals[val_len] = '\0';

	memcpy(index, left->index->data,l_ind_len);
	memcpy(index+l_ind_len,right->index->data,r_ind_len);
	index[ind_len] = '\0';

	sdata->vals  = makeStringInfoFromData(vals,val_len);
	sdata->index = makeStringInfoFromData(index,ind_len);

	sdata->type_of_data = left->type_of_data;
	sdata->unique_value_count = left->unique_value_count +
		                    right->unique_value_count;
	sdata->total_value_count  = left->total_value_count +
		                    right->total_value_count;
	return sdata;
}

/**
 * @param rep The SparseData to be replicated
 * @param multiplier The number of times to replicate rep
 * @return The input rep SparseData replicated multiplier times.
 */
SparseData concat_replicate(SparseData rep, int multiplier) {
	if (rep == NULL) return NULL;

	SparseData sdata = makeEmptySparseData();
	char *vals,*index;
	int l_val_len = rep->vals->len;
	int l_ind_len = rep->index->len;
	int val_len = l_val_len*multiplier;
	int ind_len = l_ind_len*multiplier;

	vals = (char *)palloc(sizeof(char)*val_len + 1);
	index = (char *)palloc(sizeof(char)*ind_len + 1);

	for (int i=0;i<multiplier;i++) {
		memcpy(vals+i*l_val_len,rep->vals->data,l_val_len);
		memcpy(index+i*l_ind_len,rep->index->data,l_ind_len);
	}
	vals[val_len] = '\0';
	index[ind_len] = '\0';

	sdata->vals  = makeStringInfoFromData(vals,val_len);
	sdata->index = makeStringInfoFromData(index,ind_len);
	sdata->type_of_data = rep->type_of_data;
	sdata->unique_value_count = multiplier * rep->unique_value_count;
	sdata->total_value_count  = multiplier * rep->total_value_count;

	return sdata;
}

static bool lapply_error_checking(Oid foid, List * funcname);

/**
 * This function applies an input function on all elements of a sparse data.
 * The function is modelled after the corresponding function in R.
 *
 * @param func The name of the function to apply
 * @param sdata The input sparse data to be modified
 * @return A SparseData with the same dimension as sdata but with each element sdata[i] replaced by func(sdata[i])
 */
SparseData lapply(text * func, SparseData sdata) {
	Oid argtypes[1] = { FLOAT8OID };
	List * funcname = textToQualifiedNameList(func);
	SparseData result = makeSparseDataCopy(sdata);
	Oid foid = LookupFuncName(funcname, 1, argtypes, false);

	lapply_error_checking(foid, funcname);

	for (int i=0; i<sdata->unique_value_count; i++)
		valref(float8,result,i) =
		    DatumGetFloat8(
		      OidFunctionCall1(foid,
				    Float8GetDatum(valref(float8,sdata,i))));
	return result;
}

/* This function checks for error conditions in lapply() function calls.
 */
static bool lapply_error_checking(Oid foid, List * func) {
	/* foid != InvalidOid; otherwise LookupFuncName would raise error.
	   Here we check that the return type of foid is float8. */
	HeapTuple ftup = SearchSysCache(PROCOID,
					ObjectIdGetDatum(foid), 0, 0, 0);
	Form_pg_proc pform = (Form_pg_proc) GETSTRUCT(ftup);

	if (pform->prorettype != FLOAT8OID)
		ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("return type of %s is not double",
				NameListToString(func))));

	// check volatility

	ReleaseSysCache(ftup);
	return true;
}
