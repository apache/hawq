/**
 * @file
 * Sparse Vector Datatype
 *   We would like to store sparse arrays in a terse representation that fits 
 *   in a small amount of memory.
 *   We also want to be able to compare the number of instances where the svec 
 *   of one document intersects another.
 */

#include <postgres.h>

#include <stdio.h>
#include <string.h>
#include <search.h>
#include <stdlib.h>
#include <math.h>

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/fmgroids.h"
#include "lib/stringinfo.h"
#include "utils/memutils.h"
#include "sparse_vector.h"

/**
 * @return An array of float8s obtained by converting a given sparse vector
 */
ArrayType *svec_return_array_internal(SvecType *svec)
{
	SparseData sdata = sdata_from_svec(svec);
	double *array = sdata_to_float8arr(sdata);

	ArrayType *pgarray = construct_array((Datum *)array,
					     sdata->total_value_count,FLOAT8OID,
					     sizeof(float8),true,'d');

	pfree(array);
	return(pgarray);
}

/* 
 * Must serialize for binary communication with libpq by
 * creating a StringInfo and sending individual data items like:
 *   (from backend/libpq/pqformat.c):
 *      pq_beginmessage - initialize StringInfo buffer
 *      pq_sendbyte     - append a raw byte to a StringInfo buffer
 *      pq_sendint      - append a binary integer to a StringInfo buffer
 *      pq_sendint64    - append a binary 8-byte int to a StringInfo buffer
 *      pq_sendfloat4   - append a float4 to a StringInfo buffer
 *      pq_sendfloat8   - append a float8 to a StringInfo buffer
 *      pq_sendbytes    - append raw data to a StringInfo buffer
 *      pq_sendcountedtext - append a counted text string (with character set conversion)
 *      pq_sendtext     - append a text string (with conversion)
 *      pq_sendstring   - append a null-terminated text string (with conversion)
 *      pq_send_ascii_string - append a null-terminated text string (without conversion)
 *      pq_endmessage   - send the completed message to the frontend
 *
 */

PG_FUNCTION_INFO_V1(svec_send);
/**
 *  svec_send - converts text to binary format
 */
Datum svec_send(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	SparseData sdata = sdata_from_svec(svec);

	pq_begintypsend(&buf);
	pq_sendint(&buf,sdata->type_of_data,sizeof(Oid));
	pq_sendint(&buf,sdata->unique_value_count,sizeof(int));
	pq_sendint(&buf,sdata->total_value_count,sizeof(int));
	pq_sendint(&buf,sdata->vals->len,sizeof(int));
	pq_sendint(&buf,sdata->index->len,sizeof(int));
	pq_sendbytes(&buf,sdata->vals->data,sdata->vals->len);
	pq_sendbytes(&buf,sdata->index->data,sdata->index->len);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

PG_FUNCTION_INFO_V1(svec_recv);
/**
 *  svec_recv - converts external binary format to text
 */
Datum svec_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	SvecType *svec;

	SparseData sdata=makeEmptySparseData();;
	sdata->type_of_data       = pq_getmsgint(buf, sizeof(int));
	sdata->unique_value_count = pq_getmsgint(buf, sizeof(int));
	sdata->total_value_count  = pq_getmsgint(buf, sizeof(int));
	sdata->vals->len          = pq_getmsgint(buf, sizeof(int));
	sdata->index->len         = pq_getmsgint(buf, sizeof(int));
	sdata->vals->data         = (char *)pq_getmsgbytes(buf,sdata->vals->len);
	sdata->index->data        = (char *)pq_getmsgbytes(buf,sdata->index->len);
	svec = svec_from_sparsedata(sdata,true); //Note this copies the data
//	freeSparseDataAndData(sdata);
	pfree(sdata);

	PG_RETURN_SVECTYPE_P(svec);
}

PG_FUNCTION_INFO_V1( svec_return_array );
/**
 *  svec_return_array - returns an uncompressed Array
 */
Datum svec_return_array(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	ArrayType *pgarray = svec_return_array_internal(svec);
	PG_RETURN_ARRAYTYPE_P(pgarray);
}

PG_FUNCTION_INFO_V1(svec_out);
/**
 *  svec_out - outputs a sparse vector as a C string
 */
Datum svec_out(PG_FUNCTION_ARGS)
{
	SvecType *svec = PG_GETARG_SVECTYPE_P(0);
	char *result = svec_out_internal(svec);
	PG_RETURN_CSTRING(result);
}

char * svec_out_internal(SvecType *svec)
{
	char *ix_string,*vals_string,*result;
	int ixlen,vslen;
	SparseData sdata=sdata_from_svec(svec);
	int64 *array_ix =sdata_index_to_int64arr(sdata);
	ArrayType *pgarray_ix,*pgarray_vals;

	pgarray_ix = construct_array((Datum *)array_ix,
				     sdata->unique_value_count,INT8OID,
				     sizeof(int64),true,'d');

	ix_string = DatumGetPointer(OidFunctionCall1(F_ARRAY_OUT,
					 PointerGetDatum(pgarray_ix)));
	ixlen = strlen(ix_string);

	pgarray_vals = construct_array((Datum *)sdata->vals->data,
				       sdata->unique_value_count,FLOAT8OID,
				       sizeof(float8),true,'d');

	vals_string = DatumGetPointer(OidFunctionCall1(F_ARRAY_OUT,
					 PointerGetDatum(pgarray_vals)));
	vslen = strlen(vals_string);

	result = (char *)palloc(sizeof(char)*(vslen+ixlen+1+1));

	/* NULLs are represented as NaN internally; see svec_in();
	 * Here we print each NaN as an NVP. */
	for (int i=0; i!=vslen; i++) 
		if (vals_string[i] == 'N') 
		{
			vals_string[i+1] = 'V';
			vals_string[i+2] = 'P';
			i = i+2;
		}

	sprintf(result,"%s:%s",ix_string,vals_string);
	pfree(ix_string);
	pfree(vals_string);
	pfree(array_ix);

	return(result);
}

SvecType * svec_in_internal(char * str);

PG_FUNCTION_INFO_V1(svec_in);
/**
 *  svec_in - reads in a string and convert that to an svec
 */
Datum svec_in(PG_FUNCTION_ARGS)
{
	char *str = pstrdup(PG_GETARG_CSTRING(0));
	SvecType *result = svec_in_internal(str);
	PG_RETURN_SVECTYPE_P(result);
}

SvecType * svec_in_internal(char * str)
{
	char *values;
	ArrayType *pgarray_vals,*pgarray_ix;
	double *vals, *vals_temp;
	StringInfo index;
	int64 *u_index;
	int32_t num_values,total_value_count;
	SparseData sdata;
	SvecType *result;
	bits8 *bitmap;
	int bitmask;
	int i,j;

	/* Read in the two arrays defining the Sparse Vector, first is the array
	 * of run lengths (the count array), the second is an array of the 
	 * unique values (the data array).
	 *
	 * The input format is a pair of standard Postgres arrays separated by 
	 * a colon, like this:
	 * 	{1,10,1,5,1}:{4.3,0,0.2,0,7.4}
	 *
	 * For now, the data array must only have float8 elements.
	 */
	if ((values=strchr(str,':')) == NULL) {
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Invalid input string for svec")));
	} else {
		*values = '\0';
		values = values+1;
	}
	/* Get the count and data arrays */
	pgarray_ix = DatumGetArrayTypeP(
			    OidFunctionCall3(F_ARRAY_IN,CStringGetDatum(str),
			    ObjectIdGetDatum(INT8OID),Int32GetDatum(-1)));

	pgarray_vals = DatumGetArrayTypeP(
			    OidFunctionCall3(F_ARRAY_IN,CStringGetDatum(values),
			    ObjectIdGetDatum(FLOAT8OID),Int32GetDatum(-1)));

	num_values = *(ARR_DIMS(pgarray_ix));
	u_index = (int64 *)ARR_DATA_PTR(pgarray_ix);
	vals = (double *)ARR_DATA_PTR(pgarray_vals);

	/* The count and value arrays must be non-empty */
	int size1 = ARR_NDIM(pgarray_ix);
	int size2 = ARR_NDIM(pgarray_vals);
	if (size1 == 0 || size2 == 0)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("The count and value arrays must be non-empty")));

	/* The count and value arrays must have the same dimension */
	if (num_values != *(ARR_DIMS(pgarray_vals)))
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Unique value count not equal to run length count %d != %d", num_values, *(ARR_DIMS(pgarray_vals)))));

	/* Count array cannot have NULLs */
	if (ARR_HASNULL(pgarray_ix))
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			 errmsg("NULL value in the count array.")));

	/* If the data array has NULLs, then we need to create an array to
	 * store the NULL values as NVP values defined in float_specials.h. 
	 */
	if (ARR_HASNULL(pgarray_vals)) {
		vals_temp = vals;
		vals = (double *)palloc(sizeof(float8) * num_values);
		bitmap = ARR_NULLBITMAP(pgarray_vals);
		bitmask = 1;
		j = 0;
		for (i=0; i<num_values; i++) {
			if (bitmap && (*bitmap & bitmask) == 0) // if NULL
				vals[i] = NVP;
			else { 
				vals[i] = vals_temp[j];
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

	/* Make an empty StringInfo because we have the data array already */
	index = makeStringInfo();
	total_value_count = 0;
	for (int i=0;i<num_values;i++) {
		if (u_index[i] <= 0) 
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Non-positive run length in input")));

		total_value_count+=u_index[i]; 
		append_to_rle_index(index,u_index[i]);
	}

	sdata = makeInplaceSparseData((char *)vals,index->data,
			num_values*sizeof(double),index->len,FLOAT8OID,
			num_values,total_value_count);
	sdata->type_of_data = FLOAT8OID;

	result = svec_from_sparsedata(sdata,true);
	if (total_value_count == 1) result->dimension = -1; //Scalar

	if (ARR_HASNULL(pgarray_vals)) pfree(vals);
	pfree(str); /* str is allocated from a strdup */
	pfree(pgarray_ix);
	pfree(pgarray_vals);

	return result;
}

/**
 * Produces an svec from a SparseData
 */
SvecType *svec_from_sparsedata(SparseData sdata, bool trim)
{
	int size;

	if (trim)
	{
		/* Trim the extra space off of the StringInfo dynamic strings
		 * before serializing the SparseData
		 */
		sdata->vals->maxlen=sdata->vals->len;
		sdata->index->maxlen=sdata->index->len;
	}

	size = SVECHDRSIZE + SIZEOF_SPARSEDATASERIAL(sdata);

	SvecType *result = (SvecType *)palloc(size);
	SET_VARSIZE(result,size);
	serializeSparseData(SVEC_SDATAPTR(result),sdata);
	result->dimension = sdata->total_value_count;
	if (result->dimension == 1) result->dimension=-1; //Scalar
	return (result);
}

/**
 * Produces an svec from an array
 */
SvecType *svec_from_float8arr(float8 *array, int dimension)
{
	SparseData sdata = float8arr_to_sdata(array,dimension);
	SvecType *result = svec_from_sparsedata(sdata,true);
	return result;
}

/**
 * Makes an empty svec with sufficient memory allocated for the input number
 */
SvecType *makeEmptySvec(int allocation)
{
	int val_len = sizeof(float8)*allocation+1;
	int ind_len = 9*allocation+1;
	SvecType *svec;
	SparseData sdata = makeEmptySparseData();
	sdata->vals->data    = (char *)palloc(val_len);
	sdata->vals->len = 0;
	sdata->vals->maxlen  = val_len;
	sdata->index->data   = (char *)palloc(ind_len);
	sdata->index->len = 0;
	sdata->index->maxlen = ind_len;
	svec = svec_from_sparsedata(sdata,false);
	freeSparseDataAndData(sdata);
	return(svec);
}

/**
 * Allocates more space for the count and data arrays of an svec
 */
SvecType *reallocSvec(SvecType *source)
{
	SvecType *svec;
	SparseData sdata = sdata_from_svec(source);
	int val_newmaxlen = Max(2*sizeof(float8)+1, 2 * (Size) sdata->vals->maxlen);
	char *newvals = (char *)palloc(val_newmaxlen);
	int ind_newmaxlen = Max(2*sizeof(int8)+1, 2 * (Size) sdata->index->maxlen);
	char *newindex = (char *)palloc(ind_newmaxlen);
	/*
	 * This space was never allocated with palloc, so we can't repalloc it!
	 */
	memcpy(newvals ,sdata->vals->data ,sdata->vals->len);
	memcpy(newindex,sdata->index->data,sdata->index->len);
	sdata->vals->data    = newvals;
	sdata->vals->maxlen  = val_newmaxlen;
	sdata->index->data   = newindex;
	sdata->index->maxlen = ind_newmaxlen;
	svec = svec_from_sparsedata(sdata,false);
//	pfree(source);
	return(svec);
}

			// 
