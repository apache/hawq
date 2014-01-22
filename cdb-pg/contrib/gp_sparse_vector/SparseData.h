/**
 * @file
 * \brief This file defines the SparseData structure and some basic
 * functions on SparseData.
 *
 * SparseData provides array storage for repetitive data as commonly found
 * in numerical analysis of sparse arrays and matrices.
 * A general run-length encoding scheme is adopted.
 * Sequential duplicate values in the array are represented in an index
 * structure that stores the count of the number of times a given value
 * is duplicated.
 * All storage is allocated with palloc().
 *
 * NOTE:
 * The SparseData structure is an in-memory structure and so must be
 * serialized into a persisted structure like a VARLENA when leaving
 * a GP / Postgres function.  This implies a COPY from the SparseData
 * to the VARLENA.
 */

#ifndef SPARSEDATA_H
#define SPARSEDATA_H

#include <math.h>
#include <string.h>
#include "postgres.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "catalog/pg_type.h"

/*!
 * \internal
 * SparseData holds information about a sparse array of values
 * \endinternal
 */
typedef struct
{
	Oid type_of_data; 	/**< The native type of the data entries */
	int unique_value_count; /**< The number of unique values in the data array */
	int total_value_count;  /**< The total number of values, including duplicates */
	StringInfo vals;        /**< The unique number values are stored here as a stream of bytes */
	StringInfo index; 	/**< A count of each value is stored in the index */
} SparseDataStruct;

/*
 * Sometimes we wish to store an uncompressed array inside a SparseDataStruct;
 * instead of storing an array of ones [1,1,..,1,1] in the index field, which
 * is wasteful, we choose to use index->data == NULL to represent this special
 * case.
 */

/**
 * Pointer to a SparseDataStruct
 */
typedef SparseDataStruct *SparseData;

/*------------------------------------------------------------------------------
 * Serialized SparseData
 *------------------------------------------------------------------------------
 * SparseDataStruct Contents
 * StringInfoData Contents for "vals"
 * StringInfoData Contents for "index"
 * data contents for "vals" (size is vals->maxlen)
 * data contents for "index" (size is index->maxlen)
 *
 * 	The vals and index fields are serialized as StringInfoData, then the
 * 	data contents are serialized at the end.
 *
 * 	Since two StringInfoData structs together are 64-bit aligned, there's
 * 	no need for padding.
 *
 * 	For reference, here is the format of the StringInfoData:
 * 		char * dataptr;
 * 			-> a placeholder in the serialized version, is filled
 * 			   when the serialized version is used in-place
 * 		int len;
 * 		int maxlen;
 * 		int cursor;
 */
/**
 * @return The size of a serialized SparseData based on the actual consumed
 * length of the StringInfo data and StringInfoData structures.
 */
#define SIZEOF_SPARSEDATAHDR	MAXALIGN(sizeof(SparseDataStruct))
/**
 * @param x a SparseData
 * @return The size of x minus the dynamic variables, plus two
 * integers describing the length of the data area and index
 */
#define SIZEOF_SPARSEDATASERIAL(x) (SIZEOF_SPARSEDATAHDR + \
		(2*sizeof(StringInfoData)) + \
		(x)->vals->maxlen + (x)->index->maxlen)

/*
 * The following take a serialized SparseData as an argument and return
 * pointers to locations inside.
 */
#define SDATA_DATA_SINFO(x)	((char *)(x)+SIZEOF_SPARSEDATAHDR)
#define SDATA_INDEX_SINFO(x)	(SDATA_DATA_SINFO(x)+sizeof(StringInfoData))
#define SDATA_DATA_SIZE(x)	(((StringInfo)SDATA_DATA_SINFO(x))->maxlen)
#define SDATA_INDEX_SIZE(x)	(((StringInfo)SDATA_INDEX_SINFO(x))->maxlen)
#define SDATA_VALS_PTR(x)       (SDATA_INDEX_SINFO(x)+sizeof(StringInfoData))
#define SDATA_INDEX_PTR(x) 	(SDATA_VALS_PTR(x)+SDATA_DATA_SIZE(x))

#define SDATA_UNIQUE_VALCNT(x)	(((SparseData)(x))->unique_value_count)
#define SDATA_TOTAL_VALCNT(x)	(((SparseData)(x))->total_value_count)

/**
 * @param x a SparseData
 * @return True if x is a scalar */
#define SDATA_IS_SCALAR(x)	(((((x)->unique_value_count)==((x)->total_value_count))&&((x)->total_value_count==1)) ? 1 : 0)

/**
 * @param ptr Pointer to the start of the count entry of a SparseData
 * @return The size of the integer count in an RLE index pointed to by ptr
 */
#define	int8compstoragesize(ptr) \
 (((ptr) == NULL) ? 0 : (((*((char *)(ptr)) < 0) ? 1 : (1 + *((char *)(ptr))))))
/* The size of a compressed int8 is stored in the first element of the ptr
 * array; see the explanation at the int8_to_compword() function below.
 *
 * Note that if the ptr is NULL, a zero size is returned
 */

void int8_to_compword(int64 num, char entry[9]);
int64 compword_to_int8(const char *entry);

/** Serialization function */
void serializeSparseData(char *target, SparseData source);

/* Constructors and destructors */
SparseData makeEmptySparseData(void);
SparseData makeInplaceSparseData(char *vals, char *index,
		int datasize, int indexsize, Oid datatype,
		int unique_value_count, int total_value_count);

SparseData makeSparseDataCopy(SparseData source_sdata);
SparseData makeSparseDataFromDouble(double scalar,int64 dimension);

SparseData makeSparseData(void);

void freeSparseData(SparseData sdata);
void freeSparseDataAndData(SparseData sdata);

StringInfo copyStringInfo(StringInfo source_sinfo);
StringInfo makeStringInfoFromData(char *data,int len);

/* Conversion to and from arrays */
double *sdata_to_float8arr(SparseData sdata);
int64 *sdata_index_to_int64arr(SparseData sdata);
SparseData float8arr_to_sdata(double *array, int count);
SparseData position_to_sdata(double *array_val, int64 *array_pos, Oid type_of_data, int count, int64 end, double default_val);
SparseData arr_to_sdata(char *array, size_t width, Oid type_of_data, int count);
SparseData posit_to_sdata(char *array, int64* array_pos, size_t width, Oid type_of_data, int count, int64 end, char *base_val);

/* Some functions for accessing and changing elements of a SparseData */
SparseData lapply(text * func, SparseData sdata);
double sd_proj(SparseData sdata, int idx);
SparseData subarr(SparseData sdata, int start, int end);
SparseData reverse(SparseData sdata);
SparseData concat(SparseData left, SparseData right);
SparseData concat_replicate(SparseData rep, int multiplier);


enum operation_t { subtract, add, multiply, divide };

double sum_sdata_values_double(SparseData sdata);
SparseData op_sdata_by_sdata(enum operation_t operation, SparseData left,
    SparseData right);
bool sparsedata_eq(SparseData left, SparseData right);
bool sparsedata_eq_zero_is_equal(SparseData left, SparseData right);
bool sparsedata_contains(SparseData left, SparseData right);
SparseData pow_sdata_by_scalar(SparseData sdata, char *scalar);
SparseData square_sdata(SparseData sdata);
SparseData cube_sdata(SparseData sdata);
SparseData quad_sdata(SparseData sdata);
void op_sdata_by_scalar_inplace(enum operation_t operation, char *scalar,
    SparseData sdata, bool scalar_is_right);
void append_to_rle_index(StringInfo index, int64 run_len);

SparseData op_sdata_by_scalar_copy(enum operation_t operation, char *scalar,
    SparseData source_sdata, bool scalar_is_right);

double l2norm_sdata_values_double(SparseData sdata);
double l1norm_sdata_values_double(SparseData sdata);

size_t size_of_type(Oid type);
void printout_double(double *vals, int num_values, int stop);
void printout_index(char *ix, int num_values, int stop);
void printout_sdata(SparseData sdata, char *msg, int stop);

void add_run_to_sdata(char *run_val, int64 run_len, size_t width,
		SparseData sdata);

/*------------------------------------------------------------------------------
 * macros that will test whether a given double value is in the normal
 * range or is in the special range (denormals, exceptions).
 *------------------------------------------------------------------------------
 */
/* Anything between LOW and HIGH is a denormal or exception */
#define SPEC_MASK_HIGH 0xFFF0000000000000
#define SPEC_MASK_LOW  0x7FF0000000000000
#define MASKTEST(x,y)	(((x)&(y))==x) /* MASKTEST checks for the presence of
					  the bits in x in the input y */

/* The input to MASKTEST_double should be an int64 mask and a (double *) to be
 * tested
 */
#define MASKTEST_double(x,y)	MASKTEST((x),*((int64 *)(&(y))))

#define DBL_IS_A_SPECIAL(x) \
  (MASKTEST_double(SPEC_MASK_HIGH,(x)) || MASKTEST_double(SPEC_MASK_LOW,(x)) \
   || ((x) == 0.))

#endif	/* SPARSEDATA_H */
