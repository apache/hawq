#ifndef VAGG_H
#define VAGG_H

#include "executor/execHHashagg.h"

/* batch hashagg group linklist header */
typedef struct GroupData {
	HashAggEntry *entry;	// pointer to agg_hash_entry
	int idx; 				// pointer to idx_list
} GroupData;

/* batch hashagg group data */
typedef struct BatchAggGroupData{
	GroupData 	group_header[1024];	//group linklist header

	int 		group_idx;					//current group header index
	int 		group_cnt;					//group header count

	//linklist elements
	//each item's index is the index to columnData
	//each item's value is the next pointer, -1 is the end of a linklist
	int 		idx_list[1024];
}BatchAggGroupData;

/* it is copyed from src/backend/utils/adt/numeric.c */
typedef struct IntFloatAvgTransdata
{
	int32   _len; /* len for varattrib, do not touch directly */
#if 1
	int32   pad;  /* pad so int64 and float64 will be 8 bytes aligned */
#endif

	int64 	count;
	float8 sum;
} IntFloatAvgTransdata;

/*
 * the first args typs of vectorized aggregate functions must be 'bytea',
 * so we can encapsulate the old transdata in the VectorizedAggData,
 * then the grouping information and skip information can be transfer in
 * the aggregate functions.
 */
typedef struct VectorizedAggData
{
	int32   _len; /* len for varattrib, do not touch directly */
#if 1
	int32   pad;  /* pad so int64 and float64 will be 8 bytes aligned */
#endif

	/* grouping information */
	BatchAggGroupData *groupData;

	/* old data */
	Datum data;
	bool isnull;
	bool isnovalue;
	bool isalloc;

	/* skip information */
	bool	 *skip;
	int nrows;
}VectorizedAggData;

#define _VACCUM_NUMERIC_HEADER(type) \
Datum v##type##_accum(PG_FUNCTION_ARGS);

#define _VAVG_NUMERIC_HEADER(type) \
Datum v##type##_avg_accum(PG_FUNCTION_ARGS);

#define _VINC_NUMERIC_HEADER(type) \
Datum v##type##_inc(PG_FUNCTION_ARGS);

#define VACCUM_NUMERIC_HEADER(type) \
	_VACCUM_NUMERIC_HEADER(type) \
	_VAVG_NUMERIC_HEADER(type) \
	_VINC_NUMERIC_HEADER(type)

VACCUM_NUMERIC_HEADER(int2)
VACCUM_NUMERIC_HEADER(int4)
VACCUM_NUMERIC_HEADER(int8)
VACCUM_NUMERIC_HEADER(float4)
VACCUM_NUMERIC_HEADER(float8)

extern Datum vec_inc_any(PG_FUNCTION_ARGS);
extern Datum vec_inc_fin(PG_FUNCTION_ARGS);
extern Datum vec_avg_fin(PG_FUNCTION_ARGS);
extern Datum vec_accum_fin(PG_FUNCTION_ARGS);
extern Datum vec_avg_prelim(PG_FUNCTION_ARGS);

extern TupleTableSlot * ExecVAgg(AggState *node);
extern AggState * VExecInitAgg(Agg *node, EState *estate, int eflags);
extern VectorizedAggData * InitAggVectorizedData(AggState *aggstate);

#endif
