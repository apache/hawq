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

#include <unistd.h>
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/hash.h"
#include "catalog/catquery.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "executor/execHHashagg.h"
#include "executor/instrument.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

#include "vagg.h"
#include "vtype.h"
#include "execVQual.h"
#include "tuplebatch.h"
#include "vexecutor.h"

#define HHA_MSG_LVL DEBUG2

extern int BATCHSIZE;

/*
 * copy from src/backend/executor/execHHashagg.c
 * Represent different types for input records to be inserted
 * into the hash table.
 */
#define GET_BUFFER_SIZE(hashtable) \
	((hashtable)->entry_buf.nfull_total * (hashtable)->entry_buf.cellbytes + \
	 mpool_total_bytes_allocated((hashtable)->group_buf))

#define GET_USED_BUFFER_SIZE(hashtable) \
	((hashtable)->entry_buf.nfull_total * (hashtable)->entry_buf.cellbytes + \
	mpool_bytes_used((hashtable)->group_buf))

#define SANITY_CHECK_METADATA_SIZE(hashtable) \
	do { \
		if ((hashtable)->mem_for_metadata >= (hashtable)->max_mem) \
			ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR), \
							errmsg(ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY)));\
    } while (0)

#define GET_TOTAL_USED_SIZE(hashtable) \
   (GET_USED_BUFFER_SIZE(hashtable) + (hashtable)->mem_for_metadata)

#define HAVE_FREESPACE(hashtable) \
   (GET_TOTAL_USED_SIZE(hashtable) < (hashtable)->max_mem)


/*
 * implement the SUM aggregate functions.
 * With the different column type of aggregate function, the transfer
 * data type is different too.
 *
 * this MACRO only used by the int2 and int4.
 */
#define _VACCUM_NUMERIC(type) \
PG_FUNCTION_INFO_V1(v##type##_accum); \
Datum v##type##_accum(PG_FUNCTION_ARGS) \
{ \
	VectorizedAggData *vectransdata = \
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0); \
	BatchAggGroupData *groupdata = vectransdata->groupData; \
	GroupData *curHeader = NULL; \
	v##type *vvalue = (v##type *)DatumGetPointer(PG_GETARG_DATUM(1)); \
	int idx = 0;\
	int64 sum = 0; \
	bool allnull = true; \
	Assert(NULL != vectransdata); \
	if(NULL != groupdata) \
	{ \
		curHeader = &(groupdata->group_header[groupdata->group_idx]); \
	} \
	if(vectransdata->isnovalue) \
	{ \
		vectransdata->data = 0; \
		vectransdata->isnull = true; \
		vectransdata->isnovalue = false; \
	} \
	if(curHeader) \
	{ \
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx]) \
		{ \
			Assert(NULL != vvalue->isnull); \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			allnull = false; \
			sum = sum + vvalue->values[idx];\
		} \
	} \
	else \
	{ \
		for(idx = 0; idx < vvalue->dim; idx++) \
		{ \
			Assert(NULL != vvalue->isnull); \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			allnull = false; \
			sum = sum + vvalue->values[idx];\
		} \
	} \
	if(!allnull) \
		vectransdata->isnull = false; \
	vectransdata->data += sum; \
	return PointerGetDatum(vectransdata); \
}

/*
 * for sum(int8)
 * the transfer data type of sum(int8) is numeric
 */
PG_FUNCTION_INFO_V1(vint8_accum);
Datum vint8_accum(PG_FUNCTION_ARGS)
{
	VectorizedAggData *vectransdata =
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0);
	BatchAggGroupData *groupdata = vectransdata->groupData;
	GroupData *curHeader = NULL;
	vint8 *vvalue = (vint8 *)DatumGetPointer(PG_GETARG_DATUM(1));
	int idx = 0;
	int64 sum = 0;
	Datum newval;
	bool allnull = true;
	Assert(NULL != vectransdata);
	if(NULL != groupdata)
	{
		curHeader = &(groupdata->group_header[groupdata->group_idx]);
	}
	if( vectransdata->isnull || vectransdata->isnovalue)
	{
		vectransdata->data = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));
		vectransdata->isnull = true;
		vectransdata->isnovalue = false;
		vectransdata->isalloc = true;
	}

	if(curHeader)
	{
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx])
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetInt64(vvalue->values[idx]);
			allnull = false;
		}
	}
	else
	{
		for(idx = 0; idx < vvalue->dim; idx++)
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetInt64(vvalue->values[idx]);
			allnull = false;
		}
	}

	if(!allnull)
	{
		vectransdata->isnull = false;
		vectransdata->isalloc = true;
		/* OK to do the addition. */
		newval = DirectFunctionCall1(int8_numeric, Int64GetDatum(sum));
		vectransdata->data = DirectFunctionCall2(numeric_add,
												vectransdata->data, newval);
	}
	return PointerGetDatum(vectransdata);
}

/*
 * for sum(float4)
 * the transfer data type of sum(float4) is float4
 */
PG_FUNCTION_INFO_V1(vfloat4_accum);
Datum vfloat4_accum(PG_FUNCTION_ARGS)
{
	VectorizedAggData *vectransdata =
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0);
	BatchAggGroupData *groupdata = vectransdata->groupData;
	GroupData *curHeader = NULL;
	vfloat4 *vvalue = (vfloat4 *)DatumGetPointer(PG_GETARG_DATUM(1));
	int idx = 0;
	float4 sum = 0;
	bool allnull = true;
	Datum newval;
	Assert(NULL != vectransdata);
	if(NULL != groupdata)
	{
		curHeader = &(groupdata->group_header[groupdata->group_idx]);
	}
	if( vectransdata->isnull || vectransdata->isnovalue)
	{
		vectransdata->data = Float4GetDatum(0);
		vectransdata->isnull = true;
		vectransdata->isnovalue = false;
	}

	if(curHeader)
	{
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx])
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetFloat4(vvalue->values[idx]);
			allnull = false;
		}
	}
	else
	{
		for(idx = 0; idx < vvalue->dim; idx++)
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetFloat4(vvalue->values[idx]);
			allnull = false;
		}
	}

	if(!allnull)
		vectransdata->isnull = false;

	/* OK to do the addition. */
	sum = sum + DatumGetFloat4(vectransdata->data);
	vectransdata->data = Float4GetDatum(sum);
	return PointerGetDatum(vectransdata);
}

/*
 * for sum(float8)
 * the transfer data type of sum(float8) is float8
 */
PG_FUNCTION_INFO_V1(vfloat8_accum);
Datum vfloat8_accum(PG_FUNCTION_ARGS)
{
	VectorizedAggData *vectransdata =
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0);
	BatchAggGroupData *groupdata = vectransdata->groupData;
	GroupData *curHeader = NULL;
	vfloat8 *vvalue = (vfloat8 *)DatumGetPointer(PG_GETARG_DATUM(1));
	int idx = 0;
	float8 sum = 0;
	bool allnull = true;
	Datum newval;
	Assert(NULL != vectransdata);
	if(NULL != groupdata)
	{
		curHeader = &(groupdata->group_header[groupdata->group_idx]);
	}
	if( vectransdata->isnull || vectransdata->isnovalue)
	{
		vectransdata->data = Float8GetDatum(0);
		vectransdata->isnull = true;
		vectransdata->isnovalue = false;
	}

	if(curHeader)
	{
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx])
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetFloat8(vvalue->values[idx]);
			allnull = false;
		}
	}
	else
	{
		for(idx = 0; idx < vvalue->dim; idx++)
		{
			Assert(NULL != vvalue->isnull);
			if(vvalue->isnull[idx])
				continue;
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			sum = sum + DatumGetFloat8(vvalue->values[idx]);
			allnull = false;
		}
	}

	if(!allnull)
		vectransdata->isnull = false;

	/* OK to do the addition. */
	sum = sum + DatumGetFloat8(vectransdata->data);
	vectransdata->data = Float8GetDatumFast(sum);
	return PointerGetDatum(vectransdata);
}

/*
 * implement the AVG aggregate functions.
 */
#define _VAVG_NUMERIC(type, XTYPE) \
PG_FUNCTION_INFO_V1(v##type##_avg_accum); \
Datum v##type##_avg_accum(PG_FUNCTION_ARGS) \
{ \
	VectorizedAggData *vectransdata = \
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0); \
	IntFloatAvgTransdata *transdata = NULL;\
	BatchAggGroupData *groupdata = vectransdata->groupData; \
	GroupData *curHeader = NULL; \
	v##type *vvalue = (v##type *)DatumGetPointer(PG_GETARG_DATUM(1)); \
	int idx = 0;\
	int64 count = 0; \
	float8 sum = 0; \
	Assert(NULL != vectransdata); \
	if(NULL != groupdata) \
	{ \
		curHeader = &(groupdata->group_header[groupdata->group_idx]); \
	} \
	if(vectransdata->isnull || vectransdata->isnovalue) \
	{ \
		transdata = \
			(IntFloatAvgTransdata *) palloc(sizeof(IntFloatAvgTransdata)); \
		SET_VARSIZE(transdata, sizeof(IntFloatAvgTransdata)); \
		transdata->count = 0; \
		transdata->sum = 0; \
		vectransdata->isnull = false; \
		vectransdata->isnovalue = false; \
		vectransdata->isalloc = true;\
		vectransdata->data = PointerGetDatum(transdata); \
	} \
	else \
	{ \
		transdata= (IntFloatAvgTransdata*)DatumGetPointer(vectransdata->data);\
	} \
	if(curHeader) \
	{ \
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx]) \
		{ \
			Assert(NULL != vvalue->isnull); \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			sum = sum + DatumGet##XTYPE(vvalue->values[idx]);\
			count++; \
		} \
	} \
	else \
	{ \
		for(idx = 0; idx < vvalue->dim; idx++) \
		{ \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			sum = sum + DatumGet##XTYPE(vvalue->values[idx]);\
			count++; \
		} \
	} \
	transdata->count += count; \
	transdata->sum += sum; \
	return PointerGetDatum(vectransdata); \
}


/*
 * implement the COUNT(column) aggregate functions.
 */
#define _VINC_NUMERIC(type) \
PG_FUNCTION_INFO_V1(v##type##_inc); \
Datum v##type##_inc(PG_FUNCTION_ARGS) \
{ \
	VectorizedAggData *vectransdata = \
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0); \
	BatchAggGroupData *groupdata = vectransdata->groupData; \
	GroupData *curHeader = NULL; \
	int64 tval = 0; \
	v##type* vvalue = (v##type *)DatumGetPointer(PG_GETARG_DATUM(1)); \
	int idx; \
	Assert(NULL != vectransdata); \
	if(NULL != groupdata) \
	{ \
		curHeader = &(groupdata->group_header[groupdata->group_idx]); \
	} \
	if( vectransdata->isnull || vectransdata->isnovalue) \
	{ \
		vectransdata->data = Int64GetDatum(0); \
		vectransdata->isnull = false; \
		vectransdata->isnovalue = false; \
	} \
	if(curHeader) \
	{ \
		Assert(NULL != vvalue->isnull); \
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx]) \
		{ \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			tval++; \
		} \
	} \
	else \
	{ \
		Assert(NULL != vvalue->isnull); \
		for(idx = 0; idx < vvalue->dim; idx++) \
		{ \
			if(vvalue->isnull[idx]) \
				continue; \
			if(NULL != vectransdata->skip && vectransdata->skip[idx]) \
				continue; \
			tval++; \
		} \
	} \
	vectransdata->data = (vectransdata->data + tval); \
	return PointerGetDatum(vectransdata); \
}


/*
 * implement the COUNT(*) aggregate functions.
 * NOTE:we create an new aggregate functions 'veccount' to implement the
 * vectorized couter aggregate, In the CHECK phase(vcheck.c), if we found
 * an count(*), we use veccount(*) to replace it.
 */
PG_FUNCTION_INFO_V1(vec_inc_any);
Datum vec_inc_any(PG_FUNCTION_ARGS)
{
	VectorizedAggData *vectransdata =
				(VectorizedAggData *) PG_GETARG_BYTEA_P(0);
	BatchAggGroupData *groupdata = vectransdata->groupData;
	GroupData *curHeader = NULL;
	int64 tval = 0;
	int idx;
	Assert(NULL != vectransdata);
	if(NULL != groupdata)
	{
		curHeader = &(groupdata->group_header[groupdata->group_idx]);
	}
	if( vectransdata->isnull || vectransdata->isnovalue)
	{
		vectransdata->data = Int64GetDatum(0);
		vectransdata->isnull = false;
		vectransdata->isnovalue = false;
	}
	if(curHeader)
	{
		for(idx = curHeader->idx; idx != -1; idx = groupdata->idx_list[idx])
		{
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			tval++;
		}
	}
	else
	{
		for(idx = 0; idx < vectransdata->nrows; idx++)
		{
			if(NULL != vectransdata->skip && vectransdata->skip[idx])
				continue;
			tval++;
		}
	}
	vectransdata->data = Int64GetDatum(DatumGetInt64(vectransdata->data) + tval);
	return PointerGetDatum(vectransdata);
}

_VACCUM_NUMERIC(int2)
_VACCUM_NUMERIC(int4)

#define VACCUM_NUMERIC(type, XTYPE) \
	_VAVG_NUMERIC(type, XTYPE) \
	_VINC_NUMERIC(type)

VACCUM_NUMERIC(int2, Int16)
VACCUM_NUMERIC(int4, Int32)
VACCUM_NUMERIC(int8, Int64)
VACCUM_NUMERIC(float4, Float4)
VACCUM_NUMERIC(float8, Float8)


/*
 * copy from src/backend/executor/nodeagg.c
 *
 * Advance all the aggregates for one input tuple.	The input tuple
 * has been stored in tmpcontext->ecxt_scantuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_vaggregates(AggState *aggstate, AggStatePerGroup pergroup,
				   MemoryManagerContainer *mem_manager)
{
	int			aggno;
	TupleBatch	tb;
	VectorizedState *vstate = ((PlanState*)aggstate)->vectorized;

	Assert(NULL != vstate);

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		Aggref	   *aggref = peraggstate->aggref;
		PercentileExpr *perc = peraggstate->perc;
		int			i;
		TupleTableSlot *slot;
		int nargs;

		/* We can apply the transition function immediately */
		FunctionCallInfoData fcinfo;

		if (aggref)
			nargs = list_length(aggref->args);
		else
		{
			Assert (perc);
			nargs = list_length(perc->args);
		}

		/* Evaluate the current input expressions for this aggregate */
		Assert(NULL != vstate->aggslot);
		slot = vstate->aggslot[aggno];

		Assert(NULL != slot);

		tb = (TupleBatch)slot->PRIVATE_tb;

		/* Load values into fcinfo */
		/* Start from 1, since the 0th arg will be the transition value */
		if (aggref)
		{
			for (i = 0; i < nargs; i++)
			{
				tb->datagroup[i]->skipref = tb->skip;
				fcinfo.arg[i + 1] = PointerGetDatum(tb->datagroup[i]);
				fcinfo.argnull[i + 1] = false;
			}

		}
		else
		{
			/*
			 * In case of percentile functions, put everything into
			 * fcinfo's argument since there should be the required
			 * attributes as arguments in the tuple.
			 */
			int		natts;

			Assert(perc);
			natts = slot->tts_tupleDescriptor->natts;
			for (i = 0; i < natts; i++)
			{
				fcinfo.arg[i + 1] = PointerGetDatum(tb->datagroup[i]);
				fcinfo.argnull[i + 1] = false;
			}
		}

		advance_transition_function(aggstate, peraggstate, pergroupstate,
									&fcinfo, mem_manager);
	} /* aggno loop */
}

/*
 * copy from src/backend/executor/nodeagg.c
 * Function: setGroupAggs
 *
 * Set the groupaggs buffer in the hashtable to point to the right place
 * in the given hash entry.
 */
static inline void
setGroupAggs(HashAggTable *hashtable, MemTupleBinding *mt_bind, HashAggEntry *entry)
{
	Assert(mt_bind != NULL);

	if (entry != NULL)
	{
		int tup_len = memtuple_get_size((MemTuple)entry->tuple_and_aggs, mt_bind);
		hashtable->groupaggs->tuple = (MemTuple)entry->tuple_and_aggs;
		hashtable->groupaggs->aggs = (AggStatePerGroup)
			((char *)entry->tuple_and_aggs + MAXALIGN(tup_len));
	}
}

/*
 * copy from src/backend/executor/execHHashagg.c
 *
 * Calculate the hash value for the given input tuple.
 *
 * This based on but different from get_hash_value from the dynahash
 * API.  Use a different name to underline that we don't use dynahash.
 */
static uint32
calc_hash_value(AggState* aggstate, TupleTableSlot *inputslot)
{
	Agg *agg;
	ExprContext *econtext;
	MemoryContext oldContext;
	int			i;
	FmgrInfo* info = aggstate->hashfunctions;
	HashAggTable *hashtable = aggstate->hhashtable;

	agg = (Agg*)aggstate->ss.ps.plan;
	econtext = aggstate->tmpcontext; /* short-lived, per-input-tuple */

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	for (i = 0; i < agg->numCols; i++, info++)
	{
		AttrNumber	att = agg->grpColIdx[i];
		bool isnull = false;
		Datum value = slot_getattr(inputslot, att, &isnull);

		if (!isnull)			/* treat nulls as having hash key 0 */
		{
			hashtable->hashkey_buf[i] = DatumGetUInt32(FunctionCall1(info, value));
		}

		else
			hashtable->hashkey_buf[i] = 0xdeadbeef;

	}

	MemoryContextSwitchTo(oldContext);
	return (uint32) hash_any((unsigned char *) hashtable->hashkey_buf, agg->numCols * sizeof(HashKey));
}

/*
 * Vectorized Data
 *
 * To implement an aggregate function often have 3 phase:
 * 1) Set the initialized values.
 * 2) process data, it used "transfer data" for communication.
 * 3) finalize aggregate functions, to get the final result.
 * To implement the vectorized aggregate functions, we refactor the phase 2.
 * we don't use the "transfer data" directly when the aggregate functions
 * can be vectorized, we create a VectorizedAggData to encapsulates the
 * "transfer data", and also set the groupping data in it, then the vectorized
 * aggregate functions can get the grouping data and process it.
 * Because "transfer data" is used as a Datum, and we use
 * VectorizedAggData as a Datum too, so it can communicate properly.
 * when the phase 2 is over, we strip the VectorizedAggData, it convert to
 * "transfer data" again.
 * Because the result of phase 2 is a scalar, so we
 * can use the non-vectorized finalize aggregate functions directly.
 */

/*
 * Init Vectorized data, in order to transfer group data into the agg functions
 */
VectorizedAggData *
InitAggVectorizedData(AggState *aggstate)
{
	int aggno;

	VectorizedAggData *trans =
			(VectorizedAggData *) palloc0(sizeof(VectorizedAggData) * aggstate->numaggs);

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		SET_VARSIZE(&trans[aggno], sizeof(VectorizedAggData));
	}

	return trans;
}

/*
 * Use the new transfer data, it can send the group data to agg functions.
 */
static inline void
AddAggVectorizedData(AggState *aggstate,
					  AggStatePerGroup pergroup, VectorizedAggData *trans,
					  BatchAggGroupData *groupdata, bool *skip, int nrows)
{
	int aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		trans[aggno].data = pergroupstate->transValue;
		trans[aggno].isnull = pergroupstate->transValueIsNull;
		trans[aggno].isnovalue = pergroupstate->noTransValue;
		trans[aggno].groupData = groupdata;
		trans[aggno].skip = skip;
		trans[aggno].nrows = nrows;
		trans[aggno].isalloc = false;
		pergroupstate->transValueIsNull = false;
		pergroupstate->noTransValue = false;
		pergroupstate->transValue = PointerGetDatum(&(trans[aggno]));
	}

	return;
}

/*
 * set skip array to filter some tuples
 */
static inline void
SetAggVectorizedSkip(AggState *aggstate, VectorizedAggData *trans, bool *skip, int nrows)
{
	int aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		trans[aggno].skip = skip;
		trans[aggno].nrows = nrows;
	}

	return;
}

/*
 * remove the vectorized data struct
 */
static inline void
RemoveAggVectorizedData(AggState *aggstate,
						AggStatePerGroup pergroup)
{
	int aggno;
	MemoryManagerContainer *mem_manager = &(aggstate->mem_manager);

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		VectorizedAggData *trans =
			(VectorizedAggData *) DatumGetPointer(pergroupstate->transValue);

		pergroupstate->transValueIsNull = trans->isnull;
		pergroupstate->noTransValue = trans->isnovalue;
		if(trans->isalloc)
			pergroupstate->transValue = datumCopyWithMemManager(0,
																trans->data,
																peraggstate->transtypeByVal,
																peraggstate->transtypeLen,
																mem_manager);
		else
			pergroupstate->transValue = trans->data;
	}

	return;
}


/*
 * copied from src/backend/executor/execHHashagg.c
 * agg_hash_table_stat_upd
 *      collect hash chain statistics for EXPLAIN ANALYZE
 */
static void
agg_hash_table_stat_upd(HashAggTable *ht)
{
    unsigned int	 i;

    char hostname[SEGMENT_IDENTITY_NAME_LENGTH];
    gethostname(hostname,SEGMENT_IDENTITY_NAME_LENGTH);
    for (i = 0; i < ht->nbuckets; i++)
    {
        HashAggEntry   *entry = ht->buckets[i];
        int             chainlength = 0;

        if (entry)
        {
            for (chainlength = 0; entry; chainlength++)
                entry = entry->next;

            cdbexplain_agg_upd(&ht->chainlength, chainlength, i,hostname);
        }
    }
}                               /* agg_hash_table_stat_upd */

/*
 * copied from src/backend/executor/nodeAgg.c
 *
 * agg_retrieve_scalar
 *   Compute the scalar aggregates.
 *
 */
static TupleTableSlot *
agg_retrieve_scalar(AggState *aggstate)
{
        AggStatePerAgg peragg = aggstate->peragg;
        AggStatePerGroup pergroup = aggstate->pergroup ;
        VectorizedState *vstate = ((PlanState*)aggstate)->vectorized;
        VectorizedAggData *trans = (VectorizedAggData*)vstate->transdata;

        initialize_aggregates(aggstate, peragg, pergroup, &(aggstate->mem_manager));

        /*
         *  In fact if there is no Group By clause, VectorizedData is not useful,
         *  we add it in order to keep compatibility.
         */
        AddAggVectorizedData(aggstate, pergroup, trans, NULL, NULL, 0);

        /*
         * We loop through input tuples, and compute the aggregates.
         */
        while (!aggstate->agg_done)
        {
                ExprContext *tmpcontext = aggstate->tmpcontext;
                /* Reset the per-input-tuple context */
                ResetExprContext(tmpcontext);
                PlanState *outerPlan = outerPlanState(aggstate);
                TupleTableSlot *outerslot = ExecProcNode(outerPlan);
                TupleBatch tb = NULL;
                if (TupIsNull(outerslot))
                {
                        aggstate->agg_done = true;
                        break;
                }
                Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                tmpcontext->ecxt_scantuple = outerslot;

                tb = (TupleBatch)outerslot->PRIVATE_tb;

                SetAggVectorizedSkip(aggstate, trans, tb->skip, tb->nrows);

                /*
                 *  In fact if there is no Group By clause, we can do the
                 *  projection in the advance_vaggregates.
                 *  we add it in order to keep compatibility.
                 */
                int aggno;
                for (aggno = 0; aggno < aggstate->numaggs; aggno++)
                {
                    AggStatePerAgg peraggstate = &aggstate->peragg[aggno];

                    /* Evaluate the current input expressions for this aggregate */
                    vstate->aggslot[aggno] = ExecVProject(peraggstate->evalproj, NULL);
                }

                advance_vaggregates(aggstate, pergroup, &(aggstate->mem_manager));
        }

        RemoveAggVectorizedData(aggstate, pergroup);

        finalize_aggregates(aggstate, pergroup);


        ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
        Agg *node = (Agg*)aggstate->ss.ps.plan;
        econtext->grouping = node->grouping;
        econtext->group_id = node->rollupGSTimes;
        /* Check the qual (HAVING clause). */
        if (ExecQual(aggstate->ss.ps.qual, econtext, false))
        {
                Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
                CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

                /*
                 * Form and return a projection tuple using the aggregate results
                 * and the representative input tuple.
                 */
                return ExecProject(aggstate->ss.ps.ps_ProjInfo, NULL);
        }
        return NULL;
}

/*
 * ExecAgg for non-hashed case.
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
        if (aggstate->agg_done)
        {
                return NULL;
        }

        switch(aggstate->aggType)
        {
                case AggTypeScalar:
                        return agg_retrieve_scalar(aggstate);

                case AggTypeGroup:
                case AggTypeIntermediateRollup:
                case AggTypeFinalRollup:
                default:
                        insist_log(false, "invalid Agg node: type %d", aggstate->aggType);
        }
        return NULL;
}


/* copy from src/backend/executor/execHHashagg.c*/
static bool
agg_hash_initial_1pass(AggState *aggstate)
{
	HashAggTable *hashtable = aggstate->hhashtable;
	ExprContext *tmpcontext = aggstate->tmpcontext; /* per input tuple context */
	TupleTableSlot *outerslot = NULL;
	bool streaming = ((Agg *) aggstate->ss.ps.plan)->streaming;
	bool tuple_remaining = true;
	MemTupleBinding *mt_bind = aggstate->hashslot->tts_mt_bind;
	VectorizedState *vstate = ((PlanState*)aggstate)->vectorized;
	VectorizedAggData *trans = (VectorizedAggData*)vstate->transdata;

	Assert(hashtable);
	AssertImply(!streaming, hashtable->state == HASHAGG_BEFORE_FIRST_PASS);
	elog(HHA_MSG_LVL,
		 "HashAgg: initial pass -- beginning to load hash table");

	/* NOTE: Now we cannot use GUC parameters directly because vexecutor is a plug-in module */
	/* If we found cached workfiles, initialize and load the batch data here */
	if (gp_workfile_caching && aggstate->cached_workfiles_found)
	{
		elog(HHA_MSG_LVL, "Found existing SFS, reloading data from %s", hashtable->work_set->path);
		/* Initialize all structures as if we just spilled everything */
		hashtable->spill_set = read_spill_set(aggstate);
		aggstate->hhashtable->is_spilling = true;
		aggstate->cached_workfiles_loaded = true;

		elog(gp_workfile_caching_loglevel, "HashAgg reusing cached workfiles, initiating Squelch walker");
		PlanState *outerNode = outerPlanState(aggstate);
		ExecSquelchNode(outerNode);

		/* tuple table initialization */
		ScanState *scanstate = & aggstate->ss;
		PlanState  *outerPlan = outerPlanState(scanstate);
		TupleDesc tupDesc = ExecGetResultType(outerPlan);

		if (aggstate->ss.ps.instrument)
		{
			aggstate->ss.ps.instrument->workfileReused = true;
		}

		/* Initialize hashslot by cloning input slot. */
		ExecSetSlotDescriptor(aggstate->hashslot, tupDesc);
		ExecStoreAllNullTuple(aggstate->hashslot);
		mt_bind = aggstate->hashslot->tts_mt_bind;


		return tuple_remaining;
	}

	/*
	 * Check if an input tuple has been read, but not processed
	 * because of lack of space before streaming the results
	 * in the last call.
	 */
	if (aggstate->hashslot->tts_tupleDescriptor != NULL &&
		hashtable->prev_slot != NULL)
	{
		outerslot = hashtable->prev_slot;
		hashtable->prev_slot = NULL;
	}
	else
	{
		outerslot = ExecProcNode(outerPlanState(aggstate));
	}

	/*
	 * Process outer-plan tuples, until we exhaust the outer plan.
	 */
	hashtable->pass = 0;

	while(true)
	{
		HashKey hashkey;
		bool isNew;
		HashAggEntry *entry;
		TupleBatch tb;
		int i = 0;
		BatchAggGroupData *agg_groupdata;
		GroupData *cur_header = NULL;

		/* no more tuple. Done */
		if (TupIsNull(outerslot))
		{
			tuple_remaining = false;
			break;
		}

		Assert(NULL != vstate->batchGroupData);
		Assert(NULL != vstate->groupData);
		Assert(NULL != vstate->indexList);
		memset(vstate->batchGroupData, 0, sizeof(BatchAggGroupData));
		memset(vstate->groupData, 0, sizeof(GroupData) * BATCHSIZE);
		memset(vstate->indexList, -1, sizeof(int) * BATCHSIZE);

		agg_groupdata = vstate->batchGroupData;
		agg_groupdata->group_header = vstate->groupData;
		agg_groupdata->idx_list = vstate->indexList;

		tb = (TupleBatch)outerslot->PRIVATE_tb;

		if(NULL == tb || tb->nrows == 0)
		{
			tuple_remaining = false;
			break;
		}

		/*
		 * we have to convert the vectorized tuple to non-vectorized tuple,
		 * and process it one by one.
		 * the initialize value of i is -1, because outerslot may have no
		 * valid tuple(all tuple can not pass the qualification), then -1
		 * indicate that no valid tuple. if there have valid tuple, we will
		 * reset the i to correct value.
		 */

		for (i = -1; i < tb->nrows; i++)
		{
			if(!VirtualNodeProc(outerslot))
				break;

			/* set the correct value */
			i = tb->iter - 1;

			Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);

			if (aggstate->hashslot->tts_tupleDescriptor == NULL)
			{
				int size;

				/* Initialize hashslot by cloning input slot. */
				ExecSetSlotDescriptor(aggstate->hashslot, outerslot->tts_tupleDescriptor);
				ExecStoreAllNullTuple(aggstate->hashslot);
				mt_bind = aggstate->hashslot->tts_mt_bind;

				size = ((Agg *)aggstate->ss.ps.plan)->numCols * sizeof(HashKey);

				hashtable->hashkey_buf = (HashKey *)palloc0(size);
				hashtable->mem_for_metadata += size;
			}

			/* set up for advance_aggregates call */
			tmpcontext->ecxt_scantuple = outerslot;

			/* Find or (if there's room) build a hash table entry for the
			 * input tuple's group. */
			hashkey = calc_hash_value(aggstate, outerslot);
			entry = lookup_agg_hash_entry(aggstate, (void *)outerslot,
										  INPUT_RECORD_TUPLE, 0, hashkey, 0, &isNew);

			if (entry == NULL)
			{
				if (GET_TOTAL_USED_SIZE(hashtable) > hashtable->mem_used)
					hashtable->mem_used = GET_TOTAL_USED_SIZE(hashtable);

				if (hashtable->num_ht_groups <= 1)
					ereport(ERROR,
							(errcode(ERRCODE_GP_INTERNAL_ERROR),
									 ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY));

				/*
				 * If stream_bottom is on, we store outerslot into hashslot, so that
				 * we can process it later.
				 */
				if (streaming)
				{
					Assert(tuple_remaining);
					hashtable->prev_slot = outerslot;
					break;
				}

				/* CDB: Report statistics for EXPLAIN ANALYZE. */
				if (!hashtable->is_spilling && aggstate->ss.ps.instrument)
					agg_hash_table_stat_upd(hashtable);

				spill_hash_table(aggstate);

				entry = lookup_agg_hash_entry(aggstate, (void *)outerslot,
											  INPUT_RECORD_TUPLE, 0, hashkey, 0, &isNew);
			}

			if (isNew)
			{
				int tup_len = memtuple_get_size((MemTuple)entry->tuple_and_aggs, mt_bind);
				setGroupAggs(hashtable, mt_bind, entry);
				MemSet((char *)entry->tuple_and_aggs + MAXALIGN(tup_len), 0,
					   aggstate->numaggs * sizeof(AggStatePerGroupData));
				initialize_aggregates(aggstate, aggstate->peragg, hashtable->groupaggs->aggs,
									  &(aggstate->mem_manager));
			}

			//find current group_header if exists, just O(n) find
			for (int j = 0; j < agg_groupdata->group_cnt; j++)
			{
				cur_header = NULL;
				if (agg_groupdata->group_header[j].entry == entry)
				{
					cur_header = &(agg_groupdata->group_header[j]);
					break;
				}
			}

			if (cur_header == NULL)
			{
				// add a new group header
				agg_groupdata->group_header[agg_groupdata->group_cnt].idx = i;
				agg_groupdata->group_header[agg_groupdata->group_cnt].entry = entry;
				agg_groupdata->group_cnt++;
			}
			else
			{
				//group header already exists, just insert the current tuple to the "neck"
				agg_groupdata->idx_list[i] = cur_header->idx;
				cur_header->idx = i;
			}
		}

		/*
		 * if i == -1, it indicate that all tuple in outerslot is invalid,
		 * we need not to process it.
		 */
		if(-1 != i)
		{
			tmpcontext->ecxt_scantuple = outerslot;

			/* To avoid wasteful duplication of work, we do the projection here */
			int aggno;
			for (aggno = 0; aggno < aggstate->numaggs; aggno++)
			{
				AggStatePerAgg peraggstate = &aggstate->peragg[aggno];

				/* Evaluate the current input expressions for this aggregate */
				vstate->aggslot[aggno] = ExecVProject(peraggstate->evalproj, NULL);
			}

			/* we have known the group counts, so we process it one by one. */
			for (int i = 0; i < agg_groupdata->group_cnt; i++) {
				GroupData *cur_header = &(agg_groupdata->group_header[i]);
				agg_groupdata->group_idx = i;

				//set hashtable->groupaggs to the agg_hash_entry
				setGroupAggs(hashtable, aggstate->hashslot->tts_mt_bind, cur_header->entry);

				/* HACK... */
				AddAggVectorizedData(aggstate, hashtable->groupaggs->aggs, trans, agg_groupdata, tb->skip, tb->nrows);
				advance_vaggregates(aggstate, hashtable->groupaggs->aggs, &(aggstate->mem_manager));
				RemoveAggVectorizedData(aggstate, hashtable->groupaggs->aggs);

			}

			/* it is batch count now */
			hashtable->num_tuples++;

			/* Reset per-input-tuple context after each tuple */
			ResetExprContext(tmpcontext);

			if (streaming && !HAVE_FREESPACE(hashtable))
			{
				Assert(tuple_remaining);
				ExecClearTuple(aggstate->hashslot);
				break;
			}
		}

		/* Read the next tuple */
		outerslot = ExecProcNode(outerPlanState(aggstate));
	}

	if (GET_TOTAL_USED_SIZE(hashtable) > hashtable->mem_used)
		hashtable->mem_used = GET_TOTAL_USED_SIZE(hashtable);

	if (hashtable->is_spilling)
	{
		int freed_size = 0;

		/*
		 * Split out the rest of groups in the hashtable if spilling has already
		 * happened. This is because none of these groups can be immediately outputted
		 * any more.
		 */
		spill_hash_table(aggstate);
		freed_size = suspendSpillFiles(hashtable->spill_set);
		hashtable->mem_for_metadata -= freed_size;

		if (aggstate->ss.ps.instrument)
		{
			aggstate->ss.ps.instrument->workfileCreated = true;
		}
	}

	/* CDB: Report statistics for EXPLAIN ANALYZE. */
	if (!hashtable->is_spilling && aggstate->ss.ps.instrument)
		agg_hash_table_stat_upd(hashtable);

	AssertImply(tuple_remaining, streaming);
	if(tuple_remaining)
		elog(HHA_MSG_LVL, "HashAgg: streaming out the intermediate results.");

	return tuple_remaining;
}

/*
 * copy from src/backend/executor/nodeAgg.c
 * ExecAggExplainEnd
 *      Called before ExecutorEnd to finish EXPLAIN ANALYZE reporting.
 */
static void
ExecAggExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
    AggState   *aggstate = (AggState *)planstate;

    /* Report executor memory used by our memory context. */
    planstate->instrument->execmemused +=
        (double)MemoryContextGetPeakSpace(aggstate->aggcontext);
}                               /* ExecAggExplainEnd */

/*
 * ExecAgg - copy from src/backend/executor/nodeAgg.c
 */

TupleTableSlot *
ExecVAgg(AggState *node)
{
	if (node->agg_done)
	{
		ExecEagerFreeAgg(node);
		return NULL;
	}


	if (((Agg *) node->ss.ps.plan)->aggstrategy == AGG_HASHED)
	{
		TupleTableSlot *tuple = NULL;
		bool streaming = ((Agg *) node->ss.ps.plan)->streaming;

		/*
		 * ExecAgg processing for hashed aggregation -- returns the
		 * next result tuple or NULL. When returning NULL also sets
		 * aggstate to prevent future calls.
		 */

		if (node->hhashtable == NULL)
		{
			bool tupremain;

			node->hhashtable = create_agg_hash_table(node);
			tupremain = agg_hash_initial_1pass(node);

			if ( streaming )
			{
				if ( tupremain )
					node->hhashtable->state = HASHAGG_STREAMING;
				else
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
			}
			else
				node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
		}



		/* On each call we either return a tuple corresponding to a hash
		 * entry (consuming the entry) or fall through to a state machine
		 * that tries to make additional hash entries available and continue
		 * the loop.  (This may result in reaching the "exit" state and
		 * returning a NULL tuple).
		 */
		for (;;)
		{
			if (!node->hhashtable->is_spilling)
			{

				tuple = agg_retrieve_hash_table(node);
				node->agg_done = false; /* Not done 'til batches used up. */

				if (tuple != NULL)
					return tuple;
			}

			switch (node->hhashtable->state)
			{
				case HASHAGG_BETWEEN_PASSES:
					Assert(!streaming);
					if (agg_hash_next_pass(node))
					{
						node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
						continue;
					}
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
					/*
					 * pass through. Be sure that the next case statment
					 * is HASHAGG_END_OF_PASSES.
					 */

				case HASHAGG_END_OF_PASSES:
					node->agg_done = true;
					if (gp_workfile_caching && node->workfiles_created)
					{
						/*
						 * HashAgg closes each spill file after it is done with
						 * them. Since we got here on the regular path, all
						 * files should be closed.
						 */
						Assert(node->hhashtable->work_set);
						Assert(node->hhashtable->spill_set == NULL);
						agg_hash_close_state_file(node->hhashtable);
						agg_hash_mark_spillset_complete(node);
					}
					ExecEagerFreeAgg(node);
					return NULL;

				case HASHAGG_STREAMING:
					Assert(streaming);
					/*expand agg_hash_stream */
					reset_agg_hash_table(node);
					if (!agg_hash_initial_1pass(node))
						node->hhashtable->state = HASHAGG_END_OF_PASSES;
					continue;

				case HASHAGG_BEFORE_FIRST_PASS:
				default:
					elog(ERROR,"hybrid hash aggregation sequencing error");
			}
		}
	}
	else
	{
		return agg_retrieve_direct(node);
	}
}

/*
 * getAggType
 *   Get the aggType for the given Agg node.
 *
 * We should really store the type in the Agg struct, and let the planner set the
 * correct type. As an intermediate step, we compute the type here.
 */
static int
getAggType(Agg *node)
{
        int aggType = AggTypeScalar;
        if (node->numCols > 0 &&
                (node->lastAgg &&
                 (node->inputHasGrouping && node->numNullCols > 0)))
        {
                aggType = AggTypeFinalRollup;
        }
        else if (node->numCols > 0 &&
                         (node->inputHasGrouping && node->numNullCols > 0))
        {
                aggType = AggTypeIntermediateRollup;
        }
        else if (node->numCols > 0)
        {
                aggType = AggTypeGroup;
        }
        else
        {
                insist_log(node->aggstrategy == AGG_PLAIN, "wrong Agg strategy: %d", node->aggstrategy);
        }

        return aggType;
}


extern Datum ExecEvalVar(ExprState *exprstate, ExprContext *econtext,
			bool *isNull, ExprDoneCond *isDone);

/*
 * Copied from src/backend/executor/nodeAgg.c
 *
 * We need convert some vectorized tupleDesc to non-vectorzied tupleDesc.
 */
AggState *
VExecInitAgg(Agg *node, EState *estate, int eflags)
{
	AggState   *aggstate;
	AggStatePerAgg peragg;
	Plan	   *outerPlan;
	ExprContext *econtext;
	int			numaggs,
				aggno;
	ListCell   *l;
	List *nagglist = NULL;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->eqfunctions = NULL;
	aggstate->hashfunctions = NULL;
	aggstate->peragg = NULL;
	aggstate->agg_done = false;
	aggstate->pergroup = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->hashtable = NULL;
	agg_hash_reset_workfile_state(aggstate);

	/*
	 * Create expression contexts.	We need two, one for per-input-tuple
	 * processing and one for per-output-tuple processing.	We cheat a little
	 * by using ExecAssignExprContext() to build both.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;
	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * We also need a long-lived memory context for holding hashtable data
	 * structures and transition values.  NOTE: the details of what is stored
	 * in aggcontext and what is stored in the regular per-query memory
	 * context are driven by a simple decision: we want to reset the
	 * aggcontext in ExecReScanAgg to recover no-longer-wanted space.
	 */
	aggstate->aggcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AggContext",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

#define AGG_NSLOTS 3

	/*
	 * tuple table initialization
	 */
	aggstate->ss.ss_ScanTupleSlot = ExecInitExtraTupleSlot(estate);
	ExecInitResultTupleSlot(estate, &aggstate->ss.ps);
	aggstate->hashslot = ExecInitExtraTupleSlot(estate);

	/*
	 * initialize child expressions
	 *
	 * Note: ExecInitExpr finds Aggrefs for us, and also checks that no aggs
	 * contain other agg calls in their arguments.	This would make no sense
	 * under SQL semantics anyway (and it's forbidden by the spec). Because
	 * that is true, we don't need to worry about evaluating the aggs in any
	 * particular order.
	 */
	/*
	 * if there are aggregate functions in the target list, and the argument
	 * of the aggregate functions are expression(such as operators),when we
	 * initialize the expression, the argument should set to be vectorized
	 * and the result of aggregate functions should set to be non-vectorzied.
	 * we initialize the targetlist two times here, the first time, we get
	 * the non-vectorized target list, then we reset the aggregate in
	 * aggstate and get the vectorized aggregate functions.
	 */
	/* get the non-vectorized expressions */
	aggstate->ss.ps.plan->vectorized = false;
	aggstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) aggstate);
	aggstate->ss.ps.plan->vectorized = true;

	/*
	 * reset the aggregate functions,
	 * NOTE: we have to save the aggregate function list,
	 * because the the aggregate functions in the non-vectrozied
	 * target list too
	 */
	nagglist = aggstate->aggs;
	aggstate->aggs = NULL;
	aggstate->numaggs = 0;

	/*
	 * it is the second time to initialize the expression, we don't care
	 * about the result of ExecInitExpr, it is only used to initialize the
	 * vectorized argument of the aggregate functions.
	 */
	ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) aggstate);

	aggstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) aggstate);

    /*
     * CDB: Offer extra info for EXPLAIN ANALYZE.
     */
    if (estate->es_instrument)
    {
	    /* Allocate string buffer. */
        aggstate->ss.ps.cdbexplainbuf = makeStringInfo();

        /* Request a callback at end of query. */
        aggstate->ss.ps.cdbexplainfun = ExecAggExplainEnd;
    }

	/*
	 * initialize child nodes
	 */
	outerPlan = outerPlan(node);
	if (IsA(outerPlan, ExternalScan)) {
		/*
		 * Hack to indicate to PXF when there is an external scan
		 */
		if (list_length(aggstate->aggs) == 1) {
				AggrefExprState *aggrefstate = (AggrefExprState *) linitial(aggstate->aggs);
				Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
				//Only dealing with one agg
				if (aggref->aggfnoid == COUNT_ANY_OID || aggref->aggfnoid == COUNT_STAR_OID) {
					eflags |= EXEC_FLAG_EXTERNAL_AGG_COUNT;
				}
		}
	}
	outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize source tuple type.
	 */
	ExecAssignScanTypeFromOuterPlan(&aggstate->ss);
	TupleDesc tdesc = ((ScanState*)aggstate)->ss_ScanTupleSlot->tts_tupleDescriptor;
	BackportTupleDescriptor((PlanState*)aggstate, tdesc);
	ExecSetSlotDescriptor(((ScanState*)aggstate)->ss_ScanTupleSlot, tdesc);
	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&aggstate->ss.ps);
	TupleDesc ttupDesc = CreateTupleDescCopy(((PlanState*)aggstate)->ps_ResultTupleSlot->tts_tupleDescriptor);

	BackportTupleDescriptor((PlanState*)aggstate, ttupDesc);
	ExecAssignResultType((PlanState*)aggstate, ttupDesc);

	ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

	/*
	 * get the count of aggregates in targetlist and quals
	 */
	numaggs = aggstate->numaggs;
	Assert(numaggs == list_length(aggstate->aggs) + list_length(aggstate->percs));
	if (numaggs <= 0)
	{
		/*
		 * This is not an error condition: we might be using the Agg node just
		 * to do hash-based grouping.  Even in the regular case,
		 * constant-expression simplification could optimize away all of the
		 * Aggrefs in the targetlist and qual.	So keep going, but force local
		 * copy of numaggs positive so that palloc()s below don't choke.
		 */
		numaggs = 1;
	}

	/*
	 * If we are grouping, precompute fmgr lookup data for inner loop. We need
	 * both equality and hashing functions to do it by hashing, but only
	 * equality if not hashing.
	 */
	if (node->numCols > 0)
	{
		TupleDesc desc = ExecGetScanType(&aggstate->ss);
		desc = CreateTupleDescCopy(desc);
		BackportTupleDescriptor((PlanState*)aggstate, desc);

		if (node->aggstrategy == AGG_HASHED)
		{
			execTuplesHashPrepare(desc,
								  node->numCols,
								  node->grpColIdx,
								  &aggstate->eqfunctions,
								  &aggstate->hashfunctions);
		}
		else
			aggstate->eqfunctions =
				execTuplesMatchPrepare(desc,
									   node->numCols,
									   node->grpColIdx);
	}

	/*
	 * Set up aggregate-result storage in the output expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numaggs);
	econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numaggs);

	peragg = (AggStatePerAgg) palloc0(sizeof(AggStatePerAggData) * numaggs);
	aggstate->peragg = peragg;

	if (node->aggstrategy == AGG_HASHED)
	{
		aggstate->hash_needed = get_agg_hash_collist(aggstate);
	}
	else
	{
		AggStatePerGroup pergroup;

		pergroup = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData) * numaggs);
		aggstate->pergroup = pergroup;
	}

	/*
	 * set the NO of aggregate functions, it is used in the non-vectorzied
	 * target list.
	 */
	aggno = -1;
	foreach(l, nagglist)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
		Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
		AggStatePerAgg peraggstate;
		int i;

		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			if (equal(aggref, peragg[i].aggref) &&
				!contain_volatile_functions((Node *) aggref))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			aggrefstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peragg[++aggno].aggref = aggref;

		/* Mark Aggref state node with assigned index in the result array */
		aggrefstate->aggno = aggno;
	}

	/*
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg data.  We also detect duplicate
	 * aggregates (for example, "SELECT sum(x) ... HAVING sum(x) > 0"). When
	 * duplicates are detected, we only make an AggStatePerAgg struct for the
	 * first one.  The clones are simply pointed at the same result entry by
	 * giving them duplicate aggno values.
	 */
	aggno = -1;
	foreach(l, aggstate->aggs)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
		Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
		AggStatePerAgg peraggstate;
		List	   *inputTargets = NIL;
		List	   *inputSortClauses = NIL;
		Oid		   *inputTypes = NULL;
		int			numInputs;
		int			numArguments;
		int			numSortCols;
		List	   *sortlist;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtranstype;
		AclResult	aclresult;
		Oid			transfn_oid = InvalidOid,
					finalfn_oid = InvalidOid;
		Expr	   *transfnexpr,
				   *finalfnexpr,
				   *prelimfnexpr;
		Datum		textInitVal;
		int			i;
		ListCell   *lc;
		cqContext  *pcqCtx;

		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			if (equal(aggref, peragg[i].aggref) &&
				!contain_volatile_functions((Node *) aggref))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			aggrefstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peraggstate = &peragg[++aggno];

		/* Mark Aggref state node with assigned index in the result array */
		aggrefstate->aggno = aggno;

		/* Fill in the peraggstate data */
		peraggstate->aggrefstate = aggrefstate;
		peraggstate->aggref = aggref;
		numArguments = list_length(aggref->args);
		peraggstate->numArguments = numArguments;

		/*
		 * Use these information from ExecInitExpr for per agg info.
		 */
		inputTargets = aggrefstate->inputTargets;
		inputSortClauses = aggrefstate->inputSortClauses;
		numInputs = list_length(inputTargets);
		numSortCols = list_length(inputSortClauses);

		peraggstate->numSortCols = numSortCols;
		peraggstate->numInputs = numInputs;

		/* MPP has some restrictions. */
		Assert(!(aggref->aggdistinct && aggref->aggorder));
		Assert(numArguments == 1 || !aggref->aggdistinct);

		/* Get actual datatypes of the inputs.	These could be different from
		 * the agg's declared input types, when the agg accepts ANY, ANYARRAY
		 * or ANYELEMENT. The result will have argument types at 0 through
		 * numArguments-1 and sort key types mixed in or at numArguments through
		 * numInputs.
		 */
		inputTypes = (Oid*)palloc0(sizeof(Oid)*(numInputs));
		i = 0;
		foreach(lc, inputTargets)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			inputTypes[i++] = exprType((Node*)tle->expr);
		}

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_aggregate "
					" WHERE aggfnoid = :1 ",
					ObjectIdGetDatum(aggref->aggfnoid)));

		aggTuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC,
						   get_func_name(aggref->aggfnoid));

		switch ( aggref->aggstage) /* MPP */
		{
		case AGGSTAGE_NORMAL: /* Single-stage aggregation */
			peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
			peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
			break;

		case AGGSTAGE_PARTIAL:/* Two-stage aggregation -- preliminary stage */
			/* the perliminary stage for two-stage aggregation */
			peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
			peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
			break;

		case AGGSTAGE_INTERMEDIATE:
			peraggstate->transfn_oid = transfn_oid = aggform->aggprelimfn;
			peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
			break;

		case AGGSTAGE_FINAL: /* Two-stage aggregation - final stage */
			peraggstate->transfn_oid = transfn_oid = aggform->aggprelimfn;
			peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
			break;
		}

		peraggstate->prelimfn_oid = aggform->aggprelimfn;

		/* Check that aggregate owner has permission to call component fns */
		{
			Oid			aggOwner;
			int			fetchCount;

			aggOwner = caql_getoid_plus(
					NULL,
					&fetchCount,
					NULL,
					cql("SELECT proowner FROM pg_proc "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(aggref->aggfnoid)));

			if (!fetchCount)
				elog(ERROR, "cache lookup failed for function %u",
					 aggref->aggfnoid);

			aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_PROC,
							   get_func_name(transfn_oid));
			if (OidIsValid(finalfn_oid))
			{
				aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(finalfn_oid));
			}
			if (OidIsValid(peraggstate->prelimfn_oid))
			{
				aclresult = pg_proc_aclcheck(peraggstate->prelimfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(peraggstate->prelimfn_oid));
			}
		}

		/* check if the transition type is polymorphic and if so resolve it */
		aggtranstype = resolve_polymorphic_transtype(aggform->aggtranstype,
													 aggref->aggfnoid,
											   		 inputTypes);

		/* build expression trees using actual argument & result types */
		build_aggregate_fnexprs(inputTypes,
								numArguments,
								aggtranstype,
								aggref->aggtype,
								transfn_oid,
								finalfn_oid,
								InvalidOid /* prelim */,
								InvalidOid /* invtrans */,
								InvalidOid /* invprelim */,
								&transfnexpr,
								&finalfnexpr,
								&prelimfnexpr,
								NULL,
								NULL);

		fmgr_info(transfn_oid, &peraggstate->transfn);
		peraggstate->transfn.fn_expr = (Node *) transfnexpr;

		if (OidIsValid(finalfn_oid))
		{
			fmgr_info(finalfn_oid, &peraggstate->finalfn);
			peraggstate->finalfn.fn_expr = (Node *) finalfnexpr;
		}

		if (OidIsValid(peraggstate->prelimfn_oid))
		{
			fmgr_info(peraggstate->prelimfn_oid, &peraggstate->prelimfn);
			peraggstate->prelimfn.fn_expr = (Node *) prelimfnexpr;
		}

		get_typlenbyval(aggref->aggtype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);
		get_typlenbyval(aggtranstype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * initval is potentially null, so don't try to access it as a struct
		 * field. Must do it the hard way with caql_getattr
		 */
		textInitVal = caql_getattr(pcqCtx,
								   Anum_pg_aggregate_agginitval,
								   &peraggstate->initValueIsNull);

		if (peraggstate->initValueIsNull)
			peraggstate->initValue = (Datum) 0;
		else
			peraggstate->initValue = GetAggInitVal(textInitVal,
												   aggtranstype);

		/*
		 * If the transfn is strict and the initval is NULL, make sure input
		 * type and transtype are the same (or at least binary-compatible), so
		 * that it's OK to use the first input value as the initial
		 * transValue.	This should have been checked at agg definition time,
		 * but just in case...
		 */
		if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull)
		{
			if (numArguments < 1 ||
				!IsBinaryCoercible(inputTypes[0], aggtranstype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("aggregate %u needs to have compatible input type and transition type",
								aggref->aggfnoid)));
		}

		/*
		 * Get a tupledesc corresponding to the inputs (including sort
		 * expressions) of the agg.
		 */
		peraggstate->evaldesc = ExecTypeFromTL(inputTargets, false);

		/* Create slot we're going to do argument evaluation in */
		peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

		/* Set up projection info for evaluation */
		peraggstate->evalproj = ExecBuildProjectionInfo(aggrefstate->args,
														aggstate->tmpcontext,
														peraggstate->evalslot,
														NULL);

		/*
		 * If we're doing either DISTINCT or ORDER BY, then we have a list of
		 * SortGroupClause nodes; fish out the data in them and stick them
		 * into arrays.
		 *
		 * Note that by construction, if there is a DISTINCT clause then the
		 * ORDER BY clause is a prefix of it (see transformDistinctClause).
		 */
		if (aggref->aggdistinct)
		{
			TargetEntry *tle;
			SortClause *sc;
			Oid			eq_function;

			/*
			 * GPDB 4 doesh't implement DISTINCT aggs for aggs having more than
			 * than one argument, nor does it allow an ordered aggregate to
			 * specify distinct, but PG 9 does.  The SQL standard allows the
			 * one-arg-for-DISTINCT restriction, but we really we ought to
			 * implement it the way PG 9 does eventually.
			 *
			 * For now we use the scalar equalfn field of AggStatePerAggData
			 * for DQAs instead of treating DQAs more generally.
			 */
			if (numArguments != 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is supported only for single-argument aggregates")));

			eq_function = equality_oper_funcid(inputTypes[0]);
			fmgr_info(eq_function, &(peraggstate->equalfn));

			tle = (TargetEntry*)linitial(inputTargets);
			tle->ressortgroupref = 1;

			sc = makeNode(SortClause);
			sc->tleSortGroupRef = tle->ressortgroupref;
			sc->sortop = ordering_oper_opid(inputTypes[0]);

			sortlist = list_make1(sc);
			numSortCols = 1;
		}
		else if ( aggref->aggorder )
		{
			sortlist = aggref->aggorder->sortClause;
			numSortCols = list_length(sortlist);
		}
		else
		{
			sortlist = NULL;
			numSortCols = 0;
		}


		peraggstate->numSortCols = numSortCols;

		if (numSortCols > 0)
		{
			/*
			 * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
			 * (yet)
			 */
			Assert(node->aggstrategy != AGG_HASHED);

			/* If we have only one input, we need its len/byval info. */
			if (numInputs == 1)
			{
				get_typlenbyval(inputTypes[0],
								&peraggstate->inputtypeLen,
								&peraggstate->inputtypeByVal);
			}

			/* Extract the sort information for use later */
			peraggstate->sortColIdx =
				(AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
			peraggstate->sortOperators =
				(Oid *) palloc(numSortCols * sizeof(Oid));

			i = 0;
			foreach(lc, sortlist)
			{
				SortClause *sortcl = (SortClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl,
														   inputTargets);

				/* the parser should have made sure of this */
				Assert(OidIsValid(sortcl->sortop));

				peraggstate->sortColIdx[i] = tle->resno;
				peraggstate->sortOperators[i] = sortcl->sortop;
				i++;
			}
			Assert(i == numSortCols);
		}

		if (aggref->aggdistinct)
		{
			Oid eqfunc;

			Assert(numArguments == 1);
			Assert(numSortCols == 1);

			/*
			 * We need the equal function for the DISTINCT comparison we will
			 * make.
			 */
			eqfunc = equality_oper_funcid(inputTypes[0]);
			fmgr_info(eqfunc, &peraggstate->equalfn);
		}

		caql_endscan(pcqCtx);
	}

	/*
	 * Process percentile expressions.  These are treated separately from
	 * Aggref expressions at the moment as we cannot change the catalog, but
	 * this will be incorporated into the existing Agggref architecture
	 * when we can change the catalog.  The operation for percentile functions
	 * is very similar to the Aggref operation except that there is no
	 * function oid for transition function.  We manually manupilate
	 * FmgrInfo without the oid.
	 * In case the Agg handles PercentileExpr, there shouldn't be Aggref
	 * in conjunction with PercentileExpr in the target list (and havingQual),
	 * or vice versa, from the current design of percentile functions.
	 * However, we don't assert anything to keep that assumption, for the
	 * later extensibility.
	 */
	foreach (l, aggstate->percs)
	{
		PercentileExprState *percstate = (PercentileExprState *) lfirst(l);
		PercentileExpr	   *perc = (PercentileExpr *) percstate->xprstate.expr;
		AggStatePerAgg		peraggstate;
		FmgrInfo		   *transfn;
		int					numArguments;
		int					i;
		Oid					trans_argtypes[FUNC_MAX_ARGS];
		ListCell		   *lc;
		Expr			   *dummy_expr;

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			/*
			 * In practice, percentile expression doesn't contain
			 * volatile functions since everything is evaluated and
			 * becomes Var during the preprocess such as ordering operations.
			 * However, adding a check for volatile may be robust and
			 * consistent with Aggref initialization.
			 */
			if (equal(perc, peragg[i].perc) &&
				!contain_volatile_functions((Node *) perc))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			percstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peraggstate = &peragg[++aggno];

		/* Mark Aggref state node with assigned index in the result array */
		percstate->aggno = aggno;

		/* Fill in the peraggstate data */
		peraggstate->percstate = percstate;
		peraggstate->perc = perc;
		/*
		 * numArguments = arg + ORDER BY + pc + tc
		 * See notes on percentile_cont_trans() and ExecInitExpr() for
		 * PercentileExpr.
		 */
		numArguments = list_length(perc->args) + list_length(perc->sortClause) + 2;
		peraggstate->numArguments = numArguments;

		/*
		 * Set up transfn.  In general, we should use fmgr_info, but we don't
		 * have the catalog function (thus no oid for functions) due to the
		 * difficulity of changing the catalog at the moment.  This should
		 * be cleaned when we can change the catalog.
		 */
		transfn = &peraggstate->transfn;
		transfn->fn_nargs = list_length(perc->args) + 1;
		transfn->fn_strict = false;
		transfn->fn_retset = false;
		transfn->fn_mcxt = CurrentMemoryContext;
		transfn->fn_addr = perc->perckind == PERC_DISC ?
			percentile_disc_trans : percentile_cont_trans;
		transfn->fn_oid = InvalidOid;

		/*
		 * trans type is the same as result type, as they don't have final func.
		 */
		trans_argtypes[0] = perc->perctype;
		i = 1;
		/*
		 * Literal arguments.
		 */
		foreach (lc, perc->args)
		{
			Node	   *arg = lfirst(lc);
			trans_argtypes[i++] = exprType(arg);
		}
		/*
		 * ORDER BY arguments.
		 */
		foreach (lc, perc->sortTargets)
		{
			TargetEntry	   *tle = lfirst(lc);
			trans_argtypes[i++] = exprType((Node *) tle->expr);
		}
		/*
		 * Peer count and total count.
		 */
		trans_argtypes[i++] = INT8OID;
		trans_argtypes[i++] = INT8OID;
		/*
		 * Build FuncExpr for the transition function.
		 */
		build_aggregate_fnexprs(trans_argtypes,
								i,
								perc->perctype,
								perc->perctype,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								InvalidOid,
								(Expr **) &transfn->fn_expr,
								&dummy_expr, NULL, NULL, NULL);

		get_typlenbyval(perc->perctype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);
		get_typlenbyval(perc->perctype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * Hard code for the known information.
		 */
		peraggstate->initValueIsNull = true;
		peraggstate->initValue = (Datum) 0;

		peraggstate->finalfn_oid = InvalidOid;
		peraggstate->prelimfn_oid = InvalidOid;

		/*
		 * Get a tupledesc corresponding to the inputs (including sort
		 * expressions) of the agg.
		 */
		peraggstate->evaldesc = ExecTypeFromTL(percstate->tlist, false);

		/* Create slot we're going to do argument evaluation in */
		peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

		/* Set up projection info for evaluation */
		peraggstate->evalproj = ExecBuildProjectionInfo(percstate->args,
														aggstate->tmpcontext,
														peraggstate->evalslot,
														NULL);
	}

	/* Update numaggs to match number of unique aggregates found */
	aggstate->numaggs = aggno + 1;

	/* MPP */
	aggstate->hhashtable = NULL;

	/* ROLLUP */
	aggstate->perpassthru = NULL;

	if (node->inputHasGrouping)
	{
		AggStatePerGroup perpassthru;

		perpassthru = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData) * numaggs);
		aggstate->perpassthru = perpassthru;

	}

	aggstate->num_attrs = 0;

	aggstate->aggType = getAggType(node);

	/* Set the default memory manager */
	aggstate->mem_manager.alloc = cxt_alloc;
	aggstate->mem_manager.free = cxt_free;
	aggstate->mem_manager.manager = aggstate->aggcontext;
	aggstate->mem_manager.realloc_ratio = 1;

	initGpmonPktForAgg((Plan *)node, &aggstate->ss.ps.gpmon_pkt, estate);

	return aggstate;
}

