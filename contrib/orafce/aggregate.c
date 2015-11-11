#include "postgres.h"
#include "funcapi.h"
#include "orafunc.h"

#include "utils/builtins.h"

#include "builtins.h"
#include "lib/stringinfo.h"

PG_FUNCTION_INFO_V1(orafce_listagg1_transfn);
PG_FUNCTION_INFO_V1(orafce_listagg2_transfn);
PG_FUNCTION_INFO_V1(orafce_listagg_finalfn);

PG_FUNCTION_INFO_V1(orafce_median4_transfn);
PG_FUNCTION_INFO_V1(orafce_median4_finalfn);
PG_FUNCTION_INFO_V1(orafce_median8_transfn);
PG_FUNCTION_INFO_V1(orafce_median8_finalfn);

typedef struct
{
	int	alen;		/* allocated length */
	int	nextlen;	/* next allocated length */
	int	nelems;		/* number of valid entries */
	union 
	{
		float4	*float4_values;
		float8  *float8_values;
	} d;
} MedianState;

int orafce_float4_cmp(const void *a, const void *b);
int orafce_float8_cmp(const void *a, const void *b);

#if PG_VERSION_NUM >= 80400 && PG_VERSION_NUM < 90000
static int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		if (aggcontext)
			*aggcontext = ((AggState *) fcinfo->context)->aggcontext;
		return 1;
	}
	else if (fcinfo->context && IsA(fcinfo->context, WindowAggState))
	{
		if (aggcontext)
			*aggcontext = ((WindowAggState *) fcinfo->context)->wincontext;
		return 2;
	}

	/* this is just to prevent "uninitialized variable" warnings */
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}
#endif

/****************************************************************
 * listagg
 *  
 * Concates values and returns string.
 *
 * Syntax:
 *     FUNCTION listagg(string varchar, delimiter varchar = '')
 *      RETURNS varchar;
 *
 * Note: any NULL value is ignored.
 *
 ****************************************************************/
#if PG_VERSION_NUM >= 80400
/* subroutine to initialize state */
static StringInfo
makeStringAggState(FunctionCallInfo fcinfo)
{
	StringInfo	state;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "listagg_transfn called in non-aggregate context");
	}

	/*
	 * Create state in aggregate context.  It'll stay there across subsequent
	 * calls.
	 */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	state = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	return state;
}

static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
}
#endif

Datum
orafce_listagg1_transfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 80400
	StringInfo	state;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the element unless null. */
	if (!PG_ARGISNULL(1))
	{
		if (state == NULL)
			state = makeStringAggState(fcinfo);
		appendStringInfoText(state, PG_GETARG_TEXT_PP(1));		/* value */
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
#else
	if (!PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
		{
			/* 
			 * Just return the input. No need to copy, our caller is responsible
			 * for this.
			 */
			PG_RETURN_DATUM(PG_GETARG_DATUM(1));
		}
		else
			return DirectFunctionCall2(textcat, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
	}

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
#endif
}

Datum 
orafce_listagg2_transfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 90000
	return string_agg_transfn(fcinfo);
#elif PG_VERSION_NUM >= 80400
	StringInfo	state;

	state = PG_ARGISNULL(0) ? NULL : (StringInfo) PG_GETARG_POINTER(0);

	/* Append the value unless null. */
	if (!PG_ARGISNULL(1))
	{
		/* On the first time through, we ignore the delimiter. */
		if (state == NULL)
			state = makeStringAggState(fcinfo);
		else if (!PG_ARGISNULL(2))
			appendStringInfoText(state, PG_GETARG_TEXT_PP(2));	/* delimiter */

		appendStringInfoText(state, PG_GETARG_TEXT_PP(1));		/* value */
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
#else
	
	/* ignore if null */
	if (!PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			return PointerGetDatum(DatumGetTextPCopy(PG_GETARG_DATUM(1)));
		else
		{
			/* delimiter is NULL, so a normal append */
			if (PG_ARGISNULL(2))
				return DirectFunctionCall2(textcat, PG_GETARG_DATUM(0),
													PG_GETARG_DATUM(1));
			else
			{
				/* 
				 * Convoluted, yes, but we might be operating on large amounts
				 * of memory here. So, be careful to free the intermediate
				 * memory.
				 */
				Datum d1 = DirectFunctionCall2(textcat, PG_GETARG_DATUM(2),
													   PG_GETARG_DATUM(1));
				Pointer p = DatumGetPointer(d1);
				Datum d2;
		
				d2 = DirectFunctionCall2(textcat, PG_GETARG_DATUM(0), d1);
				pfree(p);
				PG_RETURN_DATUM(d2);
			}
		}
	}

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
#endif
}

#if PG_VERSION_NUM >= 80400

static MedianState *
accumFloat4(MedianState *mstate, float4 value, MemoryContext aggcontext)
{
	MemoryContext oldcontext;

	if (mstate == NULL)
	{
		/* First call - initialize */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		mstate = palloc(sizeof(MedianState));
		mstate->alen = 1024;
		mstate->nextlen = 2 * 1024;
		mstate->nelems = 0;
		mstate->d.float4_values = palloc(mstate->alen * sizeof(float4));
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* enlarge float4_values if needed */
		if (mstate->nelems >= mstate->alen)
		{
			int	newlen = mstate->nextlen;
			
			oldcontext = MemoryContextSwitchTo(aggcontext);
			mstate->nextlen += mstate->alen;
			mstate->alen = newlen;
			mstate->d.float4_values = repalloc(mstate->d.float4_values, 
									    mstate->alen * sizeof(float4));
			MemoryContextSwitchTo(oldcontext); 
		}
	}	
	
	mstate->d.float4_values[mstate->nelems++] = value;
	
	return mstate;    
}

static MedianState *
accumFloat8(MedianState *mstate, float8 value, MemoryContext aggcontext)
{
	MemoryContext oldcontext;

	if (mstate == NULL)
	{
		/* First call - initialize */
		oldcontext = MemoryContextSwitchTo(aggcontext);
		mstate = palloc(sizeof(MedianState));
		mstate->alen = 1024;
		mstate->nextlen = 2 * 1024;
		mstate->nelems = 0;
		mstate->d.float8_values = palloc(mstate->alen * sizeof(float8));
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* enlarge float4_values if needed */
		if (mstate->nelems >= mstate->alen)
		{
			int	newlen = mstate->nextlen;
			
			oldcontext = MemoryContextSwitchTo(aggcontext);
			mstate->nextlen += mstate->alen;
			mstate->alen = newlen;
			mstate->d.float8_values = repalloc(mstate->d.float8_values, 
									    mstate->alen * sizeof(float8));
			MemoryContextSwitchTo(oldcontext); 
		}
	}	

	mstate->d.float8_values[mstate->nelems++] = value;
	
	return mstate;    
}

#endif

Datum
orafce_median4_transfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 80400

	MemoryContext	aggcontext;
	MedianState *state = NULL;
	float4 elem;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "median4_transfn called in non-aggregate context");
	}
	
	state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);
	if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(state);
	
	elem = PG_GETARG_FLOAT4(1);
	state = accumFloat4(state, elem, aggcontext);
	
	PG_RETURN_POINTER(state);
	
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not suppported"),
			 errdetail("This functions is blocked on PostgreSQL 8.3 and older (from security reasons).")));
		
	PG_RETURN_NULL();

#endif
}

int 
orafce_float4_cmp(const void *a, const void *b)
{
	return *((float4 *) a) - *((float4*) b);
}

Datum
orafce_median4_finalfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 80400
	MedianState *state = NULL;
	int	lidx;
	int	hidx;
	float4 result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
		
	state = (MedianState *) PG_GETARG_POINTER(0);
	qsort(state->d.float4_values, state->nelems, sizeof(float4), orafce_float4_cmp);

	lidx = state->nelems / 2 + 1 - 1;
	hidx = (state->nelems + 1) / 2 - 1;
	
	if (lidx == hidx)
		result = state->d.float4_values[lidx];
	else
		result = (state->d.float4_values[lidx] + state->d.float4_values[hidx]) / 2.0;

	PG_RETURN_FLOAT4(result);

#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not suppported"),
			 errdetail("This functions is blocked on PostgreSQL 8.3 and older (from security reasons).")));
		
	PG_RETURN_NULL();
#endif
}

Datum
orafce_median8_transfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 80400

	MemoryContext	aggcontext;
	MedianState *state = NULL;
	float8 elem;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "median4_transfn called in non-aggregate context");
	}
	
	state = PG_ARGISNULL(0) ? NULL : (MedianState *) PG_GETARG_POINTER(0);
	if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(state);
		
	elem = PG_GETARG_FLOAT8(1);
	state = accumFloat8(state, elem, aggcontext);
	
	PG_RETURN_POINTER(state);

#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not suppported"),
			 errdetail("This functions is blocked on PostgreSQL 8.3 and older (from security reasons).")));
		
	PG_RETURN_NULL();
#endif
}

int 
orafce_float8_cmp(const void *a, const void *b)
{
	return *((float8 *) a) - *((float8*) b);
}


Datum
orafce_median8_finalfn(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 80400
	MedianState *state = NULL;
	int	lidx;
	int	hidx;
	float8 result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (MedianState *) PG_GETARG_POINTER(0);
	qsort(state->d.float8_values, state->nelems, sizeof(float8), orafce_float8_cmp);

	lidx = state->nelems / 2 + 1 - 1;
	hidx = (state->nelems + 1) / 2 - 1;
	
	if (lidx == hidx)
		result = state->d.float8_values[lidx];
	else
		result = (state->d.float8_values[lidx] + state->d.float8_values[hidx]) / 2.0;

	PG_RETURN_FLOAT8(result);
		
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not suppported"),
			 errdetail("This functions is blocked on PostgreSQL 8.3 and older (from security reasons).")));

	PG_RETURN_NULL();		
#endif
}
