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

#include "access/hd_work_mgr.h"
#include "catalog/external/externalmd.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"

typedef struct ItemContext
{
	ListCell *current_item;
	ListCell *current_field;
} ItemContext;

static ListCell* pxf_item_fields_enum_start(text *profile, text *pattern);
static ItemContext* pxf_item_fields_enum_next(ItemContext *item_context);
static void pxf_item_fields_enum_end(void);

static ListCell*
pxf_item_fields_enum_start(text *profile, text *pattern)
{
	List *items = NIL;

	char *profile_cstr = text_to_cstring(profile);
	char *pattern_cstr = text_to_cstring(pattern);

	items = get_pxf_item_metadata(profile_cstr, pattern_cstr, InvalidOid);

	if (items == NIL)
		return NULL;

	return list_head(items);
}

static ItemContext*
pxf_item_fields_enum_next(ItemContext *item_context)
{

	/* first time call */
	if (item_context->current_item && !item_context->current_field)
		item_context->current_field = list_head(((PxfItem *) lfirst(item_context->current_item))->fields);

	/* next field for the same item */
	else if (lnext(item_context->current_field))
		item_context->current_field = lnext(item_context->current_field);
	/* next item */
	else if (item_context->current_item && lnext(item_context->current_item))
	{
		item_context->current_item = lnext(item_context->current_item);
		item_context->current_field = list_head(((PxfItem *) lfirst(item_context->current_item))->fields);

	/* no items, no fields left */
	} else
		item_context = NULL;

	return item_context;
}

static void pxf_item_fields_enum_end(void)
{
	/* cleanup */
}

Datum pxf_get_item_fields(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	FuncCallContext *funcctx;
	HeapTuple tuple;
	Datum result;
	Datum values[5];
	bool nulls[5];

	ItemContext *item_context;

	text *profile = PG_GETARG_TEXT_P(0);
	text *pattern = PG_GETARG_TEXT_P(1);

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* initialize item fileds metadata scanning code */

		ListCell *items_cell = pxf_item_fields_enum_start(profile, pattern);

		if (items_cell == NULL)
		{
			pxf_item_fields_enum_end();
			funcctx->user_fctx = NULL;
			SRF_RETURN_DONE(funcctx);
		}

		item_context = (ItemContext *) palloc0(sizeof(ItemContext));
		item_context->current_item = items_cell;
		funcctx->user_fctx = (void *) item_context;

		/*
		 * build tupdesc for result tuples. This must match this function's
		 * pg_proc entry!
		 */
		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "path",
		TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "itemname",
		TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "fieldname",
		TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "fieldtype",
		TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "sourcefieldtype",
		TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	item_context = (ItemContext *) funcctx->user_fctx;

	/* search for next entry to display */
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
	item_context = pxf_item_fields_enum_next(item_context);
	;
	funcctx->user_fctx = item_context;
	MemoryContextSwitchTo(oldcontext);

	if (!item_context)
	{
		pxf_item_fields_enum_end();
		funcctx->user_fctx = NULL;
		SRF_RETURN_DONE(funcctx);
	}

	PxfItem *item = (PxfItem *) lfirst(
			item_context->current_item);
	PxfField *field = (PxfField *) lfirst(
			item_context->current_field);

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum(item->path);
	values[1] = CStringGetTextDatum(item->name);
	values[2] = CStringGetTextDatum(field->name);
	values[3] = CStringGetTextDatum(field->type);
	values[4] = CStringGetTextDatum(field->sourceType);

	tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	SRF_RETURN_NEXT(funcctx, result);

}
