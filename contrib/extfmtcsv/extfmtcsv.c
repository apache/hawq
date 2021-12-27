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

#include <json-c/json.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "c.h"
#include "access/filesplit.h"
#include "utils/builtins.h"
#include "utils/uri.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbfilesystemcredential.h"

#include "storage/cwrapper/hdfs-file-system-c.h"
#include "storage/cwrapper/text-format-c.h"

/* Do the module magic dance */
PG_MODULE_MAGIC
;
PG_FUNCTION_INFO_V1(extfmtcsv_out);
PG_FUNCTION_INFO_V1(extfmtcsv_in);
PG_FUNCTION_INFO_V1(extfmttext_out);
PG_FUNCTION_INFO_V1(extfmttext_in);

Datum extfmtcsv_out(PG_FUNCTION_ARGS);
Datum extfmtcsv_in(PG_FUNCTION_ARGS);
Datum extfmttext_out(PG_FUNCTION_ARGS);
Datum extfmttext_in(PG_FUNCTION_ARGS);

typedef struct FmtUserData
{
	TextFormatC *fmt;
	char **colNames;
	int numberOfColumns;
	char **colRawValues;
	Datum *colValues;
	uint64_t *colValLength;
	bool *colIsNulls;
	bool *colToReads;

	int nSplits;
	TextFormatFileSplit *splits;
} FmtUserData;

char externalFmtType = '\0';
char externalFmtNameIn[64];
char externalFmtNameOut[64];

void setExtFormatterTupleDesc(TextFormatC *fmt, TupleDesc tupdesc);
void buildFormatterOptionsInJson(PG_FUNCTION_ARGS, char **jsonStr);
void beginFormatterForRead(PG_FUNCTION_ARGS);
void beginFormatterForWrite(PG_FUNCTION_ARGS);

Datum extfmtcommon_in(PG_FUNCTION_ARGS);
Datum extfmtcommon_out(PG_FUNCTION_ARGS);

Datum extfmtcommon_in(PG_FUNCTION_ARGS)
{
	HeapTuple tuple; /* The result tuple to return at last */
	TupleDesc tupdesc;
	MemoryContext oldMemCtx = NULL;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_FORMATTER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), errmsg("%s: not called by format manager", externalFmtNameIn)));

	/* Check if this is the first time calling the formatter */
	if (FORMATTER_GET_MASK(fcinfo) == FMT_UNSET)
	{
		FORMATTER_GET_MASK(fcinfo) = FMT_SET;
		externalFmtType = '\0';
		PG_RETURN_VOID() ;
	}

	if (((FormatterData *) (fcinfo->context))->fmt_splits == NULL)
	{
		FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_DONE);
		PG_RETURN_VOID() ;
	}

	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);

	/* Check if the user data was created ever */
	FmtUserData *userData = FORMATTER_GET_USER_CTX(fcinfo);
	if (userData == NULL)
	{
		/* Create user data instance and set in context to keep it */
		userData = palloc0(sizeof(FmtUserData));
		FORMATTER_SET_USER_CTX(fcinfo, userData);
		userData->numberOfColumns = tupdesc->natts;
		userData->colNames = palloc0(
				sizeof(char *) * userData->numberOfColumns);
		userData->colValues = palloc0(
				sizeof(Datum) * userData->numberOfColumns);
		userData->colIsNulls = palloc0(
				sizeof(bool) * userData->numberOfColumns);
		userData->colRawValues = palloc0(
				sizeof(char *) * userData->numberOfColumns);
		userData->colValLength = palloc0(
				sizeof(uint64_t) * userData->numberOfColumns);

		/* Prepare formatter options */
		char *fmtOptions = NULL;
		buildFormatterOptionsInJson(fcinfo, &fmtOptions);

		/* Create formatter instance */
		userData->fmt = TextFormatNewTextFormatC(externalFmtType, fmtOptions);
		/* Begin scanning by passing in split and column setting */
		beginFormatterForRead(fcinfo);

		if (fmtOptions != NULL)
		{
			pfree(fmtOptions);
		}
	}
	bool lastBatchRow = false;
	bool res = TextFormatNextTextFormatC(userData->fmt, userData->colRawValues,
			userData->colValLength, userData->colIsNulls, &lastBatchRow);

	if (res)
	{
		MemoryContext m = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
		MemoryContext oldcontext = MemoryContextSwitchTo(m);

		/* We have one tuple ready */
		for (int i = 0; i < userData->numberOfColumns; ++i)
		{
			if (userData->colIsNulls[i])
			{
				continue;
			}

			/* Prepare the tuple to return. */
			if (!((FormatterData *) (fcinfo->context))->fmt_needs_transcoding)
			{
				if (!lastBatchRow)
				{
					char *val = (char *) (userData->colRawValues[i]);
					char oldc = *(val + userData->colValLength[i]);
					*(val + userData->colValLength[i]) = '\0';
					userData->colValues[i] = InputFunctionCall(
							&(FORMATTER_GET_CONVERSION_FUNCS(fcinfo)[i]), val,
							FORMATTER_GET_TYPIOPARAMS(fcinfo)[i],
							tupdesc->attrs[i]->atttypmod);
					*(val + userData->colValLength[i]) = oldc;
				}
				else
				{
					char *val = (char *) palloc(userData->colValLength[i] + 1);
					memcpy(val, userData->colRawValues[i],
							userData->colValLength[i]);
					val[userData->colValLength[i]] = '\0';
					userData->colValues[i] = InputFunctionCall(
							&(FORMATTER_GET_CONVERSION_FUNCS(fcinfo)[i]), val,
							FORMATTER_GET_TYPIOPARAMS(fcinfo)[i],
							tupdesc->attrs[i]->atttypmod);
				}
			}
			else
			{
				char *cvt = NULL;
				if (!lastBatchRow)
				{
					char *val = (char *) (userData->colRawValues[i]);
					char oldc = *(val + userData->colValLength[i]);
					*(val + userData->colValLength[i]) = '\0';
					FORMATTER_ENCODE_STRING(fcinfo, val, userData->colValLength[i],
							cvt, true); /* is import */
					Assert(cvt != NULL);
					userData->colValues[i] = InputFunctionCall(
							&(FORMATTER_GET_CONVERSION_FUNCS(fcinfo)[i]), cvt,
							FORMATTER_GET_TYPIOPARAMS(fcinfo)[i],
							tupdesc->attrs[i]->atttypmod);
					*(val + userData->colValLength[i]) = oldc;
				}
				else
				{
					char *val = (char *) palloc(userData->colValLength[i] + 1);
					memcpy(val, userData->colRawValues[i],
							userData->colValLength[i]);
					val[userData->colValLength[i]] = '\0';
					FORMATTER_ENCODE_STRING(fcinfo, val, userData->colValLength[i],
							cvt, true); /* is import */
					Assert(cvt != NULL);
					userData->colValues[i] = InputFunctionCall(
							&(FORMATTER_GET_CONVERSION_FUNCS(fcinfo)[i]), cvt,
							FORMATTER_GET_TYPIOPARAMS(fcinfo)[i],
							tupdesc->attrs[i]->atttypmod);
				}
			}
		}
		MemoryContextSwitchTo(oldcontext);

		TextFormatCompleteNextTextFormatC(userData->fmt);
		tuple = heap_form_tuple(tupdesc, userData->colValues,
				userData->colIsNulls);
		FORMATTER_SET_TUPLE(fcinfo, tuple);
		FORMATTER_RETURN_TUPLE(tuple);
	}
	else
	{
		externalFmtType = '\0';
		TextFormatCompleteNextTextFormatC(userData->fmt);
		/* If there is no error caught, it should be an end of reading split */
		TextFormatCatchedError *err = TextFormatGetErrorTextFormatC(
				userData->fmt);
		if (err->errCode == ERRCODE_SUCCESSFUL_COMPLETION)
		{
			TextFormatEndTextFormatC(userData->fmt);
			err = TextFormatGetErrorTextFormatC(userData->fmt);
			if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
			{
				elog(ERROR, "%s: failed to get next tuple. %s (%d)",
				externalFmtNameIn,
				err->errMessage, err->errCode);
			}
			TextFormatFreeTextFormatC(&(userData->fmt));
			pfree(userData->colIsNulls);
			pfree(userData->colRawValues);
			pfree(userData->colValues);
			pfree(userData->colToReads);
			pfree(userData->colValLength);
			if (userData->splits != NULL)
			{
				for (int i = 0; i < userData->nSplits; ++i)
				{
					pfree(userData->splits[i].fileName);
				}
				pfree(userData->splits);
			}
			for (int i = 0; i < userData->numberOfColumns; ++i)
			{
				pfree(userData->colNames[i]);
			}
			pfree(userData->colNames);
			pfree(userData);
			FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_DONE);
		}
		else
		{
			elog(ERROR, "%s: failed to get next tuple. %s (%d)",
			externalFmtNameIn,
			err->errMessage, err->errCode);
		}
	}
	PG_RETURN_VOID() ;
}

/*
 * extfmtcsv_in. each time this function is called, it builds one tuple from
 * the input data buffer.
 */
Datum extfmtcsv_in(PG_FUNCTION_ARGS)
{
	if (externalFmtType == '\0')
	{
		externalFmtType = TextFormatTypeCSV;
		strcpy(externalFmtNameIn, "csv_in");
		strcpy(externalFmtNameOut, "csv_out");
	}
	return extfmtcommon_in(fcinfo);
}

Datum extfmttext_in(PG_FUNCTION_ARGS)
{
	if (externalFmtType == '\0')
	{
		externalFmtType = TextFormatTypeTXT;
		strcpy(externalFmtNameIn, "text_in");
		strcpy(externalFmtNameOut, "text_out");
	}
	return extfmtcommon_in(fcinfo);
}

Datum extfmtcommon_out(PG_FUNCTION_ARGS)
{
	static char DUMMY[1] = "";
	TupleDesc tupdesc = NULL;
	HeapTupleData tuple;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_FORMATTER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), errmsg("%s: not called by format manager", externalFmtNameOut)));

	/* Check if this is the first time calling the formatter */
	if (FORMATTER_GET_MASK(fcinfo) == FMT_UNSET)
	{
		FORMATTER_GET_MASK(fcinfo) = FMT_SET;
		externalFmtType = '\0';
		PG_RETURN_VOID() ;
	}

	/* Get tuple desc */
	tupdesc = FORMATTER_GET_TUPDESC(fcinfo);

	/* Get our internal description of the formatter */

	FmtUserData *userData = FORMATTER_GET_USER_CTX(fcinfo);
	if (userData == NULL)
	{
		userData = palloc0(sizeof(FmtUserData));
		FORMATTER_SET_USER_CTX(fcinfo, userData);
		userData->numberOfColumns = tupdesc->natts;
		userData->colValues = palloc0(
				sizeof(Datum) * userData->numberOfColumns);
		userData->colIsNulls = palloc0(
				sizeof(bool) * userData->numberOfColumns);
		userData->colRawValues = palloc0(
				sizeof(char *) * userData->numberOfColumns);
		userData->colNames = palloc0(
				sizeof(char *) * userData->numberOfColumns);
		/* Prepare formatter options */
		char *fmtOptions = NULL;
		buildFormatterOptionsInJson(fcinfo, &fmtOptions);

		/* Create formatter instance */
		userData->fmt = TextFormatNewTextFormatC(externalFmtType, fmtOptions);

		if (fmtOptions != NULL)
		{
			pfree(fmtOptions);
		}
		/* Begin scanning by passing in split and column setting */
		beginFormatterForWrite(fcinfo);
	}

	if (FORMATTER_GET_MASK(fcinfo) & FMT_WRITE_END)
	{
		externalFmtType = '\0';
		TextFormatEndInsertTextFormatC(userData->fmt);
		TextFormatCatchedError *err = TextFormatGetErrorTextFormatC(
				userData->fmt);
		if (err->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
		{
			elog(ERROR, "%s: failed to insert: %s(%d)",
			externalFmtNameOut,
			err->errMessage, err->errCode);
		}

		TextFormatFreeTextFormatC(&(userData->fmt));
		pfree(userData->colIsNulls);
		pfree(userData->colRawValues);
		pfree(userData->colValues);
		for (int i = 0; i < userData->numberOfColumns; ++i)
		{
			pfree(userData->colNames[i]);
		}
		pfree(userData->colNames);
		pfree(userData);
		PG_RETURN_VOID() ;
	}

	/* break the input tuple into fields */
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_data = rec;
	heap_deform_tuple(&tuple, tupdesc, userData->colValues,
			userData->colIsNulls);

	MemoryContext m = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
	MemoryContext oldcontext = MemoryContextSwitchTo(m);
	/* convert to string */
	for (int i = 0; i < userData->numberOfColumns; ++i)
	{
		userData->colRawValues[i] = DUMMY;
		if (userData->colIsNulls[i])
		{
			continue;
		}
		/* workaround for preserving float precision */
		int bak_extra_float_digits = extra_float_digits;
		extra_float_digits = 3;
		userData->colRawValues[i] = OutputFunctionCall(
				&(FORMATTER_GET_CONVERSION_FUNCS(fcinfo)[i]),
				userData->colValues[i]);
		extra_float_digits = bak_extra_float_digits;
		if (((FormatterData *) (fcinfo->context))->fmt_needs_transcoding)
		{
			char *cvt = NULL;
			FORMATTER_ENCODE_STRING(fcinfo,
					(char * )(userData->colRawValues[i]),
					strlen(userData->colRawValues[i]), cvt, false); /* is export */
			userData->colRawValues[i] = cvt;
		}
	}

	/* pass to formatter to output */
	TextFormatInsertTextFormatC(userData->fmt, userData->colRawValues,
			userData->colIsNulls);
	TextFormatCatchedError *e = TextFormatGetErrorTextFormatC(userData->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		elog(ERROR, "%s: failed to insert: %s(%d)",
		externalFmtNameOut,
		e->errMessage, e->errCode);
	}

	MemoryContextSwitchTo(oldcontext);
	PG_RETURN_VOID() ;
}

Datum extfmtcsv_out(PG_FUNCTION_ARGS)
{
	if (externalFmtType == '\0')
	{
		externalFmtType = TextFormatTypeCSV;
		strcpy(externalFmtNameIn, "csv_in");
		strcpy(externalFmtNameOut, "csv_out");
	}
	return extfmtcommon_out(fcinfo);
}

Datum extfmttext_out(PG_FUNCTION_ARGS)
{
	if (externalFmtType == '\0')
	{
		externalFmtType = TextFormatTypeTXT;
		strcpy(externalFmtNameIn, "text_in");
		strcpy(externalFmtNameOut, "text_out");
	}
	return extfmtcommon_out(fcinfo);
}

void buildFormatterOptionsInJson(PG_FUNCTION_ARGS, char **jsonStr)
{
	struct json_object *optJsonObject = json_object_new_object();
	/* add those predefined */
	char *keyStr = NULL;
	char *valStr = NULL;
	int nArgs = FORMATTER_GET_NUM_ARGS(fcinfo);
	for (int i = 1; i <= nArgs; ++i)
	{
		keyStr = FORMATTER_GET_NTH_ARG_KEY(fcinfo, i);
		valStr = FORMATTER_GET_NTH_ARG_VAL(fcinfo, i);
		/* convert the delimiter and null to external table encoding */
		valStr = pg_do_encoding_conversion(valStr, strlen(valStr),
				GetDatabaseEncoding(),
				((FormatterData*) fcinfo->context)->fmt_external_encoding);

		if (strcmp(keyStr, "reject_limit") == 0)
		{
			json_object_object_add(optJsonObject, "reject_limit",
					json_object_new_int(atoi(valStr)));
		}
		else if (strcmp(keyStr, "force_notnull") == 0
				|| strcmp(keyStr, "force_quote") == 0)
		{
			/* ext formatter accepts commar splitted column names instead of dot */
			int l = strlen(valStr);
			for (int i = 0; i < l; ++i)
			{
				if (valStr[i] == '.')
					valStr[i] = ',';
				json_object_object_add(optJsonObject, keyStr,
						json_object_new_string(valStr));
			}
		}
		else
		{
			json_object_object_add(optJsonObject, keyStr,
					json_object_new_string(valStr));
		}
	}

	/* add default settings for this formatter */
        int encoding = ((FormatterData *)fcinfo->context)->fmt_external_encoding;
        buildDefaultFormatterOptionsInJson(encoding, externalFmtType, optJsonObject);
	*jsonStr = NULL;
	if (optJsonObject != NULL)
	{
		const char *str = json_object_to_json_string(optJsonObject);
		*jsonStr = (char *) palloc0(strlen(str) + 1);
		strcpy(*jsonStr, str);
		json_object_put(optJsonObject);
		// jsonStr is already in table encoding, elog needs db encoding
		// elog(LOG, "formatter options are %s", *jsonStr);
	}
}

void beginFormatterForRead(PG_FUNCTION_ARGS)
{
	FmtUserData *userData = FORMATTER_GET_USER_CTX(fcinfo);
	FormatterData *fmtData = (FormatterData *) (fcinfo->context);

	/* parse URL to get server location etc. */
	Uri *uri = ParseExternalTableUri(fmtData->fmt_url);
	userData->nSplits = list_length(fmtData->fmt_splits);
	userData->splits = palloc0(sizeof(TextFormatFileSplit) * userData->nSplits);
	ListCell *cell = NULL;
	int i = 0;
	foreach(cell, fmtData->fmt_splits)
	{
		FileSplit origFS = (FileSplit) lfirst(cell);
		userData->splits[i].len = origFS->lengths;
		userData->splits[i].start = origFS->offsets;

		/* build file path containing host address */
		int fileNameLen = 7 +   // "hdfs://"
		                  (uri->hostname == NULL ? 0 : strlen(uri->hostname)) +
		                  1 +   // ':'
		                  5 +   // "65535"
		                  (origFS->ext_file_uri_string == NULL ? 0 : strlen(origFS->ext_file_uri_string)) +
		                  1;    // '\0'

		userData->splits[i].fileName = palloc(fileNameLen * sizeof(char));
		sprintf(userData->splits[i].fileName, "hdfs://%s:%d%s",
		        uri->hostname == NULL ? "" : uri->hostname, uri->port,
		        origFS->ext_file_uri_string == NULL ? "" : origFS->ext_file_uri_string);
		i++;
	}

	if (enable_secure_filesystem && Gp_role == GP_ROLE_EXECUTE)
	{
		char *token = find_filesystem_credential_with_uri(fmtData->fmt_url);
		SetToken(fmtData->fmt_url, token);
	}

	FreeExternalTableUri(uri);

	userData->colToReads = palloc0(sizeof(bool) * userData->numberOfColumns);
	for (int i = 0; i < userData->numberOfColumns; ++i)
	{
		userData->colToReads[i] = true;
		/* 64 is the name type length */
		userData->colNames[i] = palloc(sizeof(char) * 64);
		strcpy(userData->colNames[i],
				fmtData->fmt_relation->rd_att->attrs[i]->attname.data);
	}

	TextFormatBeginTextFormatC(userData->fmt, userData->splits,
			userData->nSplits, userData->colToReads, userData->colNames,
			userData->numberOfColumns);
	TextFormatCatchedError *e = TextFormatGetErrorTextFormatC(userData->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		elog(ERROR, "%s: failed to begin scan: %s(%d)",
		externalFmtNameIn,
		e->errMessage, e->errCode);
	}
}

void beginFormatterForWrite(PG_FUNCTION_ARGS)
{
	FmtUserData *userData = FORMATTER_GET_USER_CTX(fcinfo);
	FormatterData *fmtData = (FormatterData *) (fcinfo->context);

	/* prepare column names */
	for (int i = 0; i < userData->numberOfColumns; ++i)
	{
		/* 64 is the name type length */
		userData->colNames[i] = palloc(sizeof(char) * 64);
		strcpy(userData->colNames[i],
				fmtData->fmt_relation->rd_att->attrs[i]->attname.data);
	}

	if (enable_secure_filesystem && Gp_role == GP_ROLE_EXECUTE)
	{
		char *token = find_filesystem_credential_with_uri(fmtData->fmt_url);
		SetToken(fmtData->fmt_url, token);
	}

	TextFormatBeginInsertTextFormatC(userData->fmt, fmtData->fmt_url,
			userData->colNames, userData->numberOfColumns);

	TextFormatCatchedError *e = TextFormatGetErrorTextFormatC(userData->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		elog(ERROR, "%s: failed to begin insert: %s(%d)",
		externalFmtNameOut,
		e->errMessage, e->errCode);
	}
}
