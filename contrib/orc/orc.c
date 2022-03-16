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

#include "c.h"
#include "port.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "utils/hawq_type_mapping.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/uri.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "mb/pg_wchar.h"
#include "commands/defrem.h"
#include "commands/copy.h"
#include "access/tupdesc.h"
#include "access/filesplit.h"
#include "access/fileam.h"
#include "access/plugstorage.h"
#include "cdb/cdbvars.h"
#include "catalog/pg_exttable.h"
#include "catalog/namespace.h"
#include "postmaster/identity.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "parser/parse_type.h"
#include "utils/uri.h"
#include "utils/numeric.h"
#include "cdb/cdbfilesystemcredential.h"
#include "optimizer/newPlanner.h"

#include "storage/cwrapper/orc-format-c.h"
#include "storage/cwrapper/hdfs-file-system-c.h"
#include "cdb/cdbvars.h"

#define ORC_TIMESTAMP_EPOCH_JDATE	2457024 /* == date2j(2015, 1, 1) */
#define MAX_ORC_ARRAY_DIMS        10000
#define ORC_NUMERIC_MAX_PRECISION 38

/* Do the module magic dance */
PG_MODULE_MAGIC
;

/* Validators for pluggable storage format ORC */
PG_FUNCTION_INFO_V1(orc_validate_interfaces);
PG_FUNCTION_INFO_V1(orc_validate_options);
PG_FUNCTION_INFO_V1(orc_validate_encodings);
PG_FUNCTION_INFO_V1(orc_validate_datatypes);

/* Accessors for pluggable storage format ORC */
PG_FUNCTION_INFO_V1(orc_beginscan);
PG_FUNCTION_INFO_V1(orc_getnext_init);
PG_FUNCTION_INFO_V1(orc_getnext);
PG_FUNCTION_INFO_V1(orc_rescan);
PG_FUNCTION_INFO_V1(orc_endscan);
PG_FUNCTION_INFO_V1(orc_stopscan);
PG_FUNCTION_INFO_V1(orc_insert_init);
PG_FUNCTION_INFO_V1(orc_insert);
PG_FUNCTION_INFO_V1(orc_insert_finish);

/* Definitions of validators for pluggable storage format ORC */
Datum orc_validate_interfaces(PG_FUNCTION_ARGS);
Datum orc_validate_options(PG_FUNCTION_ARGS);
Datum orc_validate_encodings(PG_FUNCTION_ARGS);
Datum orc_validate_datatypes(PG_FUNCTION_ARGS);

/* Definitions of accessors for pluggable storage format ORC */
Datum orc_beginscan(PG_FUNCTION_ARGS);
Datum orc_getnext_init(PG_FUNCTION_ARGS);
Datum orc_getnext(PG_FUNCTION_ARGS);
Datum orc_rescan(PG_FUNCTION_ARGS);
Datum orc_endscan(PG_FUNCTION_ARGS);
Datum orc_stopscan(PG_FUNCTION_ARGS);
Datum orc_insert_init(PG_FUNCTION_ARGS);
Datum orc_insert(PG_FUNCTION_ARGS);
Datum orc_insert_finish(PG_FUNCTION_ARGS);

typedef struct
{
	int64_t second;
	int64_t nanosecond;
} TimestampType;

typedef struct ORCFormatUserData
{
	ORCFormatC *fmt;
	char **colNames;
	int *colDatatypes;
	int64_t *colDatatypeMods;
	int32_t numberOfColumns;
	char **colRawValues;
	Datum *colValues;
	uint64_t *colValLength;
	bits8 **colValNullBitmap;
	int **colValDims;
	char **colAddresses;
	bool *colToReads;

	CommonPlanContext ctx;

	int nSplits;
	ORCFormatFileSplit *splits;

	// for write only
	TimestampType *colTimestamp;
} ORCFormatUserData;

static FmgrInfo *get_orc_function(char *formatter_name, char *function_name);
static void get_scan_functions(FileScanDesc file_scan_desc);
static void get_insert_functions(ExternalInsertDesc ext_insert_desc);
static void init_format_user_data_for_read(TupleDesc tup_desc,
		ORCFormatUserData *user_data);
static void init_format_user_data_for_write(TupleDesc tup_desc,
		ORCFormatUserData *user_data);
static void build_options_in_json(List *fmt_opts_defelem, int encoding,
		char **json_str, TupleDesc tupDesc);
static ORCFormatC *create_formatter_instance(List *fmt_opts_defelem,
		int encoding, int segno, TupleDesc tupDesc, bool forWrite);
static void build_file_splits(Uri *uri, ScanState *scan_state,
		ORCFormatUserData *user_data);
static void build_tuple_descrition_for_read(Plan *plan, Relation relation,
		ORCFormatUserData *user_data);
static void build_tuple_descrition_for_write(Relation relation,
		ORCFormatUserData *user_data);
static void orc_scan_error_callback(void *arg);
static void orc_parse_format_string(CopyState pstate, char *fmtstr);
static char *orc_strtokx2(const char *s, const char *whitespace,
		const char *delim, const char *quote, char escape, bool e_strings,
		bool del_quotes, int encoding);
static void orc_strip_quotes(char *source, char quote, char escape,
		int encoding);

/* Implementation of validators for pluggable storage format ORC */

/*
 * void
 * orc_validate_interfaces(char *formatName)
 */
Datum orc_validate_interfaces(PG_FUNCTION_ARGS)
{
	PlugStorageValidator psv_interface =
			(PlugStorageValidator) (fcinfo->context);

	if (pg_strncasecmp(psv_interface->format_name, "orc", strlen("orc")) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("ORC: incorrect format name \'%s\'", psv_interface->format_name)));
	}

	PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_options(List *formatOptions,
 *                      char *formatStr,
 *                      bool isWritable)
 */
Datum orc_validate_options(PG_FUNCTION_ARGS)
{
	PlugStorageValidator psv = (PlugStorageValidator) (fcinfo->context);

	List *format_opts = psv->format_opts;
	char *format_str = psv->format_str;
	bool is_writable = psv->is_writable;
	TupleDesc tup_desc = psv->tuple_desc;

	char *formatter = NULL;
	char *compresstype = NULL;
	char *bloomfilter = NULL;
	char *dicthreshold = NULL;
	char *bucketnum = NULL;
	char *category = NULL;

	ListCell *opt;

	const int maxlen = 8 * 1024 - 1;
	int len = 0;

	foreach(opt, format_opts)
	{
		DefElem *defel = (DefElem *) lfirst(opt);
		char *key = defel->defname;
		bool need_free_value = false;
		char *val = (char *) defGetString(defel, &need_free_value);

		/* check formatter */
		if (strncasecmp(key, "formatter", strlen("formatter")) == 0)
		{
			char *formatter_values[] =
			{ "orc" };
			checkPlugStorageFormatOption(&formatter, key, val,
			true, 1, formatter_values);
		}

		/* check option for orc format */
		if (strncasecmp(key, "compresstype", strlen("compresstype")) == 0)
		{
			char *compresstype_values[] =
			{ "none", "snappy", "lz4" };
			checkPlugStorageFormatOption(&compresstype, key, val, is_writable,
					3, compresstype_values);
		}

		if (strncasecmp(key, "bloomfilter", strlen("bloomfilter")) == 0)
		{
		  int attnum = tup_desc->natts;
		  char **attribute_names = palloc0(attnum * sizeof(char*));
		  for (int i = 0; i < attnum; ++i) {
		    int name_len = strlen(((Form_pg_attribute) (tup_desc->attrs[i]))->attname.data);
		    char *attribute = palloc0(name_len + 1);
		    strncpy(attribute, ((Form_pg_attribute) (tup_desc->attrs[i]))->attname.data, name_len);
		    attribute_names[i] = attribute;
		  }
		  char *dup_val = pstrdup(val);
		  char *token = strtok(dup_val, ",");
		  while (token) {
		    checkPlugStorageFormatOption(&bloomfilter, key, token, true, attnum, attribute_names);
		    bloomfilter = NULL;
		    token = strtok(NULL, ",");
		  }
		}

		if (strncasecmp(key, "dicthreshold", strlen("dicthreshold")) == 0)
		{
			checkPlugStorageFormatOption(&dicthreshold, key, val,
			true, 0, NULL);
			char *end;
			double threshold = strtod(val, &end);
			if (end == val || *end != '\0' || threshold < 0 || threshold > 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("dicthreshold \"%s\" must be within [0-1]", val), errOmitLocation(true)));
			}
		}

		if (strncasecmp(key, "bucketnum", strlen("bucketnum")) == 0)
		{
			checkPlugStorageFormatOption(&bucketnum, key, val,
			true, 0, NULL);
			char *end;
			long bucketnumber = strtol(val, &end, 10);
			if (end == val || *end != '\0' || bucketnumber <= 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("bucketnum \"%s\" must be > 0", val), errOmitLocation(true)));
			}
		}

		/* check category orc format */
		if (strncasecmp(key, "category", strlen("category")) == 0)
		{
			char *category_values[] =
			{ "internal", "external" };
			checkPlugStorageFormatOption(&category, key, val,
			true, 2, category_values);
		}

		if (strncasecmp(key, "formatter", strlen("formatter"))
				&& strncasecmp(key, "compresstype", strlen("compresstype"))
				&& strncasecmp(key, "bloomfilter", strlen("bloomfilter"))
				&& strncasecmp(key, "dicthreshold", strlen("dicthreshold"))
				&& strncasecmp(key, "bucketnum", strlen("bucketnum"))
				&& strncasecmp(key, "category", strlen("category")))
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("Option \"%s\" for ORC table is invalid", key), errOmitLocation(true)));
		}

		sprintf((char * ) format_str + len, "%s '%s' ", key, val);
		len += strlen(key) + strlen(val) + 4;

		if (need_free_value)
		{
			pfree(val);
			val = NULL;
		}

		AssertImply(need_free_value, NULL == val);

		if (len > maxlen)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("format options must be less than %d bytes in size", maxlen), errOmitLocation(true)));
		}
	}

	if (!formatter)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("no formatter function specified"), errOmitLocation(true)));
	}

	PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_encodings(char *encodingName)
 */
Datum orc_validate_encodings(PG_FUNCTION_ARGS)
{
	PlugStorageValidator psv = (PlugStorageValidator) (fcinfo->context);
	char *encoding_name = psv->encoding_name;

	if (strncasecmp(encoding_name, "utf8", strlen("utf8")))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("\"%s\" is not a valid encoding for ORC external table. Encoding for ORC external table must be UTF8.", encoding_name), errOmitLocation(true)));
	}

	PG_RETURN_VOID() ;
}

/*
 * void
 * orc_validate_datatypes(TupleDesc tupDesc)
 */
Datum orc_validate_datatypes(PG_FUNCTION_ARGS) {
  PlugStorageValidator psv = (PlugStorageValidator)(fcinfo->context);
  TupleDesc tup_desc = psv->tuple_desc;

  for (int i = 0; i < tup_desc->natts; ++i) {
    int32_t datatype =
        (int32_t)(((Form_pg_attribute)(tup_desc->attrs[i]))->atttypid);
    int4 typmod = ((Form_pg_attribute)(tup_desc->attrs[i]))->atttypmod;

    if (checkORCUnsupportedDataType(datatype)) {
      ereport(ERROR,
              (errcode(ERRCODE_SYNTAX_ERROR),
               errmsg("unsupported data types %s for columns of external ORC "
                      "table is specified.",
                      TypeNameToString(makeTypeNameFromOid(datatype, -1))),
               errOmitLocation(true)));
    }
    if (HAWQ_TYPE_NUMERIC == datatype) {
      int4 tmp_typmod = typmod - VARHDRSZ;
      int precision = (tmp_typmod >> 16) & 0xffff;
      int scale = tmp_typmod & 0xffff;

      if (typmod == -1 && strcasecmp(orc_enable_no_limit_numeric, "ON") == 0) continue;  // for numeric without precision and scale.

      if (precision < 1 || 38 < precision)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("ORC DECIMAL precision must be between 1 and 38")));
      if (scale == 0)
        ereport(NOTICE, (errmsg("Using a scale of zero for ORC DECIMAL")));
    }
    if (HAEQ_TYPE_UDT(datatype))
      ereport(ERROR,
              (errcode(ERRCODE_SYNTAX_ERROR),
               errmsg("unsupported data types %s for columns of external ORC "
                      "table is specified.",
                      TypeNameToString(makeTypeNameFromOid(datatype, -1))),
               errOmitLocation(true)));
  }

  PG_RETURN_VOID();
}

/*
 * FileScanDesc
 * orc_beginscan(ExternalScan *extScan,
 *               ScanState *scanState,
 *               Relation relation,
 *               int formatterType,
 *               char *formatterName)
 */
Datum orc_beginscan(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	ExternalScan *ext_scan = ps->ps_ext_scan;
	ScanState *scan_state = ps->ps_scan_state;
	Relation relation = ps->ps_relation;
	int formatterType = ps->ps_formatter_type;
	char *formatterName = ps->ps_formatter_name;

	Index scan_rel_id = ext_scan->scan.scanrelid;
	uint32 scan_counter = ext_scan->scancounter;
	List *uri_list = ext_scan->uriList;
	List *fmt_opts = ext_scan->fmtOpts;
	int fmt_encoding = ext_scan->encoding;
	List *scan_quals = ext_scan->scan.plan.qual;

	/* 1. Increment relation reference count while scanning relation */
	/*
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/* 2. Allocate and initialize the select descriptor */
	FileScanDesc file_scan_desc = palloc(sizeof(FileScanDescData));
	file_scan_desc->fs_inited = false;
	file_scan_desc->fs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&file_scan_desc->fs_ctup.t_self);
	file_scan_desc->fs_cbuf = InvalidBuffer;
	file_scan_desc->fs_rd = relation;
	file_scan_desc->fs_scanrelid = scan_rel_id;
	file_scan_desc->fs_scancounter = scan_counter;
	file_scan_desc->fs_scanquals = scan_quals;
	file_scan_desc->fs_noop = false;
	file_scan_desc->fs_file = NULL;
	file_scan_desc->fs_formatter = NULL;
	file_scan_desc->fs_formatter_type = formatterType;
	file_scan_desc->fs_formatter_name = formatterName;

	/* 2.1 Setup scan functions */
	get_scan_functions(file_scan_desc);

	/* 2.2 Get URI for the scan */
	/*
	 * get the external URI assigned to us.
	 *
	 * The URI assigned for this segment is normally in the uriList list
	 * at the index of this segment id. However, if we are executing on
	 * MASTER ONLY the (one and only) entry which is destined for the master
	 * will be at the first entry of the uriList list.
	 */
	char *uri_str = NULL;
	int segindex = GetQEIndex();

	Value *v = NULL;

	v = (Value *) list_nth(uri_list, 0);

	if (v->type == T_Null)
		uri_str = NULL;
	else
		uri_str = (char *) strVal(v);

	/*
	 * If a uri is assigned to us - get a reference to it. Some executors
	 * don't have a uri to scan (if # of uri's < # of primary segdbs).
	 * in which case uri will be NULL. If that's the case for this
	 * segdb set to no-op.
	 */
	if (uri_str)
	{
    if (IS_HIVE_URI(uri_str)) {
      uri_str = pstrdup(ps->ps_hive_url);
    }
		/* set external source (uri) */
		file_scan_desc->fs_uri = uri_str;
	  ereport(DEBUG3, (errmsg_internal("fs_uri (%d) is set as %s", segindex, uri_str)));
		/* NOTE: we delay actually opening the data source until external_getnext() */
	}
	else
	{
		/* segdb has no work to do. set to no-op */
		file_scan_desc->fs_noop = true;
		file_scan_desc->fs_uri = NULL;
	}

	/* 2.3 Allocate values and nulls structure */
	TupleDesc tup_desc = RelationGetDescr(relation);
	file_scan_desc->fs_tupDesc = tup_desc;
	file_scan_desc->attr = tup_desc->attrs;
	file_scan_desc->num_phys_attrs = tup_desc->natts;

	file_scan_desc->values = (Datum *) palloc(
			file_scan_desc->num_phys_attrs * sizeof(Datum));
	file_scan_desc->nulls = (bool *) palloc(
			file_scan_desc->num_phys_attrs * sizeof(bool));

	/* 2.5 Allocate and initialize the structure which track data parsing state */
	file_scan_desc->fs_pstate = (CopyStateData *) palloc0(
			sizeof(CopyStateData));

	/* 2.5.1 Initialize basic information */
	CopyState pstate = file_scan_desc->fs_pstate;
	pstate->fe_eof = false;
	pstate->eol_type = EOL_UNKNOWN;
	pstate->eol_str = NULL;
	pstate->cur_relname = RelationGetRelationName(relation);
	pstate->cur_lineno = 0;
	pstate->err_loc_type = ROWNUM_ORIGINAL;
	pstate->cur_attname = NULL;
	pstate->raw_buf_done = true; /* true so we will read data in first run */
	pstate->line_done = true;
	pstate->bytesread = 0;
	pstate->custom = false;
	pstate->header_line = false;
	pstate->fill_missing = false;
	pstate->line_buf_converted = false;
	pstate->raw_buf_index = 0;
	pstate->processed = 0;
	pstate->filename = uri_str;
	pstate->copy_dest = COPY_EXTERNAL_SOURCE;
	pstate->missing_bytes = 0;
	pstate->csv_mode = false;
	pstate->custom = true;
	pstate->custom_formatter_func = NULL;
	pstate->custom_formatter_name = NULL;
	pstate->rel = relation;

	/* 2.5.2 Setup encoding information */
	/*
	 * Set up encoding conversion info.  Even if the client and server
	 * encodings are the same, we must apply pg_client_to_server() to validate
	 * data in multibyte encodings.
	 *
	 * Each external table specifies the encoding of its external data. We will
	 * therefore set a client encoding and client-to-server conversion procedure
	 * in here (server-to-client in WET) and these will be used in the data
	 * conversion routines (in copy.c CopyReadLineXXX(), etc).
	 */
	Insist(PG_VALID_ENCODING(fmt_encoding));
	pstate->client_encoding = fmt_encoding;
	Oid conversion_proc = FindDefaultConversionProc(fmt_encoding,
			GetDatabaseEncoding());

	if (OidIsValid(conversion_proc))
	{
		/* conversion proc found */
		pstate->enc_conversion_proc = palloc(sizeof(FmgrInfo));
		fmgr_info(conversion_proc, pstate->enc_conversion_proc);
	}
	else
	{
		/* no conversion function (both encodings are probably the same) */
		pstate->enc_conversion_proc = NULL;
	}

	pstate->need_transcoding = pstate->client_encoding != GetDatabaseEncoding();
	pstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(
			pstate->client_encoding);

	/* 2.5.3 Parse the data format options */
	char *format_str = pstrdup((char *) strVal(linitial(fmt_opts)));

	orc_parse_format_string(pstate, format_str);

	/* 2.5.4 Generate or convert list of attributes to process */
	pstate->attr_offsets = (int *) palloc(tup_desc->natts * sizeof(int));
	pstate->attnumlist = CopyGetAttnums(tup_desc, relation, NIL);

	/* 2.5.5 Convert FORCE NOT NULL name list to per-column flags, check validity */
	pstate->force_notnull_flags = (bool *) palloc0(
			tup_desc->natts * sizeof(bool));
	if (pstate->force_notnull)
	{
		List *attnums;
		ListCell *cur;

		attnums = CopyGetAttnums(tup_desc, relation, pstate->force_notnull);

		foreach(cur, attnums)
		{
			int attnum = lfirst_int(cur);
			pstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* 2.5.6 Take care of state that is RET specific */
	initStringInfo(&pstate->attribute_buf);
	initStringInfo(&pstate->line_buf);

	/* Set up data buffer to hold a chunk of data */
	MemSet(pstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	pstate->raw_buf[RAW_BUF_SIZE] = '\0';

	/* 2.5.7 Create temporary memory context for per row process */
	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	pstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
			"ExtTableMemCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* 2.6 Setup formatter information */
	file_scan_desc->fs_formatter = (FormatterData *) palloc0(
			sizeof(FormatterData));
	initStringInfo(&file_scan_desc->fs_formatter->fmt_databuf);
	file_scan_desc->fs_formatter->fmt_perrow_ctx =
			file_scan_desc->fs_pstate->rowcontext;
	file_scan_desc->fs_formatter->fmt_user_ctx = NULL;

	/* 2.7 Set up callback to identify error line number */
	file_scan_desc->errcontext.callback = orc_scan_error_callback;
	file_scan_desc->errcontext.arg = (void *) file_scan_desc->fs_pstate;
	file_scan_desc->errcontext.previous = error_context_stack;

	/* 3. Setup user data */
	/* 3.1 Initialize user data */
	ORCFormatUserData *user_data = palloc0(sizeof(ORCFormatUserData));
	init_format_user_data_for_read(tup_desc, user_data);

	/* 3.2 Create formatter instance */
	List *fmt_opts_defelem = pstate->custom_formatter_params;
	user_data->fmt = create_formatter_instance(fmt_opts_defelem, fmt_encoding,
			ps->ps_segno, tup_desc, false);

	/* 3.3 Build file splits */
	Uri *uri = ParseExternalTableUri(uri_str);
	if (enable_secure_filesystem)
	{
		char *token = find_filesystem_credential_with_uri(uri_str);
		SetToken(uri_str, token);
	}
	build_file_splits(uri, scan_state, user_data);

	FreeExternalTableUri(uri);

	/* 3.4 Build tuple description */
	Plan *plan = &(ext_scan->scan.plan);
	file_scan_desc->fs_ps_plan = plan;
	build_tuple_descrition_for_read(plan, relation, user_data);

	/* 3.5 Save user data */
	file_scan_desc->fs_ps_user_data = (void *) user_data;

	void *qualList = NULL;
	user_data->ctx.univplan = NULL;
  if (strcasecmp(orc_enable_filter_pushdown, "ON") == 0 &&
      list_length(plan->qual) > 0)
    qualList = convert_orcscan_qual_to_common_plan(plan, &user_data->ctx);

  /* 4. Begin scan with the formatter */
  ORCFormatBeginORCFormatC(
      user_data->fmt, user_data->splits, user_data->nSplits,
      user_data->colToReads, user_data->colNames, user_data->colDatatypes,
      user_data->colDatatypeMods, user_data->numberOfColumns, qualList);

        ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
	}

	/* 5. Save file_scan_desc */
	ps->ps_file_scan_desc = file_scan_desc;

	PG_RETURN_POINTER(file_scan_desc);
}

/*
 * ExternalSelectDesc
 * orc_getnext_init(PlanState *planState,
 *                  ExternalScanState *extScanState)
 */
Datum orc_getnext_init(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);

	ExternalSelectDesc ext_select_desc = NULL;
	/*
	 ExternalSelectDesc ext_select_desc = (ExternalSelectDesc)palloc0(
	 sizeof(ExternalSelectDescData));

	 Plan *rootPlan = NULL;

	 if (plan_state != NULL)
	 {
	 ext_select_desc->projInfo = plan_state->ps_ProjInfo;

	 // If we have an agg type then our parent is an Agg node
	 rootPlan = plan_state->state->es_plannedstmt->planTree;
	 if (IsA(rootPlan, Agg) && ext_scan_state->parent_agg_type)
	 {
	 ext_select_desc->agg_type = ext_scan_state->parent_agg_type;
	 }
	 }
	 */

	ps->ps_ext_select_desc = ext_select_desc;

	PG_RETURN_POINTER(ext_select_desc);
}

Datum orc_getnext(PG_FUNCTION_ARGS) {
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  FileScanDesc fsd = ps->ps_file_scan_desc;
  ORCFormatUserData *user_data = (ORCFormatUserData *)(fsd->fs_ps_user_data);
  TupleTableSlot *slot = ps->ps_tuple_table_slot;
  bool *nulls = slot_get_isnull(slot);
  memset(nulls, true, user_data->numberOfColumns);

  bool res = ORCFormatNextORCFormatC(user_data->fmt, user_data->colRawValues,
                                     user_data->colValLength, nulls);
  if (res) {
    for (int32_t i = 0; i < user_data->numberOfColumns; ++i) {
      // Column not to read or column is null
      if (nulls[i]) continue;

      switch (fsd->attr[i]->atttypid) {
        case HAWQ_TYPE_BOOL: {
          user_data->colValues[i] =
              BoolGetDatum(*(bool *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT2: {
          user_data->colValues[i] =
              Int16GetDatum(*(int16_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT4: {
          user_data->colValues[i] =
              Int32GetDatum(*(int32_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_INT8:
        case HAWQ_TYPE_TIME:
        case HAWQ_TYPE_TIMESTAMP:
        case HAWQ_TYPE_TIMESTAMPTZ: {
          user_data->colValues[i] =
              Int64GetDatum(*(int64_t *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT4: {
          user_data->colValues[i] =
              Float4GetDatum(*(float *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_FLOAT8: {
          user_data->colValues[i] =
              Float8GetDatum(*(double *)(user_data->colRawValues[i]));
          break;
        }
        case HAWQ_TYPE_VARCHAR:
        case HAWQ_TYPE_TEXT:
        case HAWQ_TYPE_BPCHAR:
        case HAWQ_TYPE_BYTE:
        case HAWQ_TYPE_NUMERIC: {
          SET_VARSIZE((struct varlena *)(user_data->colRawValues[i]),
                      user_data->colValLength[i]);
          user_data->colValues[i] = PointerGetDatum(user_data->colRawValues[i]);
          break;
        }
        case HAWQ_TYPE_DATE: {
          user_data->colValues[i] =
              Int32GetDatum(*(int32_t *)(user_data->colRawValues[i]) -
                            POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
          break;
        }
        default: {
          ereport(ERROR, (errmsg_internal("ORC:%d", fsd->attr[i]->atttypid)));

          break;
        }
      }
    }

    ps->ps_has_tuple = true;
    slot->PRIVATE_tts_values = user_data->colValues;
    TupSetVirtualTupleNValid(slot, user_data->numberOfColumns);
    PG_RETURN_BOOL(true);
  }

  ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
  if (e->errCode == ERRCODE_SUCCESSFUL_COMPLETION) {
    ORCFormatEndORCFormatC(user_data->fmt);
    e = ORCFormatGetErrorORCFormatC(user_data->fmt);
    if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
      ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
    }

    ORCFormatFreeORCFormatC(&(user_data->fmt));
    if (user_data->ctx.univplan)
      planner_destroy_common_plan_context(&user_data->ctx, false);

    pfree(user_data->colRawValues);
    pfree(user_data->colValues);
    pfree(user_data->colToReads);
    pfree(user_data->colValLength);
    if (user_data->splits != NULL) {
      for (int i = 0; i < user_data->nSplits; ++i) {
        pfree(user_data->splits[i].fileName);
      }
      pfree(user_data->splits);
    }
    for (int i = 0; i < user_data->numberOfColumns; ++i) {
      pfree(user_data->colNames[i]);
    }

    pfree(user_data->colNames);
    pfree(user_data->colDatatypes);
    pfree(user_data);
    fsd->fs_ps_user_data = NULL;

    ps->ps_has_tuple = false;
    slot->PRIVATE_tts_values = NULL;
    ExecClearTuple(slot);
  } else {
    ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
  }
  PG_RETURN_BOOL(false);
}

/*
 * void
 * orc_rescan(FileScanDesc scan)
 */
Datum orc_rescan(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	FileScanDesc fsd = ps->ps_file_scan_desc;
	ScanState *scan_state = ps->ps_scan_state;
	Relation relation = fsd->fs_rd;
	TupleDesc tup_desc = RelationGetDescr(relation);

	ORCFormatUserData *user_data = (ORCFormatUserData *) (fsd->fs_ps_user_data);

	if (user_data == NULL)
	{
		/* 1 Initialize user data */
		user_data = palloc0(sizeof(ORCFormatUserData));
		init_format_user_data_for_read(fsd->fs_tupDesc, user_data);

		/* 2 Create formatter instance */
		List *fmt_opts_defelem = fsd->fs_pstate->custom_formatter_params;
		int fmt_encoding = fsd->fs_pstate->client_encoding;
		user_data->fmt = create_formatter_instance(fmt_opts_defelem,
				fmt_encoding, ps->ps_segno, tup_desc, false);

		/* 3 Build file splits */
		Uri *uri = ParseExternalTableUri(fsd->fs_uri);
		build_file_splits(uri, scan_state, user_data);

		/* 4 Build tuple description */
		Plan *plan = fsd->fs_ps_plan;
		build_tuple_descrition_for_read(plan, fsd->fs_rd, user_data);

		/* 5 Save user data */
		fsd->fs_ps_user_data = (void *) user_data;

	  if (enable_secure_filesystem)
	  {
	    char *token = find_filesystem_credential_with_uri(fsd->fs_uri);
	    SetToken(fsd->fs_uri, token);
	  }
		FreeExternalTableUri(uri);

		void *qualList = NULL;
		user_data->ctx.univplan = NULL;
		if (strcasecmp(orc_enable_filter_pushdown, "ON") == 0 &&
		      list_length(plan->qual) > 0)
		  qualList = convert_orcscan_qual_to_common_plan(plan, &user_data->ctx);

		/* 6 Begin scan with the formatter */
		ORCFormatBeginORCFormatC(user_data->fmt, user_data->splits,
				user_data->nSplits, user_data->colToReads, user_data->colNames,
				user_data->colDatatypes, user_data->colDatatypeMods,
				user_data->numberOfColumns, qualList);

		ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
		if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
		{
			ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
		}
	} else {
		ORCFormatRescanORCFormatC(user_data->fmt);
		ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
		if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
		{
			ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
		}
	}

	/* reset some parse state variables */
	fsd->fs_pstate->fe_eof = false;
	fsd->fs_pstate->cur_lineno = 0;
	fsd->fs_pstate->cur_attname = NULL;
	fsd->fs_pstate->raw_buf_done = true; /* true so we will read data in first run */
	fsd->fs_pstate->line_done = true;
	fsd->fs_pstate->bytesread = 0;

	PG_RETURN_VOID() ;
}

/*
 * void
 * orc_endscan(FileScanDesc scan)
 */
Datum orc_endscan(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	FileScanDesc fsd = ps->ps_file_scan_desc;

	/* Clean up scan descriptor */
	char *relname = pstrdup(RelationGetRelationName(fsd->fs_rd));

	if (fsd->fs_pstate != NULL)
	{
		/*
		 * decrement relation reference count and free scan descriptor storage
		 */
		RelationDecrementReferenceCount(fsd->fs_rd);
	}

	if (fsd->values)
	{
		pfree(fsd->values);
		fsd->values = NULL;
	}
	if (fsd->nulls)
	{
		pfree(fsd->nulls);
		fsd->nulls = NULL;
	}

	if (fsd->fs_pstate != NULL && fsd->fs_pstate->rowcontext != NULL)
	{
		/*
		 * delete the row context
		 */
		MemoryContextDelete(fsd->fs_pstate->rowcontext);
		fsd->fs_pstate->rowcontext = NULL;
	}

	if (fsd->fs_formatter)
	{
		/* TODO: check if this space is automatically freed.
		 * if not, then see what about freeing the user context */
		if (fsd->fs_formatter->fmt_databuf.data)
			pfree(fsd->fs_formatter->fmt_databuf.data);
		pfree(fsd->fs_formatter);
		fsd->fs_formatter = NULL;
	}

	/*
	 * free formatter information
	 */
	if (fsd->fs_formatter_name)
	{
		pfree(fsd->fs_formatter_name);
		fsd->fs_formatter_name = NULL;
	}

	/*
	 * free parse state memory
	 */
	if (fsd->fs_pstate != NULL)
	{
		if (fsd->fs_pstate->attribute_buf.data)
			pfree(fsd->fs_pstate->attribute_buf.data);
		if (fsd->fs_pstate->line_buf.data)
			pfree(fsd->fs_pstate->line_buf.data);
		if (fsd->fs_pstate->attr_offsets)
			pfree(fsd->fs_pstate->attr_offsets);
		if (fsd->fs_pstate->force_quote_flags)
			pfree(fsd->fs_pstate->force_quote_flags);
		if (fsd->fs_pstate->force_notnull_flags)
			pfree(fsd->fs_pstate->force_notnull_flags);

		pfree(fsd->fs_pstate);
		fsd->fs_pstate = NULL;
	}

	/* clean up error context */
	error_context_stack = fsd->errcontext.previous;

	pfree(relname);

	PG_RETURN_VOID() ;
}

/*
 * void
 * orc_stopscan(FileScanDesc scan)
 */
Datum orc_stopscan(PG_FUNCTION_ARGS)
{
  PlugStorage ps = (PlugStorage)(fcinfo->context);
  FileScanDesc fsd = ps->ps_file_scan_desc;
  ORCFormatUserData *user_data = (ORCFormatUserData *)(fsd->fs_ps_user_data);
	TupleTableSlot *tts = ps->ps_tuple_table_slot;

	if (!user_data) PG_RETURN_VOID();

  /* If there is no error caught, it should be an end of reading split */
  ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
  if (e->errCode == ERRCODE_SUCCESSFUL_COMPLETION) {
    ORCFormatEndORCFormatC(user_data->fmt);
    e = ORCFormatGetErrorORCFormatC(user_data->fmt);
    if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION) {
      ereport(LOG, (errcode(e->errCode), errmsg("%s", e->errMessage)));
    }

    ORCFormatFreeORCFormatC(&(user_data->fmt));
    if (user_data->ctx.univplan)
      planner_destroy_common_plan_context(&user_data->ctx, false);

    pfree(user_data->colRawValues);
    pfree(user_data->colValues);
    if (user_data->colToReads) {
      pfree(user_data->colToReads);
      user_data->colToReads = NULL;
    }
    pfree(user_data->colValLength);
    if (user_data->splits != NULL) {
      for (int i = 0; i < user_data->nSplits; ++i) {
        pfree(user_data->splits[i].fileName);
      }
      pfree(user_data->splits);
    }
    for (int i = 0; i < user_data->numberOfColumns; ++i) {
      pfree(user_data->colNames[i]);
    }

    pfree(user_data->colNames);
    pfree(user_data->colDatatypes);
    pfree(user_data);
    fsd->fs_ps_user_data = NULL;

    /* form empty tuple */
    ps->ps_has_tuple = false;

    tts->PRIVATE_tts_values = NULL;
    tts->PRIVATE_tts_isnull = NULL;
    ExecClearTuple(tts);
  } else {
    ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
  }

  PG_RETURN_VOID();
}

/*
 * ExternalInsertDesc
 * orc_insert_init(Relation relation,
 *                 int formatterType,
 *                 char *formatterName)
 */
Datum orc_insert_init(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	Relation relation = ps->ps_relation;
	int formatterType = ps->ps_formatter_type;
	char *formatterName = ps->ps_formatter_name;

	/* 1. Allocate and initialize the insert descriptor */
	ExternalInsertDesc eid = palloc(sizeof(ExternalInsertDescData));
	eid->ext_formatter_type = formatterType;
	eid->ext_formatter_name = formatterName;

	/* 1.1 Setup insert functions */
	get_insert_functions(eid);

	/* 1.2 Initialize basic information */
	eid->ext_rel = relation;
	eid->ext_noop = (Gp_role == GP_ROLE_DISPATCH);
	eid->ext_formatter_data = NULL;

	/* 1.3 Get URI string */
	ExtTableEntry *ete = GetExtTableEntry(RelationGetRelid(relation));
	Value *v;
	char *uri_str = NULL;
	int segindex = GetQEIndex();
	int num_segs = GetQEGangNum();
	int num_urls = list_length(ete->locations);
	int my_url = segindex % num_urls;

	if (num_urls > num_segs)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("External table has more URLs then available primary " "segments that can write into them")));

	v = list_nth(ete->locations, my_url);

  /* when it is hive protocol, transfer the url*/
  if (IS_HIVE_URI(v->val.str)) {
    uri_str = pstrdup(ps->ps_hive_url);
  }
	else
	{
	  uri_str = pstrdup(v->val.str);
	}
  eid->ext_uri = uri_str;

	/* 1.4 Allocate and initialize structure which track data parsing state */
	eid->ext_pstate = (CopyStateData *) palloc0(sizeof(CopyStateData));
	eid->ext_tupDesc = RelationGetDescr(relation);
	eid->ext_values = (Datum *) palloc(eid->ext_tupDesc->natts * sizeof(Datum));
	eid->ext_nulls = (bool *) palloc(eid->ext_tupDesc->natts * sizeof(bool));

	/* 1.5 Get format options */
	List *fmt_opts = NIL;
	fmt_opts = lappend(fmt_opts, makeString(pstrdup(ete->fmtopts)));

	/* 1.6 Initialize parse state */
	/* 1.6.1 Initialize basic information for pstate */
	CopyState pstate = eid->ext_pstate;
	pstate->fe_eof = false;
	pstate->eol_type = EOL_UNKNOWN;
	pstate->eol_str = NULL;
	pstate->cur_relname = RelationGetRelationName(relation);
	pstate->cur_lineno = 0;
	pstate->err_loc_type = ROWNUM_ORIGINAL;
	pstate->cur_attname = NULL;
	pstate->raw_buf_done = true; /* true so we will read data in first run */
	pstate->line_done = true;
	pstate->bytesread = 0;
	pstate->custom = false;
	pstate->header_line = false;
	pstate->fill_missing = false;
	pstate->line_buf_converted = false;
	pstate->raw_buf_index = 0;
	pstate->processed = 0;
	pstate->filename = uri_str;
	pstate->copy_dest = COPY_EXTERNAL_SOURCE;
	pstate->missing_bytes = 0;
	pstate->rel = relation;

	/* 1.6.2 Setup encoding information */
	/*
	 * Set up encoding conversion info.  Even if the client and server
	 * encodings are the same, we must apply pg_client_to_server() to validate
	 * data in multibyte encodings.
	 *
	 * Each external table specifies the encoding of its external data. We will
	 * therefore set a client encoding and client-to-server conversion procedure
	 * in here (server-to-client in WET) and these will be used in the data
	 * conversion routines (in copy.c CopyReadLineXXX(), etc).
	 */
	int fmt_encoding = ete->encoding;
	Insist(PG_VALID_ENCODING(fmt_encoding));
	pstate->client_encoding = fmt_encoding;
	Oid conversion_proc = FindDefaultConversionProc(GetDatabaseEncoding(),
			fmt_encoding);

	if (OidIsValid(conversion_proc))
	{
		/* conversion proc found */
		pstate->enc_conversion_proc = palloc(sizeof(FmgrInfo));
		fmgr_info(conversion_proc, pstate->enc_conversion_proc);
	}
	else
	{
		/* no conversion function (both encodings are probably the same) */
		pstate->enc_conversion_proc = NULL;
	}

	pstate->need_transcoding = pstate->client_encoding != GetDatabaseEncoding();
	pstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(
			pstate->client_encoding);

	/* 1.6.3 Parse format options */
	char *format_str = pstrdup((char *) strVal(linitial(fmt_opts)));
	orc_parse_format_string(pstate, format_str);

	/* 1.6.4 Setup tuple description */
	TupleDesc tup_desc = eid->ext_tupDesc;
	pstate->attr_offsets = (int *) palloc(tup_desc->natts * sizeof(int));

	/* 1.6.5 Generate or convert list of attributes to process */
	pstate->attnumlist = CopyGetAttnums(tup_desc, relation, NIL);

	/* 1.6.6 Convert FORCE NOT NULL name list to per-column flags, check validity */
	pstate->force_notnull_flags = (bool *) palloc0(
			tup_desc->natts * sizeof(bool));
	if (pstate->force_notnull)
	{
		List *attnums;
		ListCell *cur;

		attnums = CopyGetAttnums(tup_desc, relation, pstate->force_notnull);

		foreach(cur, attnums)
		{
			int attnum = lfirst_int(cur);
			pstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* 1.6.7 Take care of state that is WET specific */
	Form_pg_attribute *attr = tup_desc->attrs;
	ListCell *cur;

	pstate->null_print_client = pstate->null_print; /* default */
	pstate->fe_msgbuf = makeStringInfo(); /* use fe_msgbuf as a per-row buffer */
	pstate->out_functions = (FmgrInfo *) palloc(
			tup_desc->natts * sizeof(FmgrInfo));

	foreach(cur, pstate->attnumlist)
	/* Get info about the columns need to process */
	{
		int attnum = lfirst_int(cur);
		Oid out_func_oid;
		bool isvarlena;

		getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid,
				&isvarlena);
		fmgr_info(out_func_oid, &pstate->out_functions[attnum - 1]);
	}

	/*
	 * We need to convert null_print to client encoding, because it
	 * will be sent directly with CopySendString.
	 */
	if (pstate->need_transcoding)
	{
		pstate->null_print_client = pg_server_to_custom(pstate->null_print,
				pstate->null_print_len, pstate->client_encoding,
				pstate->enc_conversion_proc);
	}

	/* 1.6.8 Create temporary memory context for per row process */
	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype input or output routines, and should be faster than retail
	 * pfree's anyway.
	 */
	pstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
			"ExtTableMemCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* 1.7 Initialize formatter data */
	eid->ext_formatter_data = (FormatterData *) palloc0(sizeof(FormatterData));
	eid->ext_formatter_data->fmt_perrow_ctx = eid->ext_pstate->rowcontext;

	/* 2. Setup user data */

	/* 2.1 Initialize user data */
	ORCFormatUserData *user_data = (ORCFormatUserData *) palloc0(
			sizeof(ORCFormatUserData));
	init_format_user_data_for_write(tup_desc, user_data);

	/* 2.2 Create formatter instance */
	List *fmt_opts_defelem = pstate->custom_formatter_params;
	user_data->fmt = create_formatter_instance(fmt_opts_defelem, fmt_encoding,
			ps->ps_segno, tup_desc, true);

	/* 2.4 Build tuple description */
	build_tuple_descrition_for_write(relation, user_data);

	/* 2.5 Save user data */
	eid->ext_ps_user_data = (void *) user_data;

  if (enable_secure_filesystem && Gp_role == GP_ROLE_EXECUTE)
  {
    char *token = find_filesystem_credential_with_uri(uri_str);
    SetToken(uri_str, token);
  }
	/* 3. Begin insert with the formatter */
	ORCFormatBeginInsertORCFormatC(user_data->fmt, uri_str,
			user_data->colNames, user_data->colDatatypes,
			user_data->colDatatypeMods, user_data->numberOfColumns);

	ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
	}

	/* 4. Save the result */
	ps->ps_ext_insert_desc = eid;

	PG_RETURN_POINTER(eid);
}

/*
 * Oid
 * orc_insert(ExternalInsertDesc extInsertDesc,
 *            TupleTableSlot *tupTableSlot)
 */
Datum orc_insert(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	ExternalInsertDesc eid = ps->ps_ext_insert_desc;
	TupleTableSlot *tts = ps->ps_tuple_table_slot;

	ORCFormatUserData *user_data = (ORCFormatUserData *) (eid->ext_ps_user_data);

	user_data->colValues = slot_get_values(tts);
	bool *nulls = slot_get_isnull(tts);

	static bool DUMMY_BOOL = true;
	static int8_t DUMMY_INT8 = 0;
	static int16_t DUMMY_INT16 = 0;
	static int32_t DUMMY_INT32 = 0;
	static int64_t DUMMY_INT64 = 0;
	static float DUMMY_FLOAT = 0.0;
	static double DUMMY_DOUBLE = 0.0;
	static char DUMMY_TEXT[1] = "";
	static int32_t DUMMY_DATE = 0;
	static int64_t DUMMY_TIME = 0;
	static TimestampType DUMMY_TIMESTAMP;
	DUMMY_TIMESTAMP.second = 0;
	DUMMY_TIMESTAMP.nanosecond = 0;
	static int16_t DUMMY_INT16_ARRAY[1] =
	{ 0.0 };
	static int32_t DUMMY_INT32_ARRAY[1] =
	{ 0.0 };
	static int64_t DUMMY_INT64_ARRAY[1] =
	{ 0.0 };
	static float DUMMY_FLOAT_ARRAY[1] =
	{ 0.0 };
	static double DUMMY_DOUBLE_ARRAY[1] =
	{ 0.0 };

	TupleDesc tupdesc = tts->tts_tupleDescriptor;
	user_data->numberOfColumns = tupdesc->natts;

	MemoryContext per_row_context = eid->ext_pstate->rowcontext;
	MemoryContextReset(per_row_context);
	MemoryContext old_context = MemoryContextSwitchTo(per_row_context);

	/* Get column values */
	for (int i = 0; i < user_data->numberOfColumns; ++i)
	{
		int dataType = (int) (tupdesc->attrs[i]->atttypid);

		user_data->colRawValues[i] = NULL;
		user_data->colValNullBitmap[i] = NULL;

		if (nulls[i])
		{
			if (dataType == HAWQ_TYPE_CHAR)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT8);
			}
			else if (dataType == HAWQ_TYPE_INT2)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT16);
			}
			else if (dataType == HAWQ_TYPE_INT4)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT32);
			}
			else if (dataType == HAWQ_TYPE_INT8)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT64);
			}
			else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BYTE
					|| dataType == HAWQ_TYPE_BPCHAR
					|| dataType == HAWQ_TYPE_VARCHAR
					|| dataType == HAWQ_TYPE_NUMERIC)
			{
				user_data->colRawValues[i] = (char *) (DUMMY_TEXT);
			}
			else if (dataType == HAWQ_TYPE_FLOAT4)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_FLOAT);
			}
			else if (dataType == HAWQ_TYPE_FLOAT8)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_DOUBLE);
			}
			else if (dataType == HAWQ_TYPE_BOOL)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_BOOL);
			}
			else if (dataType == HAWQ_TYPE_DATE)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_DATE);
			}
			else if (dataType == HAWQ_TYPE_TIME)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_TIME);
			}
			else if (dataType == HAWQ_TYPE_TIMESTAMP
			    || dataType == HAWQ_TYPE_TIMESTAMPTZ)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_TIMESTAMP);
			}
			else if (dataType == HAWQ_TYPE_INT2_ARRAY)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT16_ARRAY);
			}
			else if (dataType == HAWQ_TYPE_INT4_ARRAY)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT32_ARRAY);
			}
			else if (dataType == HAWQ_TYPE_INT8_ARRAY)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_INT64_ARRAY);
			}
			else if (dataType == HAWQ_TYPE_FLOAT4_ARRAY)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_FLOAT_ARRAY);
			}
			else if (dataType == HAWQ_TYPE_FLOAT8_ARRAY)
			{
				user_data->colRawValues[i] = (char *) (&DUMMY_DOUBLE_ARRAY);
			}
			else if (dataType == HAWQ_TYPE_INVALID)
			{
				ereport(ERROR, (errmsg_internal("HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)))));
			}
			else
			{
				ereport(ERROR, (errmsg_internal("HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)))));
			}

			continue;
		}

		if (dataType == HAWQ_TYPE_CHAR || dataType == HAWQ_TYPE_INT2
				|| dataType == HAWQ_TYPE_INT4 || dataType == HAWQ_TYPE_INT8
				|| dataType == HAWQ_TYPE_FLOAT4 || dataType == HAWQ_TYPE_FLOAT8
				|| dataType == HAWQ_TYPE_BOOL || dataType == HAWQ_TYPE_TIME)
		{
			user_data->colRawValues[i] = (char *) (&(user_data->colValues[i]));
		}
		else if (dataType == HAWQ_TYPE_TIMESTAMP || dataType == HAWQ_TYPE_TIMESTAMPTZ)
		{
			int64_t *timestamp = (int64_t *) (&(user_data->colValues[i]));
			user_data->colTimestamp[i].second = *timestamp / 1000000
					+ (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24;
			user_data->colTimestamp[i].nanosecond = *timestamp % 1000000 * 1000;
			int64_t days = user_data->colTimestamp[i].second / 60 / 60 / 24;
			if (user_data->colTimestamp[i].nanosecond < 0 &&
			    (days > POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE || days < 0))
				user_data->colTimestamp[i].nanosecond += 1000000000;
			if(user_data->colTimestamp[i].second<0&&user_data->colTimestamp[i].nanosecond)
			  user_data->colTimestamp[i].second-=1;
			user_data->colRawValues[i] =
					(char *) (&(user_data->colTimestamp[i]));
		}
		else if (dataType == HAWQ_TYPE_DATE)
		{
			int *date = (int *) (&(user_data->colValues[i]));
			*date += POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
			user_data->colRawValues[i] = (char *) (&(user_data->colValues[i]));
		}
		else if (dataType == HAWQ_TYPE_TEXT || dataType == HAWQ_TYPE_BPCHAR || dataType == HAWQ_TYPE_VARCHAR)
		{
			if (eid->ext_pstate->need_transcoding)
			{
				char *cvt = NULL;

				user_data->colRawValues[i] = OutputFunctionCall(
					&(eid->ext_pstate->out_functions[i]),
					user_data->colValues[i]);

				cvt = pg_server_to_custom(user_data->colRawValues[i],
						strlen(user_data->colRawValues[i]),
						eid->ext_pstate->client_encoding,
						eid->ext_pstate->enc_conversion_proc);

				user_data->colRawValues[i] = cstring_to_text(cvt);
			}
			else {
				struct varlena *data = PG_DETOAST_DATUM(user_data->colValues[i]);
				user_data->colRawValues[i] = data;
			}
		}
		else if (dataType == HAWQ_TYPE_BYTE)
		{
			user_data->colRawValues[i] = PG_DETOAST_DATUM(user_data->colValues[i]);
		}
		else if (dataType == HAWQ_TYPE_NUMERIC)
		{
			Numeric num = DatumGetNumeric(user_data->colValues[i]);
			user_data->colRawValues[i] = num;
			if (NUMERIC_IS_NAN(num)) // XXX(chiyang): problematic legacy ORC NaN
			{
				nulls[i] = true;
			}
		}
		else if (dataType == HAWQ_TYPE_INT2_ARRAY
				|| dataType == HAWQ_TYPE_INT4_ARRAY
				|| dataType == HAWQ_TYPE_INT8_ARRAY
				|| dataType == HAWQ_TYPE_FLOAT4_ARRAY
				|| dataType == HAWQ_TYPE_FLOAT8_ARRAY)
		{
			ArrayType *arr = DatumGetArrayTypeP(user_data->colValues[i]);
			user_data->colValLength[i] = ARR_SIZE(arr) - ARR_DATA_OFFSET(arr);
			user_data->colRawValues[i] = ARR_DATA_PTR(arr);
			user_data->colValNullBitmap[i] = ARR_NULLBITMAP(arr);
			// Now we only support 1 dimension array
			if (ARR_NDIM(arr) > 1)
			{
		    ereport(ERROR, (errmsg_internal("Now we only support 1 dimension array in orc format,"
            " your array dimension is %d", ARR_NDIM(arr))));
			}
			else if (ARR_NDIM(arr) == 1)
			{
				user_data->colValDims[i] = ARR_DIMS(arr);
			}
			else
			{
				user_data->colValDims[i] = NULL;
			}
		}
		else if (dataType == HAWQ_TYPE_INVALID)
		{
		  ereport(ERROR, (errmsg_internal("HAWQ data type %s is invalid", TypeNameToString(makeTypeNameFromOid(dataType, -1)))));
		}
		else
		{
			ereport(ERROR, (errmsg_internal("HAWQ data type %s is not supported yet", TypeNameToString(makeTypeNameFromOid(dataType, -1)))));

		}
	}

	/* Pass to formatter to output */
	ORCFormatInsertORCFormatC(user_data->fmt, user_data->colDatatypes,
			user_data->colRawValues, user_data->colValLength,
			user_data->colValNullBitmap, user_data->colValDims,
			nulls);

	ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		ereport(ERROR,(errcode(e->errCode),errmsg("%s", e->errMessage)));
	}

	ps->ps_tuple_oid = InvalidOid;

	MemoryContextSwitchTo(old_context);

	PG_RETURN_OID(InvalidOid);
}

/*
 * void
 * orc_insert_finish(ExternalInsertDesc extInsertDesc)
 */
Datum orc_insert_finish(PG_FUNCTION_ARGS)
{
	PlugStorage ps = (PlugStorage) (fcinfo->context);
	ExternalInsertDesc eid = ps->ps_ext_insert_desc;

	ORCFormatUserData *user_data = (ORCFormatUserData *) (eid->ext_ps_user_data);

	ORCFormatEndInsertORCFormatC(user_data->fmt);
	ORCFormatCatchedError *e = ORCFormatGetErrorORCFormatC(user_data->fmt);
	if (e->errCode != ERRCODE_SUCCESSFUL_COMPLETION)
	{
		ereport(ERROR, (errcode(e->errCode), errmsg("%s", e->errMessage)));
	}

	ORCFormatFreeORCFormatC(&(user_data->fmt));

	pfree(user_data->colDatatypes);
	pfree(user_data->colRawValues);
	pfree(user_data->colValLength);
	for (int i = 0; i < user_data->numberOfColumns; ++i)
	{
		pfree(user_data->colNames[i]);
	}
	pfree(user_data->colNames);
	pfree(user_data);

	if (eid->ext_formatter_data)
		pfree(eid->ext_formatter_data);

	if (eid->ext_formatter_name)
		pfree(eid->ext_formatter_name);

	pfree(eid);

	PG_RETURN_VOID() ;
}

static FmgrInfo *get_orc_function(char *formatter_name, char *function_name)
{
	Assert(formatter_name);Assert(function_name);

	Oid procOid = InvalidOid;
	FmgrInfo *procInfo = NULL;

	procOid = LookupPlugStorageValidatorFunc(formatter_name, function_name);

	if (OidIsValid(procOid))
	{
		procInfo = (FmgrInfo *) palloc(sizeof(FmgrInfo));
		fmgr_info(procOid, procInfo);
	}
	else
	{
		ereport(ERROR, (errmsg_internal("%s_%s function was not found for pluggable storage",
		                                formatter_name, function_name)));
	}

	return procInfo;
}

static void get_scan_functions(FileScanDesc file_scan_desc)
{
	file_scan_desc->fs_ps_scan_funcs.beginscan = get_orc_function("orc",
			"beginscan");

	file_scan_desc->fs_ps_scan_funcs.getnext_init = get_orc_function("orc",
			"getnext_init");

	file_scan_desc->fs_ps_scan_funcs.getnext = get_orc_function("orc",
			"getnext");

	file_scan_desc->fs_ps_scan_funcs.rescan = get_orc_function("orc", "rescan");

	file_scan_desc->fs_ps_scan_funcs.endscan = get_orc_function("orc",
			"endscan");

	file_scan_desc->fs_ps_scan_funcs.stopscan = get_orc_function("orc",
			"stopscan");
}

static void get_insert_functions(ExternalInsertDesc ext_insert_desc)
{
	ext_insert_desc->ext_ps_insert_funcs.insert_init = get_orc_function("orc",
			"insert_init");

	ext_insert_desc->ext_ps_insert_funcs.insert = get_orc_function("orc",
			"insert");

	ext_insert_desc->ext_ps_insert_funcs.insert_finish = get_orc_function("orc",
			"insert_finish");
}

static void init_format_user_data_for_read(TupleDesc tup_desc,
		ORCFormatUserData *user_data)
{
	user_data->numberOfColumns = tup_desc->natts;
	user_data->colNames = palloc0(sizeof(char *) * user_data->numberOfColumns);
	user_data->colDatatypes = palloc0(sizeof(int) * user_data->numberOfColumns);
	user_data->colDatatypeMods = palloc0(
			sizeof(int64_t) * user_data->numberOfColumns);
	user_data->colValues = palloc0(sizeof(Datum) * user_data->numberOfColumns);
	user_data->colRawValues = palloc0(
			sizeof(char *) * user_data->numberOfColumns);
	user_data->colValLength = palloc0(
			sizeof(uint64_t) * user_data->numberOfColumns);
	user_data->colValNullBitmap = palloc0(
			sizeof(bits8 *) * user_data->numberOfColumns);

	for (int i = 0; i < user_data->numberOfColumns; i++)
	{
		user_data->colNames[i] = NULL;
		user_data->colValues[i] = NULL;
		user_data->colRawValues[i] = NULL;
		user_data->colValLength[i] = 0;
		user_data->colValNullBitmap[i] = (bits8 *) palloc0(
				sizeof(bits8) * MAX_ORC_ARRAY_DIMS);
	}
}

static void init_format_user_data_for_write(TupleDesc tup_desc,
		ORCFormatUserData *user_data)
{
	user_data->numberOfColumns = tup_desc->natts;
	user_data->colNames = palloc0(sizeof(char *) * user_data->numberOfColumns);
	user_data->colDatatypes = palloc0(sizeof(int) * user_data->numberOfColumns);
	user_data->colDatatypeMods = palloc0(
			sizeof(int64_t) * user_data->numberOfColumns);
	user_data->colRawValues = palloc0(
			sizeof(char *) * user_data->numberOfColumns);
	user_data->colValLength = palloc0(
			sizeof(uint64_t) * user_data->numberOfColumns);
	user_data->colValNullBitmap = palloc0(
			sizeof(bits8 *) * user_data->numberOfColumns);
	user_data->colValDims = palloc0(sizeof(int *) * user_data->numberOfColumns);
	user_data->colTimestamp = palloc0(
			sizeof(TimestampType) * user_data->numberOfColumns);
}

static void build_options_in_json(List *fmt_opts_defelem, int encoding,
		char **json_str, TupleDesc tupDesc)
{
	struct json_object *opt_json_object = json_object_new_object();

	/* add format options for the formatter */
	char *key_str = NULL;
	char *val_str = NULL;
	const char *whitespace = " \t\n\r";

	int nargs = list_length(fmt_opts_defelem);
	for (int i = 0; i < nargs; ++i)
	{
		key_str = ((DefElem *) (list_nth(fmt_opts_defelem, i)))->defname;
		val_str =
				((Value *) ((DefElem *) (list_nth(fmt_opts_defelem, i)))->arg)->val.str;

		if ((strncasecmp(key_str, "bloomfilter", strlen("bloomfilter")) == 0))
		{
		  int attnum = tupDesc->natts;
      json_object *j_array = json_object_new_array();
      char *token = orc_strtokx2(val_str, whitespace, ",", NULL, 0, false, false, encoding);
      while (token)
      {
        for (int j = 0; j < attnum; ++j)
        {
          if ((strncasecmp(token, ((Form_pg_attribute) (tupDesc->attrs[j]))->attname.data, strlen(token)) == 0))
          {
            json_object *j_obj = json_object_new_int(j + 1);
            json_object_array_add(j_array, j_obj);
          }
        }
        token = orc_strtokx2(NULL, whitespace, ",", NULL, 0, false, false, encoding);;
      }
      json_object_object_add(opt_json_object, key_str, j_array);
    }
    else {
      json_object_object_add(opt_json_object, key_str,
        json_object_new_string(val_str));
    }
  }

	/* add encoding option for orc */
	if (json_object_object_get(opt_json_object, "encoding") == NULL)
	{
		const char *encodingStr = pg_encoding_to_char(encoding);
		char *encodingStrLower = str_tolower(encodingStr, strlen(encodingStr));

		json_object_object_add(opt_json_object, "encoding",
				json_object_new_string(encodingStrLower));

		if (encodingStrLower)
			pfree(encodingStrLower);
	}

	*json_str = NULL;
	if (opt_json_object != NULL)
	{
		const char *str = json_object_to_json_string(opt_json_object);
		*json_str = (char *) palloc0(strlen(str) + 1);
		strcpy(*json_str, str);
		json_object_put(opt_json_object);

		ereport(DEBUG3, (errmsg_internal("formatter options are %s", *json_str)));
	}
}

static ORCFormatC *create_formatter_instance(List *fmt_opts_defelem,
		int fmt_encoding, int segno, TupleDesc tupDesc, bool forWrite)
{
	char *fmt_opts_str = NULL;

	ORCFormatC *orc_format_c = NULL;
	if (forWrite) {
		build_options_in_json(fmt_opts_defelem, fmt_encoding, &fmt_opts_str, tupDesc);
		orc_format_c = ORCFormatNewORCFormatC(fmt_opts_str, segno);
	} else {
		orc_format_c = ORCFormatNewORCFormatC("{}", segno);
	}

	if (fmt_opts_str != NULL)
	{
		pfree(fmt_opts_str);
	}

	return orc_format_c;
}

static void build_file_splits(Uri *uri, ScanState *scan_state,
		ORCFormatUserData *user_data)
{
	if (scan_state)
	{
		user_data->nSplits = list_length(scan_state->splits);
		user_data->splits = palloc0(
				sizeof(ORCFormatFileSplit) * user_data->nSplits);
		ListCell *cell = NULL;
		int i = 0;
		foreach(cell, scan_state->splits)
		{
			FileSplit origFS = (FileSplit) lfirst(cell);
			user_data->splits[i].len = origFS->lengths;
			user_data->splits[i].start = origFS->offsets;
			user_data->splits[i].eof = -1;

			/* build file path containing host address */
			int fileNameLen = 7 +   // "hdfs://"
			                  (uri->hostname == NULL ? 0 : strlen(uri->hostname)) +
			                  1 +   // ':'
			                  5 +   // "65535"
			                  (origFS->ext_file_uri_string == NULL ? 0 : strlen(origFS->ext_file_uri_string)) +
			                  1;    // '\0'

			user_data->splits[i].fileName = palloc(fileNameLen * sizeof(char));
			sprintf(user_data->splits[i].fileName, "hdfs://%s:%d%s",
			        uri->hostname == NULL ? "" : uri->hostname, uri->port,
			        origFS->ext_file_uri_string == NULL ? "" : origFS->ext_file_uri_string);
			ereport(LOG, (errmsg_internal("fileinformation:%s",user_data->splits[i].fileName)));

			i++;
		}
	}
	else
	{
		user_data->nSplits = 0;
		user_data->splits = NULL;
	}
}

static void build_tuple_descrition_for_read(Plan *plan, Relation relation,
		ORCFormatUserData *user_data)
{
	user_data->colToReads = palloc0(sizeof(bool) * user_data->numberOfColumns);

	for (int i = 0; i < user_data->numberOfColumns; ++i)
	{
		user_data->colToReads[i] = plan ? false : true;

		/* 64 is the name type length */
		user_data->colNames[i] = palloc(sizeof(char) * 64);

		strcpy(user_data->colNames[i],
				relation->rd_att->attrs[i]->attname.data);

		int data_type = (int) (relation->rd_att->attrs[i]->atttypid);
		user_data->colDatatypes[i] = map_hawq_type_to_common_plan(data_type);
		user_data->colDatatypeMods[i] = relation->rd_att->attrs[i]->atttypmod;
	}

	if (plan)
	{
		/* calculate columns to read for seqscan */
		GetNeededColumnsForScan((Node *) plan->targetlist,
				user_data->colToReads, user_data->numberOfColumns);

		GetNeededColumnsForScan((Node *) plan->qual, user_data->colToReads,
				user_data->numberOfColumns);
	}
}

static void build_tuple_descrition_for_write(Relation relation,
		ORCFormatUserData *user_data)
{
	for (int i = 0; i < user_data->numberOfColumns; ++i)
	{
		/* 64 is the name type length */
		user_data->colNames[i] = palloc(sizeof(char) * 64);

		strcpy(user_data->colNames[i],
				relation->rd_att->attrs[i]->attname.data);

		user_data->colDatatypes[i] = map_hawq_type_to_common_plan(
				(int) (relation->rd_att->attrs[i]->atttypid));

		if (user_data->colDatatypes[i] == CHARID &&
				relation->rd_att->attrs[i]->atttypmod == -1) {
			user_data->colDatatypeMods[i] =
			strlen(relation->rd_att->attrs[i]->attname.data) + VARHDRSZ;
		} else {
			user_data->colDatatypeMods[i] = relation->rd_att->attrs[i]->atttypmod;
		}
	}
}

static void orc_scan_error_callback(void *arg)
{
	CopyState cstate = (CopyState) arg;

	errcontext("External table %s", cstate->cur_relname);
}

static void orc_parse_format_string(CopyState pstate, char *fmtstr)
{
	char *token;
	const char *whitespace = " \t\n\r";
	char nonstd_backslash = 0;
	int encoding = GetDatabaseEncoding();

	token = orc_strtokx2(fmtstr, whitespace, NULL, NULL, 0, false, true,
			encoding);
	/* parse user custom options. take it as is. no validation needed */

	List *l = NIL;
	bool formatter_found = false;

	if (token)
	{
		char *key = token;
		char *val = NULL;
		StringInfoData key_modified;

		initStringInfo(&key_modified);

		while (key)
		{

			/* MPP-14467 - replace meta chars back to original */
			resetStringInfo(&key_modified);
			appendStringInfoString(&key_modified, key);
			replaceStringInfoString(&key_modified, "<gpx20>", " ");

			val = orc_strtokx2(NULL, whitespace, NULL, "'", nonstd_backslash,
			true, true, encoding);
			if (val)
			{

				if (pg_strcasecmp(key, "formatter") == 0)
				{
					pstate->custom_formatter_name = pstrdup(val);
					formatter_found = true;
				}
				else

					l = lappend(l,
							makeDefElem(pstrdup(key_modified.data),
									(Node *) makeString(pstrdup(val))));
			}
			else
				goto error;

			key = orc_strtokx2(NULL, whitespace, NULL, NULL, 0, false, false,
					encoding);
		}

	}

	if (!formatter_found)
	{
		/*
		 * If there is no formatter option specified, use format name. So
		 * we don't report error here.
		 */
	}

	pstate->custom_formatter_params = l;

	return;

	error: if (token)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("external table internal parse error at \"%s\"", token)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERNAL_ERROR), errmsg("external table internal parse error at end of " "line")));

}

static char *orc_strtokx2(const char *s, const char *whitespace,
		const char *delim, const char *quote, char escape, bool e_strings,
		bool del_quotes, int encoding)
{
	static char *storage = NULL;/* store the local copy of the users string
	 * here */
	static char *string = NULL; /* pointer into storage where to continue on
	 * next call */

	/* variously abused variables: */
	unsigned int offset;
	char *start;
	char *p;

	if (s)
	{
		//pfree(storage);

		/*
		 * We may need extra space to insert delimiter nulls for adjacent
		 * tokens.	2X the space is a gross overestimate, but it's unlikely
		 * that this code will be used on huge strings anyway.
		 */
		storage = palloc(2 * strlen(s) + 1);
		strcpy(storage, s);
		string = storage;
	}

	if (!storage)
		return NULL;

	/* skip leading whitespace */
	offset = strspn(string, whitespace);
	start = &string[offset];

	/* end of string reached? */
	if (*start == '\0')
	{
		/* technically we don't need to free here, but we're nice */
		pfree(storage);
		storage = NULL;
		string = NULL;
		return NULL;
	}

	/* test if delimiter character */
	if (delim && strchr(delim, *start))
	{
		/*
		 * If not at end of string, we need to insert a null to terminate the
		 * returned token.	We can just overwrite the next character if it
		 * happens to be in the whitespace set ... otherwise move over the
		 * rest of the string to make room.  (This is why we allocated extra
		 * space above).
		 */
		p = start + 1;
		if (*p != '\0')
		{
			if (!strchr(whitespace, *p))
				memmove(p + 1, p, strlen(p) + 1);
			*p = '\0';
			string = p + 1;
		}
		else
		{
			/* at end of string, so no extra work */
			string = p;
		}

		return start;
	}

	/* check for E string */
	p = start;
	if (e_strings && (*p == 'E' || *p == 'e') && p[1] == '\'')
	{
		quote = "'";
		escape = '\\'; /* if std strings before, not any more */
		p++;
	}

	/* test if quoting character */
	if (quote && strchr(quote, *p))
	{
		/* okay, we have a quoted token, now scan for the closer */
		char thisquote = *p++;

		/* MPP-6698 START
		 * unfortunately, it is possible for an external table format
		 * string to be represented in the catalog in a way which is
		 * problematic to parse: when using a single quote as a QUOTE
		 * or ESCAPE character the format string will show [quote '''].
		 * since we do not want to change how this is stored at this point
		 * (as it will affect previous versions of the software already
		 * in production) the following code block will detect this scenario
		 * where 3 quote characters follow each other, with no forth one.
		 * in that case, we will skip the second one (the first is skipped
		 * just above) and the last trailing quote will be skipped below.
		 * the result will be the actual token (''') and after stripping
		 * it due to del_quotes we'll end up with (').
		 * very ugly, but will do the job...
		 */
		char qt = quote[0];

		if (strlen(p) >= 3 && p[0] == qt && p[1] == qt && p[2] != qt)
			p++;
		/* MPP-6698 END */

		for (; *p; p += pg_encoding_mblen(encoding, p))
		{
			if (*p == escape && p[1] != '\0')
				p++; /* process escaped anything */
			else if (*p == thisquote && p[1] == thisquote)
				p++; /* process doubled quote */
			else if (*p == thisquote)
			{
				p++; /* skip trailing quote */
				break;
			}
		}

		/*
		 * If not at end of string, we need to insert a null to terminate the
		 * returned token.	See notes above.
		 */
		if (*p != '\0')
		{
			if (!strchr(whitespace, *p))
				memmove(p + 1, p, strlen(p) + 1);
			*p = '\0';
			string = p + 1;
		}
		else
		{
			/* at end of string, so no extra work */
			string = p;
		}

		/* Clean up the token if caller wants that */
		if (del_quotes)
			orc_strip_quotes(start, thisquote, escape, encoding);

		return start;
	}

	/*
	 * Otherwise no quoting character.	Scan till next whitespace, delimiter
	 * or quote.  NB: at this point, *start is known not to be '\0',
	 * whitespace, delim, or quote, so we will consume at least one character.
	 */
	offset = strcspn(start, whitespace);

	if (delim)
	{
		unsigned int offset2 = strcspn(start, delim);

		if (offset > offset2)
			offset = offset2;
	}

	if (quote)
	{
		unsigned int offset2 = strcspn(start, quote);

		if (offset > offset2)
			offset = offset2;
	}

	p = start + offset;

	/*
	 * If not at end of string, we need to insert a null to terminate the
	 * returned token.	See notes above.
	 */
	if (*p != '\0')
	{
		if (!strchr(whitespace, *p))
			memmove(p + 1, p, strlen(p) + 1);
		*p = '\0';
		string = p + 1;
	}
	else
	{
		/* at end of string, so no extra work */
		string = p;
	}

	return start;
}

static void orc_strip_quotes(char *source, char quote, char escape,
		int encoding)
{
	char *src;
	char *dst;

	Assert(source);Assert(quote);

	src = dst = source;

	if (*src && *src == quote)
		src++; /* skip leading quote */

	while (*src)
	{
		char c = *src;
		int i;

		if (c == quote && src[1] == '\0')
			break; /* skip trailing quote */
		else if (c == quote && src[1] == quote)
			src++; /* process doubled quote */
		else if (c == escape && src[1] != '\0')
			src++; /* process escaped character */

		i = pg_encoding_mblen(encoding, src);
		while (i--)
			*dst++ = *src++;
	}

	*dst = '\0';
}
