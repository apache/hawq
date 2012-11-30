/*-------------------------------------------------------------------------
 *
 * reloptions.c
 *	  Core support for relation options (pg_class.reloptions)
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/access/common/reloptions.c,v 1.3 2007/01/05 22:19:21 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "miscadmin.h"

static int setDefaultCompressionLevel(char* compresstype);

/*
 * Transform a relation options list (list of DefElem) into the text array
 * format that is kept in pg_class.reloptions.
 *
 * This is used for three cases: CREATE TABLE/INDEX, ALTER TABLE SET, and
 * ALTER TABLE RESET.  In the ALTER cases, oldOptions is the existing
 * reloptions value (possibly NULL), and we replace or remove entries
 * as needed.
 *
 * If ignoreOids is true, then we should ignore any occurrence of "oids"
 * in the list (it will be or has been handled by interpretOidsOption()).
 *
 * Note that this is not responsible for determining whether the options
 * are valid.
 *
 * Both oldOptions and the result are text arrays (or NULL for "default"),
 * but we declare them as Datums to avoid including array.h in reloptions.h.
 */
Datum
transformRelOptions(Datum oldOptions, List *defList,
					bool ignoreOids, bool isReset)
{
	Datum		result;
	ArrayBuildState *astate;
	ListCell   *cell;

	/* no change if empty list */
	if (defList == NIL)
		return oldOptions;

	/* We build new array using accumArrayResult */
	astate = NULL;

	/* Copy any oldOptions that aren't to be replaced */
	if (DatumGetPointer(oldOptions) != 0)
	{
		ArrayType  *array = DatumGetArrayTypeP(oldOptions);
		Datum	   *oldoptions;
		int			noldoptions;
		int			i;

		Assert(ARR_ELEMTYPE(array) == TEXTOID);

		deconstruct_array(array, TEXTOID, -1, false, 'i',
						  &oldoptions, NULL, &noldoptions);

		for (i = 0; i < noldoptions; i++)
		{
			text	   *oldoption = DatumGetTextP(oldoptions[i]);
			char	   *text_str = VARDATA(oldoption);
			int			text_len = VARSIZE(oldoption) - VARHDRSZ;

			/* Search for a match in defList */
			foreach(cell, defList)
			{
				DefElem    *def = lfirst(cell);
				int			kw_len = strlen(def->defname);

				if (text_len > kw_len && text_str[kw_len] == '=' &&
					pg_strncasecmp(text_str, def->defname, kw_len) == 0)
					break;
			}
			if (!cell)
			{
				/* No match, so keep old option */
				astate = accumArrayResult(astate, oldoptions[i],
										  false, TEXTOID,
										  CurrentMemoryContext);
			}
		}
	}

	/*
	 * If CREATE/SET, add new options to array; if RESET, just check that the
	 * user didn't say RESET (option=val).  (Must do this because the grammar
	 * doesn't enforce it.)
	 */
	foreach(cell, defList)
	{
		DefElem    *def = lfirst(cell);

		if (isReset)
		{
			if (def->arg != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("RESET must not include values for parameters")));
		}
		else
		{
			text	   *t;
			const char *value;
			Size		len;

			if (ignoreOids && pg_strcasecmp(def->defname, "oids") == 0)
				continue;

			/*
			 * Flatten the DefElem into a text string like "name=arg". If we
			 * have just "name", assume "name=true" is meant.
			 */
			if (def->arg != NULL)
				value = defGetString(def);
			else
				value = "true";
			len = VARHDRSZ + strlen(def->defname) + 1 + strlen(value);
			/* +1 leaves room for sprintf's trailing null */
			t = (text *) palloc(len + 1);
			SET_VARSIZE(t, len);
			sprintf(VARDATA(t), "%s=%s", def->defname, value);

			astate = accumArrayResult(astate, PointerGetDatum(t),
									  false, TEXTOID,
									  CurrentMemoryContext);
		}
	}

	if (astate)
		result = makeArrayResult(astate, CurrentMemoryContext);
	else
		result = (Datum) 0;

	return result;
}


/*
 * Convert the text-array format of reloptions into a List of DefElem.
 * This is the inverse of transformRelOptions().
 */
List *
untransformRelOptions(Datum options)
{
	List	   *result = NIL;
	ArrayType  *array;
	Datum	   *optiondatums;
	int			noptions;
	int			i;

	/* Nothing to do if no options */
	if (options == (Datum) 0)
		return result;

	array = DatumGetArrayTypeP(options);

	Assert(ARR_ELEMTYPE(array) == TEXTOID);

	deconstruct_array(array, TEXTOID, -1, false, 'i',
					  &optiondatums, NULL, &noptions);

	for (i = 0; i < noptions; i++)
	{
		char	   *s;
		char	   *p;
		Node	   *val = NULL;

		s = DatumGetCString(DirectFunctionCall1(textout, optiondatums[i]));
		p = strchr(s, '=');
		if (p)
		{
			*p++ = '\0';
			val = (Node *) makeString(pstrdup(p));
		}
		result = lappend(result, makeDefElem(pstrdup(s), val));
	}

	return result;
}


/*
 * Interpret reloptions that are given in text-array format.
 *
 *	options: array of "keyword=value" strings, as built by transformRelOptions
 *	numkeywords: number of legal keywords
 *	keywords: the allowed keywords
 *	values: output area
 *	validate: if true, throw error for unrecognized keywords.
 *
 * The keywords and values arrays must both be of length numkeywords.
 * The values entry corresponding to a keyword is set to a palloc'd string
 * containing the corresponding value, or NULL if the keyword does not appear.
 */
void
parseRelOptions(Datum options, int numkeywords, const char *const * keywords,
				char **values, bool validate)
{
	ArrayType  *array;
	Datum	   *optiondatums;
	int			noptions;
	int			i;

	/* Initialize to "all defaulted" */
	MemSet(values, 0, numkeywords * sizeof(char *));

	/* Done if no options */
	if (DatumGetPointer(options) == 0)
		return;

	array = DatumGetArrayTypeP(options);

	Assert(ARR_ELEMTYPE(array) == TEXTOID);

	deconstruct_array(array, TEXTOID, -1, false, 'i',
					  &optiondatums, NULL, &noptions);

	for (i = 0; i < noptions; i++)
	{
		text	   *optiontext = DatumGetTextP(optiondatums[i]);
		char	   *text_str = VARDATA(optiontext);
		int			text_len = VARSIZE(optiontext) - VARHDRSZ;
		int			j;

		/* Search for a match in keywords */
		for (j = 0; j < numkeywords; j++)
		{
			int			kw_len = strlen(keywords[j]);

			if (text_len > kw_len && text_str[kw_len] == '=' &&
				pg_strncasecmp(text_str, keywords[j], kw_len) == 0)
			{
				char	   *value;
				int			value_len;

				if (values[j] && validate)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("duplicate parameter \"%s\"",
									keywords[j])));
				value_len = text_len - kw_len - 1;
				value = (char *) palloc(value_len + 1);
				memcpy(value, text_str + kw_len + 1, value_len);
				value[value_len] = '\0';
				values[j] = value;
				break;
			}
		}
		if (j >= numkeywords && validate)
		{
			char	   *s;
			char	   *p;

			s = DatumGetCString(DirectFunctionCall1(textout, optiondatums[i]));
			p = strchr(s, '=');
			if (p)
				*p = '\0';
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter \"%s\"", s),
							   errOmitLocation(true)));
		}
	}
}


/*
 * Parse reloptions for anything using StdRdOptions
 */
bytea *
default_reloptions(Datum reloptions, bool validate, char relkind,
				   int minFillfactor, int defaultFillfactor)
{
	static const char *const default_keywords[] = {
		"fillfactor",
		"appendonly",
		"blocksize",
		"compresstype",
		"compresslevel",
		"checksum",
		"orientation",
		"errortable",
	};

	char	   *values[ARRAY_SIZE(default_keywords)];
	int32		fillfactor = defaultFillfactor;
	int32		blocksize = DEFAULT_APPENDONLY_BLOCK_SIZE;
	bool		appendonly = false;
	bool		checksum = false;
	char*		compresstype = NULL;
	int32		compresslevel;  /* Not set yet.  Need to derive from compresstype */
	char*		defaultCompressor = "zlib"; /* don't precede with 'const' */
	bool 		columnstore = false;
	bool		forceHeap = false;
	bool		errorTable = false;
	int			j = 0;

	StdRdOptions *result;

	parseRelOptions(reloptions, ARRAY_SIZE(default_keywords), default_keywords, values, validate);

	/* fillfactor */
	if (values[0] != NULL)
	{
		fillfactor = pg_atoi(values[0], sizeof(int32), 0);
		if (fillfactor < minFillfactor || fillfactor > 100)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("fillfactor=%d is out of range (should "
								"be between %d and 100)",
								fillfactor, minFillfactor),
										   errOmitLocation(true)));

			fillfactor = defaultFillfactor;
		}
	}

	/* appendonly */
	if (values[1] != NULL)
	{
		if (relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"appendonly\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!(pg_strcasecmp(values[1], "true") == 0 ||
			  pg_strcasecmp(values[1], "false") == 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value for \"appendonly\": \"%s\"",
							values[1]),
									   errOmitLocation(true)));
		}
		appendonly = (pg_strcasecmp(values[1], "true") == 0 ? true : false);
		forceHeap = !appendonly;
	}

	/* blocksize */
	if (values[2] != NULL)
	{
		if (relkind != RELKIND_RELATION && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"blocksize\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!appendonly && validate)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \'blocksize\' for base relation. "
							"Only valid for Append Only relations"),
									   errOmitLocation(true)));

		blocksize = pg_atoi(values[2], sizeof(int32), 0);

		if (blocksize < MIN_APPENDONLY_BLOCK_SIZE || blocksize > MAX_APPENDONLY_BLOCK_SIZE ||
			blocksize % MIN_APPENDONLY_BLOCK_SIZE != 0)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("block size must be between 8KB and 2MB and "
								"be an 8KB multiple. Got %d", blocksize),
										   errOmitLocation(true)));

			blocksize = DEFAULT_APPENDONLY_BLOCK_SIZE;
		}

	}

	/* compression type */
	if (values[3] != NULL)
	{
		if (relkind != RELKIND_RELATION && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"compresstype\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!appendonly && validate)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \'compresstype\' for base relation. "
							"Only valid for Append Only relations"),
									   errOmitLocation(true)));

		compresstype = values[3];

		if (!compresstype_is_valid(compresstype))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("unknown compresstype \"%s\"", compresstype),
					 errOmitLocation(true)));
	}

	/* compression level */
	if (values[4] != NULL)
	{
		if (relkind != RELKIND_RELATION && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"compresslevel\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!appendonly && validate)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \'compresslevel\' for base relation. "
							"Only valid for Append Only relations"),
									   errOmitLocation(true)));

		compresslevel = pg_atoi(values[4], sizeof(int32), 0);

		if (compresstype && strcmp(compresstype, "none") != 0
					&& compresslevel == 0 && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresstype can\'t be used with compresslevel 0"),
							   errOmitLocation(true)));
		if (compresslevel < 0 || compresslevel > 9)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("compresslevel=%d is out of range (should be "
								"between 0 and 9)",
								compresslevel),
										   errOmitLocation(true)));

			compresslevel = setDefaultCompressionLevel(compresstype);
		}

		/*
		 * use the default compressor if compresslevel was indicated but not
		 * compresstype. must make a copy otherwise str_tolower below will
		 * crash.
		 */
		if (compresslevel > 0 && !compresstype)
			compresstype = pstrdup(defaultCompressor);

		if (compresstype && (pg_strcasecmp(compresstype, "quicklz") == 0) &&
			(compresslevel != 1))
		{
			/* allow quicklz level 3 only if the debug guc is on */
			if (!(compresslevel == 3 && Test_enable_broken_quicklz3))
			{
				if (validate)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("compresslevel=%d is out of range for quicklz "
									 "(should be 1)", compresslevel),
											   errOmitLocation(true)));

				compresslevel = setDefaultCompressionLevel(compresstype);
			}
		}
		
		if (compresstype && (pg_strcasecmp(compresstype, "rle_type") == 0) &&
			(compresslevel > 4))
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("compresslevel=%d is out of range for rle_type "
								"(should be in the range 1 to 4)", compresslevel),
						 errOmitLocation(true)));
			
			compresslevel = setDefaultCompressionLevel(compresstype);
		}
	}
	else
	{
		compresslevel = setDefaultCompressionLevel(compresstype);
	}

	/* checksum */
	if (values[5] != NULL)
	{
		if (relkind != RELKIND_RELATION && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"checksum\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!appendonly && validate)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \'checksum\' for base relation. "
							"Only valid for Append Only relations"),
									   errOmitLocation(true)));

		if (!(pg_strcasecmp(values[5], "true") == 0 ||
			  pg_strcasecmp(values[5], "false") == 0) &&
			validate)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value for \"checksum\": \"%s\"",
							values[5]),
									   errOmitLocation(true)));
		}
		checksum = (pg_strcasecmp(values[5], "true") == 0 ? true : false);
	}

	/* columnstore */
	if (values[6] != NULL)
	{
		if (relkind != RELKIND_RELATION && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"orientation\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (!appendonly && validate)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \"orientation\" for base relation. "
							"Only valid for Append Only relations"),
									   errOmitLocation(true)));

		if (!(pg_strcasecmp(values[6], "column") == 0 ||
			  pg_strcasecmp(values[6], "row") == 0) &&
			validate)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value for \"orientation\": \"%s\"",
							values[6]),
									   errOmitLocation(true)));
		}

		columnstore = (pg_strcasecmp(values[6], "column") == 0 ? true : false);
		
		if (compresstype && 
			(pg_strcasecmp(compresstype, "rle_type") == 0) &&
			! columnstore)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("%s cannot be used with Append Only relations row orientation",
								compresstype),
						 errOmitLocation(true)));
		}
	}

	/* errortable */
	if (values[7] != NULL)
	{
		if (relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"errortable\" in a non relation object is not supported"),
					 errOmitLocation(false)));

		if (appendonly)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("invalid option \"errortable\" for base relation. "
							"Only valid for heap relations"),
									   errOmitLocation(true)));

		if (!(pg_strcasecmp(values[7], "true") == 0 ||
			  pg_strcasecmp(values[7], "false") == 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid parameter value for \"errortable\": \"%s\"",
							values[7]),
									   errOmitLocation(true)));
		}

		errorTable = (pg_strcasecmp(values[7], "true") == 0 ? true : false);
	}

	result = (StdRdOptions *) palloc(sizeof(StdRdOptions));
	SET_VARSIZE(result, sizeof(StdRdOptions));

	result->fillfactor = fillfactor;
	result->appendonly = appendonly;
	result->blocksize = blocksize;
	result->compresslevel = compresslevel;
	if (compresstype != NULL)
		for (j = 0;j < strlen(compresstype); j++)
			compresstype[j] = pg_tolower(compresstype[j]);
	result->compresstype = compresstype;
	result->checksum = checksum;
	result->columnstore = columnstore;
	result->forceHeap = forceHeap;
	result->errorTable = errorTable;

	return (bytea *) result;
}

/**
 *  This function parses the tidycat option.
 *  In the tidycat definition, the WITH clause contains "shared",
 *  "reloid", etc. Those are the tidycat option.
 */
TidycatOptions*
tidycat_reloptions(Datum reloptions)
{
	static const char *const default_keywords[] = {
		/* tidycat option for table */
		"relid",
		"reltype_oid",
		"toast_oid",
		"toast_index",
		"toast_reltype",

		/* tidycat option for index */
		"indexid",
	};

	TidycatOptions *result;
	char	       *values[ARRAY_SIZE(default_keywords)];

	parseRelOptions(reloptions, ARRAY_SIZE(default_keywords), default_keywords, values, false);

	result = (TidycatOptions *) palloc(sizeof(TidycatOptions));
	result->relid         = (values[0] != NULL) ? pg_atoi(values[0], sizeof(int32), 0):InvalidOid;
	result->reltype_oid   = (values[1] != NULL) ? pg_atoi(values[1], sizeof(int32), 0):InvalidOid;
	result->toast_oid     = (values[2] != NULL) ? pg_atoi(values[2], sizeof(int32), 0):InvalidOid;
	result->toast_index   = (values[3] != NULL) ? pg_atoi(values[3], sizeof(int32), 0):InvalidOid;
	result->toast_reltype = (values[4] != NULL) ? pg_atoi(values[4], sizeof(int32), 0):InvalidOid;
	result->indexid       = (values[5] != NULL) ? pg_atoi(values[5], sizeof(int32), 0):InvalidOid;

	return result;
}

void
heap_test_override_reloptions(char relkind, StdRdOptions *stdRdOptions, int *safewrite)
{
	 char* default_compressor = "zlib";

	if (IsBootstrapProcessingMode())
		return;

	if (relkind != RELKIND_RELATION)
		return;

	/*
	 * Normally, the default is a regular table.
	 * If the override is on, change the default to be an appendonly
	 * table.
	 */
	if (!stdRdOptions->appendonly && Test_appendonly_override)
	{
		stdRdOptions->appendonly = true;
	}

	if (!stdRdOptions->appendonly)
		return;

	/*
	 * Normally, appendonly tables are not compressed by default.
	 * If the override is on, compress it with the override value.
	 */
	if (Test_compresslevel_override != DEFAULT_COMPRESS_LEVEL)
	{
		stdRdOptions->compresslevel = Test_compresslevel_override;

		stdRdOptions->compresstype = default_compressor;
	}

	/*
	 * Normally, appendonly tables are created with the default 32KB block
	 * size. If the override has a different value - use the override value.
	 */
	if (Test_blocksize_override != DEFAULT_APPENDONLY_BLOCK_SIZE)
	{
		stdRdOptions->blocksize = Test_blocksize_override;
	}

	/*
	 * Normally, appendonly tables don't use checksum on the data.
	 * If the override is specified - use the override value.
	 */
	if (Test_checksum_override)
	{
		stdRdOptions->checksum = Test_checksum_override;
	}

	/*
	 * Same for safefswritesize.
	 */
	if (Test_safefswritesize_override != DEFAULT_FS_SAFE_WRITE_SIZE)
	{
		*safewrite = Test_safefswritesize_override;
	}

    /*
     * Testing for columnstore override
     */
    if (gp_test_orientation_override)
    {
        stdRdOptions->columnstore = true;
    }
}

/*
 * Parse options for heaps (and perhaps someday toast tables).
 */
bytea *
heap_reloptions(char relkind __attribute__((unused)), Datum reloptions, bool validate)
{
	return default_reloptions(reloptions, validate,
							  RELKIND_RELATION,
							  HEAP_MIN_FILLFACTOR,
							  HEAP_DEFAULT_FILLFACTOR);
}


/*
 * Parse options for indexes.
 *
 *	amoptions	Oid of option parser
 *	reloptions	options as text[] datum
 *	validate	error flag
 */
bytea *
index_reloptions(RegProcedure amoptions, Datum reloptions, bool validate)
{
	FmgrInfo	flinfo;
	FunctionCallInfoData fcinfo;
	Datum		result;

	Assert(RegProcedureIsValid(amoptions));

	/* Assume function is strict */
	if (DatumGetPointer(reloptions) == 0)
		return NULL;

	/* Can't use OidFunctionCallN because we might get a NULL result */
	fmgr_info(amoptions, &flinfo);

	InitFunctionCallInfoData(fcinfo, &flinfo, 2, NULL, NULL);

	fcinfo.arg[0] = reloptions;
	fcinfo.arg[1] = BoolGetDatum(validate);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	result = FunctionCallInvoke(&fcinfo);

	if (fcinfo.isnull || DatumGetPointer(result) == NULL)
		return NULL;

	return DatumGetByteaP(result);
}

/*
 * validateAppendOnlyRelOptions
 *
 *		Various checks for validity of appendonly relation rules.
 */
void validateAppendOnlyRelOptions(bool ao,
								  int blocksize,
								  int safewrite,
								  int complevel,
								  char* comptype,
								  bool checksum,
								  char relkind,
								  bool co)
{
	if (relkind != RELKIND_RELATION)
	{
		if(ao)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("appendonly may only be specified for base relations")));

		if(checksum)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("checksum may only be specified for base relations")));

		if(comptype)
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_SUPPORTED),
					 errmsg("compresstype may only be specified for base relations")));
	}

	if (comptype &&
		(pg_strcasecmp(comptype, "quicklz") == 0 ||
		 pg_strcasecmp(comptype, "zlib") == 0 ||
		 pg_strcasecmp(comptype, "rle_type") == 0))
	{
		
		if (! co &&
			pg_strcasecmp(comptype, "rle_type") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s cannot be used with Append Only relations row orientation",
							comptype)));			
		}
		
		if (comptype && complevel == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresstype cannot be used with compresslevel 0")));

		if (complevel < 0 || complevel > 9)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresslevel=%d is out of range (should be between 0 and 9)",
							complevel)));

		if (comptype && (pg_strcasecmp(comptype, "quicklz") == 0) &&
			(complevel != 1))
		{
			/* allow quicklz level 3 only if the debug guc is on */
			if (!(complevel == 3 && Test_enable_broken_quicklz3))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("compresslevel=%d is out of range for quicklz "
								 "(should be 1)", complevel)));
			}
		}
		if (comptype && (pg_strcasecmp(comptype, "rle_type") == 0) &&
			(complevel > 4))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresslevel=%d is out of range for rle_type "
							"(should be in the range 1 to 4)", complevel)));
		}		
	}

	if (blocksize < MIN_APPENDONLY_BLOCK_SIZE || blocksize > MAX_APPENDONLY_BLOCK_SIZE ||
	    blocksize % MIN_APPENDONLY_BLOCK_SIZE != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block size must be between 8KB and 2MB and "
						"be an 8KB multiple, Got %d", blocksize)));

	if (safewrite > MAX_APPENDONLY_BLOCK_SIZE || safewrite % 8 != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("safefswrite size must be less than 8MB and "
						"be a multiple of 8")));

	if (gp_safefswritesize > blocksize)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block size (%d) is smaller gp_safefswritesize (%d). "
						"increase blocksize or decrease gp_safefswritesize if it "
						"is safe to do so on this file system",
						blocksize, gp_safefswritesize)));
}

/*
 * if no compressor type was specified, we set to no compression (level 0)
 * otherwise default for both zlib and quicklz is level 1. RLE_TYPE does
 * not have a compression level.
 */
static int setDefaultCompressionLevel(char* compresstype)
{
	if(!compresstype || pg_strcasecmp(compresstype, "none") == 0)
		return 0;
	else
		return 1;
}




