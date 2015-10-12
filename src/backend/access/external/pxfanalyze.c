/*-------------------------------------------------------------------------
 *
 * pxfanalyze.c
 *	  Helper functions to perform ANALYZE on PXF tables.
 *
 * Copyright (c) 2007-2012, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <curl/curl.h>
#include <json/json.h>
#include "access/pxfanalyze.h"
#include "catalog/namespace.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbanalyze.h"
#include "commands/analyzeutils.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"


static void buildPxfTableCopy(Oid relationOid,
		float4	samplingRatio,
		int pxfStatMaxFragments,
		const char* schemaName, const char* tableName,
		const char* sampleSchemaName, const char* pxfSampleTable);
static void buildSampleFromPxf(const char* sampleSchemaName,
		const char* sampleTableName,
		const char* pxfSampleTable,
		List *lAttributeNames,
		float4 *sampleTableRelTuples);

static float4 calculateSamplingRatio(float4 relTuples,
									 float4 relFrags,
									 float4 requestedSampleSize);

static char* parseFormat(char fmtcode);
static char* escape_unprintables(const char *src);
static char* escape_fmtopts_string(const char *src);
static char* custom_fmtopts_string(const char *src);
static void printExtTable(Oid relationOid, ExtTableEntry* extTable);
static char* createPxfSampleStmt(Oid relationOid,
		const char* schemaName, const char* tableName,
		const char* sampleSchemaName, const char* pxfSampleTable,
		float4 pxf_sample_ratio, int pxf_max_fragments);
static float4 getPxfFragmentTupleCount(Oid relationOid);
static float4 countFirstFragmentTuples(const char* schemaName,
									   const char* tableName);


void analyzePxfEstimateReltuplesRelpagesRelFrags(Relation relation,
		StringInfo location,
		float4* estimatedRelTuples,
		float4* estimatedRelPages,
		float4* relFrags,
		StringInfo err_msg)
{

	float4 firstFragTuples = 0.0;
	float4 estimatedTuples = 0.0;

	/* get number of fragments and pages (tuples will be ignored) */
	/* TODO: return the number of fragments, the size of the first fragment and the total size
	 *  - tuple estimate can be calculated based on that */
	gp_statistics_estimate_reltuples_relpages_relfrags_external_pxf(relation, location, estimatedRelTuples, estimatedRelPages, relFrags, err_msg);
	if (err_msg->len > 0)
		return;

	/* get number of tuples from first fragment */
	firstFragTuples = getPxfFragmentTupleCount(relation->rd_id);

	/* calculate estimated tuple count */
	if (firstFragTuples > 0)
		estimatedTuples = firstFragTuples * (*relFrags);

	elog(DEBUG2, "Estimated tuples for PXF table: %f. (first fragment count %f, fragments number %f, old estimate %f)",
		 estimatedTuples, firstFragTuples, *relFrags, *estimatedRelTuples);
	*estimatedRelTuples = estimatedTuples;
}

/*
 * Creates a sample table with data from a PXF table.
 * We need to create a copy of the PXF table, in order to pass the sampling
 * parameters pxf_sample_ratio and pxf_max_fragments as attributes,
 * and to create a segment reject limit of 25 percent.
 *
 * The new PXF table is sampled and the results are saved in the returned sample table.
 * Note that ANALYZE can be executed only by the database owner.
 * It is safe to assume that the database owner has permissions to create temp tables.
 * The sampling is done by uniformly sampling pxf_sample_ratio records of each fragments,
 * up to pxf_max_fragments.
 *
 * Input:
 * 	relationOid 	- relation to be sampled
 * 	sampleTableName - sample table name, moderately unique
 * 	lAttributeNames - attributes to be included in the sample
 * 	relTuples		- estimated size of relation
 * 	relFrags		- estimated number of fragments in relation
 * 	requestedSampleSize - as determined by attribute statistics requirements.
 * 	sampleTableRelTuples	- limit on size of the sample.
 * Output:
 * 	sampleTableRelTuples - number of tuples in the sample table created.
 */
Oid buildPxfSampleTable(Oid relationOid,
		char* sampleTableName,
		List *lAttributeNames,
		float4	relTuples,
		float4  relFrags,
		float4 	requestedSampleSize,
		float4 *sampleTableRelTuples)
{
	const char *schemaName = get_namespace_name(get_rel_namespace(relationOid)); /* must be pfreed */
	const char *tableName = get_rel_name(relationOid); /* must be pfreed */
	char	*sampleSchemaName = pstrdup("pg_temp");
	char	*pxfSampleTable = temporarySampleTableName(relationOid, "pg_analyze_pxf"); /* must be pfreed */
	Oid			sampleTableOid = InvalidOid;
	Oid			pxfSampleTableOid = InvalidOid;
	RangeVar 	*rangeVar = NULL;
	float4 pxfSamplingRatio = 0.0;

	Assert(requestedSampleSize > 0.0);
	Assert(relTuples > 0.0);

	/* calculate pxf_sample_ratio */
	pxfSamplingRatio = calculateSamplingRatio(relTuples, relFrags, requestedSampleSize);

	/* build copy of original pxf table */
	buildPxfTableCopy(relationOid,
					  pxfSamplingRatio,
					  pxf_stat_max_fragments,
					  schemaName, tableName,
					  sampleSchemaName, pxfSampleTable);

	rangeVar = makeRangeVar(NULL /*catalogname*/, sampleSchemaName, pxfSampleTable, -1);
	pxfSampleTableOid = RangeVarGetRelid(rangeVar, true /* failOK */, false /*allowHcatalog*/);

	buildSampleFromPxf(sampleSchemaName, sampleTableName, pxfSampleTable,
					   lAttributeNames, sampleTableRelTuples);

	rangeVar = makeRangeVar(NULL /*catalogname*/, sampleSchemaName, sampleTableName, -1);
	sampleTableOid = RangeVarGetRelid(rangeVar, true /* failOK */, false /*allowHcatalog*/);

	Assert(sampleTableOid != InvalidOid);

	/**
	 * MPP-10723: Very rarely, we may be unlucky and generate an empty sample table. We error out in this case rather than
	 * generate bad statistics.
	 */

	if (*sampleTableRelTuples < 1.0)
	{
		elog(ERROR, "ANALYZE unable to generate accurate statistics on table %s.%s. Try lowering gp_analyze_relative_error",
				quote_identifier(schemaName),
				quote_identifier(tableName));
	}

	if (pxfSampleTableOid != InvalidOid)
	{
		elog(DEBUG2, "ANALYZE dropping PXF sample table");
		dropSampleTable(pxfSampleTableOid, true);
	}

	pfree((void *) rangeVar);
	pfree((void *) pxfSampleTable);
	pfree((void *) tableName);
	pfree((void *) schemaName);
	pfree((void *) sampleSchemaName);
	return sampleTableOid;
}

/*
 * Creates an external PXF table, with the same properties
 * as the given PXF table to be sampled, other than additional
 * 2 attributes in the location clause -
 * pxf_stats_sample_ratio and pxf_stats_max_fragments,
 * and a segment reject limit of 25 percent.
 */
static void buildPxfTableCopy(Oid relationOid,
		float4 samplingRatio,
		int pxfStatMaxFragments,
		const char* schemaName, const char* tableName,
		const char* sampleSchemaName, const char* pxfSampleTable)
{

	/* create table string */
	char* createPxfSampleStr = createPxfSampleStmt(relationOid,
			schemaName, tableName,
			sampleSchemaName, pxfSampleTable,
			samplingRatio, pxfStatMaxFragments);

	spiExecuteWithCallback(createPxfSampleStr, false /*readonly*/, 0 /*tcount */,
			NULL, NULL);

	pfree(createPxfSampleStr);

	elog(DEBUG2, "Created PXF table %s.%s for sampling PXF table %s.%s",
			quote_identifier(sampleSchemaName),
			quote_identifier(pxfSampleTable),
			quote_identifier(schemaName),
			quote_identifier(tableName));
}

/*
 * Creates and populates a sample table for a PXF table.
 * The actual queried table is not the original PXF table but a copy of it
 * with additional attributes to enable sampling.
 *
 * The results are stored in sampleTableRelTuples.
 */
static void buildSampleFromPxf(const char* sampleSchemaName,
		const char* sampleTableName,
		const char* pxfSampleTable,
		List *lAttributeNames,
		float4 *sampleTableRelTuples)
{
	int nAttributes = -1;
	int i = 0;
	ListCell *le = NULL;
	StringInfoData str;

	initStringInfo(&str);

	appendStringInfo(&str, "create table %s.%s as (select ",
			quote_identifier(sampleSchemaName), quote_identifier(sampleTableName));

	nAttributes = list_length(lAttributeNames);

	foreach_with_count(le, lAttributeNames, i)
	{
		appendStringInfo(&str, "Ta.%s", quote_identifier((const char *) lfirst(le)));
		if (i < nAttributes - 1)
		{
			appendStringInfo(&str, ", ");
		}
		else
		{
			appendStringInfo(&str, " ");
		}
	}

	appendStringInfo(&str, "from %s.%s as Ta) distributed randomly",
			quote_identifier(sampleSchemaName),
			quote_identifier(pxfSampleTable));

	/* in case of PXF error, analyze on this table will reverted */
	spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
			spiCallback_getProcessedAsFloat4, sampleTableRelTuples);

	pfree(str.data);

	elog(DEBUG2, "Created sample table %s.%s with nrows=%.0f",
			quote_identifier(sampleSchemaName),
			quote_identifier(sampleTableName),
			*sampleTableRelTuples);
}

/*
 * Returns a sampling ratio - a fraction between 1.0 and 0.0001
 * representing how many samples should be returned from each fragment
 * of a PXF table.
 * The ratio is calculated based on the tuples estimate of the table
 * and on the number of the actually sampled fragments
 * (GUC pxf_stat_max_fragments), by the following formula:
 * ratio = (<sample size> / <tuples estimate>) * (<total # fragments> / <fragments to be sampled>)
 * If the ratio is too big or small, it is corrected to 1.0 or 0.0001 respectively.
 *
 * Input:
 * 	relTuples		- number of tuples in the table
 * 	relFrags		- number of fragments in the table
 * 	requestedSampleSize - number of sample tuples required
 * Output:
 * 	the sampling ratio for the table.
 */
static float4 calculateSamplingRatio(float4 relTuples,
		 float4 relFrags,
		 float4 requestedSampleSize)
{
	float4 sampleRatio = 0.0;

	Assert(relFrags > 0);
	Assert(relTuples > 0);
	Assert(requestedSampleSize > 0);

	/* sample ratio for regular tables */
	sampleRatio = requestedSampleSize / relTuples;

	if (pxf_stat_max_fragments < relFrags)
	{
		/*
		 * Correct ratio according to the number of sampled fragments.
		 * If there are less fragments to sample, the ratio should be increased.
		 * If the corrected sampling ratio is > 100%, make it 100%
		 */
		sampleRatio = sampleRatio * (relFrags / pxf_stat_max_fragments);
		if (sampleRatio > 1.0)
		{
			sampleRatio = 1.0;
		}
	}

	/*
	 * If the ratio is too low (< 0.0001), correct it to 0.0001.
	 * That means that the lowest rate we will get is 1 tuple per 10,000.
	 */
	if (sampleRatio < 0.0001)
	{
		sampleRatio = 0.0001;
	}

	elog(DEBUG2, "PXF ANALYZE: pxf_stats_sample_ratio = %f, pxf_stats_max_fragments = %d, table fragments = %f",
		 sampleRatio, pxf_stat_max_fragments, relFrags);
	return sampleRatio;
}

static char* parseFormat(char fmtcode)
{
	if (fmttype_is_custom(fmtcode))
		return "CUSTOM";
	if (fmttype_is_text(fmtcode))
		return "TEXT";
	if (fmttype_is_csv(fmtcode))
		return "CSV";

	elog(ERROR, "Unrecognized external table format '%c'", fmtcode);
	return NULL;
}

/* Helper functions from dumputils.c, modified to backend (malloc->palloc) */

/*
 * Escape any unprintables (0x00 - 0x1F) in given string
 */
char *
escape_unprintables(const char *src)
{
	int			len = strlen(src),
				i,
				j;
	char	   *result = palloc0(len * 4 + 1);
	if (!result)
		return NULL; /* out of memory */

	for (i = 0, j = 0; i < len; i++)
	{
		if ((src[i] <= '\x1F') && (src[i] != '\x09' /* TAB */))
		{
			snprintf(&(result[j]), 5, "\\x%02X", src[i]);
			j += 4;
		}
		else
			result[j++] = src[i];
	}
	result[j] = '\0';
	return result;
}

/*
 * Escape backslashes and apostrophes in EXTERNAL TABLE format strings.
 *
 * The fmtopts field of a pg_exttable tuple has an odd encoding -- it is
 * partially parsed and contains "string" values that aren't legal SQL.
 * Each string value is delimited by apostrophes and is usually, but not
 * always, a single character.	The fmtopts field is typically something
 * like {delimiter '\x09' null '\N' escape '\'} or
 * {delimiter ',' null '' escape '\' quote '''}.  Each backslash and
 * apostrophe in a string must be escaped and each string must be
 * prepended with an 'E' denoting an "escape syntax" string.
 *
 * Usage note: A field value containing an apostrophe followed by a space
 * will throw this algorithm off -- it presumes no embedded spaces.
 */
static char* escape_fmtopts_string(const char *src)
{
	int			len = strlen(src);
	int			i;
	int			j;
	char	   *result = palloc0(len * 2 + 1);
	bool		inString = false;

	for (i = 0, j = 0; i < len; i++)
	{
		switch (src[i])
		{
			case '\'':
				if (inString)
				{
					/*
					 * Escape apostrophes *within* the string. If the
					 * apostrophe is at the end of the source string or is
					 * followed by a space, it is presumed to be a closing
					 * apostrophe and is not escaped.
					 */
					if ((i + 1) == len || src[i + 1] == ' ')
						inString = false;
					else
						result[j++] = '\\';
				}
				else
				{
					result[j++] = 'E';
					inString = true;
				}
				break;
			case '\\':
				result[j++] = '\\';
				break;
		}

		result[j++] = src[i];
	}

	result[j] = '\0';
	return result;
}

/*
 * Tokenize a fmtopts string (for use with 'custom' formatters)
 * i.e. convert it to: a = b, format.
 * (e.g.:  formatter E'fixedwidth_in null E' ' preserve_blanks E'on')
 */
static char* custom_fmtopts_string(const char *src)
{
		int			len = src ? strlen(src) : 0;
		char	   *result = palloc0(len * 2 + 1);
		char	   *srcdup = src ? pstrdup(src) : NULL;
		char	   *srcdup_start = srcdup;
		char       *find_res = NULL;
		int        last = 0;

		if(!src || !srcdup || !result)
			return NULL;

		while (srcdup)
		{
			/* find first word (a) */
			find_res = strchr(srcdup, ' ');
			if (!find_res)
				break;
			strncat(result, srcdup, (find_res - srcdup));
			/* skip space */
			srcdup = find_res + 1;
			/* remove E if E' */
			if((strlen(srcdup) > 2) && (srcdup[0] == 'E') && (srcdup[1] == '\''))
				srcdup++;
			/* add " = " */
			strncat(result, " = ", 3);
			/* find second word (b) until second '
			   find \' combinations and ignore them */
			find_res = strchr(srcdup + 1, '\'');
			while (find_res && (*(find_res - 1) == '\\') /* ignore \' */)
			{
				find_res = strchr(find_res + 1, '\'');
			}
			if (!find_res)
				break;
			strncat(result, srcdup, (find_res - srcdup + 1));
			srcdup = find_res + 1;
			/* skip space and add ',' */
			if (srcdup && srcdup[0] == ' ')
			{
				srcdup++;
				strncat(result, ",", 1);
			}
		}

		/* fix string - remove trailing ',' or '=' */
		last = strlen(result)-1;
		if(result[last] == ',' || result[last] == '=')
			result[last]='\0';

		pfree(srcdup_start);
		return result;
}

static void printExtTable(Oid relationOid, ExtTableEntry* extTable)
{

	if (extTable == NULL)
		return;

	elog(DEBUG2, "extTable params: oid: %d command: %s, encoding: %d, "
			"format: %c (%s), error table oid: %d, format options: %s, "
			"is web: %d, is writable: %d, locations size: %d, "
			"reject limit: %d, reject limit type: %c",
			relationOid,
			extTable->command ? extTable->command : "NULL",
			extTable->encoding,
			extTable->fmtcode,
			parseFormat(extTable->fmtcode),
			extTable->fmterrtbl,
			extTable->fmtopts,
			extTable->isweb,
			extTable->iswritable,
			list_length(extTable->locations),
			extTable->rejectlimit,
			extTable->rejectlimittype == -1 ? 'n' : extTable->rejectlimittype);
}

/*
 * This method returns an SQL command to create a PXF table
 * which is a copy of a given PXF table relationOid, with the following changes:
 * - PXF sample table name is pg_temp.pg_analyze_pxf_<relationOid>
 * - LOCATION part is appended 2 attributes - pxf_sample_ratio, pxf_max_fragments.
 * - in case of error table - SEGMENT REJECT LIMIT 25 PERCENT
 *
 * Input:
 * 	relationOid 		- relation to be copied
 * 	schemaName 			- schema name of original table
 * 	tableName			- table name of original table
 * 	sampleSchemaName	- schema name of new table
 * 	pxfSampleTable		- table name or new table
 * 	pxf_sample_ratio	- ratio of samplings to be done per fragment
 * 	pxf_max_fragments	- max number of fragments to be sampled
 * Output:
 * 	SQL statement string for creating the new table
 */
static char* createPxfSampleStmt(Oid relationOid,
		const char* schemaName, const char* tableName,
		const char* sampleSchemaName, const char* pxfSampleTable,
		float4 pxf_sample_ratio, int pxf_max_fragments)
{
	ExtTableEntry *extTable = GetExtTableEntry(relationOid);
	StringInfoData str;
	initStringInfo(&str);
	char* location = NULL;
	char* tmpstring = NULL;
	char* escapedfmt = NULL;
	char* tabfmt = NULL;
	char* customfmt = NULL;

	printExtTable(relationOid, extTable);

	location = escape_unprintables(((Value*)list_nth(extTable->locations, 0))->val.str /*pxfLocation*/);

	appendStringInfo(&str, "CREATE EXTERNAL TABLE %s.%s (LIKE %s.%s) "
			"LOCATION(E'%s&STATS-SAMPLE-RATIO=%.4f&STATS-MAX-FRAGMENTS=%d') ",
			quote_identifier(sampleSchemaName),
			quote_identifier(pxfSampleTable),
			quote_identifier(schemaName),
			quote_identifier(tableName),
			location,
			pxf_sample_ratio,
			pxf_max_fragments);

	pfree(location);

	/* add FORMAT clause */
	escapedfmt = escape_fmtopts_string((const char *) extTable->fmtopts);
	tmpstring = escape_unprintables((const char *) escapedfmt);
	pfree(escapedfmt);
	escapedfmt = NULL;

	switch (extTable->fmtcode)
	{
	case 't':
		tabfmt = "text";
		break;
	case 'b':
		/*
		 * b denotes that a custom format is used.
		 * the fmtopts string should be formatted as:
		 * a1 = 'val1',...,an = 'valn'
		 *
		 */
		tabfmt = "custom";
		customfmt = custom_fmtopts_string(tmpstring);
		break;
	default:
		tabfmt = "csv";
	}
	appendStringInfo(&str, "FORMAT '%s' (%s) ",
			tabfmt,
			customfmt ? customfmt : tmpstring);
	pfree(tmpstring);
	tmpstring = NULL;
	if (customfmt)
	{
		pfree(customfmt);
		customfmt = NULL;
	}
	/* add ENCODING clause */
	appendStringInfo(&str, "ENCODING '%s' ", pg_encoding_to_char(extTable->encoding));

	/* add error control clause */
	if (extTable->rejectlimit != -1)
	{
		appendStringInfo(&str, "%s", "SEGMENT REJECT LIMIT 25 PERCENT ");
	}

	elog(DEBUG2, "createPxfSampleStmt SQL statement: %s", str.data);

	return str.data;
}

/*
 * Returns the number of tuples in the first fragment of given
 * PXF table.
 * This is done by creating a copy of the PXF table, with additional parameters
 * limiting the query to the first fragment only (pxf_max_fragments = 1, pxf_sample_ratio = 1.0),
 * and running a COUNT query on it.
 * The tuple count result is returned.
 *
 * Input:
 * 	relationOid 	- relation to be sampled
 */
static float4 getPxfFragmentTupleCount(Oid relationOid)
{
	const char *schemaName = get_namespace_name(get_rel_namespace(relationOid)); /* must be pfreed */
	const char *tableName = get_rel_name(relationOid); /* must be pfreed */
	char	*sampleSchemaName = pstrdup("pg_temp");
	char	*pxfEstimateTable = temporarySampleTableName(relationOid, "pg_analyze_pxf_est"); /* must be pfreed */
	Oid			pxfEstimateTableOid = InvalidOid;
	RangeVar 	*rangeVar = NULL;
	float4	ntuples = -1.0;

	/* build copy of original pxf table */
	buildPxfTableCopy(relationOid,
					  1.0, /* get all tuples */
					  1, /* query only first fragment */
					  schemaName, tableName,
					  sampleSchemaName, pxfEstimateTable);

	rangeVar = makeRangeVar(NULL /*catalogname*/, sampleSchemaName, pxfEstimateTable, -1);
	pxfEstimateTableOid = RangeVarGetRelid(rangeVar, true /* failOK */, false /*allowHcatalog*/);

	if (pxfEstimateTableOid == InvalidOid)
	{
		elog(ERROR, "Unable to create a copy of PXF table %s.%s", schemaName, tableName);
	}

	/* run count query */
	ntuples = countFirstFragmentTuples(sampleSchemaName, pxfEstimateTable);

	Assert(pxfEstimateTable != InvalidOid);

	elog(DEBUG2, "ANALYZE dropping PXF estimate table %s.%s (%d)",
		 sampleSchemaName, pxfEstimateTable, pxfEstimateTableOid);
	dropSampleTable(pxfEstimateTableOid, true);

	pfree((void *) rangeVar);
	pfree((void *) pxfEstimateTable);
	pfree((void *) tableName);
	pfree((void *) schemaName);
	pfree((void *) sampleSchemaName);

	return ntuples;
}

static float4 countFirstFragmentTuples(const char* schemaName,
									   const char* tableName)
{
	float ntuples = -1.0;
	StringInfoData str;

	initStringInfo(&str);
	appendStringInfo(&str, "select count(*)::float4 from %s.%s",
			quote_identifier(schemaName),
			quote_identifier(tableName));

	/* in case of PXF error, analyze on this table will be reverted */
	spiExecuteWithCallback(str.data, false /*readonly*/, 0 /*tcount */,
						   spiCallback_getSingleResultRowColumnAsFloat4, &ntuples);

	pfree(str.data);

	elog(DEBUG3, "count() of first pxf fragment gives %f values.", ntuples);

	return ntuples;
}
