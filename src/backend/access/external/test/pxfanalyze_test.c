#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../pxfanalyze.c"
#include "catalog/pg_exttable.h"


static void runTest__calculateSamplingRatio(float4 relTuples, float4 relFrags, float4 requestedSampleSize,
		int maxFrags, float4 expectedResult);
static void runTest__createPxfSampleStmt(float4 pxf_sample_ratio,
		const char* expectedRatio,
		char fmtcode,
		const char* expectedFmt,
		const char* fmtopts,
		const char* expectedFmtopts,
		int rejectLimit);


static void runTest__calculateSamplingRatio(float4 relTuples, float4 relFrags, float4 requestedSampleSize,
		int maxFrags, float4 expectedResult)
{
	int pxf_stat_max_fragments_orig = pxf_stat_max_fragments;

	pxf_stat_max_fragments = maxFrags;

	float4 result = calculateSamplingRatio(relTuples, relFrags, requestedSampleSize);

	pxf_stat_max_fragments = pxf_stat_max_fragments_orig;

	assert_true(fabs(expectedResult - result) <= 0.00001);
}

void
test__calculateSamplingRatio__relFragsLTmaxFrags(void **state)
{
	/*
	 * original ratio: 20,000/100,000 = 0.20
	 */
	runTest__calculateSamplingRatio(100000, 1000, 20000, 1500, 0.2);
}

void
test__calculateSamplingRatio__relFragsGTmaxFrags(void **state)
{
	/*
	 * original ratio: 20,000/100,000 = 0.20
	 * corrected ratio: 0.20*(1000/900) ~ 0.22
	 */
	runTest__calculateSamplingRatio(100000, 1000, 20000, 900, 0.222223);
}

void
test__calculateSamplingRatio__ratioGT1(void **state)
{
	/*
	 * original ratio: 20,000/100,000 = 0.20
	 * corrected ratio: 0.20*(1000/100)=2.0 -> 1.0
	 */
	runTest__calculateSamplingRatio(100000, 1000, 20000, 100, 1.0);
}

void
test__calculateSamplingRatio__ratioTooLow(void **state)
{
	/*
	 * original ratio: 2,000/100,000,000 = 0.00002
	 * corrected ratio: 0.0001
	 */
	runTest__calculateSamplingRatio(100000000, 1000, 2000, 1000, 0.0001);
}

static void runTest__createPxfSampleStmt(float4 pxf_sample_ratio,
		const char* expectedRatio,
		char fmtcode,
		const char* expectedFmt,
		const char* fmtopts,
		const char* expectedFmtopts,
		int rejectLimit)
{
		/* input */
		Oid relationOid = 13;
		const char* schemaName = "orig_schema";
		const char* tableName = "orig_table";
		const char* sampleSchemaName = "sample_schema";
		const char* pxfSampleTable = "pxf_sample_table";

		int pxf_max_fragments = 1000;

		const char* location = "the_table-s_location";

		Value* locationValue = (Value *) palloc0(sizeof(Value));
		locationValue->type = T_String;
		locationValue->val.str = location;

		ExtTableEntry *extTable = (ExtTableEntry *) palloc0(sizeof(ExtTableEntry));
		extTable->encoding = 6;
		extTable->fmtcode = fmtcode;
		extTable->fmtopts = fmtopts;
		extTable->rejectlimit = rejectLimit;
		extTable->locations = lappend(extTable->locations, locationValue);

		/* get fake external table details */
		expect_value(GetExtTableEntry, relid, relationOid);
		will_return(GetExtTableEntry, extTable);

		char* expectedResult = palloc0(1024);
		sprintf(expectedResult,
				"CREATE EXTERNAL TABLE %s.%s (LIKE %s.%s) "
				"LOCATION(E'%s&STATS-SAMPLE-RATIO=%s&STATS-MAX-FRAGMENTS=%d') "
				"FORMAT '%s' (%s) "
				"ENCODING 'UTF8' "
				"%s",
				sampleSchemaName, pxfSampleTable, schemaName, tableName,
				location, expectedRatio, pxf_max_fragments,
				expectedFmt, expectedFmtopts,
				(rejectLimit != -1) ? "SEGMENT REJECT LIMIT 25 PERCENT " : "");

		char* result = createPxfSampleStmt(relationOid, schemaName, tableName, sampleSchemaName, pxfSampleTable,
				pxf_sample_ratio, pxf_max_fragments);

		assert_string_equal(expectedResult, result);

		pfree(locationValue);
		pfree(extTable);
		pfree(expectedResult);
		pfree(result);
}

void
test__createPxfSampleStmt__textFormat(void **state)
{
	const char* fmtopts = "delimiter ',' null '\\N' escape '\\'";
	const char* expectedFmtopts = "delimiter E',' null E'\\\\N' escape E'\\\\'";
	runTest__createPxfSampleStmt(0.12, "0.1200", 't', "text", fmtopts, expectedFmtopts, 30);
}

void
test__createPxfSampleStmt__customFormatNoRejectLimit(void **state)
{
	const char* fmtopts = "formatter 'pxfwritable_import'";
	const char* expectedFmtopts = "formatter = 'pxfwritable_import'";
	runTest__createPxfSampleStmt(0.5555555, "0.5556", 'b', "custom", fmtopts, expectedFmtopts, -1);
}

void
test__createPxfSampleStmt__csvFormatUnprintableOptions(void **state)
{
	const char* fmtopts = "delimiter '\x01' null '\\N' escape '\x02\x03'";
	const char* expectedFmtopts = "delimiter E'\\x01' null E'\\\\N' escape E'\\x02\\x03'";
	runTest__createPxfSampleStmt(0.003, "0.0030", 'c', "csv", fmtopts, expectedFmtopts, 100);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__calculateSamplingRatio__relFragsLTmaxFrags),
			unit_test(test__calculateSamplingRatio__relFragsGTmaxFrags),
			unit_test(test__calculateSamplingRatio__ratioGT1),
			unit_test(test__calculateSamplingRatio__ratioTooLow),
			unit_test(test__createPxfSampleStmt__textFormat),
			unit_test(test__createPxfSampleStmt__customFormatNoRejectLimit),
			unit_test(test__createPxfSampleStmt__csvFormatUnprintableOptions)
	};
	return run_tests(tests);
}
