#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../cdb_dump_util.c"

void 
test__isFilteringAllowed1(void **state)
{
	int role = ROLE_MASTER;
	bool incremental = true;
	bool result = isFilteringAllowedNow(role, incremental);
	assert_false(result);
}

void 
test__isFilteringAllowed2(void **state)
{
	int role = ROLE_MASTER;
	bool incremental = false;
	bool result = isFilteringAllowedNow(role, incremental);
	assert_true(result);
}

void 
test__isFilteringAllowed3(void **state)
{
	int role = ROLE_SEGDB;
	bool incremental = true;
	bool result = isFilteringAllowedNow(role, incremental);
	assert_true(result);
}

void 
test__isFilteringAllowed4(void **state)
{
	int role = ROLE_SEGDB;
	bool incremental = false;
	bool result = isFilteringAllowedNow(role, incremental);
	assert_true(result);
}

void 
test__backupTypeString5(void **state)
{
	bool incremental = false;
	const char* result = getBackupTypeString(incremental);
	assert_true(strcmp(result, "Backup Type: Full") == 0);
}

void 
test__backupTypeString6(void **state)
{
	bool incremental = true;
	const char* result = getBackupTypeString(incremental);
	assert_true(strcmp(result, "Backup Type: Incremental") == 0);
}

void
test__validateTimeStamp7(void **state)
{
	char* ts = "20100729093000";
	bool result = ValidateTimestampKey(ts);
	assert_true(result);
}

void
test__validateTimeStamp8(void **state)
{
	// too few chars
	char* ts = "2010072909300";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp9(void **state)
{
	// too many chars
	char* ts = "201007290930000";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp10(void **state)
{
	// as long as its a digit its valid
	char* ts = "00000000000000";
	bool result = ValidateTimestampKey(ts);
	assert_true(result);
}

void
test__validateTimeStamp11(void **state)
{
	// as long as its a digit its valid
	char* ts = "0a000000000000";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp12(void **state)
{
	// as long as its a digit its valid
	char* ts = "0q000000000000";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp13(void **state)
{
	// leading space
	char* ts = " 00000000000000";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp14(void **state)
{
	// trailing space
	char* ts = "00000000000000 ";
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp15(void **state)
{
	// NULL pointer
	char* ts = NULL;
	bool result = ValidateTimestampKey(ts);
	assert_false(result);
}

void
test__validateTimeStamp16(void **state)
{
	// non-terminated string
	char ts[20];
	int i = 0;
	for (i = 0; i < sizeof(ts); ++i){
		ts[i] = '1';
	}
	bool result = ValidateTimestampKey((char*)ts);
	assert_false(result);
}

void
test__getTimeStampKey17(void **state)
{
	char* ts = "20100729093000";
	assert_true(!strcmp(ts, GetTimestampKey(ts)));
}

void
test__getTimeStampKey18(void **state)
{
	char* ts = NULL;
	assert_true(GetTimestampKey(ts) != NULL);
}

void 
test__formCompressionProgramString1(void **state)
{
	char* tmp = calloc(10, 1);
	strcat(tmp, "/bin/gzip");
	tmp = formCompressionProgramString(tmp);
	assert_string_equal(tmp, "/bin/gzip -c ");
	free(tmp);
}

void 
test__formCompressionProgramString2(void **state)
{
	char* tmp = calloc(100, 1);
	strcat(tmp, "/bin/gzip");
	tmp = formCompressionProgramString(tmp);
	assert_string_equal(tmp, "/bin/gzip -c ");
	free(tmp);
}

void test__formSegmentPsqlCommandLine1(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   filter_script, table_filter_file, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | gzip -c | filter.py -t filter.conf | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine2(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   NULL, NULL, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | gzip -c | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine3(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   filter_script, table_filter_file, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | filter.py -t filter.conf | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine4(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   NULL, NULL, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine5(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   filter_script, table_filter_file, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | gzip -c | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine6(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   NULL, NULL, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | gzip -c | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine7(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   filter_script, table_filter_file, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formSegmentPsqlCommandLine8(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formSegmentPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   NULL, NULL, role,
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

#ifdef USE_DDBOOST
void test__formDDBoostPsqlCommandLine1(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   filter_script, table_filter_file,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename.gz --dd_boost_buf_size=512MB | gzip -c | filter.py -t filter.conf | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine2(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c ";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   NULL, NULL,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename.gz --dd_boost_buf_size=512MB | gzip -c | psql";

	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine3(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   filter_script, table_filter_file,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename --dd_boost_buf_size=512MB | filter.py -t filter.conf | psql";

	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine4(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	int role = ROLE_SEGDB;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   NULL, NULL,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename --dd_boost_buf_size=512MB | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine5(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   filter_script, table_filter_file,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename.gz --dd_boost_buf_size=512MB | gzip -c | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine6(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c ";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   NULL, NULL,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename.gz --dd_boost_buf_size=512MB | gzip -c | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine7(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	const char* filter_script = "filter.py"; 
	const char* table_filter_file = "filter.conf";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   filter_script, table_filter_file,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename --dd_boost_buf_size=512MB | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}

void test__formDDBoostPsqlCommandLine8(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c ";
	int role = ROLE_MASTER;
	const char* psqlPg = "psql"; 
	const char* ddboostPg = "ddboostPg";
	const char* ddp_file_name = "ddb_filename";
	const char* dd_boost_buf_size = "512MB";

	formDDBoostPsqlCommandLine(&cmdLine, compUsed, ddboostPg, compProg, 
							   ddp_file_name, dd_boost_buf_size, 
							   NULL, NULL,
							   role, psqlPg);

    char *e = "ddboostPg --readFile --from-file=ddb_filename --dd_boost_buf_size=512MB | psql";
	assert_string_equal(cmdLine, e);
	free(cmdLine);
}
#endif

void test__shouldExpandChildren1(void **state)
{
    bool g_gp_supportsPartitioning = false;
    bool no_expand_children = false;
    bool result = shouldExpandChildren(g_gp_supportsPartitioning, no_expand_children);
    assert_false(result);
}

void test__shouldExpandChildren2(void **state)
{
    bool g_gp_supportsPartitioning = true;
    bool no_expand_children = false;
    bool result = shouldExpandChildren(g_gp_supportsPartitioning, no_expand_children);
    assert_true(result);
}

void test__shouldExpandChildren3(void **state)
{
    bool g_gp_supportsPartitioning = true;
    bool no_expand_children = true;
    bool result = shouldExpandChildren(g_gp_supportsPartitioning, no_expand_children);
    assert_false(result);
}

void test__shouldExpandChildren4(void **state)
{
    bool g_gp_supportsPartitioning = false;
    bool no_expand_children = true;
    bool result = shouldExpandChildren(g_gp_supportsPartitioning, no_expand_children);
    assert_false(result);
}

void test__shouldDumpSchemaOnly1(void **state)
{
    int role = ROLE_SEGDB;
    bool incrementalBackup = true;
    void *list = NULL;   
 
    bool result = shouldDumpSchemaOnly(role, incrementalBackup, list);
    assert_true(result);
}

void test__shouldDumpSchemaOnly2(void **state)
{
    int role = ROLE_MASTER;
    bool incrementalBackup = true;
    void *list = NULL;   
 
    bool result = shouldDumpSchemaOnly(role, incrementalBackup, list);
    assert_false(result);
}

void test__shouldDumpSchemaOnly3(void **state)
{
    int role = ROLE_SEGDB;
    bool incrementalBackup = true;
    void *list = "test";
 
    bool result = shouldDumpSchemaOnly(role, incrementalBackup, list);
    assert_false(result);
}

void test__formPostDataSchemaOnlyPsqlCommandLine1(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = true;
	const char* compProg = "gzip -c";
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formPostDataSchemaOnlyPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   psqlPg, catPg);

	char *expected_output = "cat fileSpec | gzip -c | psql";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

void test__formPostDataSchemaOnlyPsqlCommandLine2(void **state)
{
	char *cmdLine = calloc(1000000, 1);
	char *inputFileSpec = "fileSpec";
	bool compUsed = false;
	const char* compProg = "gzip -c";
	const char* psqlPg = "psql"; 
	const char* catPg = "cat";

	formPostDataSchemaOnlyPsqlCommandLine(&cmdLine, inputFileSpec, compUsed, compProg, 
							   psqlPg, catPg);

	char *expected_output = "psql -f fileSpec";
	assert_string_equal(cmdLine, expected_output);
	free(cmdLine);
}

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__isFilteringAllowed1),
			unit_test(test__isFilteringAllowed2),
			unit_test(test__isFilteringAllowed3), 
			unit_test(test__isFilteringAllowed4), 
			unit_test(test__backupTypeString5),
			unit_test(test__backupTypeString6),
			unit_test(test__validateTimeStamp7),
			unit_test(test__validateTimeStamp8),
			unit_test(test__validateTimeStamp9),
			unit_test(test__validateTimeStamp10),
			unit_test(test__validateTimeStamp11),
			unit_test(test__validateTimeStamp12),
			unit_test(test__validateTimeStamp13),
			unit_test(test__validateTimeStamp14),
			unit_test(test__validateTimeStamp15),
			unit_test(test__validateTimeStamp16),
			unit_test(test__getTimeStampKey17),
			unit_test(test__getTimeStampKey18),
			unit_test(test__formCompressionProgramString1),
			unit_test(test__formCompressionProgramString2),
			unit_test(test__formSegmentPsqlCommandLine1),
			unit_test(test__formSegmentPsqlCommandLine2),
			unit_test(test__formSegmentPsqlCommandLine3),
			unit_test(test__formSegmentPsqlCommandLine4),
			unit_test(test__formSegmentPsqlCommandLine5),
			unit_test(test__formSegmentPsqlCommandLine6),
			unit_test(test__formSegmentPsqlCommandLine7),
			unit_test(test__formSegmentPsqlCommandLine8),
            #ifdef USE_DDBOOST
			unit_test(test__formDDBoostPsqlCommandLine1),
			unit_test(test__formDDBoostPsqlCommandLine2),
			unit_test(test__formDDBoostPsqlCommandLine3),
			unit_test(test__formDDBoostPsqlCommandLine4),
			unit_test(test__formDDBoostPsqlCommandLine5),
			unit_test(test__formDDBoostPsqlCommandLine6),
			unit_test(test__formDDBoostPsqlCommandLine7),
			unit_test(test__formDDBoostPsqlCommandLine8),
            #endif
			unit_test(test__shouldExpandChildren1),
			unit_test(test__shouldExpandChildren2),
			unit_test(test__shouldExpandChildren3),
			unit_test(test__shouldExpandChildren4),
			unit_test(test__formPostDataSchemaOnlyPsqlCommandLine1),
			unit_test(test__formPostDataSchemaOnlyPsqlCommandLine2),
            unit_test(test__shouldDumpSchemaOnly1),
            unit_test(test__shouldDumpSchemaOnly2),
            unit_test(test__shouldDumpSchemaOnly3),
	};
	return run_tests(tests);
}
