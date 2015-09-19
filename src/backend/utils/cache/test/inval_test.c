#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"

#include "../inval.c"

#if defined(__i386)
#define TRANS_INVALIDATION_INFO_SIZE 40
#define INVALIDATION_LIST_HEADER_SIZE 12
#define INVALIDATION_CHUNK_SIZE 56
#elif defined(__x86_64__)
#define TRANS_INVALIDATION_INFO_SIZE 80
#define INVALIDATION_LIST_HEADER_SIZE 24
#define INVALIDATION_CHUNK_SIZE 64
#else
#error unsupported platform: only x86 and x86_64 supported by this test
#endif


/*
 * Tests that the size of the data structures used for the transaction
 * invalidation information has not changed.
 *
 * Background: To test metadata versioning, these structures are also
 * declared in the test UDF that lives in tincrepo (mdversion_test.c).
 *
 * This test will fail if any change is made to these structs in the product
 * If that happens, two things are needed:
 * - update the sizes here to reflect the new definition
 * - update the definitions in mdversion_test.c (in tincrepo)
 *
 */
void
test__sizeof__TransInvalidationInfo(void **state)
{
	assert_true(sizeof(TransInvalidationInfo) == TRANS_INVALIDATION_INFO_SIZE);
}


void
test__sizeof__InvalidationListHeader(void **state)
{
	assert_true(sizeof(InvalidationListHeader) == INVALIDATION_LIST_HEADER_SIZE);
}

void
test__sizeof__InvalidationChunk(void **state)
{
	assert_true(sizeof(InvalidationChunk) == INVALIDATION_CHUNK_SIZE);
}


/* ==================== InvalidateSystemCaches ==================== */
/*
 * Tests that InvalidateSystemCaches resets the local MDVSN cache when
 * MD Versioning is enabled
 */
void test__InvalidateSystemCaches__resets_mdvsn_enabled(void **state)
{
	will_be_called(ResetCatalogCaches);
	will_be_called(RelationCacheInvalidate);

	/*
	 * Initialize a fake transInvalInfo and transInvalInfo->local_mdvsn.
	 * They are used by GetCurrentLocalMDVSN()
	 */
	mdver_local_mdvsn local_mdvsn_fake;
	TransInvalidationInfo transInvalInfo_fake;
	transInvalInfo_fake.local_mdvsn = &local_mdvsn_fake;
	transInvalInfo = &transInvalInfo_fake;

	will_return(mdver_enabled, true);

	expect_value(mdver_local_mdvsn_nuke, local_mdvsn, &local_mdvsn_fake);
	will_be_called(mdver_local_mdvsn_nuke);

	InvalidateSystemCaches();
}

/*
 * Tests that InvalidateSystemCaches does not reset the local MDVSN cache when
 * MD Versioning is disabled
 */
void test__InvalidateSystemCaches__resets_mdvsn_disabled(void **state)
{
	will_be_called(ResetCatalogCaches);
	will_be_called(RelationCacheInvalidate);

	/*
	 * Initialize a fake transInvalInfo and transInvalInfo->local_mdvsn.
	 * They are used by GetCurrentLocalMDVSN()
	 */
	mdver_local_mdvsn local_mdvsn_fake;
	TransInvalidationInfo transInvalInfo_fake;
	transInvalInfo_fake.local_mdvsn = &local_mdvsn_fake;
	transInvalInfo = &transInvalInfo_fake;

	will_return(mdver_enabled, false);

	InvalidateSystemCaches();

	/*
	 * Part of the test we're implicitly asserting that mdver_local_mdvsn_nuke
	 * does not get called in this scenario.
	 */
}

/*
 * Tests that InvalidateSystemCaches does not reset the local MDVSN cache when
 * there is no transaction invalidation information (not in a transaction, special
 * backend)
 */
void test__InvalidateSystemCaches__resets_mdvsn_no_xact(void **state)
{
	will_be_called(ResetCatalogCaches);
	will_be_called(RelationCacheInvalidate);

	/*
	 * Set transInvalInfo to NULL to simulate an case where we don't have
	 * a transaction invalidation information (e.g. auxiliary process)
	 */
	transInvalInfo = NULL;

	InvalidateSystemCaches();

	/*
	 * Part of the test we're implicitly asserting that mdver_local_mdvsn_nuke
	 * does not get called in this scenario.
	 */
}


int
main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__sizeof__TransInvalidationInfo),
			unit_test(test__sizeof__InvalidationListHeader),
			unit_test(test__sizeof__InvalidationChunk),
			unit_test(test__InvalidateSystemCaches__resets_mdvsn_enabled),
			unit_test(test__InvalidateSystemCaches__resets_mdvsn_disabled),
			unit_test(test__InvalidateSystemCaches__resets_mdvsn_no_xact)
	};
	return run_tests(tests);
}


