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

/* ==================== MdVer_IsRedundantNukeEvent ==================== */
/*
 * Trivial cases: not a nuke event, or the list is empty
 */
void test__MdVer_IsRedundantNukeEvent__no_action(void **state)
{
	InvalidationListHeader hdr;
	hdr.cclist = hdr.rclist = hdr.velist = NULL;

	/* First case, when the event is not a nuke */
	mdver_event *mdev = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev->key = 100;
	mdev->new_ddl_version = 1;
	mdev->new_dml_version = 2;

	bool result = MdVer_IsRedundantNukeEvent(&hdr, mdev);
	assert_false(result);

	/* Second case, when the event is a nuke, but queue is empty */
	mdev->key = MDVER_NUKE_KEY;
	result = MdVer_IsRedundantNukeEvent(&hdr, mdev);
	assert_false(result);
}

/*
 * Non-trivial case: We have one chunk, with some events. Test that we are
 * correctly looking at the last event in the chunk.
 */
void test__MdVer_IsRedundantNukeEvent__chunks(void **state)
{
	InvalidationListHeader hdr;
	hdr.cclist = hdr.rclist = hdr.velist = NULL;

	/* Create an event to add to the list */
	mdver_event *mdev_list = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev_list->key = 100;
	mdev_list->new_ddl_version = 1;
	mdev_list->new_dml_version = 2;

	/* Create a chunk */
	InvalidationChunk *first_chunk = (InvalidationChunk *)
					MemoryContextAlloc(CurTransactionContext,
							sizeof(InvalidationChunk) +
							(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));
	first_chunk->nitems = 0;
	first_chunk->maxitems = FIRSTCHUNKSIZE;
	first_chunk->next = NULL;

	/* Create a message */
	SharedInvalidationMessage msg;
	msg.ve.id = SHAREDVERSIONINGMSG_ID;
	msg.ve.local = true;
	msg.ve.verEvent = *mdev_list;

	/* Add it to the chunk */
	first_chunk->msgs[first_chunk->nitems++] = msg;

	/* Add chunk to the list */
	hdr.velist = first_chunk;

	/* Create a new nuke event to be added */
	mdver_event *mdev_nuke = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev_nuke->key = MDVER_NUKE_KEY;


	/* First case, last event in chunk is not nuke. */
	bool result = MdVer_IsRedundantNukeEvent(&hdr, mdev_nuke);
	assert_false(result);

	/* Second case, last event in chunk is not nuke. */

	/* Create a new nuke event and add it to the chunk */
	mdver_event *mdev_list_nuke = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev_list_nuke->key = MDVER_NUKE_KEY;
	msg.ve.verEvent = *mdev_list_nuke;
	first_chunk->msgs[first_chunk->nitems++] = msg;

	result = MdVer_IsRedundantNukeEvent(&hdr, mdev_nuke);
	assert_true(result);

	/* Multiple chunk case.
	 * Let's add a new chunk in the list. We'll add it as the first
	 * chunk, so we don't have to add more messages to it. Just test that we
	 * correctly skip over it. */
	InvalidationChunk *second_chunk = (InvalidationChunk *)
								MemoryContextAlloc(CurTransactionContext,
										sizeof(InvalidationChunk) +
										(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));
	second_chunk->nitems = 0;
	second_chunk->maxitems = FIRSTCHUNKSIZE;

	/* Add chunk to the list. List now looks like this: hdr -> second_chunk -> first_chunk */
	hdr.velist = second_chunk;
	second_chunk->next = first_chunk;

	/* Last message in the list is the last message in first_chunk, which is a nuke */
	result = MdVer_IsRedundantNukeEvent(&hdr, mdev_nuke);
	assert_true(result);


	/* Add another non-nuke message to the last chunk */
	/* Create an event to add to the list */
	mdver_event *mdev_list_last = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev_list_last->key = 200;
	mdev_list_last->new_ddl_version = 3;
	mdev_list_last->new_dml_version = 4;

	msg.ve.verEvent = *mdev_list_last;
	first_chunk->msgs[first_chunk->nitems++] = msg;

	/* Last message in the list is the last message in first_chunk, which is not a nuke */
	result = MdVer_IsRedundantNukeEvent(&hdr, mdev_nuke);
	assert_false(result);
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
			unit_test(test__InvalidateSystemCaches__resets_mdvsn_no_xact),
			unit_test(test__MdVer_IsRedundantNukeEvent__no_action),
			unit_test(test__MdVer_IsRedundantNukeEvent__chunks)
	};
	return run_tests(tests);
}


