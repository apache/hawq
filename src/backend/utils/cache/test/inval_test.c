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


/* Helper functions for the MdVer queue pre-processing */
static InvalidationListHeader *
create_list_one_chunk(void) {
	InvalidationListHeader *hdr = (InvalidationListHeader *) palloc0(sizeof(InvalidationListHeader));

	/* Create a chunk */
	InvalidationChunk *first_chunk = (InvalidationChunk *)
					palloc0(sizeof(InvalidationChunk) +
							(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));

	first_chunk->nitems = 0;
	first_chunk->maxitems = FIRSTCHUNKSIZE;
	first_chunk->next = NULL;

	hdr->velist = first_chunk;
	return hdr;
}

static void add_event_to_chunk(InvalidationChunk *chunk, bool is_nuke, int key) {

	/* Create a message */
	SharedInvalidationMessage msg;
	msg.ve.id = SHAREDVERSIONINGMSG_ID;
	msg.ve.local = true;
	if (is_nuke)
	{
		msg.ve.verEvent.key = MDVER_NUKE_KEY;
		msg.ve.verEvent.old_ddl_version = 0;
		msg.ve.verEvent.old_dml_version = 0;
		msg.ve.verEvent.new_ddl_version = 0;
		msg.ve.verEvent.new_dml_version = 0;
	}
	else
	{
		msg.ve.verEvent.key = key;
		msg.ve.verEvent.old_ddl_version = key + 1;
		msg.ve.verEvent.old_dml_version = key + 2;
		msg.ve.verEvent.new_ddl_version = key + 3;
		msg.ve.verEvent.new_dml_version = key + 4;
	}

	chunk->msgs[chunk->nitems++] = msg;
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
	InvalidationListHeader *hdr = create_list_one_chunk();
	add_event_to_chunk(hdr->velist, false /* is_nuke */, 100 /* key */);


	/* Create a new nuke event to be added */
	mdver_event *mdev_nuke = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev_nuke->key = MDVER_NUKE_KEY;


	/* First case, last event in chunk is not nuke. */
	bool result = MdVer_IsRedundantNukeEvent(hdr, mdev_nuke);
	assert_false(result);

	/* Second case, last event in chunk is a nuke. */
	add_event_to_chunk(hdr->velist, true /* is_nuke */, 101);

	result = MdVer_IsRedundantNukeEvent(hdr, mdev_nuke);
	assert_true(result);

	/* Multiple chunk case. Let's add a new chunk in the list. */
	InvalidationChunk *second_chunk = (InvalidationChunk *)
								palloc0(sizeof(InvalidationChunk) +
										(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));
	second_chunk->nitems = 0;
	second_chunk->maxitems = FIRSTCHUNKSIZE;
	second_chunk->next = NULL;

	/* Add chunk to the list. List now looks like this: hdr -> first_chunk -> second_chunk  */
	hdr->velist->next = second_chunk;

	/* Last message in the list is not a nuke */
	add_event_to_chunk(second_chunk, false /* is_nuke */, 200 /* key */);

	result = MdVer_IsRedundantNukeEvent(hdr, mdev_nuke);
	assert_false(result);


	/* Add a nuke message to the last chunk */
	add_event_to_chunk(second_chunk, true /* is_nuke */, 210 /* key */);

	/* Last message in the list is the last message in first_chunk, which is not a nuke */
	result = MdVer_IsRedundantNukeEvent(hdr, mdev_nuke);
	assert_true(result);
}

/* ==================== MdVer_PreProcessInvalidMsgs ==================== */
/*
 *
 */

/* Test that when appending a list with no nukes to dest, nothing changes */
void test__MdVer_PreProcessInvalidMsgs__no_nuke(void **state)
{
	InvalidationListHeader* dest = create_list_one_chunk();
	add_event_to_chunk(dest->velist, false /* is_nuke */, 100 /* key */);

	InvalidationListHeader *src = create_list_one_chunk();
	add_event_to_chunk(src->velist, false /* is_nuke */, 200 /* key */);
	add_event_to_chunk(src->velist, false /* is_nuke */, 210 /* key */);

	MdVer_PreProcessInvalidMsgs(dest, src);

	assert_int_equal(dest->velist->nitems, 1);
	assert_int_equal(dest->velist->msgs[0].ve.verEvent.key, 100);

	assert_int_equal(src->velist->nitems, 2);
	assert_int_equal(src->velist->msgs[0].ve.verEvent.key, 200);
	assert_int_equal(src->velist->msgs[1].ve.verEvent.key, 210);

}

/* Test that when appending a list with a nuke in first chunk, dest gets updated */
void test__MdVer_PreProcessInvalidMsgs__nuke_first_chunk(void **state)
{
	InvalidationListHeader* dest = create_list_one_chunk();
	add_event_to_chunk(dest->velist, false /* is_nuke */, 100 /* key */);

	InvalidationListHeader *src = create_list_one_chunk();
	add_event_to_chunk(src->velist, false /* is_nuke */, 200 /* key */);
	add_event_to_chunk(src->velist, true /* is_nuke */, 210 /* key */);
	add_event_to_chunk(src->velist, false /* is_nuke */, 220 /* key */);
	add_event_to_chunk(src->velist, true /* is_nuke */, 230 /* key */);
	add_event_to_chunk(src->velist, true /* is_nuke */, 240 /* key */);
	add_event_to_chunk(src->velist, false /* is_nuke */, 250 /* key */);
	/* src now is: 200->nuke->220->nuke->nuke->250 */

	MdVer_PreProcessInvalidMsgs(dest, src);

	/* After processing, we should have:
	 *    src: null
	 *    dest: nuke->250
	 */
	assert_int_equal(dest->velist->nitems, 2);
	assert_int_equal(dest->velist->msgs[0].ve.verEvent.key, MDVER_NUKE_KEY);
	assert_int_equal(dest->velist->msgs[1].ve.verEvent.key, 250);

	assert_true(NULL == src->velist);
}

/* Test that when appending a list with a nuke in second chunk, dest gets updated */
void test__MdVer_PreProcessInvalidMsgs__nuke_second_chunk(void **state)
{
	InvalidationListHeader* dest = create_list_one_chunk();
	add_event_to_chunk(dest->velist, false /* is_nuke */, 100 /* key */);

	InvalidationListHeader *src = create_list_one_chunk();
	add_event_to_chunk(src->velist, false /* is_nuke */, 200 /* key */);
	add_event_to_chunk(src->velist, true /* is_nuke */, 210 /* key */);

	/* Create a chunk */
	InvalidationChunk *second_chunk = (InvalidationChunk *)
					palloc0(sizeof(InvalidationChunk) +
							(FIRSTCHUNKSIZE - 1) *sizeof(SharedInvalidationMessage));
	second_chunk->nitems = 0;
	second_chunk->maxitems = FIRSTCHUNKSIZE;
	second_chunk->next = NULL;

	/* Add events to second chunk */
	add_event_to_chunk(second_chunk, false /* is_nuke */, 220 /* key */);
	add_event_to_chunk(second_chunk, true /* is_nuke */, 230 /* key */);
	add_event_to_chunk(second_chunk, true /* is_nuke */, 240 /* key */);
	add_event_to_chunk(second_chunk, false /* is_nuke */, 250 /* key */);
	/* src now is: [200->nuke]->[220->nuke->nuke->250] */

	/* Link second chunk into list */
	src->velist->next = second_chunk;

	MdVer_PreProcessInvalidMsgs(dest, src);

	/* After processing, we should have:
	 *    src: null
	 *    dest: nuke->250
	 */
	assert_int_equal(dest->velist->nitems, 2);
	assert_int_equal(dest->velist->msgs[0].ve.verEvent.key, MDVER_NUKE_KEY);
	assert_int_equal(dest->velist->msgs[1].ve.verEvent.key, 250);

	assert_true(NULL == src->velist);
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
			unit_test(test__MdVer_IsRedundantNukeEvent__chunks),
			unit_test(test__MdVer_PreProcessInvalidMsgs__no_nuke),
			unit_test(test__MdVer_PreProcessInvalidMsgs__nuke_first_chunk),
			unit_test(test__MdVer_PreProcessInvalidMsgs__nuke_second_chunk)
	};
	return run_tests(tests);
}


