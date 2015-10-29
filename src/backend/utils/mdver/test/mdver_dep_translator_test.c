#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"

#include "../mdver_dep_translator.c"


/* ==================== mdver_add_nuke_event ==================== */
/*
 * Tests that mdver_add_nuke_event doesn't do anything when passed
 * a NIL event list
 */
void test__mdver_add_nuke_event_nil(void **state)
{
	List *events = NIL;
	mdver_add_nuke_event(&events);
}

/*
 * Tests that mdver_add_nuke_event adds an event when the last
 * event is not nuke
 */
void test__mdver_add_nuke_event_no_nuke(void **state)
{

	/* Create an empty list of events */
	List *events = NIL;

	/* Let's create some non-nuke event and add it to the list */
	mdver_event *mdev = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev->key = 100;
	mdev->new_ddl_version = 1;
	mdev->new_dml_version = 2;
	events = lappend(events, mdev);

	/* Now add a nuke event */
	mdver_add_nuke_event(&events);

	/* Adding the nuke increased the length, it should be 2 */
	assert_int_equal(2 /* length */, length(events));

}

/*
 * Tests that mdver_add_nuke_event adds an event when the last
 * event is not nuke
 */
void test__mdver_add_nuke_event_after_nuke(void **state)
{
	/* Create an empty list of events */
	List *events = NIL;

	/* Let's create some non-nuke event and add it to the list */
	mdver_event *mdev = (mdver_event *) palloc0(sizeof(mdver_event));
	mdev->key = 100;
	mdev->new_ddl_version = 1;
	mdev->new_dml_version = 2;
	events = lappend(events, mdev);

	/* Create a nuke event and add it to the list */
	mdev =  (mdver_event *) palloc0(sizeof(mdver_event));
	mdev->key = MDVER_NUKE_KEY;
	events = lappend(events, mdev);

	/* Now add a nuke event */
	mdver_add_nuke_event(&events);

	/* Adding the nuke shouldn't have changed the length - it's still 2 */
	assert_int_equal(2 /* length */, length(events));
}

int
main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__mdver_add_nuke_event_nil),
			unit_test(test__mdver_add_nuke_event_no_nuke),
			unit_test(test__mdver_add_nuke_event_after_nuke)
	};
	return run_tests(tests);
}


