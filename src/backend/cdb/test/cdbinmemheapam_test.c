#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../cdbinmemheapam.c"

InMemHeapScanDesc scan = NULL;
ScanDirection direction;

void
test__InMemHeap_GetNextIndex__forward(void **state)
{
	direction = ForwardScanDirection;
	scan->rs_rd->tupsize = 3;

	for (int i = 0; i < 3; i ++)
	{
		assert_true(InMemHeap_GetNextIndex(scan, direction));
		assert_int_equal(i, scan->rs_index);
	}

	assert_false(InMemHeap_GetNextIndex(scan, direction));
	assert_int_equal(3, scan->rs_index);
}

void
test__InMemHeap_GetNextIndex__forwardEmpty(void **state)
{
	direction = ForwardScanDirection;
	scan->rs_rd->tupsize = 0;

	assert_false(InMemHeap_GetNextIndex(scan, direction));
	assert_int_equal(0, scan->rs_index);
}

void
test__InMemHeap_GetNextIndex__backward(void **state)
{
	direction = BackwardScanDirection;
	scan->rs_rd->tupsize = 3;

	for (int i = 2; i > -1; i--)
	{
		assert_true(InMemHeap_GetNextIndex(scan, direction));
		assert_int_equal(i, scan->rs_index);
	}

	assert_false(InMemHeap_GetNextIndex(scan, direction));
	assert_int_equal(-1, scan->rs_index);
}

void
test__InMemHeap_GetNextIndex__backwardEmpty(void **state)
{
	direction = BackwardScanDirection;
	scan->rs_rd->tupsize = 0;

	assert_false(InMemHeap_GetNextIndex(scan, direction));
	assert_int_equal(-1, scan->rs_index);
}

void InMemHeap_GetNextIndex__prepareScan(void **state)
{
	scan = palloc0(sizeof(InMemHeapScanDescData));
	scan->rs_rd = palloc0(sizeof(InMemHeapRelationData));
	scan->rs_index = -1;
}

void InMemHeap_GetNextIndex__freeScan(void **state)
{
	if (scan)
	{
		if (scan->rs_rd)
		{
			pfree(scan->rs_rd);
		}
		pfree(scan);
	}
}

int		
main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test_setup_teardown(test__InMemHeap_GetNextIndex__forward,
					InMemHeap_GetNextIndex__prepareScan, InMemHeap_GetNextIndex__freeScan),
			unit_test_setup_teardown(test__InMemHeap_GetNextIndex__forwardEmpty,
					InMemHeap_GetNextIndex__prepareScan, InMemHeap_GetNextIndex__freeScan),
			unit_test_setup_teardown(test__InMemHeap_GetNextIndex__backward,
					InMemHeap_GetNextIndex__prepareScan, InMemHeap_GetNextIndex__freeScan),
			unit_test_setup_teardown(test__InMemHeap_GetNextIndex__backwardEmpty,
					InMemHeap_GetNextIndex__prepareScan, InMemHeap_GetNextIndex__freeScan)
	};
	return run_tests(tests);
}
