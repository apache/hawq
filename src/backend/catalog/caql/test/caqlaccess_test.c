#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "../caqlaccess.c"


/*
 * Verify that OidGetInMemHeapRelation was called (by caql_insert_inmem)
 * with the given parameters.
 * The returned value retval is used for the next steps of the function.
 */
static void
expect__OidGetInMemHeapRelation(Oid relid, InMemHeapRelation retrel)
{
	expect_value(OidGetInMemHeapRelation, relid, relid);
	expect_value(OidGetInMemHeapRelation, mappingType, INMEM_ONLY_MAPPING);

	will_return(OidGetInMemHeapRelation, retrel);
}

/*
 * Verify that InMemHeapRelation was called (by caql_insert_inmem)
 * with the given parameters.
 * The returned value retval is used for the next steps of the function.
 */
static void
expect__InMemHeap_Create(Relation rel, LOCKMODE lockmode, InMemHeapRelation retrel)
{
	expect_value(InMemHeap_Create, relid, rel->rd_id);
	expect_value(InMemHeap_Create, rel, rel);
	expect_value(InMemHeap_Create, ownrel, false);
	expect_value(InMemHeap_Create, initSize, 10);
	expect_value(InMemHeap_Create, lock, lockmode);
	expect_any(InMemHeap_Create, relname);
	expect_value(InMemHeap_Create, createIndex, false);
	expect_value(InMemHeap_Create, keyAttrno, 0);
	expect_value(InMemHeap_Create, mappingType, INMEM_ONLY_MAPPING);

	will_return(InMemHeap_Create, retrel);
}

/*
 * Verify that InMemHeap_CheckConstraints was called (by caql_insert_inmem)
 * with the given parameters.
 */
static void
expect__InMemHeap_CheckConstraints(InMemHeapRelation inmemrel, HeapTuple tup)
{
	expect_value(InMemHeap_CheckConstraints, relation, inmemrel);
	expect_value(InMemHeap_CheckConstraints, newTuple, tup);

	will_be_called(InMemHeap_CheckConstraints);
}

/*
 * Verify that InMemHeap_Insert was called (by caql_insert_inmem)
 * with the given parameters.
 */
static void
expect__InMemHeap_Insert(InMemHeapRelation inmemrel, HeapTuple tup)
{
	expect_value(InMemHeap_Insert, relation, inmemrel);
	expect_value(InMemHeap_Insert, tup, tup);
	expect_value(InMemHeap_Insert, contentid, -1);

	will_be_called(InMemHeap_Insert);
}

void
test__caql_insert_inmem__insert_two_tuples(void **state)
{

	NameData relname = {"pg_class"};

	Form_pg_class rd_rel = palloc0(sizeof(FormData_pg_class));
	rd_rel->relname = relname;

	Relation rel = palloc0(sizeof(RelationData));
	rel->rd_id = 1234;
	rel->rd_rel = rd_rel;

	cqContext* pCtx = palloc0(sizeof(cqContext));
	pCtx->cq_heap_rel = rel;
	pCtx->cq_lockmode = AccessShareLock;

	InMemHeapRelation inmemrel = palloc0(sizeof(InMemHeapRelationData));

	HeapTuple tup = NULL;

	/* expect OidGetInMemHeapRelation to return NULL */
	expect__OidGetInMemHeapRelation(rel->rd_id, NULL);
	/* expect InMemHeap_Create to create new in memory table
	 * because OidGetInMemHeapRelation returned NULL */
	expect__InMemHeap_Create(rel, pCtx->cq_lockmode, inmemrel);
	/* expect InMemHeap_CheckConstraints to validate the new
	 * tuple satisfies the in-memory catalog constraints */
	expect__InMemHeap_CheckConstraints(inmemrel, tup);
	/* expect InMemHeap_Insert to insert new tuple */
	expect__InMemHeap_Insert(inmemrel, tup);

	/* TEST - insert first tuple to table */
	will_return(AmIMaster, true);
	caql_insert_inmem(pCtx, tup);

	/* expect OidGetInMemHeapRelation to return already created inmemrel */
	expect__OidGetInMemHeapRelation(rel->rd_id, inmemrel);
	/* expect InMemHeap_CheckConstraints to validate the new
	 * tuple satisfies the in-memory catalog constraints */
	expect__InMemHeap_CheckConstraints(inmemrel, tup);
	/* expect InMemHeap_Insert to insert new tuple */
	expect__InMemHeap_Insert(inmemrel, tup);

	/* TEST - insert second tuple to table */
	will_return(AmIMaster, true);
	caql_insert_inmem(pCtx, tup);

	pfree(rd_rel);
	pfree(rel);
	pfree(inmemrel);
	pfree(pCtx);
}

int main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__caql_insert_inmem__insert_two_tuples)
	};
	return run_tests(tests);
}
