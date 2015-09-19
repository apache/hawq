#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"
#include "nodes/nodes.h"
#include "../postgres.c"

/*List with multiple elements, return FALSE.*/
void 
test__IsTransactionExitStmtList__MultipleElementList(void **state) 
{
	List *list = list_make2_int(1,2); 
	
	assert_false(IsTransactionExitStmtList(list));
	
	list_free(list);
}

/*  Transaction with Exit Statement, return TRUE. */
void 
test__IsTransactionExitStmt__IsTransactionStmt(void **state) 
{	
	TransactionStmt *stmt = makeNode(TransactionStmt);
	stmt->kind = TRANS_STMT_COMMIT;
		
	List *list = list_make1(stmt);
	
	assert_true(IsTransactionExitStmtList(list));
	
	list_free(list);
	pfree(stmt);
}

/* Query with Transaction with Exit Statement, return TRUE. */
void 
test__IsTransactionExitStmt__IsQuery(void **state) 
{	
	TransactionStmt *stmt = makeNode(TransactionStmt);
	stmt->kind = TRANS_STMT_COMMIT;
	Query *query = (Query *)palloc(sizeof(Query));
	query->type = T_Query;
	query->commandType = CMD_UTILITY;
	query->utilityStmt = stmt;
	
	List *list = list_make1(query);
	
	assert_true(IsTransactionExitStmtList(list));
	
	list_free(list);
	pfree(query);
	pfree(stmt);
}

/*
 * Test ProcessInterrupts when ClientConnectionLost flag is set
 */
void
test__ProcessInterrupts__ClientConnectionLost(void **state)
{

	/* Mocking errstart -- expect an ereport(FATAL) to be called */
	expect_value(errstart, elevel, FATAL);
	expect_any(errstart, filename);
	expect_any(errstart, lineno);
	expect_any(errstart, funcname);
	expect_any(errstart, domain);
	will_return(errstart, false);

	will_be_called(DisableNotifyInterrupt);
	will_be_called(DisableCatchupInterrupt);

	/*
	 * Setting all the flags so that ProcessInterrupts only goes in the if-block
	 * we're interested to test
	 */
	InterruptHoldoffCount = 0;
	CritSectionCount = 0;
	ProcDiePending = 0;
	ClientConnectionLost = 1;
	whereToSendOutput = DestDebug;

	/* Run function under test */
	ProcessInterrupts();

	assert_true(whereToSendOutput == DestNone);
	assert_false(QueryCancelPending);
	assert_false(ImmediateInterruptOK);

}

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);
	
	const UnitTest tests[] = {
			unit_test(test__IsTransactionExitStmtList__MultipleElementList),
			unit_test(test__IsTransactionExitStmt__IsTransactionStmt),
			unit_test(test__IsTransactionExitStmt__IsQuery),
			unit_test(test__ProcessInterrupts__ClientConnectionLost)
	};
	return run_tests(tests);
}


