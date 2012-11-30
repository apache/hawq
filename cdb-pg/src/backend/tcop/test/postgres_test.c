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


int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);
	
	const UnitTest tests[] = {
			unit_test(test__IsTransactionExitStmtList__MultipleElementList),
			unit_test(test__IsTransactionExitStmt__IsTransactionStmt),
			unit_test(test__IsTransactionExitStmt__IsQuery)
	};
	return run_tests(tests);
}


