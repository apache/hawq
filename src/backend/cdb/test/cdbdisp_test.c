#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../cdbdisp.c"

/* 
 * Expected variables for Assertions 
 */
void
_ExceptionalCondition()
{
	expect_any(ExceptionalCondition,conditionName);
	expect_any(ExceptionalCondition,errorType);
	expect_any(ExceptionalCondition,fileName);
	expect_any(ExceptionalCondition,lineNumber);
	will_be_called(ExceptionalCondition);
}

int		
main(int argc, char* argv[]) {
        cmockery_parse_arguments(argc, argv);

        const UnitTest tests[] = {
        };  
        return run_tests(tests);
}
