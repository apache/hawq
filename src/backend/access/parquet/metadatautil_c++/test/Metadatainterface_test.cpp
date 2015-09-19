#include "../Metadatainterface.cpp"
#include <stddef.h>
#include <stdarg.h>
#include <setjmp.h>

extern "C"
{

#include "cmockery.h"
#include "c.h"

void
test__readFileMetadata(void **state)
{

}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__readFileMetadata)
	};
	return run_tests(tests);
}

}
