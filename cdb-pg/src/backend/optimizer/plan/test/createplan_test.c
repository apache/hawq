#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "../createplan.c"


/*
 * Test gpxf_calc_max_participants_allowed returned value
 */
void 
test__gpxf_calc_max_participants_allowed(void **state)
{
	int test_params[11][4] =
	{
      /* total_segments, number_of_hosts, max_segs_guc, expected_result */
			{10,	3, 		64, 	10}, /* max_segs_guc > total_segments */
			{64, 	16,		64,		64}, /* max_segs_guc = total_segments */
			{64,	16,		10,		10}, /* max_segs_guc < number_of_hosts */
			{64,	16,		16,		16}, /* max_segs_guc = number_of_hosts */
			{64,	16,		32,		32}, /* max_segs_guc % number_of_hosts = 0 */
			{64,	16,		20,		32}, /* max_segs_guc % number_of_hosts <> 0 */
			{101,	16,		64,		64}, /* odd number of total_segments */
			{101,	15,		20,		30}, /* odd number of number_of_hosts */
			{64,	16,		1,		1},  /* max_segs_guc = 1 */
			{1024,	100,	64,		64}, /* large number of total_segments, max_segs_guc < number_of_hosts */
			{1024,	100,	101,	200} /* large number of total_segments, max_segs_guc > number_of_hosts */
	};
	int array_size = 11;
	int inner_array_size = 4;
	/* sanity */
	assert_true(array_size == (sizeof(test_params) / sizeof(test_params[0])));
	assert_true(inner_array_size == (sizeof(test_params[0]) / sizeof(test_params[0][0])));

	for (int i = 0; i < array_size; ++i)
	{
		int total_segments =  test_params[i][0];
		int number_of_hosts = test_params[i][1];
		int max_segs_guc = 	  test_params[i][2];
		int expected_result = test_params[i][3];

		gp_external_max_segs = max_segs_guc;
		will_return(getgphostCount, number_of_hosts);

		assert_int_equal(gpxf_calc_max_participants_allowed(total_segments), expected_result);
	}
}

/*
 * Test is_gpxf_protocol with gpxf protocol
 */
void
test__is_gpxf_protocol__CustomProtocolGPXF(void **state)
{
	Uri *uri = (Uri *) palloc0(sizeof(Uri));

	uri->protocol = URI_CUSTOM;
	uri->customprotocol = "gpxf";

	assert_true(is_gpxf_protocol(uri));

	pfree(uri);
}

/*
 * Test is_gpxf_protocol with custom protocol but null value
 * for customprotocol.
 */
void
test__is_gpxf_protocol__CustomProtocolNull(void **state)
{
	Uri *uri = (Uri *) palloc0(sizeof(Uri));

	uri->protocol = URI_CUSTOM;
	uri->customprotocol = NULL;

	assert_false(is_gpxf_protocol(uri));

	pfree(uri);
}

/*
 * Test is_gpxf_protocol with gpxf protocol but other protocol
 * defined in customprotocol.
 */
void
test__is_gpxf_protocol__CustomProtocolOther(void **state)
{
	Uri *uri = (Uri *) palloc0(sizeof(Uri));

	uri->protocol = URI_CUSTOM;
	uri->customprotocol = "some_other_protocol";

	assert_false(is_gpxf_protocol(uri));

	pfree(uri);
}

/*
 * Negative test for is_gpxf_protocol, with other protocols.
 */
void
test__is_gpxf_protocol__OtherProtocols(void **state)
{
	UriProtocol protocol_array[] =
	{
			URI_FILE,
			URI_FTP,
			URI_HTTP,
			URI_GPFDIST,
			URI_GPFDISTS
	};
	int number_of_cases = 5;
	/* sanity */
	assert_true(number_of_cases == (sizeof(protocol_array) / sizeof(protocol_array[0])));

	Uri *uri = (Uri *) palloc0(sizeof(Uri));

	for (int i = 0; i < number_of_cases; ++i)
	{
		uri->protocol = protocol_array[i];

		assert_false(is_gpxf_protocol(uri));
	}

	pfree(uri);
}


int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__gpxf_calc_max_participants_allowed),
			unit_test(test__is_gpxf_protocol__CustomProtocolGPXF),
			unit_test(test__is_gpxf_protocol__CustomProtocolNull),
			unit_test(test__is_gpxf_protocol__CustomProtocolOther),
			unit_test(test__is_gpxf_protocol__OtherProtocols)
	};
	return run_tests(tests);
}
