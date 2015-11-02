#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "hd_work_mgr_mock.c"
#include "hd_work_mgr_do_segment_clustering_by_host_test.c"
#include "hd_work_mgr_allocate_fragments_to_datanodes_test.c"
#include "hd_work_mgr_distribute_work_2_gp_segments_test.c"

int 
main(int argc, char* argv[]) 
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__do_segment_clustering_by_host__10SegmentsOn3Hosts),
			unit_test(test__get_dn_processing_load),
			unit_test(test__create_allocated_fragment__NoUserData),
			unit_test(test__create_allocated_fragment__WithUserData),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments10Hosts3Replicates),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments3Hosts2Replicates),
			unit_test(test__allocate_fragments_to_datanodes__4Fragments3Hosts1Replicates),
			unit_test(test__allocate_fragments_to_datanodes__7Fragments10Hosts1Replicates),
			unit_test(test__distribute_work_to_gp_segments__big_cluster_few_active_nodes),	
			unit_test(test__distribute_work_to_gp_segments__big_cluster_many_active_nodes),
			unit_test(test__distribute_work_to_gp_segments__small_cluster),
			unit_test(test__distribute_work_to_gp_segments__small_cluster_many_active_nodes),
			unit_test(test__distribute_work_to_gp_segments__small_cluster_few_replicas)
	};
	return run_tests(tests);
}
