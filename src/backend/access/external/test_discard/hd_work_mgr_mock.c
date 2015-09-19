#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "hd_work_mgr_mock.h"

/*
 * Helper functions to create and restore GpAliveSegmentsInfo.cdbComponentDatabases element
 * used by hd_work_mgr
 */

/*
 * Builds an array of CdbComponentDatabaseInfo.
 * Each segment is assigned a sequence number and an ip.
 * segs_num - the number of segments
 * segs_hostips - array of the ip of each segment
 * primaries_map - array of which segments are primaries
 */
void buildCdbComponentDatabases(int segs_num,
								char* segs_hostips[],
								bool primaries_map[])
{
	CdbComponentDatabases *test_cdb = palloc0(sizeof(CdbComponentDatabases));
	CdbComponentDatabaseInfo* component = NULL;
	test_cdb->total_segment_dbs = segs_num;
	test_cdb->segment_db_info =
			(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * test_cdb->total_segment_dbs);

	for (int i = 0; i < test_cdb->total_segment_dbs; ++i)
	{
		component = &test_cdb->segment_db_info[i];
		component->segindex = i;
		component->role = primaries_map[i] ? SEGMENT_ROLE_PRIMARY : SEGMENT_ROLE_MIRROR;
		component->hostip = pstrdup(segs_hostips[i]);
	}

	orig_cdb = GpAliveSegmentsInfo.cdbComponentDatabases;
	orig_seg_count = GpAliveSegmentsInfo.aliveSegmentsCount;
	GpAliveSegmentsInfo.cdbComponentDatabases = test_cdb;
	GpAliveSegmentsInfo.aliveSegmentsCount = segs_num;
}

void restoreCdbComponentDatabases()
{
	/* free test CdbComponentDatabases */
	if (GpAliveSegmentsInfo.cdbComponentDatabases)
		freeCdbComponentDatabases(GpAliveSegmentsInfo.cdbComponentDatabases);

	GpAliveSegmentsInfo.cdbComponentDatabases = orig_cdb;
	GpAliveSegmentsInfo.aliveSegmentsCount = orig_seg_count;
}
