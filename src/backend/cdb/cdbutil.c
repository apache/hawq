/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbutil.c
 *	  Internal utility support functions for Greenplum Database/PostgreSQL.
 *
 * NOTES
 *
 *	- According to src/backend/executor/execHeapScan.c
 *		"tuples returned by heap_getnext() are pointers onto disk
 *		pages and were not created with palloc() and so should not
 *		be pfree()'d"
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/catquery.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "catalog/gp_configuration.h"
#include "catalog/gp_segment_config.h"
#include "cdb/cdbcat.h"
#include "cdb/cdbutil.h"
#include "cdb/cdblink.h"
#include "nodes/execnodes.h"	/* Slice, SliceTable */
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbdisp.h"
#include "cdb/ml_ipc.h"			/* listener_setup */
#include "cdb/cdbfts.h"
#include "libpq/ip.h"
#include "postmaster/checkpoint.h"
#include "catalog/indexing.h"
#include "utils/faultinjection.h"
#include "cdb/executormgr.h"

/*
 * Helper Functions
 */
//static int	CdbComponentDatabaseInfoCompare(const void *p1, const void *p2);

//static void getAddressesForDBid(CdbComponentDatabaseInfo *c, int elevel);

static HTAB *segment_ip_cache_htab = NULL;
static char *getDnsCachedAddress(char *name, int port, int elevel);

struct segment_ip_cache_entry {
	char key[NAMEDATALEN];
	char hostinfo[NI_MAXHOST];
};

typedef struct SegmentConfigurationIterator {
	Relation		gp_seg_config_rel;
	HeapScanDesc	gp_seg_config_scan;
} SegmentConfigurationIterator;

static Segment *
ParseSegmentConfigurationRow(SegmentConfigurationIterator *iterator, HeapTuple row)
{
	Datum	attr;
	bool	isNull;
	Segment	*segment = NULL;

	segment = palloc0(sizeof(*segment));

	attr = heap_getattr(row, Anum_gp_segment_configuration_hostname, RelationGetDescr(iterator->gp_seg_config_rel), &isNull);
	segment->hostname = TextDatumGetCString(attr);

	attr = heap_getattr(row, Anum_gp_segment_configuration_port, RelationGetDescr(iterator->gp_seg_config_rel), &isNull);
	segment->port = DatumGetInt32(attr);

	/* use role to determine */
	attr = heap_getattr(row, Anum_gp_segment_configuration_role, RelationGetDescr(iterator->gp_seg_config_rel), &isNull);
	char role = DatumGetChar(attr);
	if (role == SEGMENT_ROLE_MASTER_CONFIG)
		segment->master = true;
	else if (role == SEGMENT_ROLE_STANDBY_CONFIG)
		segment->standby = true;

	/* Should remove the following code iff we have DNS module and in-memory FTS. */
	attr = heap_getattr(row, Anum_gp_segment_configuration_status, RelationGetDescr(iterator->gp_seg_config_rel), &isNull);
	segment->alive = (DatumGetChar(attr) == 'u') ? true : false;
	attr = heap_getattr(row, Anum_gp_segment_configuration_address, RelationGetDescr(iterator->gp_seg_config_rel), &isNull);
	char *address = TextDatumGetCString(attr);
	segment->hostip = getDnsCachedAddress(address, segment->port, DEBUG1);
	pfree(address);

	return segment;
}

static void
InitIterateSegmentConfiguration(SegmentConfigurationIterator *iterator)
{
	iterator->gp_seg_config_rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);
	iterator->gp_seg_config_scan = heap_beginscan(iterator->gp_seg_config_rel, SnapshotNow, 0, NULL);
}

static Segment *
IterateSegmentConfiguration(SegmentConfigurationIterator *iterator)
{
	HeapTuple	gp_seg_config_tuple;

	while (HeapTupleIsValid(gp_seg_config_tuple = heap_getnext(iterator->gp_seg_config_scan, ForwardScanDirection)))
	{
		return ParseSegmentConfigurationRow(iterator, gp_seg_config_tuple);
	}

	/* No more data. */
	heap_endscan(iterator->gp_seg_config_scan);
	heap_close(iterator->gp_seg_config_rel, AccessShareLock);
	return NULL;
}

List *
GetVirtualSegmentList(void)
{
#if 0
	List	*real_segments = GetSegmentList();
	int		real_segment_num = list_length(real_segments);
	Segment *dest;
	Segment *src;

	src = linitial(real_segments);
	dest = CopySegment(src);
	dest->id = real_segment_num;
	dest->dbid = real_segment_num + 1;

	return lappend(real_segments, dest);
#else
	return GetSegmentList();
#endif
}

Segment *
CopySegment(Segment *src, MemoryContext cxt)
{
	MemoryContext	valid_cxt = cxt ? cxt : CurrentMemoryContext;
	Segment	*dest = MemoryContextAllocZero(valid_cxt, sizeof(Segment));

	memcpy(dest, src, sizeof(Segment));
	/* memory leak risk! */
	dest->hostname = MemoryContextStrdup(valid_cxt, src->hostname);
	dest->hostip = MemoryContextStrdup(valid_cxt, src->hostip);

	return dest;
}

void
FreeSegment(Segment *segment)
{
	if (segment == NULL)
		return;

	if (segment->hostname)
	{
		pfree(segment->hostname);
	}

	pfree(segment);
}

Segment *
GetMasterSegment(void)
{
	SegmentConfigurationIterator	iterator;
	Segment	*current = NULL;
	Segment *master = NULL;

	InitIterateSegmentConfiguration(&iterator);
	while ((current = IterateSegmentConfiguration(&iterator)))
	{
		if (!current->master)
		{
			pfree(current->hostname);
			pfree(current);
		}
		else
		{
			master = current;
		}
	}

	return master;
}

Segment *
GetStandbySegment(void)
{
	SegmentConfigurationIterator	iterator;
	Segment	*current = NULL;
	Segment *standby = NULL;

	InitIterateSegmentConfiguration(&iterator);
	while ((current = IterateSegmentConfiguration(&iterator)))
	{
		if (!current->standby)
		{
			pfree(current->hostname);
			pfree(current);
		}
		else
		{
			standby = current;
		}
	}

	return standby;
}

List *
GetSegmentList(void)
{
	List	*segments = NIL;
	SegmentConfigurationIterator	iterator;
	Segment	*current = NULL;

	InitIterateSegmentConfiguration(&iterator);
	while ((current = IterateSegmentConfiguration(&iterator)))
	{
		if (current->master || current->standby)
			continue;
		segments = lappend(segments, current);
	}

	return segments;
}

/*
 * getCdbComponentDatabases
 *
 *
 * Storage for the SegmentInstances block and all subsidiary
 * structures are allocated from the caller's context.
 */
CdbComponentDatabases *
getCdbComponentInfo(bool DNSLookupAsError)
{
#if 0
	CdbComponentDatabaseInfo *pOld = NULL;
	CdbComponentDatabases *component_databases = NULL;

	Relation gp_seg_config_rel;
	HeapTuple gp_seg_config_tuple = NULL;
	HeapScanDesc gp_seg_config_scan;

	/*
	 * Initial size for info arrays.
	 */
	int			segment_array_size = 500;
	int			entry_array_size = 4; /* we currently support a max of 2 */

	/*
	 * isNull and attr are used when getting the data for a specific column from a HeapTuple
	 */
	bool		isNull;
	Datum		attr;

	/*
	 * Local variables for fields from the rows of the tables that we are reading.
	 */

	char		role;
	char		status = 0;

	int			i;

	/*
	 * Allocate component_databases return structure and
	 * component_databases->segment_db_info array with an initial size
	 * of 128, and component_databases->entry_db_info with an initial
	 * size of 4.  If necessary during row fetching, we grow these by
	 * doubling each time we run out.
	 */
	component_databases = palloc0(sizeof(CdbComponentDatabases));

	component_databases->segment_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * segment_array_size);

	component_databases->entry_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * entry_array_size);

	gp_seg_config_rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	gp_seg_config_scan = heap_beginscan(gp_seg_config_rel, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(gp_seg_config_tuple = heap_getnext(gp_seg_config_scan, ForwardScanDirection)))
	{
		/*
		 * Grab the fields that we need from gp_configuration.  We do
		 * this first, because until we read them, we don't know
		 * whether this is an entry database row or a segment database
		 * row.
		 */
		CdbComponentDatabaseInfo *pRow;

		/*
		 * dbid
		 */
		//attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_dbid, RelationGetDescr(gp_seg_config_rel), &isNull);
		//Assert(!isNull);
		//dbid = DatumGetInt16(attr);

		/*
		 * content
		 */
		//attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_content, RelationGetDescr(gp_seg_config_rel), &isNull);
		//Assert(!isNull);
		//content = DatumGetInt16(attr);

		/*
		 * role
		 */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_role, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		role = DatumGetChar(attr);

		/*
		 * preferred-role
		 */
		//attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_preferred_role, RelationGetDescr(gp_seg_config_rel), &isNull);
		//Assert(!isNull);
		//preferred_role = DatumGetChar(attr);

		/*
		 * mode
		 */
		//attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_mode, RelationGetDescr(gp_seg_config_rel), &isNull);
		//Assert(!isNull);
		//mode = DatumGetChar(attr);

		/*
		 * status
		 */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_status, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		status = DatumGetChar(attr);

		/*
		 * Determine which array to place this rows data in: entry or
		 * segment, based on the content field.
		 */
		if (role == SEGMENT_ROLE_PRIMARY)
		{
			/* if we have a dbid bigger than our array we'll have to grow the array. (MPP-2104) */
			if (dbid >= segment_array_size || component_databases->total_segment_dbs >= segment_array_size)
			{
				/*
				 * Expand CdbComponentDatabaseInfo array if we've used up currently allocated space
				 */
				segment_array_size = Max((segment_array_size * 2), dbid * 2);
				pOld = component_databases->segment_db_info;
				component_databases->segment_db_info = (CdbComponentDatabaseInfo *)
					repalloc(pOld, sizeof(CdbComponentDatabaseInfo) * segment_array_size);
			}

			pRow = &component_databases->segment_db_info[component_databases->total_segment_dbs];
			component_databases->total_segment_dbs++;
		}
		else
		{
			if (component_databases->total_entry_dbs >= entry_array_size)
			{
				/*
				 * Expand CdbComponentDatabaseInfo array if we've used up currently allocated space
				 */
				entry_array_size *= 2;
				pOld = component_databases->entry_db_info;
				component_databases->entry_db_info = (CdbComponentDatabaseInfo *)
					repalloc(pOld, sizeof(CdbComponentDatabaseInfo) * entry_array_size);
			}

			pRow = &component_databases->entry_db_info[component_databases->total_entry_dbs];
			component_databases->total_entry_dbs++;
		}

		pRow->role = role;
		pRow->status = status;

		/*
		 * hostname
		 */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_hostname, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		pRow->hostname = TextDatumGetCString(attr);

		/*
		 * address
		 */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_address, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		pRow->address = TextDatumGetCString(attr);
		
		/*
		 * port
		 */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_port, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		pRow->port = DatumGetInt32(attr);

		getAddressesForDBid(pRow, DNSLookupAsError ? ERROR : LOG);
		pRow->hostip = pRow->hostaddrs[0];
	}

	/*
	 * We're done with the catalog entries, cleanup them up, closing
	 * all the relations we opened.
	 */
	heap_endscan(gp_seg_config_scan);
	heap_close(gp_seg_config_rel, AccessShareLock);

	/*
	 * Validate that there exists at least one entry and one segment
	 * database in the configuration
	 */
	if (component_databases->total_segment_dbs == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("Greenplum Database number of segment databases cannot be 0")));
	}
	if (component_databases->total_entry_dbs == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("Greenplum Database number of entry databases cannot be 0")));
	}

	/*
	 * Now sort the data by segindex, isprimary desc
	 */
	qsort(component_databases->segment_db_info,
		  component_databases->total_segment_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	qsort(component_databases->entry_db_info,
		  component_databases->total_entry_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	/*
	 * Now count the number of distinct segindexes.
	 * Since it's sorted, this is easy.
	 */
	for (i = 0; i < component_databases->total_segment_dbs; i++)
	{
		if (i == 0 ||
			(component_databases->segment_db_info[i].segindex != component_databases->segment_db_info[i - 1].segindex))
		{
			component_databases->total_segments++;
		}
	}

	return component_databases;
#endif
	return NULL;
}

/*
 * getCdbComponentDatabases
 *
 *
 * Storage for the SegmentInstances block and all subsidiary
 * structures are allocated from the caller's context.
 */
CdbComponentDatabases *
getCdbComponentDatabases(void)
{
	return getCdbComponentInfo(true);
}

/*
 * freeCdbComponentDatabases
 *
 * Releases the storage occupied by the CdbComponentDatabases
 * struct pointed to by the argument.
 */
void
freeCdbComponentDatabases(CdbComponentDatabases *pDBs)
{
	int	i;

	if (pDBs == NULL)
		return;

	if (pDBs->segment_db_info != NULL)
	{
		for (i = 0; i < pDBs->total_segment_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &pDBs->segment_db_info[i];

			freeCdbComponentDatabaseInfo(cdi);
		}

		pfree(pDBs->segment_db_info);
	}

	if (pDBs->entry_db_info != NULL)
	{
		for (i = 0; i < pDBs->total_entry_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &pDBs->entry_db_info[i];

			freeCdbComponentDatabaseInfo(cdi);
		}

		pfree(pDBs->entry_db_info);
	}

	pfree(pDBs);
}

/*
 * freeCdbComponentDatabaseInfo:
 * Releases any storage allocated for members variables of a CdbComponentDatabaseInfo struct.
 */
void
freeCdbComponentDatabaseInfo(CdbComponentDatabaseInfo *cdi)
{
	int i;

	if (cdi == NULL)
		return;

	if (cdi->hostname != NULL)
		pfree(cdi->hostname);

	if (cdi->address != NULL)
		pfree(cdi->address);

	for (i=0; i < COMPONENT_DBS_MAX_ADDRS; i++)
	{
		if (cdi->hostaddrs[i] != NULL)
		{
			pfree(cdi->hostaddrs[i]);
			cdi->hostaddrs[i] = NULL;
		}
	}
}

/*
 * performs all necessary setup required for Greenplum Database mode.
 *
 * This includes cdblink_setup() and initializing the Motion Layer.
 */
void
cdb_setup(void)
{
	elog(DEBUG1, "Initializing Greenplum components...");

	cdblink_setup();

	/* If gp_role is UTILITY, skip this call. */
	if (Gp_role != GP_ROLE_UTILITY)
	{
		/* Initialize the Motion Layer IPC subsystem. */
		InitMotionLayerIPC();
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* check mirrored entry db configuration */
		buildMirrorQDDefinition();

		if (isQDMirroringPendingCatchup())
		{
			/* 
			 * Do a checkpoint to cause a checkpoint xlog record to
			 * to be written and force the QD mirror to get
			 * synchronized.
			 */
			RequestCheckpoint(true, false);
		}

		/*
		 * This call will generate a warning if master mirroring
		 * is not synchronized.
		 */
		QDMirroringWriteCheck();
	}
}


/*
 * performs all necessary cleanup required when leaving Greenplum
 * Database mode.  This is also called when the process exits.
 *
 * NOTE: the arguments to this function are here only so that we can
 *		 register it with on_proc_exit().  These parameters should not
 *		 be used since there are some callers to this that pass them
 *		 as NULL.
 *
 */
void
cdb_cleanup(int code __attribute__((unused)) , Datum arg __attribute__((unused)) )
{
	elog(DEBUG1, "Cleaning up Greenplum components...");

	executormgr_cleanup_env();

	if (Gp_role != GP_ROLE_UTILITY)
	{
		/* shutdown our listener socket */
		CleanUpMotionLayerIPC();

		/* Move self out from CGroup. */
		if (GetQEIndex() != -1)
		{
			OnMoveOutCGroupForQE();
		}
	}
}

/*
 * CdbComponentDatabaseInfoCompare:
 * A compare function for CdbComponentDatabaseInfo structs
 * that compares based on , isprimary desc
 * for use with qsort.
 */
#if 0
static int
CdbComponentDatabaseInfoCompare(const void *p1, const void *p2)
{
	const CdbComponentDatabaseInfo *obj1 = (CdbComponentDatabaseInfo *) p1;
	const CdbComponentDatabaseInfo *obj2 = (CdbComponentDatabaseInfo *) p2;

	int			cmp = obj1->segindex - obj2->segindex;

	if (cmp == 0)
	{
		int obj2cmp=0;
		int obj1cmp=0;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj2))
			obj2cmp = 1;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj1))
			obj1cmp = 1;

		cmp = obj2cmp - obj1cmp;
	}

	return cmp;
}
#endif
/*
 * We're going to sort interface-ids by priority.  So we need a little
 * struct, and a comparison function to hand-off to qsort().
 */
struct priority_iface {
	int priority;
	int interface_id;
};

/*
 * iface_priority_compare() A compare function for interface-priority
 * structs.  for use with qsort.
 */
#if 0
static int
iface_priority_compare(const void *p1, const void *p2)
{
	const struct priority_iface *obj1 = (struct priority_iface *) p1;
	const struct priority_iface *obj2 = (struct priority_iface *) p2;

	return (obj1->priority - obj2->priority);
}
#endif

/*
 * Maintain a cache of names.
 *
 * The keys are all NAMEDATALEN long.
 */
static char *
getDnsCachedAddress(char *name, int port, int elevel)
{
	struct segment_ip_cache_entry *e;

	if (segment_ip_cache_htab == NULL)
	{
		HASHCTL		hash_ctl;

		MemSet(&hash_ctl, 0, sizeof(hash_ctl));

		hash_ctl.keysize = NAMEDATALEN + 1;
		hash_ctl.entrysize = sizeof(struct segment_ip_cache_entry);

		segment_ip_cache_htab = hash_create("segment_dns_cache",
											256,
											&hash_ctl,
											HASH_ELEM);
		Assert(segment_ip_cache_htab != NULL);
	}

	e = (struct segment_ip_cache_entry *)hash_search(segment_ip_cache_htab,
													 name, HASH_FIND, NULL);

	/* not in our cache, we've got to actually do the name lookup. */
	if (e == NULL)
	{
		MemoryContext oldContext;
		int			ret;
		char		portNumberStr[32];
		char	   *service;
		struct addrinfo *addrs = NULL,
			*addr;
		struct addrinfo hint;
			
		/* Initialize hint structure */
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = AF_UNSPEC;
			
		snprintf(portNumberStr, sizeof(portNumberStr), "%d", port);
		service = portNumberStr;
				
		ret = pg_getaddrinfo_all(name, service, &hint, &addrs);
		if (ret || !addrs)
		{
			if (addrs)
				pg_freeaddrinfo_all(hint.ai_family, addrs);

			ereport(elevel,
					(errmsg("could not translate host name \"%s\", port \"%d\" to address: %s",
							name, port, gai_strerror(ret))));

			return NULL;
		}
			
		/* save in the cache context */
		oldContext = MemoryContextSwitchTo(TopMemoryContext);
			
		for (addr = addrs; addr; addr = addr->ai_next)
		{
#ifdef HAVE_UNIX_SOCKETS
			/* Ignore AF_UNIX sockets, if any are returned. */
			if (addr->ai_family == AF_UNIX)
				continue;
#endif
			if (addr->ai_family == AF_INET) /* IPv4 address */
			{
				char		hostinfo[NI_MAXHOST];

				pg_getnameinfo_all((struct sockaddr_storage *)addr->ai_addr, addr->ai_addrlen,
								   hostinfo, sizeof(hostinfo),
								   NULL, 0,
								   NI_NUMERICHOST);

				/* INSERT INTO OUR CACHE HTAB HERE */

				e = (struct segment_ip_cache_entry *)hash_search(segment_ip_cache_htab,
																 name,
																 HASH_ENTER,
																 NULL);
				Assert(e != NULL);
				memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));

				break;
			}
		}

#ifdef HAVE_IPV6
		/*
		 * IPv6 probably would work fine, we'd just need to make sure all the data structures are big enough for
		 * the IPv6 address.  And on some broken systems, you can get an IPv6 address, but not be able to bind to it
		 * because IPv6 is disabled or missing in the kernel, so we'd only want to use the IPv6 address if there isn't
		 * an IPv4 address.  All we really need to do is test this.
		 */
		if (e == NULL && addrs->ai_family == AF_INET6)
		{
			char		hostinfo[NI_MAXHOST];
			addr = addrs;



			pg_getnameinfo_all((struct sockaddr_storage *)addr->ai_addr, addr->ai_addrlen,
							   hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);

			/* INSERT INTO OUR CACHE HTAB HERE */

			e = (struct segment_ip_cache_entry *)hash_search(segment_ip_cache_htab,
															 name,
															 HASH_ENTER,
															 NULL);
			Assert(e != NULL);
			memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));

		}
#endif
			
		MemoryContextSwitchTo(oldContext);
			
		pg_freeaddrinfo_all(hint.ai_family, addrs);
	}

	/* return a pointer to our cache. */
	return e->hostinfo;
}

/*
 * getDnsAddress
 * 
 * same as getDnsCachedAddress, but without caching. Looks like the
 * non-cached version was used inline inside of cdbgang.c, and since
 * it is needed now elsewhere, it is factored out to this routine.
 */
char *
getDnsAddress(char *hostname, int port, int elevel)
{
	int			ret;
	char		portNumberStr[32];
	char	   *service;
	char	   *result = NULL;
	struct addrinfo *addrs = NULL,
			   *addr;
	struct addrinfo hint;

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;

	snprintf(portNumberStr, sizeof(portNumberStr), "%d", port);
	service = portNumberStr;

	ret = pg_getaddrinfo_all(hostname, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		ereport(elevel,
				(errmsg("could not translate host name \"%s\", port \"%d\" to address: %s",
						hostname, port, gai_strerror(ret))));
	}
	for (addr = addrs; addr; addr = addr->ai_next)
	{
#ifdef HAVE_UNIX_SOCKETS
		/* Ignore AF_UNIX sockets, if any are returned. */
		if (addr->ai_family == AF_UNIX)
			continue;
#endif
		if (addr->ai_family == AF_INET) /* IPv4 address */
		{
			char		hostinfo[NI_MAXHOST];

			/* Get a text representation of the IP address */
			pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
							   hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);
			result = pstrdup(hostinfo);
			break;
		}
	}

#ifdef HAVE_IPV6
	/*
	 * IPv6 should would work fine, we'd just need to make sure all the data structures are big enough for
	 * the IPv6 address.  And on some broken systems, you can get an IPv6 address, but not be able to bind to it
	 * because IPv6 is disabled or missing in the kernel, so we'd only want to use the IPv6 address if there isn't
	 * an IPv4 address.  All we really need to do is test this.
	 */
	if (result == NULL && addrs->ai_family == AF_INET6)
	{
		char		hostinfo[NI_MAXHOST];
		addr = addrs;
		/* Get a text representation of the IP address */
					pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
									   hostinfo, sizeof(hostinfo),
									   NULL, 0,
									   NI_NUMERICHOST);
					result = pstrdup(hostinfo);
	}
#endif

	pg_freeaddrinfo_all(hint.ai_family, addrs);
	
	return result;
}


/*
 * Given a component-db in the system, find the addresses at which it
 * can be reached, appropriately populate the argument-structure, and
 * maintain the ip-lookup-cache.
 *
 * We get all of the interface-ids, sort them in priority order, then
 * go get their details ... and then make sure they're cached properly.
 */
#if 0
static void
getAddressesForDBid(CdbComponentDatabaseInfo *c, int elevel)
{
	Relation	gp_db_interface_rel;
	Relation	gp_interface_rel;
	HeapTuple	tuple;
	ScanKeyData	key;
	SysScanDesc	dbscan, ifacescan;

	int			j, i=0;
	struct priority_iface *ifaces=NULL;
	int			iface_count, iface_max=0;

	Datum		attr;
	bool		isNull;

	int			dbid;
	int			iface_id;
	int			priority;

	char		*name;

	Assert(c != NULL);
		
	gp_db_interface_rel = heap_open(GpDbInterfacesRelationId, AccessShareLock);

	/* CaQL UNDONE: no test coverage */
	ScanKeyInit(&key, Anum_gp_db_interfaces_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				ObjectIdGetDatum(0));

	dbscan = systable_beginscan(gp_db_interface_rel, GpDbInterfacesDbidIndexId,
								true, SnapshotNow, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(dbscan)))
	{
		i++;
		if (i > iface_max)
		{
			/* allocate 8-more slots */
			if (ifaces == NULL)
				ifaces = palloc((iface_max + 8) * sizeof(struct priority_iface));
			else
				ifaces = repalloc(ifaces, (iface_max + 8) * sizeof(struct priority_iface));

			memset(ifaces + iface_max, 0, 8 * sizeof(struct priority_iface));
			iface_max += 8;
		}

		/* dbid is for sanity-check on scan condition only */
		attr = heap_getattr(tuple, Anum_gp_db_interfaces_dbid, gp_db_interface_rel->rd_att, &isNull);
		Assert(!isNull);
		dbid = DatumGetInt16(attr);
		//Assert(dbid == c->dbid);

		attr = heap_getattr(tuple, Anum_gp_db_interfaces_interfaceid, gp_db_interface_rel->rd_att, &isNull);
		Assert(!isNull);
		iface_id = DatumGetInt16(attr);

		attr = heap_getattr(tuple, Anum_gp_db_interfaces_priority, gp_db_interface_rel->rd_att, &isNull);
		Assert(!isNull);
		priority = DatumGetInt16(attr);

		ifaces[i-1].priority = priority;
		ifaces[i-1].interface_id = iface_id;
	}
	iface_count = i;

	/* Finish up scan and close appendonly catalog. */
	systable_endscan(dbscan);

	heap_close(gp_db_interface_rel, AccessShareLock);

	/* we now have the unsorted list, or an empty list. */
	do
	{
		/* fallback to using hostname if our list is empty */
		if (iface_count == 0)
			break;

		qsort(ifaces, iface_count, sizeof(struct priority_iface), iface_priority_compare);

		/* we now have interfaces, sorted by priority. */

		gp_interface_rel = heap_open(GpInterfacesRelationId, AccessShareLock);

		j=0;
		for (i=0; i < iface_count; i++)
		{
			int status=0;

			/* CaQL UNDONE: no test coverage */
			/* Start a new scan. */
			ScanKeyInit(&key, Anum_gp_interfaces_interfaceid,
						BTEqualStrategyNumber, F_INT2EQ,
						ObjectIdGetDatum(ifaces[i].interface_id));

			ifacescan = systable_beginscan(gp_interface_rel, GpInterfacesInterfaceidIndexId,
										   true, SnapshotNow, 1, &key);

			tuple = systable_getnext(ifacescan);

			Assert(HeapTupleIsValid(tuple));

			/* iface_id is for sanity-check on scan condition only */
			attr = heap_getattr(tuple, Anum_gp_interfaces_interfaceid, gp_interface_rel->rd_att, &isNull);
			Assert(!isNull);
			iface_id = DatumGetInt16(attr);
			Assert(iface_id == ifaces[i].interface_id); 

			attr = heap_getattr(tuple, Anum_gp_interfaces_status, gp_interface_rel->rd_att, &isNull);
			Assert(!isNull);
			status = DatumGetInt16(attr);

			/* if the status is "alive" use the interface. */
			if (status == 1)
			{
				attr = heap_getattr(tuple, Anum_gp_interfaces_address, gp_interface_rel->rd_att, &isNull);
				Assert(!isNull);
				name = getDnsCachedAddress(DatumGetCString(attr), c->port, elevel);
				if (name)
					c->hostaddrs[j++] = pstrdup(name);
			}

			systable_endscan(ifacescan);
		}

		heap_close(gp_interface_rel, AccessShareLock);

		/* fallback to using hostname if our list is empty */
		if (j == 0)
			break;

		/* successfully retrieved at least one entry. */

		return;
	}
	while (0);

	/* fallback to using hostname */
	memset(c->hostaddrs, 0, COMPONENT_DBS_MAX_ADDRS * sizeof(char *));

	/*
	 * add an entry, using the first the "address" and then the
	 * "hostname" as fallback.
	 */
	name = getDnsCachedAddress(c->address, c->port, elevel);

	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
		return;
	}

	/* now the hostname. */
	name = getDnsCachedAddress(c->hostname, c->port, elevel);
	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
	}
	else
	{
		c->hostaddrs[0] = NULL;
	}

	return;
}
#endif

/*
 * Given total number of primary segment databases and a number of
 * segments to "skip" - this routine creates a boolean map (array) the
 * size of total number of segments and randomly selects several
 * entries (total number of total_to_skip) to be marked as
 * "skipped". This is used for external tables with the 'gpfdist'
 * protocol where we want to get a number of *random* segdbs to
 * connect to a gpfdist client.
 *
 * Caller of this function should pfree skip_map when done with it.
 */
bool *
makeRandomSegMap(int total_primaries, int total_to_skip)
{
	int			randint;     /* some random int representing a seg    */
	int			skipped = 0; /* num segs already marked to be skipped */
	bool		*skip_map;
	
	skip_map = (bool *) palloc(total_primaries * sizeof(bool));
	MemSet(skip_map, false, total_primaries * sizeof(bool));
	
	while (total_to_skip != skipped)
	{
		/*
		 * create a random int between 0 and (total_primaries - 1).
		 * 
		 * NOTE that the lower and upper limits in cdb_randint() are
		 * inclusive so we take them into account. In reality the
		 * chance of those limits to get selected by the random
		 * generator is extremely small, so we may want to find a
		 * better random generator some time (not critical though).
		 */
		randint = cdb_randint(0, total_primaries - 1);
		
		/*
		 * mark this random index 'true' in the skip map (marked to be
		 * skipped) unless it was already marked.
		 */
		if (skip_map[randint] == false)
		{
			skip_map[randint] = true;
			skipped++;
		}
	}	
	
	return skip_map;
}

/*
 * get dbinfo by registration_order
 */
CdbComponentDatabaseInfo *
registration_order_get_dbinfo(int32 order)
{
	HeapTuple tuple;
	Relation rel;
	bool bOnly;
	CdbComponentDatabaseInfo *i = NULL;
	cqContext	cqc;

	if (!AmActiveMaster() && !AmStandbyMaster())
		elog(ERROR, "registration_order_get_dbinfo() executed on execution segment");

	rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	tuple = caql_getfirst_only(
			caql_addrel(cqclr(&cqc), rel),
			&bOnly,
			cql("SELECT * FROM gp_segment_configuration "
				" WHERE registration_order = :1 ",
				Int32GetDatum(order)));

	if (HeapTupleIsValid(tuple))
	{
		Datum attr;
		bool isNull;

		i = palloc(sizeof(CdbComponentDatabaseInfo));

		/*
		 * role
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_role,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->role = DatumGetChar(attr);

		/*
		 * status
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_status,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->status = DatumGetChar(attr);

		/*
		 * hostname
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_hostname,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->hostname = TextDatumGetCString(attr);

		/*
		 * address
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_address,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->address = TextDatumGetCString(attr);

		/*
		 * port
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_port,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->port = DatumGetInt32(attr);

	}
	else
	{
		heap_close(rel, NoLock);
		elog(ERROR, "could not find configuration entry for registration_order %c",
				order);
	}

	heap_close(rel, NoLock);

	return i;
}

/*
 * get dbinfo by role
 * There should be only one master in gp_segment_configuration table, one standby at most.
 */
CdbComponentDatabaseInfo *
role_get_dbinfo(char role)
{
	HeapTuple tuple;
	Relation rel;
	cqContext	cqc;
	bool bOnly;
	CdbComponentDatabaseInfo *i = NULL;

	Assert(role == SEGMENT_ROLE_PRIMARY ||
		   role == SEGMENT_ROLE_MASTER_CONFIG ||
		   role == SEGMENT_ROLE_STANDBY_CONFIG);
	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.  This may be able to be relaxed
	 * by switching to a different method of checking.
	 */
	if (!AmActiveMaster() && !AmStandbyMaster())
		elog(ERROR, "role_get_dbinfo() executed on execution segment");

	rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	tuple = caql_getfirst_only(
			caql_addrel(cqclr(&cqc), rel),
			&bOnly,
			cql("SELECT * FROM gp_segment_configuration "
				" WHERE role = :1 ",
				CharGetDatum(role)));

	if (HeapTupleIsValid(tuple))
	{
		Datum attr;
		bool isNull;

		i = palloc(sizeof(CdbComponentDatabaseInfo));

		/*
		 * role
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_role,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->role = DatumGetChar(attr);

		/*
		 * status
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_status,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->status = DatumGetChar(attr);

		/*
		 * hostname
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_hostname,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->hostname = TextDatumGetCString(attr);

		/*
		 * address
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_address,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->address = TextDatumGetCString(attr);

		/*
		 * port
		 */
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_port,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		i->port = DatumGetInt32(attr);

	}
	else
	{
		elog(ERROR, "could not find configuration entry for role %c", role);
	}

	heap_close(rel, NoLock);

	return i;
}
