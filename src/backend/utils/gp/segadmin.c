/*-------------------------------------------------------------------------
 *
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
 *
 * Functions to support administrative tasks with GPDB segments.
 *
 */
#include "postgres.h"
#include "miscadmin.h"

#include "catalog/catquery.h"
#include "catalog/gp_segment_config.h"
#include "catalog/pg_database.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbresynchronizechangetracking.h"
#include "cdb/cdbdatabaseinfo.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbmirroredfilesysobj.h"
#include "cdb/cdbpersistentdatabase.h"
#include "cdb/cdbpersistentfilespace.h"
#include "cdb/cdbpersistentrelfile.h"
#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/segadmin.h"

#define MASTER_ONLY 0x1
#define UTILITY_MODE 0x2
#define SUPERUSER 0x4
#define READ_ONLY 0x8
#define SEGMENT_ONLY 0x10
#define STANDBY_ONLY 0x20
#define SINGLE_USER_MODE 0x40

/*
 * Tell the caller whether a standby master is defined in the system.
 */
static bool
standby_exists()
{
	return caql_getcount(
			 NULL,
			 cql("SELECT COUNT(*) FROM gp_segment_configuration "
				 " WHERE role = :1 ",
				 CharGetDatum(SEGMENT_ROLE_STANDBY_CONFIG))) > 0;
}

/*
 * Check that the code is being called in right context.
 */
static void
mirroring_sanity_check(int flags, const char *func)
{
	if ((flags & MASTER_ONLY) == MASTER_ONLY)
	{
		/* TODO: Add new check */
	}

	if ((flags & UTILITY_MODE) == UTILITY_MODE)
	{
		if (Gp_role != GP_ROLE_UTILITY)
			elog(ERROR, "%s must be run in utility mode", func);
	}

	if ((flags & SINGLE_USER_MODE) == SINGLE_USER_MODE)
	{
		if (IsUnderPostmaster)
			elog(ERROR, "%s must be run in single-user mode", func);
	}

	if ((flags & SUPERUSER) == SUPERUSER)
	{
		if (!superuser())
			elog(ERROR, "%s can only be run by a superuser", func);
	}

	if ((flags & READ_ONLY) == READ_ONLY)
	{
		if (gp_set_read_only != true)
			elog(ERROR, "%s can only be run if the system is in read only mode",
				 func);
	}

	if ((flags & SEGMENT_ONLY) == SEGMENT_ONLY)
	{
		/* TODO: Add new check */
	}

	if ((flags && STANDBY_ONLY) == STANDBY_ONLY)
	{
		/* TODO: Add new check */
	}
}

/*
 * Tell the master about a new segment.
 *
 * gp_add_segment(hostname, address, port,
 *				  filespace_map)
 *
 * Args:
 *   hostname - host name string
 *   address - either hostname or something else
 *   port - port number
 *   filespace_map - A 2-d mapping of filespace to path on the new node
 *
 * Returns the dbid of the new segment.
 */
Datum
gp_add_segment(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_add_segment is not supported");
	PG_RETURN_BOOL(false);
}

/*
 * Master function to remove a segment from all catalogs
 */
static void
remove_segment(int16 order)
{
	cqContext cqc;
	int numDel;

	if(order == MASTER_ORDER_ID || order == STANDBY_ORDER_ID)
		return;

	/* remove this segment from gp_segment_configuration */
	Relation rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	numDel= caql_getcount(caql_addrel(cqclr(&cqc), rel),
				cql("DELETE FROM gp_segment_configuration "
					" WHERE registration_order = :1",
					Int16GetDatum(order)));

	elog(LOG, "Remove segment successfully! Count : %d. Registration order : %d",
			  numDel,
			  order);

	heap_close(rel, NoLock);
}

/*
 * Remove knowledge of a segment from the master.
 *
 * gp_remove_segment(order)
 *
 * Args:
 *   order - order of registration
 *
 * Returns:
 *   true on success, otherwise error.
 */
Datum
gp_remove_segment(PG_FUNCTION_ARGS)
{
	int16 order;

 	if (PG_ARGISNULL(0))
		elog(ERROR, "Registration id cannot be NULL");

 	order = PG_GETARG_INT16(0);

	mirroring_sanity_check(MASTER_ONLY | SUPERUSER | UTILITY_MODE,
						   "gp_remove_segment");

	if (order == MASTER_ORDER_ID || order == STANDBY_ORDER_ID)
		elog(ERROR, "Cannot remove master or standby");

	remove_segment(order);

	PG_RETURN_BOOL(true);
}

/*
 * Add a mirror of an existing segment.
 *
 * gp_add_segment_mirror(contentid, hostname, address, port,
 * 						 replication_port, filespace_map)
 */
Datum
gp_add_segment_mirror(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_add_segment_mirror is not supported");
	PG_RETURN_BOOL(false);
}

/*
 * Remove a segment mirror.
 *
 * gp_remove_segment_mirror(contentid)
 *
 * Args:
 *   contentid - segment index at which to remove the mirror
 *
 * Returns:
 *   true upon success, otherwise throws error.
 */
Datum
gp_remove_segment_mirror(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_remove_segment_mirror is not supported");
	PG_RETURN_BOOL(false);
}

/*
 * Add a master standby.
 *
 * gp_add_master_standby(hostname, address)
 *
 * Args:
 *  hostname - as above
 *  address - as above
 *
 * Returns:
 *  dbid of the new standby
 */
Datum
gp_add_master_standby(PG_FUNCTION_ARGS)
{
	CdbComponentDatabaseInfo *master = NULL;
	Relation	gprel;
	Datum values[Natts_gp_segment_configuration];
	bool nulls[Natts_gp_segment_configuration];
	HeapTuple tuple;
	cqContext	cqc;
	cqContext  *pcqCtx = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "host name cannot be NULL");
	if (PG_ARGISNULL(1))
		elog(ERROR, "address cannot be NULL");

	mirroring_sanity_check(MASTER_ONLY | UTILITY_MODE,
						   "gp_add_master_standby");

	if (standby_exists())
		elog(ERROR, "only a single master standby may be defined");

	/* master */
	master = registration_order_get_dbinfo(MASTER_ORDER_ID);

	/* Lock exclusively to avoid concurrent changes */
	gprel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);

	pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), gprel),
				cql("INSERT INTO gp_segment_configuration ", NULL));

	MemSet(nulls, false, sizeof(nulls));

	values[Anum_gp_segment_configuration_registration_order - 1] = Int32GetDatum(STANDBY_ORDER_ID);
	values[Anum_gp_segment_configuration_role - 1] = CharGetDatum(SEGMENT_ROLE_STANDBY_CONFIG);
	values[Anum_gp_segment_configuration_status - 1] = CharGetDatum('u');
	values[Anum_gp_segment_configuration_port - 1] = Int32GetDatum(master->port);
	values[Anum_gp_segment_configuration_hostname - 1] = PG_GETARG_DATUM(0);
	values[Anum_gp_segment_configuration_address - 1] = PG_GETARG_DATUM(1);
	nulls[Anum_gp_segment_configuration_description - 1] = true;

	tuple = caql_form_tuple(pcqCtx, values, nulls);

	/* insert a new tuple */
	caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	caql_endscan(pcqCtx);

	if(master)
		pfree(master);

	heap_close(gprel, NoLock);

	PG_RETURN_INT16(1);
}

static void
update_gp_master_mirroring(char *str)
{
	volatile bool connected = false;
	volatile bool resetModsDML = false;

	PG_TRY();
	{
		StringInfoData sql;

		initStringInfo(&sql);

		appendStringInfo(&sql, "update gp_master_mirroring set "
						 "summary_state = '%s', detail_state = null,"
						 "log_time = current_timestamp, error_message = null",
						 str);

		if (SPI_OK_CONNECT != SPI_connect())
			elog(ERROR, "cannot connect via SPI");
		connected = true;

		if (!allowSystemTableModsDML)
		{
			allowSystemTableModsDML = true;
			resetModsDML = true;
		}
		if (SPI_execute(sql.data, false, 0) < 0)
			elog(ERROR, "cannot update gp_master_mirroring");
		if (resetModsDML)
			allowSystemTableModsDML = false;
	}
	PG_CATCH();
	{
		if (connected)
			SPI_finish();
		if (resetModsDML)
			allowSystemTableModsDML = false;

		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

/*
 * Remove the master standby.
 *
 * gp_remove_master_standby()
 *
 * Returns:
 *  true upon success otherwise false
 */
Datum
gp_remove_master_standby(PG_FUNCTION_ARGS)
{
	int numDel;
	cqContext	cqc;

	mirroring_sanity_check(SUPERUSER | MASTER_ONLY | UTILITY_MODE,
						   "gp_remove_master_standby");

	if (!standby_exists())
		elog(ERROR, "no master standby defined");

	Relation rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	numDel= caql_getcount(caql_addrel(cqclr(&cqc), rel),
				cql("DELETE FROM gp_segment_configuration "
					" WHERE role = :1",
					CharGetDatum(SEGMENT_ROLE_STANDBY_CONFIG)));

	elog(LOG, "Remove standby, count : %d.", numDel);

	heap_close(rel, NoLock);

	update_gp_master_mirroring("Not Configured");

	PG_RETURN_BOOL(true);
}

/*
 * Run only on a segment, we use this to update the filespace locations for the
 * PRIMARY as part of building a full segment. Used by gpexpand.
 *
 * gp_prep_new_segment(filespace_map)
 *
 * Args:
 *   filespace_map - as above
 *
 * Returns:
 *   true upon success, otherwise throws error
 *
 * XXX: This is not effective in HAWQ.
 */
Datum
gp_prep_new_segment(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_prep_new_segment is not supported");
	PG_RETURN_BOOL(false);
}

/*
 * Activate a standby. To do this, we need to change
 *
 * 1. Check that we're actually the standby
 * 2. Remove standby from gp_segment_configuration.
 *
 * gp_activate_standby()
 *
 * Returns:
 *  true upon success, otherwise throws error.
 */
Datum
gp_activate_standby(PG_FUNCTION_ARGS)
{
	cqContext cqc;
	int numDel;
	mirroring_sanity_check(SUPERUSER | UTILITY_MODE | STANDBY_ONLY,
						   PG_FUNCNAME_MACRO);

	if (!AmIStandby())
		elog(ERROR, "%s must be run on the standby master",
			 PG_FUNCNAME_MACRO);

	/* remove standby from gp_segment_configuration */
	Relation rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	numDel= caql_getcount(caql_addrel(cqclr(&cqc), rel),
				cql("DELETE FROM gp_segment_configuration "
					" WHERE role = :1",
					CharGetDatum(SEGMENT_ROLE_STANDBY_CONFIG)));

	elog(LOG, "Remove standby while activating it, count : %d.", numDel);

	heap_close(rel, NoLock);

	/* done */
	PG_RETURN_BOOL(true);
}

/*
 * Add entries to persistent tables on a segment.
 *
 * gp_add_segment_persistent_entries(dbid, mirdbid, filespace_map)
 *
 * Args:
 *  dbid of the primary
 *  mirdbid - dbid of the mirror
 *  filespace_map - as above
 *
 * Returns:
 *  true upon success otherwise false
 *
 * Runs only at the segment level.
 */
Datum
gp_add_segment_persistent_entries(PG_FUNCTION_ARGS)
{
	elog(ERROR, "gp_add_segment_persistent_entries is not supported");
	PG_RETURN_BOOL(false);
}

/*
 * Opposite of gp_add_segment_persistent_entries()
 */
Datum
gp_remove_segment_persistent_entries(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not support");
	PG_RETURN_BOOL(false);
}

void update_segment_status_by_id(uint32_t id, char status)
{
	/* we use AccessExclusiveLock to prevent races */
	Relation rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	HeapTuple tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	Assert(status == 'u' || status == 'd');

	pcqCtx = caql_beginscan(caql_addrel(cqclr(&cqc), rel),
						cql("SELECT * FROM gp_segment_configuration "
							" WHERE registration_order = :1 "
							" FOR UPDATE ",
							Int32GetDatum(id)));

	tuple = caql_getnext(pcqCtx);

	if (tuple != NULL) {
		if (((Form_gp_segment_configuration)GETSTRUCT(tuple))->status != status) {
			((Form_gp_segment_configuration)GETSTRUCT(tuple))->status = status;
			caql_update_current(pcqCtx, tuple);
		}
	} else {
		elog(LOG, "Can not find segment id: %d when update its status", id);
	}

	caql_endscan(pcqCtx);

	heap_close(rel, NoLock);
}
