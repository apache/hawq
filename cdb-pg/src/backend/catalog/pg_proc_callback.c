/*-------------------------------------------------------------------------
 *
 * pg_proc_callback.c
 *	  
 *   Auxillary extension to pg_proc to enable defining additional callback
 *   functions.  Currently the list of allowable callback functions is small
 *   and consists of:
 *     - DESCRIBE() - to support more complex pseudotype resolution
 *
 * Copyright (c) EMC, 2011
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc_callback.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"

/* ---------------------
 * deleteProcCallbacks() - Remove callbacks from pg_proc_callback
 *
 * Parameters:
 *    profnoid - remove all callbacks for this function oid
 *
 * Notes:
 *    This function does not maintain dependencies in pg_depend, that behavior
 *    is currently controlled in pg_proc.c
 * ---------------------
 */
void 
deleteProcCallbacks(Oid profnoid)
{
	Relation    proc_callback_rel;
	HeapTuple   tup;
	ScanKeyData key[1];
	SysScanDesc scan;

	Insist(OidIsValid(profnoid));

	/* 
	 * Boiler template code to loop through the index and remove all matching
	 * rows.
	 */
	proc_callback_rel = heap_open(ProcCallbackRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_proc_callback_profnoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(profnoid));

	scan = systable_beginscan(proc_callback_rel, 
							  ProcCallbackProfnoidPromethodIndexId,
							  true, SnapshotNow, 1, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		simple_heap_delete(proc_callback_rel, &tup->t_self);
	}
	
	systable_endscan(scan);

	heap_close(proc_callback_rel, RowExclusiveLock);
}


/* ---------------------
 * addProcCallback() - Add a new callback to pg_proc_callback
 *
 * Parameters:
 *    profnoid    - oid of the function that has a callback
 *    procallback - oid of the callback function
 *    promethod   - role the callback function plays
 *
 * Notes:
 *    This function does not maintain dependencies in pg_depend, that behavior
 *    is currently controlled in pg_proc.c
 * ---------------------
 */
void 
addProcCallback(Oid profnoid, Oid procallback, char promethod)
{
	bool		nulls[Natts_pg_proc_callback];
	Datum		values[Natts_pg_proc_callback];
	Relation    proc_callback_rel;
	TupleDesc   tupDesc;
	HeapTuple   tup;
	
	Insist(OidIsValid(profnoid));
	Insist(OidIsValid(procallback));

	/* open pg_proc_callback */
	proc_callback_rel = heap_open(ProcCallbackRelationId, RowExclusiveLock);
	tupDesc = proc_callback_rel->rd_att;

	/* Build the tuple and insert it */
	nulls[Anum_pg_proc_callback_profnoid - 1]	  = false;
	nulls[Anum_pg_proc_callback_procallback - 1]  = false;
	nulls[Anum_pg_proc_callback_promethod - 1]	  = false;
	values[Anum_pg_proc_callback_profnoid - 1]	  = ObjectIdGetDatum(profnoid);
	values[Anum_pg_proc_callback_procallback - 1] = ObjectIdGetDatum(procallback);
	values[Anum_pg_proc_callback_promethod - 1]	  = CharGetDatum(promethod);
	tup = heap_form_tuple(tupDesc, values, nulls);
	
	/* Insert tuple into the relation */
	simple_heap_insert(proc_callback_rel, tup);
	CatalogUpdateIndexes(proc_callback_rel, tup);

	heap_close(proc_callback_rel, RowExclusiveLock);
}


/* ---------------------
 * lookupProcCallback() - Find a specified callback for a specified function
 *
 * Parameters:
 *    profnoid    - oid of the function that has a callback
 *    promethod   - which callback to find
 * ---------------------
 */
Oid  
lookupProcCallback(Oid profnoid, char promethod)
{
	Oid         result = InvalidOid;
	Relation    proc_callback_rel;
	HeapTuple   tup;
	ScanKeyData key[2];
	SysScanDesc scan;

	Insist(OidIsValid(profnoid));

	/* Lookup (profnoid, promethod) from index */
	proc_callback_rel = heap_open(ProcCallbackRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_proc_callback_profnoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(profnoid));
	ScanKeyInit(&key[1],
				Anum_pg_proc_callback_promethod,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(promethod));

	scan = systable_beginscan(proc_callback_rel, 
							  ProcCallbackProfnoidPromethodIndexId,
							  true, SnapshotNow, 2, key);

	/* (profnoid, promethod) is guaranteed unique by the index */
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		Form_pg_proc_callback procForm = (Form_pg_proc_callback) GETSTRUCT(tup);
		result = procForm->procallback;
	}
	
	systable_endscan(scan);

	heap_close(proc_callback_rel, AccessShareLock);	

	return result;
}
