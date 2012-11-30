/*
 * pg_attribute_encoding.c
 *
 * Routines to manipulation and retrieve column encoding information.
 *
 * Copyright (c) EMC, 2011
 */

#include "postgres.h"
#include "fmgr.h"

#include "access/reloptions.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/dependency.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "parser/analyze.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/*
 * Add a single attribute encoding entry.
 */
static void
add_attribute_encoding_entry(Oid relid, AttrNumber attnum, Datum attoptions)
{
	Relation pgaerel;
	Datum values[Natts_pg_attribute_encoding];
	bool nulls[Natts_pg_attribute_encoding];
	HeapTuple tuple;
	
	Insist(!gp_upgrade_mode);
	Insist(attnum != InvalidAttrNumber);
	
	pgaerel = heap_open(AttributeEncodingRelationId, RowExclusiveLock);

	MemSet(nulls, 0, sizeof(nulls));
	values[Anum_pg_attribute_encoding_attrelid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pg_attribute_encoding_attnum - 1] = Int16GetDatum(attnum);
	values[Anum_pg_attribute_encoding_attoptions - 1] = attoptions;

	tuple = heap_form_tuple(RelationGetDescr(pgaerel), values, nulls);

	simple_heap_insert(pgaerel, tuple);

	CatalogUpdateIndexes(pgaerel, tuple);

	heap_freetuple(tuple);

	heap_close(pgaerel, RowExclusiveLock);
}

/*
 * Get the set of functions implementing a compression algorithm.
 *
 * Intercept requests for "none", since that is not a real compression
 * implementation but a fake one to indicate no compression desired.
 */
PGFunction *
get_funcs_for_compression(char *compresstype)
{
	PGFunction *func = NULL;

	if (!compresstype)
		return func;

	if (pg_strcasecmp("none", compresstype) != 0)
	{
		func = GetCompressionImplementation(compresstype);

		Insist(PointerIsValid(func));
	}
	return func;
}

/*
 * Get datum representations of the attoptions field in pg_attribute_encoding
 * for the given relation.
 */
Datum *
get_rel_attoptions(Oid relid, AttrNumber max_attno)
{
	Form_pg_attribute attform;
	HeapTuple		tuple;
	ScanKeyData		key;
	SysScanDesc		scan;
	Datum		   *dats;
	Relation 		pgae = heap_open(AttributeEncodingRelationId,
									 AccessShareLock);

	/* used for attbyval and len below */
	attform = pgae->rd_att->attrs[Anum_pg_attribute_encoding_attoptions - 1];

	dats = palloc0(max_attno * sizeof(Datum));

	ScanKeyInit(&key,
				Anum_pg_attribute_encoding_attrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pgae,
							  AttributeEncodingAttrelidIndexId,
							  true,
							  SnapshotNow,
							  1,
							  &key);

	while ((tuple = systable_getnext(scan)))
	{
		Form_pg_attribute_encoding a = 
			(Form_pg_attribute_encoding)GETSTRUCT(tuple);
		int16 attnum = a->attnum;
		Datum attoptions;
		bool isnull;

		Insist(attnum > 0 && attnum <= max_attno);

		attoptions = heap_getattr(tuple, Anum_pg_attribute_encoding_attoptions,
								  RelationGetDescr(pgae), &isnull);
		Insist(!isnull);

		dats[attnum - 1] = datumCopy(attoptions,
									 attform->attbyval,
									 attform->attlen);
	}

	systable_endscan(scan);

	heap_close(pgae, AccessShareLock);

	return dats;

}

/*
 * Add pg_attribute_encoding entries for newrelid. Make them identical to those
 * stored for oldrelid.
 */
void
cloneAttributeEncoding(Oid oldrelid, Oid newrelid, AttrNumber max_attno)
{
	Datum *attoptions = get_rel_attoptions(oldrelid, max_attno);
	AttrNumber n;

	for (n = 0; n < max_attno; n++)
	{
		if (DatumGetPointer(attoptions[n]) != NULL)
			add_attribute_encoding_entry(newrelid,
										 n + 1,
										 attoptions[n]);
	}
	CommandCounterIncrement();
}

List **
RelationGetUntransformedAttributeOptions(Relation rel)
{
	List **l;
	int i;
	Datum *dats = get_rel_attoptions(RelationGetRelid(rel),
									 RelationGetNumberOfAttributes(rel));

	l = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(List *));

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
	{
		l[i] = untransformRelOptions(dats[i]);
	}

	return l;
}

/*
 * Get all storage options for all user attributes of the table.
 */
StdRdOptions **
RelationGetAttributeOptions(Relation rel)
{
	Datum *dats;
	StdRdOptions **opts;
	int i;

	opts = palloc0(RelationGetNumberOfAttributes(rel) * sizeof(StdRdOptions *));

	dats = get_rel_attoptions(RelationGetRelid(rel),
							  RelationGetNumberOfAttributes(rel));

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
	{
		if (DatumGetPointer(dats[i]) != NULL)
			opts[i] = (StdRdOptions *)heap_reloptions(0, dats[i], false);
	}

	return opts;
}

/*
 * Return an array of compression function arrays for
 * each attribute in a table.
 *
 * Set NULL for columns without storage options.
 */
PGFunction **
RelationGetColumnCompressionFuncs(Relation rel)
{
	StdRdOptions  **opts = RelationGetAttributeOptions(rel);
	PGFunction	  **funcs = palloc0(RelationGetNumberOfAttributes(rel)
									* sizeof(PGFunction *));
	int 			i;

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
	{
		if (opts[i])
		{
			funcs[i] = get_funcs_for_compression(opts[i]->compresstype);
		}
	}
	return funcs;
}

/* Returns an array of block sizes -- one entry for each user column in rel. */
uint32 *
RelationGetColumnBlocksize(Relation rel)
{
	uint32 		   *bz = palloc(RelationGetNumberOfAttributes(rel) * sizeof(uint32));
	StdRdOptions  **opts = RelationGetAttributeOptions(rel);
	int 			i;

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
	{
		if (opts[i] == NULL)
			bz[i] = DEFAULT_APPENDONLY_BLOCK_SIZE;
		else
			bz[i] = opts[i]->blocksize;
	}

	return bz;
}

uint32
RelationGetRelationBlocksize(Relation rel)
{

  AppendOnlyEntry *aoentry;

  aoentry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);

  return aoentry->blocksize;

}


/*
 * Has the same signature as RelationGetAttributeCompressionFuncs() even though
 * we don't actually need the full Relation data structure. I deem consistency
 * of API more important in this case.
 */
PGFunction *
RelationGetRelationCompressionFuncs(Relation rel)
{
	AppendOnlyEntry *aoentry;
	char *comptype;
	PGFunction *compFuncs;

	aoentry = GetAppendOnlyEntry(RelationGetRelid(rel), SnapshotNow);
	comptype = aoentry->compresstype;

	compFuncs =	get_funcs_for_compression(comptype);

	return compFuncs;

}

/*
 * Given a WITH(...) clause and no other column encoding directives -- such as
 * in the case of CREATE TABLE WITH () AS SELECT -- fill in the column encoding
 * catalog entries for that relation.
 */
void
AddDefaultRelationAttributeOptions(Relation rel, List *options)
{
	Datum opts;
	AttrNumber attno;
	List *ce;

	/* only supported on AOCO at this stage */
	if (!RelationIsAoCols(rel))
		return;

 	ce = form_default_storage_directive(options);
	if (!ce)
		ce = default_column_encoding_clause();

	ce = transformStorageEncodingClause(ce);

	opts = transformRelOptions(PointerGetDatum(NULL), ce, true, false);

	for (attno = 1; attno <= RelationGetNumberOfAttributes(rel); attno++)
		add_attribute_encoding_entry(RelationGetRelid(rel),
									 attno,
									 opts);
	CommandCounterIncrement();
}

/*
 * Work horse underneath DefineRelation().
 *
 * Simply adds user specified ENCODING () clause information to
 * pg_attribute_encoding. Should be absolutely valid at this point.
 */
void
AddRelationAttributeEncodings(Relation rel, List *attr_encodings)
{
	Oid relid = RelationGetRelid(rel);
	ListCell *lc;

	/* If we're in upgrade mode, shouldn't be anything to do here */
	if (gp_upgrade_mode)
	{
		Assert(attr_encodings == NIL);
		return;
	}

	foreach(lc, attr_encodings)
	{
		Datum attoptions;
		ColumnReferenceStorageDirective *c = lfirst(lc);
		List *encoding;
		AttrNumber attnum;

		Insist(IsA(c, ColumnReferenceStorageDirective));

		attnum = get_attnum(relid, strVal(c->column));

		if (attnum == InvalidAttrNumber)
			elog(ERROR, "column \"%s\" does not exist", strVal(c->column));

		if (attnum < 0)
			elog(ERROR, "column \"%s\" is a system column", strVal(c->column));

		encoding = c->encoding;

		if (!encoding)
			continue;

		attoptions = transformRelOptions(PointerGetDatum(NULL),
										 encoding,
										 true,
										 false);

		add_attribute_encoding_entry(relid, attnum, attoptions);
	}
}

static void
remove_attr_encoding(Oid relid, AttrNumber attnum, bool error_not_found)
{
	ScanKeyData skey[2];
	int			nkeys = 1;
	SysScanDesc attrencscan;
	HeapTuple	tuple;
	Relation	pgattrenc;
	bool 		found = false;
	char	   *relname;

	/* shouldn't be anything to do in upgrade mode */
	if (gp_upgrade_mode)
		return;

	relname = get_rel_name(relid);
	pgattrenc = heap_open(AttributeEncodingRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_encoding_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	if (attnum != InvalidAttrNumber)
	{
		ScanKeyInit(&skey[1],
					Anum_pg_attribute_encoding_attnum,
					BTEqualStrategyNumber, F_OIDEQ,
					Int16GetDatum(attnum));
		nkeys = 2;
	}

	attrencscan = systable_beginscan(pgattrenc,
									 AttributeEncodingAttrelidAttnumIndexId,
									 true, SnapshotNow, nkeys, skey);

	while ((tuple = systable_getnext(attrencscan)) != NULL)
	{
		found = true;
		simple_heap_delete(pgattrenc, &tuple->t_self);
	}

	if (!found && error_not_found)
		elog(ERROR, "cannot find pg_attribute_encoding entry for relation \"%s\"",
			 relname ? relname : "unknown");

	systable_endscan(attrencscan);
	heap_close(pgattrenc, RowExclusiveLock);
}

void
RemoveRelationAttributeEncoding(Oid relid, AttrNumber attnum)
{
	remove_attr_encoding(relid, attnum, false);
}

void
RemoveAttributeEncodingsByRelid(Oid relid)
{
	remove_attr_encoding(relid, InvalidAttrNumber, false);
}

