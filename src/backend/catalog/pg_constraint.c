/*-------------------------------------------------------------------------
 *
 * pg_constraint.c
 *	  routines to support manipulation of the pg_constraint relation
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/pg_constraint.c,v 1.33 2006/07/14 14:52:17 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CreateConstraintEntry
 *	Create a constraint table entry.
 *
 * Subsidiary records (such as triggers or indexes to implement the
 * constraint) are *not* created here.	But we do make dependency links
 * from the constraint to the things it depends on.
 */
Oid
CreateConstraintEntry(const char *constraintName,
					  Oid conOid,
					  Oid constraintNamespace,
					  char constraintType,
					  bool isDeferrable,
					  bool isDeferred,
					  Oid relId,
					  const int16 *constraintKey,
					  int constraintNKeys,
					  int constraintNTotalKeys,
					  Oid domainId,
					  Oid foreignRelId,
					  const int16 *foreignKey,
					  int foreignNKeys,
					  char foreignUpdateType,
					  char foreignDeleteType,
					  char foreignMatchType,
					  Oid indexRelId,
					  Node *conExpr,
					  const char *conBin,
					  const char *conSrc)
{
	HeapTuple	tup;
	bool		nulls[Natts_pg_constraint];
	Datum		values[Natts_pg_constraint];
	ArrayType  *conkeyArray;
	ArrayType  *confkeyArray;
	NameData	cname;
	int			i;
	ObjectAddress conobject;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_constraint",
				NULL));

	Assert(constraintName);
	namestrcpy(&cname, constraintName);

	/*
	 * Convert C arrays into Postgres arrays.
	 */
	if (constraintNTotalKeys > 0)
	{
		Datum	   *conkey;

		conkey = (Datum *) palloc(constraintNTotalKeys * sizeof(Datum));
		for (i = 0; i < constraintNTotalKeys; i++)
			conkey[i] = Int16GetDatum(constraintKey[i]);
		conkeyArray = construct_array(conkey, constraintNTotalKeys,
									  INT2OID, 2, true, 's');
	}
	else
		conkeyArray = NULL;

	if (foreignNKeys > 0)
	{
		Datum	   *confkey;

		confkey = (Datum *) palloc(foreignNKeys * sizeof(Datum));
		for (i = 0; i < foreignNKeys; i++)
			confkey[i] = Int16GetDatum(foreignKey[i]);
		confkeyArray = construct_array(confkey, foreignNKeys,
									   INT2OID, 2, true, 's');
	}
	else
		confkeyArray = NULL;

	/* initialize nulls and values */
	for (i = 0; i < Natts_pg_constraint; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) 0;
	}

	values[Anum_pg_constraint_conname - 1] = NameGetDatum(&cname);
	values[Anum_pg_constraint_connamespace - 1] = ObjectIdGetDatum(constraintNamespace);
	values[Anum_pg_constraint_contype - 1] = CharGetDatum(constraintType);
	values[Anum_pg_constraint_condeferrable - 1] = BoolGetDatum(isDeferrable);
	values[Anum_pg_constraint_condeferred - 1] = BoolGetDatum(isDeferred);
	values[Anum_pg_constraint_conrelid - 1] = ObjectIdGetDatum(relId);
	values[Anum_pg_constraint_contypid - 1] = ObjectIdGetDatum(domainId);
	values[Anum_pg_constraint_confrelid - 1] = ObjectIdGetDatum(foreignRelId);
	values[Anum_pg_constraint_confupdtype - 1] = CharGetDatum(foreignUpdateType);
	values[Anum_pg_constraint_confdeltype - 1] = CharGetDatum(foreignDeleteType);
	values[Anum_pg_constraint_confmatchtype - 1] = CharGetDatum(foreignMatchType);

	if (conkeyArray)
		values[Anum_pg_constraint_conkey - 1] = PointerGetDatum(conkeyArray);
	else
		nulls[Anum_pg_constraint_conkey - 1] = true;

	if (confkeyArray)
		values[Anum_pg_constraint_confkey - 1] = PointerGetDatum(confkeyArray);
	else
		nulls[Anum_pg_constraint_confkey - 1] = true;

	/*
	 * initialize the binary form of the check constraint.
	 */
	if (conBin)
		values[Anum_pg_constraint_conbin - 1] = DirectFunctionCall1(textin,
													CStringGetDatum((char *) conBin));
	else
		nulls[Anum_pg_constraint_conbin - 1] = true;

	/*
	 * initialize the text form of the check constraint
	 */
	if (conSrc)
		values[Anum_pg_constraint_consrc - 1] = DirectFunctionCall1(textin,
													CStringGetDatum((char *) conSrc));
	else
		nulls[Anum_pg_constraint_consrc - 1] = true;

	tup = caql_form_tuple(pcqCtx, values, nulls);

	/* force tuple to have the desired OID */
	if (OidIsValid(conOid))
		HeapTupleSetOid(tup, conOid);
	conOid = caql_insert(pcqCtx, tup); /* implicit update of index as well */

	conobject.classId = ConstraintRelationId;
	conobject.objectId = conOid;
	conobject.objectSubId = 0;

	caql_endscan(pcqCtx);

	if (OidIsValid(relId))
	{
		/*
		 * Register auto dependency from constraint to owning relation, or to
		 * specific column(s) if any are mentioned.
		 */
		ObjectAddress relobject;

		relobject.classId = RelationRelationId;
		relobject.objectId = relId;
		if (constraintNTotalKeys > 0)
		{
			for (i = 0; i < constraintNTotalKeys; i++)
			{
				relobject.objectSubId = constraintKey[i];

				recordDependencyOn(&conobject, &relobject, DEPENDENCY_AUTO);
			}
		}
		else
		{
			relobject.objectSubId = 0;

			recordDependencyOn(&conobject, &relobject, DEPENDENCY_AUTO);
		}
	}

	if (OidIsValid(domainId))
	{
		/*
		 * Register auto dependency from constraint to owning domain
		 */
		ObjectAddress domobject;

		domobject.classId = TypeRelationId;
		domobject.objectId = domainId;
		domobject.objectSubId = 0;

		recordDependencyOn(&conobject, &domobject, DEPENDENCY_AUTO);
	}

	if (OidIsValid(foreignRelId))
	{
		/*
		 * Register normal dependency from constraint to foreign relation, or
		 * to specific column(s) if any are mentioned.
		 */
		ObjectAddress relobject;

		relobject.classId = RelationRelationId;
		relobject.objectId = foreignRelId;
		if (foreignNKeys > 0)
		{
			for (i = 0; i < foreignNKeys; i++)
			{
				relobject.objectSubId = foreignKey[i];

				recordDependencyOn(&conobject, &relobject, DEPENDENCY_NORMAL);
			}
		}
		else
		{
			relobject.objectSubId = 0;

			recordDependencyOn(&conobject, &relobject, DEPENDENCY_NORMAL);
		}
	}

	if (OidIsValid(indexRelId))
	{
		/*
		 * Register normal dependency on the unique index that supports a
		 * foreign-key constraint.
		 */
		ObjectAddress relobject;

		relobject.classId = RelationRelationId;
		relobject.objectId = indexRelId;
		relobject.objectSubId = 0;

		recordDependencyOn(&conobject, &relobject, DEPENDENCY_NORMAL);
	}

	if (conExpr != NULL)
	{
		/*
		 * Register dependencies from constraint to objects mentioned in CHECK
		 * expression.
		 */
		recordDependencyOnSingleRelExpr(&conobject, conExpr, relId,
										DEPENDENCY_NORMAL,
										DEPENDENCY_NORMAL);
	}

	return conOid;
}


/*
 * Test whether given name is currently used as a constraint name
 * for the given object (relation or domain).
 *
 * This is used to decide whether to accept a user-specified constraint name.
 * It is deliberately not the same test as ChooseConstraintName uses to decide
 * whether an auto-generated name is OK: here, we will allow it unless there
 * is an identical constraint name in use *on the same object*.
 *
 * NB: Caller should hold exclusive lock on the given object, else
 * this test can be fooled by concurrent additions.
 */
bool
ConstraintNameIsUsed(ConstraintCategory conCat, Oid objId,
					 Oid objNamespace, const char *conname)
{
	bool		found;
	Relation	conDesc;
	HeapTuple	tup;
	cqContext  *pcqCtx;
	cqContext	cqc;

	conDesc = heap_open(ConstraintRelationId, AccessShareLock);

	found = false;

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), conDesc),
			cql("SELECT * FROM pg_constraint "
				" WHERE conname = :1 "
				" AND connamespace = :2 ",
				CStringGetDatum((char *) conname),
				ObjectIdGetDatum(objNamespace)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

		if (conCat == CONSTRAINT_RELATION && con->conrelid == objId)
		{
			found = true;
			break;
		}
		else if (conCat == CONSTRAINT_DOMAIN && con->contypid == objId)
		{
			found = true;
			break;
		}
	}

	caql_endscan(pcqCtx);
	heap_close(conDesc, AccessShareLock);

	return found;
}

/*
 * Select a nonconflicting name for a new constraint.
 *
 * The objective here is to choose a name that is unique within the
 * specified namespace.  Postgres does not require this, but the SQL
 * spec does, and some apps depend on it.  Therefore we avoid choosing
 * default names that so conflict.
 *
 * name1, name2, and label are used the same way as for makeObjectName(),
 * except that the label can't be NULL; digits will be appended to the label
 * if needed to create a name that is unique within the specified namespace.
 *
 * 'others' can be a list of string names already chosen within the current
 * command (but not yet reflected into the catalogs); we will not choose
 * a duplicate of one of these either.
 *
 * Note: it is theoretically possible to get a collision anyway, if someone
 * else chooses the same name concurrently.  This is fairly unlikely to be
 * a problem in practice, especially if one is holding an exclusive lock on
 * the relation identified by name1.
 *
 * Returns a palloc'd string.
 */
char *
ChooseConstraintName(const char *name1, const char *name2,
					 const char *label, Oid namespace,
					 List *others)
{
	int			pass = 0;
	char	   *conname = NULL;
	char		modlabel[NAMEDATALEN];
	Relation	conDesc;
	cqContext	cqc;
	bool		found;
	ListCell   *l;

	conDesc = heap_open(ConstraintRelationId, AccessShareLock);

	/* try the unmodified label first */
	StrNCpy(modlabel, label, sizeof(modlabel));

	for (;;)
	{
		conname = makeObjectName(name1, name2, modlabel);

		found = false;

		foreach(l, others)
		{
			if (strcmp((char *) lfirst(l), conname) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			found = 
					(0 != caql_getcount(
							caql_addrel(cqclr(&cqc), conDesc),
							cql("SELECT COUNT(*) FROM pg_constraint "
								" WHERE conname = :1 "
								" AND connamespace = :2 ",
								CStringGetDatum(conname),
								ObjectIdGetDatum(namespace))));
		}

		if (!found)
			break;

		/* found a conflict, so try a new name component */
		pfree(conname);
		snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
	}

	heap_close(conDesc, AccessShareLock);

	return conname;
}

/*
 * Delete a single constraint record.
 */
void
RemoveConstraintById(Oid conId)
{
	Relation	conDesc;
	HeapTuple	tup;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Form_pg_constraint con;

	conDesc = heap_open(ConstraintRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), conDesc);

	tup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_constraint "
				" WHERE oid = :1 "
				" FOR UPDATE ",
				ObjectIdGetDatum(conId)));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "could not find tuple for constraint %u", conId);
	con = (Form_pg_constraint) GETSTRUCT(tup);

	/*
	 * Special processing depending on what the constraint is for.
	 */
	if (OidIsValid(con->conrelid))
	{
		Relation	rel;
		bool		is_part_child = false;

		/*
		 * If the constraint is for a relation, open and exclusive-lock the
		 * relation it's for.
		 */
		rel = heap_open(con->conrelid, AccessExclusiveLock);

		/*
		 * We need to update the relcheck count if it is a check constraint
		 * being dropped.  This update will force backends to rebuild relcache
		 * entries when we commit.
		 */
		if (con->contype == CONSTRAINT_CHECK)
		{
			Relation	pgrel;
			HeapTuple	relTup;
			Form_pg_class classForm;
			cqContext	cqc2;
			cqContext  *pcqCtx2;

			pgrel = heap_open(RelationRelationId, RowExclusiveLock);

			pcqCtx2 = caql_addrel(cqclr(&cqc2), pgrel);

			relTup = caql_getfirst(
					pcqCtx2,
					cql("SELECT * FROM pg_class "
						" WHERE oid = :1 "
						" FOR UPDATE ",
						ObjectIdGetDatum(con->conrelid)));

			if (!HeapTupleIsValid(relTup))
				elog(ERROR, "cache lookup failed for relation %u",
					 con->conrelid);
			classForm = (Form_pg_class) GETSTRUCT(relTup);

			if (classForm->relchecks == 0)		/* should not happen */
				elog(ERROR, "relation \"%s\" has relchecks = 0",
					 RelationGetRelationName(rel));
			classForm->relchecks--;
			
			caql_update_current(pcqCtx2, relTup);
			/* and Update indexes (implicit) */

			heap_freetuple(relTup);

			heap_close(pgrel, RowExclusiveLock);
		}

		is_part_child = !rel_needs_long_lock(RelationGetRelid(rel));

		if (is_part_child)
			/* sufficiently locked, in the case of a partitioned table */
			heap_close(rel, AccessExclusiveLock);
		else
			/* Keep lock on constraint's rel until end of xact */
			heap_close(rel, NoLock);
	}
	else if (OidIsValid(con->contypid))
	{
		/*
		 * XXX for now, do nothing special when dropping a domain constraint
		 *
		 * Probably there should be some form of locking on the domain type,
		 * but we have no such concept at the moment.
		 */
	}
	else
		elog(ERROR, "constraint %u is not of a known type", conId);

	/* Fry the constraint itself */
	caql_delete_current(pcqCtx);

	/* Clean up */
	heap_close(conDesc, RowExclusiveLock);
}

/*
 * GetConstraintNameForTrigger
 *		Get the name of the constraint owning a trigger, if any
 *
 * Returns a palloc'd string, or NULL if no constraint can be found
 */
char *
GetConstraintNameForTrigger(Oid triggerId)
{
	char	   *result;
	Oid			constraintId = InvalidOid;
	Relation	depRel;
	Relation	conRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	/*
	 * We must grovel through pg_depend to find the owning constraint. Perhaps
	 * pg_trigger should have a column for the owning constraint ... but right
	 * now this is not performance-critical code.
	 */
	depRel = heap_open(DependRelationId, AccessShareLock);

	/* CaQL UNDONE: no test coverage */
	ScanKeyInit(&key[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(TriggerRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(triggerId));
	/* assume we can ignore objsubid for a trigger */

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  SnapshotNow, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

		if (foundDep->refclassid == ConstraintRelationId &&
			foundDep->deptype == DEPENDENCY_INTERNAL)
		{
			constraintId = foundDep->refobjid;
			break;
		}
	}

	systable_endscan(scan);

	heap_close(depRel, AccessShareLock);

	if (!OidIsValid(constraintId))
		return NULL;			/* no owning constraint found */

	conRel = heap_open(ConstraintRelationId, AccessShareLock);

	/* CaQL UNDONE: no test coverage */
	ScanKeyInit(&key[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(constraintId));

	scan = systable_beginscan(conRel, ConstraintOidIndexId, true,
							  SnapshotNow, 1, key);

	tup = systable_getnext(scan);

	if (HeapTupleIsValid(tup))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

		result = pstrdup(NameStr(con->conname));
	}
	else
	{
		/* This arguably should be an error, but we'll just return NULL */
		result = NULL;
	}

	systable_endscan(scan);

	heap_close(conRel, AccessShareLock);

	return result;
}

/*
 * GetConstraintNameByOid
 *		Get the name of the constraint with the given oid, if any.
 *
 * Returns a palloc'd string, or NULL if no constraint can be found.
 */
char *
GetConstraintNameByOid(Oid constraintId)
{
	char *result = NULL;
	
	if (!OidIsValid(constraintId))
		return NULL;
	
	result = caql_getcstring(
			NULL,
			cql("SELECT conname FROM pg_constraint "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(constraintId)));
	
	return result;
}

/*
 * AlterConstraintNamespaces
 *		Find any constraints belonging to the specified object,
 *		and move them to the specified new namespace.
 *
 * isType indicates whether the owning object is a type or a relation.
 */
void
AlterConstraintNamespaces(Oid ownerId, Oid oldNspId,
						  Oid newNspId, bool isType)
{
	HeapTuple	tup;
	cqContext  *pcqCtx;

	if (isType)
	{
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_constraint "
					" WHERE contypid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(ownerId)));
	}
	else
	{
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_constraint "
					" WHERE conrelid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(ownerId)));
	}

	while (HeapTupleIsValid((tup = caql_getnext(pcqCtx))))
	{
		Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(tup);

		if (conform->connamespace == oldNspId)
		{
			tup = heap_copytuple(tup);
			conform = (Form_pg_constraint) GETSTRUCT(tup);

			conform->connamespace = newNspId;

			caql_update_current(pcqCtx, tup);

			/*
			 * Note: currently, the constraint will not have its own
			 * dependency on the namespace, so we don't need to do
			 * changeDependencyFor().
			 */
		}
	}

	caql_endscan(pcqCtx);

}

/**
 * This method determines if the input attribute is a foreign key and if so,
 * retrieves the primary key's relation oid and attribute number. It looks at
 * the pg_constraint system table to determine the answer.
 * 
 * Input:
 * 	relid - relation whose attribute we are examining
 *  attno - attribute number of the said column
 *  
 * Output:
 * 	*pkrelid - relation id of the table that contains the primary key
 *  *pkattno - attribute number of the primary key
 * 	return   - true if found/ false otherwise
 * 
 * It returns a value of true if (relid, attno) is indeed a foreign key. It also
 * sets the output pkrelid and pkattno. If it returns false, then this
 * column is not a primary key and these output variables are not modified.
 */

bool
ConstraintGetPrimaryKeyOf(Oid relid, AttrNumber attno, Oid *pkrelid, AttrNumber *pkattno)
{
	bool		found;
	Relation	conDesc;
	HeapTuple	tup;
	cqContext  *pcqCtx;
	cqContext	cqc;

	conDesc = heap_open(ConstraintRelationId, AccessShareLock);

	found = false;

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), conDesc),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 ",
				ObjectIdGetDatum(relid)));

	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);
		
		if (con->conrelid == relid && con->contype == CONSTRAINT_FOREIGN)
		{
			Datum		val;
			bool		valisnull;
			Datum		*valarray;
			int          valarray_length;

			/* first ensure that this is the right key */
			val = heap_getattr(tup, Anum_pg_constraint_conkey,
					RelationGetDescr(conDesc), &valisnull);

			Assert(!valisnull);

			deconstruct_array(DatumGetArrayTypeP(val),
					INT2OID, 2, true, 's',
					&valarray, NULL, &valarray_length);

			if (valarray_length == 1 && DatumGetInt16(valarray[0]) == attno)
			{
				Datum		fval;
				bool		fvalisnull;
				Datum		*fvalarray;
				int          fvalarray_length;

				/* this is the right key, now extract the primary table,key */
				Assert(con->confrelid != InvalidOid);

				fval = heap_getattr(tup, Anum_pg_constraint_confkey,
						RelationGetDescr(conDesc), &fvalisnull);

				Assert(!fvalisnull);
				deconstruct_array(DatumGetArrayTypeP(fval),
						INT2OID, 2, true, 's',
						&fvalarray, NULL, &fvalarray_length);

				Assert(fvalarray_length == 1);

				found = true;
				*pkrelid = con->confrelid;
				*pkattno = (AttrNumber) DatumGetInt16(fvalarray[0]);
				break;
			}
		}
	}
	
	caql_endscan(pcqCtx);
	heap_close(conDesc, AccessShareLock);

	return found;
}

/**
 * This method retrieves the primary key of given relation oid. It looks at
 * the pg_constraint system table to determine the answer.
 *
 * Input:
 * 	relid - relation whose attribute we are examining
 *
 * Output:
 *  *pknatts - number of attributes of the primary key
 *  *pkattno - oid of attributes of the primary key
 */
void ConstraintGetPrimaryKeys(Oid relid, int *pknatts, AttrNumber **pkattno)
{
	bool		found;
	Relation	conDesc;
	HeapTuple	tup;
	cqContext  *pcqCtx;
	cqContext	cqc;

	conDesc = heap_open(ConstraintRelationId, AccessShareLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), conDesc),
			cql("SELECT * FROM pg_constraint "
				"WHERE conrelid = :1",
				ObjectIdGetDatum(relid)));

	found = false;
	while (HeapTupleIsValid(tup = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tup);

		if (con->conrelid == relid && con->contype == CONSTRAINT_PRIMARY)
		{
			found = true;

			Datum val;
			bool valisnull;
			Datum *valarray;
			int valarray_length, i;

			/* first ensure that this is the right key */
			val = heap_getattr(tup, Anum_pg_constraint_conkey,
							   RelationGetDescr(conDesc), &valisnull);

			if (valisnull)
				break;// the primary key can be empty in magma table

			deconstruct_array(DatumGetArrayTypeP(val),
							  INT2OID, 2, true, 's',
							  &valarray, NULL, &valarray_length);

			*pkattno = (AttrNumber *)palloc(sizeof(AttrNumber) * valarray_length);
			*pknatts = valarray_length;
			for (i = 0; i < valarray_length; ++i)
			{
				(*pkattno)[i] = (AttrNumber)DatumGetInt16(valarray[i]);
			}

			break;
		}
	}

	caql_endscan(pcqCtx);
	heap_close(conDesc, AccessShareLock);

	if (!found)
	{
		*pknatts = -1;
		*pkattno = NULL;
	}
}

/*
 * get_constraint_relation_oids
 *      Find the IDs of the relations to which a constraint refers.
 */
void
get_constraint_relation_oids(Oid constraint_oid, Oid *conrelid, Oid *confrelid)
{
	HeapTuple	tup;
	cqContext  *pcqCtx;
	Form_pg_constraint con;

	pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_constraint "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(constraint_oid)));
	tup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "cache lookup failed for constraint %u", constraint_oid);
	}

	con = (Form_pg_constraint) GETSTRUCT(tup);
 	*conrelid = con->conrelid;
	*confrelid = con->confrelid;

	caql_endscan(pcqCtx);
}
