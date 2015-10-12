/*-------------------------------------------------------------------------
 *
 * comment.c
 *
 * PostgreSQL object comments utility code.
 *
 * Copyright (c) 1996-2008, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/comment.c,v 1.93 2006/11/12 06:55:54 neilc Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_description.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/filespace.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "cdb/cdbvars.h"


/*
 * Static Function Prototypes --
 *
 * The following prototypes are declared static so as not to conflict
 * with any other routines outside this module. These routines are
 * called by the public function CommentObject() routine to create
 * the appropriate comment for the specific object type.
 */

static void CommentRelation(int objtype, List *relname, char *comment);
static void CommentAttribute(List *qualname, char *comment);
static void CommentDatabase(List *qualname, char *comment);
static void CommentNamespace(List *qualname, char *comment);
static void CommentRule(List *qualname, char *comment);
static void CommentType(List *typename, char *comment);
static void CommentAggregate(List *aggregate, List *arguments, char *comment);
static void CommentProc(List *function, List *arguments, char *comment);
static void CommentOperator(List *opername, List *arguments, char *comment);
static void CommentTrigger(List *qualname, char *comment);
static void CommentConstraint(List *qualname, char *comment);
static void CommentConversion(List *qualname, char *comment);
static void CommentLanguage(List *qualname, char *comment);
static void CommentOpClass(List *qualname, List *arguments, char *comment);
static void CommentLargeObject(List *qualname, char *comment);
static void CommentCast(List *qualname, List *arguments, char *comment);
static void CommentTablespace(List *qualname, char *comment);
static void CommentFilespace(List *qualname, char *comment);
static void CommentRole(List *qualname, char *comment);
static void CommentResourceQueue(List *qualname, char *comment);

/*
 * CommentObject --
 *
 * This routine is used to add the associated comment into
 * pg_description for the object specified by the given SQL command.
 */
void
CommentObject(CommentStmt *stmt)
{
	switch (stmt->objtype)
	{
		case OBJECT_INDEX:
		case OBJECT_SEQUENCE:
		case OBJECT_TABLE:
		case OBJECT_VIEW:
			CommentRelation(stmt->objtype, stmt->objname, stmt->comment);
			break;
		case OBJECT_COLUMN:
			CommentAttribute(stmt->objname, stmt->comment);
			break;
		case OBJECT_DATABASE:
			CommentDatabase(stmt->objname, stmt->comment);
			break;
		case OBJECT_RULE:
			CommentRule(stmt->objname, stmt->comment);
			break;
		case OBJECT_TYPE:
			CommentType(stmt->objname, stmt->comment);
			break;
		case OBJECT_AGGREGATE:
			CommentAggregate(stmt->objname, stmt->objargs, stmt->comment);
			break;
		case OBJECT_FUNCTION:
			CommentProc(stmt->objname, stmt->objargs, stmt->comment);
			break;
		case OBJECT_OPERATOR:
			CommentOperator(stmt->objname, stmt->objargs, stmt->comment);
			break;
		case OBJECT_TRIGGER:
			CommentTrigger(stmt->objname, stmt->comment);
			break;
		case OBJECT_SCHEMA:
			CommentNamespace(stmt->objname, stmt->comment);
			break;
		case OBJECT_CONSTRAINT:
			CommentConstraint(stmt->objname, stmt->comment);
			break;
		case OBJECT_CONVERSION:
			CommentConversion(stmt->objname, stmt->comment);
			break;
		case OBJECT_LANGUAGE:
			CommentLanguage(stmt->objname, stmt->comment);
			break;
		case OBJECT_OPCLASS:
			CommentOpClass(stmt->objname, stmt->objargs, stmt->comment);
			break;
		case OBJECT_LARGEOBJECT:
			CommentLargeObject(stmt->objname, stmt->comment);
			break;
		case OBJECT_CAST:
			CommentCast(stmt->objname, stmt->objargs, stmt->comment);
			break;
		case OBJECT_TABLESPACE:
			CommentTablespace(stmt->objname, stmt->comment);
			break;
		case OBJECT_FILESPACE:
			CommentFilespace(stmt->objname, stmt->comment);
			break;
		case OBJECT_ROLE:
			CommentRole(stmt->objname, stmt->comment);
			break;
		case OBJECT_RESQUEUE:
			CommentResourceQueue(stmt->objname, stmt->comment);
			break;
		default:
			elog(ERROR, "unrecognized object type: %d",
				 (int) stmt->objtype);
	}
}

/*
 * CreateComments --
 *
 * Create a comment for the specified object descriptor.  Inserts a new
 * pg_description tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
void
CreateComments(Oid oid, Oid classoid, int32 subid, char *comment)
{
	Relation	description;
	HeapTuple	oldtuple;
	HeapTuple	newtuple = NULL;
	Datum		values[Natts_pg_description];
	bool		nulls[Natts_pg_description];
	bool		replaces[Natts_pg_description];
	int			i;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/* Reduce empty-string to NULL case */
	if (comment != NULL && strlen(comment) == 0)
		comment = NULL;

	/* Prepare to form or update a tuple, if necessary */
	if (comment != NULL)
	{
		for (i = 0; i < Natts_pg_description; i++)
		{
			nulls[i] = false;
			replaces[i] = true;
		}
		i = 0;
		values[i++] = ObjectIdGetDatum(oid);
		values[i++] = ObjectIdGetDatum(classoid);
		values[i++] = Int32GetDatum(subid);
		values[i++] = CStringGetTextDatum(comment);
	}

	/* Use the index to search for a matching old tuple */
	description = heap_open(DescriptionRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), description),
			cql("SELECT * FROM pg_description" 
				 " where objoid = :1 AND "
				 " classoid = :2 AND "
				 " objsubid = :3 FOR UPDATE", 
				 ObjectIdGetDatum(oid),
				 ObjectIdGetDatum(classoid),
				Int32GetDatum(subid)));


	while (HeapTupleIsValid(oldtuple = caql_getnext(pcqCtx)))
	{
		/* Found the old tuple, so delete or update it */

		if (comment == NULL)
			caql_delete_current(pcqCtx);
		else
		{
			newtuple = caql_modify_current(pcqCtx,
										   values, nulls, replaces);
			caql_update_current(pcqCtx, newtuple);
			/* update_current updates the index -- don't do it twice */
		}

		break;					/* Assume there can be only one match */
	}

	caql_endscan(pcqCtx);

	/* If we didn't find an old tuple, insert a new one */

	if (newtuple == NULL && comment != NULL)
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), description),
				cql("INSERT INTO pg_description",
					NULL));

		newtuple = caql_form_tuple(pcqCtx, values, nulls);
		caql_insert(pcqCtx, newtuple); /* implicit update of index as well */

		heap_freetuple(newtuple);
		caql_endscan(pcqCtx);
	}

	/* Done */

	heap_close(description, NoLock);
}

/*
 * CreateSharedComments --
 *
 * Create a comment for the specified shared object descriptor.  Inserts a
 * new pg_shdescription tuple, or replaces an existing one with the same key.
 *
 * If the comment given is null or an empty string, instead delete any
 * existing comment for the specified key.
 */
void
CreateSharedComments(Oid oid, Oid classoid, char *comment)
{
	Relation	shdescription;
	HeapTuple	oldtuple;
	HeapTuple	newtuple = NULL;
	Datum		values[Natts_pg_shdescription];
	bool		nulls[Natts_pg_shdescription];
	bool		replaces[Natts_pg_shdescription];
	int			i;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/* Reduce empty-string to NULL case */
	if (comment != NULL && strlen(comment) == 0)
		comment = NULL;

	/* Prepare to form or update a tuple, if necessary */
	if (comment != NULL)
	{
		for (i = 0; i < Natts_pg_shdescription; i++)
		{
			nulls[i] = false;
			replaces[i] = true;
		}
		i = 0;
		values[i++] = ObjectIdGetDatum(oid);
		values[i++] = ObjectIdGetDatum(classoid);
		values[i++] = CStringGetTextDatum(comment);
	}

	/* Use the index to search for a matching old tuple */
	shdescription = heap_open(SharedDescriptionRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), shdescription),
			cql("SELECT * FROM pg_shdescription" 
				" where objoid  = :1 AND "
				" classoid = :2 FOR UPDATE", 
				ObjectIdGetDatum(oid),
				ObjectIdGetDatum(classoid)));

	while (HeapTupleIsValid(oldtuple = caql_getnext(pcqCtx)))
	{
		/* Found the old tuple, so delete or update it */

		if (comment == NULL)
			caql_delete_current(pcqCtx);
		else
		{
			newtuple = caql_modify_current(pcqCtx,
										   values, nulls, replaces);
			caql_update_current(pcqCtx, newtuple);
			/* update_current updates the index -- don't do it twice */
		}

		break;					/* Assume there can be only one match */
	}

	caql_endscan(pcqCtx);

	/* If we didn't find an old tuple, insert a new one */

	if (newtuple == NULL && comment != NULL)
	{
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), shdescription),
				cql("INSERT INTO pg_shdescription",
					NULL));

		newtuple = caql_form_tuple(pcqCtx, values, nulls);
		caql_insert(pcqCtx, newtuple); /* implicit update of index as well */

		heap_freetuple(newtuple);
		caql_endscan(pcqCtx);
	}

	/* Done */

	heap_close(shdescription, NoLock);
}

/*
 * DeleteComments -- remove comments for an object
 *
 * If subid is nonzero then only comments matching it will be removed.
 * If subid is zero, all comments matching the oid/classoid will be removed
 * (this corresponds to deleting a whole object).
 */
void
DeleteComments(Oid oid, Oid classoid, int32 subid)
{
	/* Use the index to search for all matching old tuples */
	if (subid != 0)
	{
		int numDel;

		numDel = caql_getcount(
				NULL,
				cql("DELETE FROM pg_description" 
					" where objoid = :1 AND "
					" classoid = :2 AND "
					" objsubid = :3", 
					ObjectIdGetDatum(oid),
					ObjectIdGetDatum(classoid),
					Int32GetDatum(subid)));
	}
	else
	{
		int numDel;

		numDel = caql_getcount(
				NULL,
				cql("DELETE FROM pg_description" 
					" where objoid = :1 AND "
					" classoid = :2",
					ObjectIdGetDatum(oid),
					ObjectIdGetDatum(classoid)));
	}

	/* Done */
}

/*
 * DeleteSharedComments -- remove comments for a shared object
 */
void
DeleteSharedComments(Oid oid, Oid classoid)
{
	int numDel;

	/* Use the index to search for all matching old tuples */
	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_shdescription" 
				" where objoid  = :1 AND "
				" classoid = :2", 
				ObjectIdGetDatum(oid),
				ObjectIdGetDatum(classoid)));

	/* Done */
}

/*
 * CommentRelation --
 *
 * This routine is used to add/drop a comment from a relation, where
 * a relation is a TABLE, SEQUENCE, VIEW or INDEX. The routine simply
 * finds the relation name by searching the system cache, locating
 * the appropriate tuple, and inserting a comment using that
 * tuple's oid. Its parameters are the relation name and comments.
 */
static void
CommentRelation(int objtype, List *relname, char *comment)
{
	Relation	relation;
	RangeVar   *tgtrel;

	tgtrel = makeRangeVarFromNameList(relname);

	/*
	 * Open the relation.  We do this mainly to acquire a lock that ensures no
	 * one else drops the relation before we commit.  (If they did, they'd
	 * fail to remove the entry we are about to make in pg_description.)
	 */

        /* make sure this is not an hcatalog table */
#ifdef USE_ASSERT_CHECKING
        Oid relid = 
#endif // USE_ASSERT_CHECKING
	RangeVarGetRelid(tgtrel, false /*failOk*/, false /*allowHcatalog*/);

	Assert(OidIsValid(relid));

	relation = relation_openrv(tgtrel, AccessShareLock);

	/* Check object security */
	if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(relation));

	/* Next, verify that the relation type matches the intent */

	switch (objtype)
	{
		case OBJECT_INDEX:
			if (relation->rd_rel->relkind != RELKIND_INDEX)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not an index",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_SEQUENCE:
			if (relation->rd_rel->relkind != RELKIND_SEQUENCE)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a sequence",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_TABLE:
			if (relation->rd_rel->relkind != RELKIND_RELATION)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a table",
								RelationGetRelationName(relation))));
			break;
		case OBJECT_VIEW:
			if (relation->rd_rel->relkind != RELKIND_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is not a view",
								RelationGetRelationName(relation))));
			break;
	}

	/* Create the comment using the relation's oid */
	CreateComments(RelationGetRelid(relation), RelationRelationId,
				   0, comment);

	/* Done, but hold lock until commit */
	relation_close(relation, NoLock);
}

/*
 * CommentAttribute --
 *
 * This routine is used to add/drop a comment from an attribute
 * such as a table's column. The routine will check security
 * restrictions and then attempt to look up the specified
 * attribute. If successful, a comment is added/dropped, else an
 * ereport() exception is thrown.	The parameters are the relation
 * and attribute names, and the comment
 */
static void
CommentAttribute(List *qualname, char *comment)
{
	int			nnames;
	List	   *relname;
	char	   *attrname;
	RangeVar   *rel;
	Relation	relation;
	AttrNumber	attnum;

	/* Separate relname and attr name */
	nnames = list_length(qualname);
	if (nnames < 2)				/* parser messed up */
		elog(ERROR, "must specify relation and attribute");
	relname = list_truncate(list_copy(qualname), nnames - 1);
	attrname = strVal(lfirst(list_tail(qualname)));

	/* Open the containing relation to ensure it won't go away meanwhile */
	rel = makeRangeVarFromNameList(relname);
	relation = relation_openrv(rel, AccessShareLock);

	/* Check object security */

	if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(relation));

	/* Now, fetch the attribute number from the system cache */

	attnum = get_attnum(RelationGetRelid(relation), attrname);
	if (attnum == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("column \"%s\" of relation \"%s\" does not exist",
						attrname, RelationGetRelationName(relation))));

	/* Create the comment using the relation's oid */
	CreateComments(RelationGetRelid(relation), RelationRelationId,
				   (int32) attnum, comment);

	/* Done, but hold lock until commit */

	relation_close(relation, NoLock);
}

/*
 * CommentDatabase --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding the specified database. The routine will check
 * security for owner permissions, and, if successful, will then
 * attempt to find the oid of the database specified. Once found,
 * a comment is added/dropped using the CreateSharedComments() routine.
 */
static void
CommentDatabase(List *qualname, char *comment)
{
	char	   *database;
	Oid			oid;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("database name may not be qualified")));
	database = strVal(linitial(qualname));

	/*
	 * When loading a dump, we may see a COMMENT ON DATABASE for the old name
	 * of the database.  Erroring out would prevent pg_restore from completing
	 * (which is really pg_restore's fault, but for now we will work around
	 * the problem here).  Consensus is that the best fix is to treat wrong
	 * database name as a WARNING not an ERROR.
	 */

	/* First get the database OID */
	oid = get_database_oid(database);
	if (!OidIsValid(oid))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", database)));
		return;
	}

	/* Check object security */
	if (!pg_database_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   database);

	/* Call CreateSharedComments() to create/drop the comments */
	CreateSharedComments(oid, DatabaseRelationId, comment);
}

/*
 * CommentTablespace --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a tablespace.  The tablepace is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateSharedComments() routine.
 *
 */
static void
CommentTablespace(List *qualname, char *comment)
{
	char	   *tablespace;
	Oid			oid;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("tablespace name may not be qualified")));
	tablespace = strVal(linitial(qualname));

	oid = get_tablespace_oid(tablespace);
	if (!OidIsValid(oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%s\" does not exist", tablespace)));
		return;
	}

	/* Check object security */
	if (!pg_tablespace_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TABLESPACE, tablespace);

	/* Call CreateSharedComments() to create/drop the comments */
	CreateSharedComments(oid, TableSpaceRelationId, comment);
}

/*
 * CommentFilespace --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a filespace.  The filespace is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateSharedComments() routine.
 *
 */
static void
CommentFilespace(List *qualname, char *comment)
{
	Relation    rel;
	char	   *filespace;
	Oid			oid;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("filespace name may not be qualified")));
	filespace = strVal(linitial(qualname));

	rel = heap_open(FileSpaceRelationId, RowShareLock);
	oid = get_filespace_oid(rel, filespace);
	heap_close(rel, RowShareLock);
	if (!OidIsValid(oid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filespace \"%s\" does not exist", filespace)));
		return;
	}

	/* Check object security */
	if (!pg_filespace_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_FILESPACE, filespace);

	/* Call CreateSharedComments() to create/drop the comments */
	CreateSharedComments(oid, FileSpaceRelationId, comment);
}

/*
 * CommentRole --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a role.  The role is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateSharedComments() routine.
 */
static void
CommentRole(List *qualname, char *comment)
{
	char	   *role;
	Oid			oid;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("role name may not be qualified")));
	role = strVal(linitial(qualname));

	oid = get_roleid_checked(role);

	/* Check object security */
	if (!has_privs_of_role(GetUserId(), oid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
		  errmsg("must be member of role \"%s\" to comment upon it", role)));

	/* Call CreateSharedComments() to create/drop the comments */
	CreateSharedComments(oid, AuthIdRelationId, comment);
}

/*
 * CommentNamespace --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding the specified namespace. The routine will check
 * security for owner permissions, and, if successful, will then
 * attempt to find the oid of the namespace specified. Once found,
 * a comment is added/dropped using the CreateComments() routine.
 */
static void
CommentNamespace(List *qualname, char *comment)
{
	Oid			oid;
	char	   *namespace;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("schema name may not be qualified")));
	namespace = strVal(linitial(qualname));

	oid = LookupInternalNamespaceId(namespace);

	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", namespace)));

	/* Check object security */
	if (!pg_namespace_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_NAMESPACE,
					   namespace);

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, NamespaceRelationId, 0, comment);
}

/*
 * CommentRule --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a specified RULE. The rule for commenting is determined by
 * both its name and the relation to which it refers. The arguments to this
 * function are the rule name and relation name (merged into a qualified
 * name), and the comment to add/drop.
 *
 * Before PG 7.3, rules had unique names across the whole database, and so
 * the syntax was just COMMENT ON RULE rulename, with no relation name.
 * For purposes of backwards compatibility, we support that as long as there
 * is only one rule by the specified name in the database.
 */
static void
CommentRule(List *qualname, char *comment)
{
	int			nnames;
	List	   *relname;
	char	   *rulename;
	RangeVar   *rel;
	Relation	relation;
	HeapTuple	tuple;
	Oid			reloid;
	Oid			ruleoid;

	/* Separate relname and trig name */
	nnames = list_length(qualname);
	if (nnames == 1)
	{
		/* Old-style: only a rule name is given */
		bool bOnly;

		rulename = strVal(linitial(qualname));

		/* Search pg_rewrite for such a rule */
		tuple = caql_getfirst_only(
				NULL,
				&bOnly,
				cql("SELECT * FROM pg_rewrite "
					" WHERE rulename = :1 ",
					PointerGetDatum(rulename)));

		if (HeapTupleIsValid(tuple))
		{
			reloid = ((Form_pg_rewrite) GETSTRUCT(tuple))->ev_class;
			ruleoid = HeapTupleGetOid(tuple);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("rule \"%s\" does not exist", rulename)));
			reloid = ruleoid = 0;		/* keep compiler quiet */
		}

		if (!bOnly)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
				   errmsg("there are multiple rules named \"%s\"", rulename),
				errhint("Specify a relation name as well as a rule name.")));
		}

		/* heap_endscan(scanDesc); */

		/* Open the owning relation to ensure it won't go away meanwhile */
		relation = heap_open(reloid, AccessShareLock);
	}
	else
	{
		cqContext  *pcqCtx;

		/* New-style: rule and relname both provided */
		Assert(nnames >= 2);
		relname = list_truncate(list_copy(qualname), nnames - 1);
		rulename = strVal(lfirst(list_tail(qualname)));

		/* Open the owning relation to ensure it won't go away meanwhile */
		rel = makeRangeVarFromNameList(relname);
		relation = heap_openrv(rel, AccessShareLock);
		reloid = RelationGetRelid(relation);

		/* Find the rule's pg_rewrite tuple, get its OID */
		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_rewrite "
					" WHERE ev_class = :1 "
					" AND rulename = :2 ",
					ObjectIdGetDatum(reloid),
					PointerGetDatum(rulename)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("rule \"%s\" for relation \"%s\" does not exist",
							rulename, RelationGetRelationName(relation))));
		Assert(reloid == ((Form_pg_rewrite) GETSTRUCT(tuple))->ev_class);
		ruleoid = HeapTupleGetOid(tuple);
		caql_endscan(pcqCtx);
	}

	/* Check object security */
	if (!pg_class_ownercheck(reloid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   get_rel_name(reloid));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(ruleoid, RewriteRelationId, 0, comment);

	heap_close(relation, NoLock);
}

/*
 * CommentType --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a TYPE. The type is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateComments() routine.
 * The type's name and the comments are the parameters to this routine.
 */
static void
CommentType(List *typename, char *comment)
{
	TypeName   *tname;
	Oid			oid;

	/* XXX a bit of a crock; should accept TypeName in COMMENT syntax */
	tname = makeTypeNameFromNameList(typename);

	/* Find the type's oid */

	oid = typenameTypeId(NULL, tname);

	/* Check object security */

	if (!pg_type_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TYPE,
					   TypeNameToString(tname));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, TypeRelationId, 0, comment);
}

/*
 * CommentAggregate --
 *
 * This routine is used to allow a user to provide comments on an
 * aggregate function. The aggregate function is determined by both
 * its name and its argument type(s).
 */
static void
CommentAggregate(List *aggregate, List *arguments, char *comment)
{
	Oid			oid;

	/* Look up function and make sure it's an aggregate */
	oid = LookupAggNameTypeNames(aggregate, arguments, false);

	/* Next, validate the user's attempt to comment */
	if (!pg_proc_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
					   NameListToString(aggregate));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, ProcedureRelationId, 0, comment);
}

/*
 * CommentProc --
 *
 * This routine is used to allow a user to provide comments on an
 * procedure (function). The procedure is determined by both
 * its name and its argument list. The argument list is expected to
 * be a series of parsed nodes pointed to by a List object. If the
 * comments string is empty, the associated comment is dropped.
 */
static void
CommentProc(List *function, List *arguments, char *comment)
{
	Oid			oid;

	/* Look up the procedure */

	oid = LookupFuncNameTypeNames(function, arguments, false);

	/* Now, validate the user's ability to comment on this function */

	if (!pg_proc_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
					   NameListToString(function));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, ProcedureRelationId, 0, comment);
}

/*
 * CommentOperator --
 *
 * This routine is used to allow a user to provide comments on an
 * operator. The operator for commenting is determined by both
 * its name and its argument list which defines the left and right
 * hand types the operator will operate on. The argument list is
 * expected to be a couple of parse nodes pointed to be a List
 * object.
 */
static void
CommentOperator(List *opername, List *arguments, char *comment)
{
	TypeName   *typenode1 = (TypeName *) linitial(arguments);
	TypeName   *typenode2 = (TypeName *) lsecond(arguments);
	Oid			oid;

	/* Look up the operator */
	oid = LookupOperNameTypeNames(NULL, opername,
								  typenode1, typenode2,
								  false, -1);

	/* Check user's privilege to comment on this operator */
	if (!pg_oper_ownercheck(oid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPER,
					   NameListToString(opername));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, OperatorRelationId, 0, comment);
}

/*
 * CommentTrigger --
 *
 * This routine is used to allow a user to provide comments on a
 * trigger event. The trigger for commenting is determined by both
 * its name and the relation to which it refers. The arguments to this
 * function are the trigger name and relation name (merged into a qualified
 * name), and the comment to add/drop.
 */
static void
CommentTrigger(List *qualname, char *comment)
{
	int			nnames;
	List	   *relname;
	char	   *trigname;
	RangeVar   *rel;
	Relation	pg_trigger,
				relation;
	cqContext	cqc;
	Oid			oid;
	int			fetchCount = 0;

	/* Separate relname and trig name */
	nnames = list_length(qualname);
	if (nnames < 2)				/* parser messed up */
		elog(ERROR, "must specify relation and trigger");
	relname = list_truncate(list_copy(qualname), nnames - 1);
	trigname = strVal(lfirst(list_tail(qualname)));

	/* Open the owning relation to ensure it won't go away meanwhile */
	rel = makeRangeVarFromNameList(relname);
	relation = heap_openrv(rel, AccessShareLock);

	/* Check object security */

	if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(relation));

	/*
	 * Fetch the trigger tuple from pg_trigger.  There can be only one because
	 * of the unique index.
	 */
	pg_trigger = heap_open(TriggerRelationId, AccessShareLock);

	oid = caql_getoid_plus(
			caql_addrel(cqclr(&cqc), pg_trigger),
			&fetchCount,
			NULL,
			cql("SELECT oid FROM pg_trigger "
				" WHERE tgrelid = :1 "
				" AND tgname = :2 ",
				ObjectIdGetDatum(RelationGetRelid(relation)),
				CStringGetDatum(trigname)));

	/* If no trigger exists for the relation specified, notify user */

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("trigger \"%s\" for table \"%s\" does not exist",
						trigname, RelationGetRelationName(relation))));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, TriggerRelationId, 0, comment);

	/* Done, but hold lock on relation */

	heap_close(pg_trigger, AccessShareLock);
	heap_close(relation, NoLock);
}


/*
 * CommentConstraint --
 *
 * Enable commenting on constraints held within the pg_constraint
 * table.  A qualified name is required as constraint names are
 * unique per relation.
 */
static void
CommentConstraint(List *qualname, char *comment)
{
	int			nnames;
	List	   *relName;
	char	   *conName;
	RangeVar   *rel;
	Relation	pg_constraint,
				relation;
	HeapTuple	tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;
	Oid			conOid = InvalidOid;

	/* Separate relname and constraint name */
	nnames = list_length(qualname);
	if (nnames < 2)				/* parser messed up */
		elog(ERROR, "must specify relation and constraint");
	relName = list_truncate(list_copy(qualname), nnames - 1);
	conName = strVal(lfirst(list_tail(qualname)));

	/* Open the owning relation to ensure it won't go away meanwhile */
	rel = makeRangeVarFromNameList(relName);
	relation = heap_openrv(rel, AccessShareLock);

	/* Check object security */

	if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
					   RelationGetRelationName(relation));

	/*
	 * Fetch the constraint tuple from pg_constraint.  There may be more than
	 * one match, because constraints are not required to have unique names;
	 * if so, error out.
	 */
	pg_constraint = heap_open(ConstraintRelationId, AccessShareLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_constraint),
			cql("SELECT * FROM pg_constraint "
				" WHERE conrelid = :1 ",
				ObjectIdGetDatum(RelationGetRelid(relation))));

	while (HeapTupleIsValid(tuple = caql_getnext(pcqCtx)))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);

		if (strcmp(NameStr(con->conname), conName) == 0)
		{
			if (OidIsValid(conOid))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("table \"%s\" has multiple constraints named \"%s\"",
						RelationGetRelationName(relation), conName)));
			conOid = HeapTupleGetOid(tuple);
		}
	}

	caql_endscan(pcqCtx);

	/* If no constraint exists for the relation specified, notify user */
	if (!OidIsValid(conOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("constraint \"%s\" for table \"%s\" does not exist",
						conName, RelationGetRelationName(relation))));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(conOid, ConstraintRelationId, 0, comment);

	/* Done, but hold lock on relation */
	heap_close(pg_constraint, AccessShareLock);
	heap_close(relation, NoLock);
}

/*
 * CommentConversion --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a CONVERSION. The conversion is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateComments() routine.
 * The conversion's name and the comment are the parameters to this routine.
 */
static void
CommentConversion(List *qualname, char *comment)
{
	Oid			conversionOid;

	conversionOid = FindConversionByName(qualname);
	if (!OidIsValid(conversionOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("conversion \"%s\" does not exist",
						NameListToString(qualname)),
				 errOmitLocation(true)));

	/* Check object security */
	if (!pg_conversion_ownercheck(conversionOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CONVERSION,
					   NameListToString(qualname));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(conversionOid, ConversionRelationId, 0, comment);
}

/*
 * CommentLanguage --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a LANGUAGE. The language is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateComments() routine.
 * The language's name and the comment are the parameters to this routine.
 */
static void
CommentLanguage(List *qualname, char *comment)
{
	Oid			oid;
	char	   *language;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("language name may not be qualified")));
	language = strVal(linitial(qualname));

	oid = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_language "
				" WHERE lanname = :1 ",
				CStringGetDatum(language)));

	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("language \"%s\" does not exist", language)));

	/* Check object security */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("must be superuser to comment on procedural language")));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(oid, LanguageRelationId, 0, comment);
}

/*
 * CommentOpClass --
 *
 * This routine is used to allow a user to provide comments on an
 * operator class. The operator class for commenting is determined by both
 * its name and its argument list which defines the index method
 * the operator class is used for. The argument list is expected to contain
 * a single name (represented as a string Value node).
 */
static void
CommentOpClass(List *qualname, List *arguments, char *comment)
{
	char	   *amname;
	char	   *schemaname;
	char	   *opcname;
	Oid			amID;
	Oid			opcID;
	int			fetchCount = 0;

	Assert(list_length(arguments) == 1);
	amname = strVal(linitial(arguments));

	/*
	 * Get the access method's OID.
	 */
	amID = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_am "
				" WHERE amname = :1 ",
				CStringGetDatum(amname)));

	if (!OidIsValid(amID))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist",
						amname)));

	/*
	 * Look up the opclass.
	 */

	/* deconstruct the name list */
	DeconstructQualifiedName(qualname, &schemaname, &opcname);

	if (schemaname)
	{
		/* Look in specific schema only */
		Oid			namespaceId;

		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);

		opcID = caql_getoid_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT oid FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(amID),
					PointerGetDatum(opcname),
					ObjectIdGetDatum(namespaceId)));
	}
	else
	{
		Oid	opcID1;
		/* Unqualified opclass name, so search the search path */
		opcID1 = OpclassnameGetOpcid(amID, opcname);
		if (!OidIsValid(opcID1))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, amname)));


		opcID = caql_getoid_plus(
				NULL,
				&fetchCount,
				NULL,
				cql("SELECT oid FROM pg_opclass "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(opcID1)));
	}

	if (0 == fetchCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
						NameListToString(qualname), amname)));
	}

	/* Permission check: must own opclass */
	if (!pg_opclass_ownercheck(opcID, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS,
					   NameListToString(qualname));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(opcID, OperatorClassRelationId, 0, comment);
}

/*
 * CommentLargeObject --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a LARGE OBJECT. The large object is specified by OID
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateComments() routine.
 * The large object's OID and the comment are the parameters to this routine.
 */
static void
CommentLargeObject(List *qualname, char *comment)
{
	Oid			loid;
	Node	   *node;

	Assert(list_length(qualname) == 1);
	node = (Node *) linitial(qualname);

	switch (nodeTag(node))
	{
		case T_Integer:
			loid = intVal(node);
			break;
		case T_Float:

			/*
			 * Values too large for int4 will be represented as Float
			 * constants by the lexer.	Accept these if they are valid OID
			 * strings.
			 */
			loid = DatumGetObjectId(DirectFunctionCall1(oidin,
											 CStringGetDatum(strVal(node))));
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			/* keep compiler quiet */
			loid = InvalidOid;
	}

	/* check that the large object exists */
	if (!LargeObjectExists(loid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("large object %u does not exist", loid)));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(loid, LargeObjectRelationId, 0, comment);
}

/*
 * CommentCast --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a CAST. The cast is specified by source and destination types
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateComments() routine.
 * The cast's source type is passed as the "name", the destination type
 * as the "arguments".
 */
static void
CommentCast(List *qualname, List *arguments, char *comment)
{
	TypeName   *sourcetype;
	TypeName   *targettype;
	Oid			sourcetypeid;
	Oid			targettypeid;
	int			fetchCount = 0;
	Oid			castOid;

	Assert(list_length(qualname) == 1);
	sourcetype = (TypeName *) linitial(qualname);
	Assert(IsA(sourcetype, TypeName));
	Assert(list_length(arguments) == 1);
	targettype = (TypeName *) linitial(arguments);
	Assert(IsA(targettype, TypeName));

	sourcetypeid = typenameTypeId(NULL, sourcetype);
	targettypeid = typenameTypeId(NULL, targettype);

	castOid = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT * FROM pg_cast "
				" WHERE castsource = :1 "
				" AND casttarget = :2 ",
				ObjectIdGetDatum(sourcetypeid),
				ObjectIdGetDatum(targettypeid)));

	if (0 == fetchCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cast from type %s to type %s does not exist",
						TypeNameToString(sourcetype),
						TypeNameToString(targettype))));
	}

	/* Permission check */
	if (!pg_type_ownercheck(sourcetypeid, GetUserId())
		&& !pg_type_ownercheck(targettypeid, GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of type %s or type %s",
						TypeNameToString(sourcetype),
						TypeNameToString(targettype))));

	/* Call CreateComments() to create/drop the comments */
	CreateComments(castOid, CastRelationId, 0, comment);
}


/*
 * CommentResourceQueue --
 *
 * This routine is used to add/drop any user-comments a user might
 * have regarding a RESOURCE QUEUE.  The resource queue is specified by name
 * and, if found, and the user has appropriate permissions, a
 * comment will be added/dropped using the CreateSharedComments() routine.
 *
 */
static void
CommentResourceQueue(List *qualname, char *comment)
{
	char		*queueName;
	Oid			 oid = InvalidOid;
	int			 fetchCount = 0;

	if (list_length(qualname) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("resource queue name may not be qualified")));
	queueName = strVal(linitial(qualname));

	oid = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT oid FROM pg_resqueue "
				" WHERE name = :1 ",
				CStringGetDatum(queueName)));

	if (0 == fetchCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource queue \"%s\" does not exist", queueName)));
		return;
	}

	/* Check object security */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to comment on resource queue")));

	/* Call CreateSharedComments() to create/drop the comments */
	CreateSharedComments(oid, ResQueueRelationId, comment);
}
