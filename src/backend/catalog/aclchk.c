/*-------------------------------------------------------------------------
 *
 * aclchk.c
 *	  Routines to check access control permissions.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/aclchk.c,v 1.133.2.1 2007/04/20 02:37:48 tgl Exp $
 *
 * NOTES
 *	  See acl.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/heap.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"

#include "catalog/indexing.h"
#include "catalog/gp_persistent.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filesystem.h"
#include "catalog/pg_type.h"
#include "cdb/cdbpartition.h"
#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/prep.h"
#include "parser/parse_func.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/rangerrest.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "cdb/dispatcher.h"

#define ACTION_LENGTH 12
static void ExecGrant_Relation(InternalGrant *grantStmt);
static void ExecGrant_Database(InternalGrant *grantStmt);
static void ExecGrant_Fdw(InternalGrant *grantStmt);
static void ExecGrant_ForeignServer(InternalGrant *grantStmt);
static void ExecGrant_Function(InternalGrant *grantStmt);
static void ExecGrant_Language(InternalGrant *grantStmt);
static void ExecGrant_Namespace(InternalGrant *grantStmt);
static void ExecGrant_Tablespace(InternalGrant *grantStmt);
static void ExecGrant_ExtProtocol(InternalGrant *grantstmt);

static List *objectNamesToOids(GrantObjectType objtype, List *objnames);
static AclMode string_to_privilege(const char *privname);
static const char *privilege_to_string(AclMode privilege);
static AclMode restrict_and_check_grant(bool is_grant, AclMode avail_goptions,
						 bool all_privs, AclMode privileges,
						 Oid objectId, Oid grantorId,
						 AclObjectKind objkind, char *objname);
static AclMode pg_aclmask(AclObjectKind objkind, Oid table_oid, Oid roleid,
		   AclMode mask, AclMaskHow how);


#ifdef ACLDEBUG
static void
dumpacl(Acl *acl)
{
	int			i;
	AclItem    *aip;

	elog(DEBUG2, "acl size = %d, # acls = %d",
		 ACL_SIZE(acl), ACL_NUM(acl));
	aip = ACL_DAT(acl);
	for (i = 0; i < ACL_NUM(acl); ++i)
		elog(DEBUG2, "	acl[%d]: %s", i,
			 DatumGetCString(DirectFunctionCall1(aclitemout,
												 PointerGetDatum(aip + i))));
}
#endif   /* ACLDEBUG */


/*
 * If is_grant is true, adds the given privileges for the list of
 * grantees to the existing old_acl.  If is_grant is false, the
 * privileges for the given grantees are removed from old_acl.
 *
 * NB: the original old_acl is pfree'd.
 */
static Acl *
merge_acl_with_grant(Acl *old_acl, bool is_grant,
					 bool grant_option, DropBehavior behavior,
					 List *grantees, AclMode privileges,
					 Oid grantorId, Oid ownerId)
{
	unsigned	modechg;
	ListCell   *j;
	Acl		   *new_acl;

	modechg = is_grant ? ACL_MODECHG_ADD : ACL_MODECHG_DEL;

#ifdef ACLDEBUG
	dumpacl(old_acl);
#endif
	new_acl = old_acl;

	foreach(j, grantees)
	{
		AclItem aclitem;
		Acl		   *newer_acl;

		aclitem.	ai_grantee = lfirst_oid(j);

		/*
		 * Grant options can only be granted to individual roles, not PUBLIC.
		 * The reason is that if a user would re-grant a privilege that he
		 * held through PUBLIC, and later the user is removed, the situation
		 * is impossible to clean up.
		 */
		if (is_grant && grant_option && aclitem.ai_grantee == ACL_ID_PUBLIC)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_GRANT_OPERATION),
					 errmsg("grant options can only be granted to roles")));

		aclitem.	ai_grantor = grantorId;

		/*
		 * The asymmetry in the conditions here comes from the spec.  In
		 * GRANT, the grant_option flag signals WITH GRANT OPTION, which means
		 * to grant both the basic privilege and its grant option. But in
		 * REVOKE, plain revoke revokes both the basic privilege and its grant
		 * option, while REVOKE GRANT OPTION revokes only the option.
		 */
		ACLITEM_SET_PRIVS_GOPTIONS(aclitem,
					(is_grant || !grant_option) ? privileges : ACL_NO_RIGHTS,
				   (!is_grant || grant_option) ? privileges : ACL_NO_RIGHTS);

		newer_acl = aclupdate(new_acl, &aclitem, modechg, ownerId, behavior);

		/* avoid memory leak when there are many grantees */
		pfree(new_acl);
		new_acl = newer_acl;

#ifdef ACLDEBUG
		dumpacl(new_acl);
#endif
	}

	return new_acl;
}

/*
 * Restrict the privileges to what we can actually grant, and emit
 * the standards-mandated warning and error messages.
 */
static AclMode
restrict_and_check_grant(bool is_grant, AclMode avail_goptions, bool all_privs,
						 AclMode privileges, Oid objectId, Oid grantorId,
						 AclObjectKind objkind, char *objname)
{
	AclMode		this_privileges;
	AclMode		whole_mask;

	switch (objkind)
	{
		case ACL_KIND_CLASS:
			whole_mask = ACL_ALL_RIGHTS_RELATION;
			break;
		case ACL_KIND_SEQUENCE:
			whole_mask = ACL_ALL_RIGHTS_SEQUENCE;
			break;
		case ACL_KIND_DATABASE:
			whole_mask = ACL_ALL_RIGHTS_DATABASE;
			break;
		case ACL_KIND_PROC:
			whole_mask = ACL_ALL_RIGHTS_FUNCTION;
			break;
		case ACL_KIND_LANGUAGE:
			whole_mask = ACL_ALL_RIGHTS_LANGUAGE;
			break;
		case ACL_KIND_NAMESPACE:
			whole_mask = ACL_ALL_RIGHTS_NAMESPACE;
			break;
		case ACL_KIND_TABLESPACE:
			whole_mask = ACL_ALL_RIGHTS_TABLESPACE;
			break;
		case ACL_KIND_FDW:
			whole_mask = ACL_ALL_RIGHTS_FDW;
			break;
		case ACL_KIND_FOREIGN_SERVER:
			whole_mask = ACL_ALL_RIGHTS_FOREIGN_SERVER;
			break;
		case ACL_KIND_EXTPROTOCOL:
			whole_mask = ACL_ALL_RIGHTS_EXTPROTOCOL;
			break;
		default:
			elog(ERROR, "unrecognized object kind: %d", objkind);
			/* not reached, but keep compiler quiet */
			return ACL_NO_RIGHTS;
	}

	/*
	 * If we found no grant options, consider whether to issue a hard error.
	 * Per spec, having any privilege at all on the object will get you by
	 * here.
	 */
	if (avail_goptions == ACL_NO_RIGHTS)
	{
	  if (enable_ranger && !fallBackToNativeCheck(objkind, objectId, grantorId)) {
	    if (pg_rangercheck(objkind, objectId, grantorId,
	        whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
	        ACLMASK_ANY) != ACLCHECK_OK)
	      aclcheck_error(ACLCHECK_NO_PRIV, objkind, objname);
	  }
	  else {
	    if (pg_aclmask(objkind, objectId, grantorId,
	        whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
	        ACLMASK_ANY) == ACL_NO_RIGHTS)
	      aclcheck_error(ACLCHECK_NO_PRIV, objkind, objname);
	  }
	}

	/*
	 * Restrict the operation to what we can actually grant or revoke, and
	 * issue a warning if appropriate.	(For REVOKE this isn't quite what the
	 * spec says to do: the spec seems to want a warning only if no privilege
	 * bits actually change in the ACL. In practice that behavior seems much
	 * too noisy, as well as inconsistent with the GRANT case.)
	 */
	this_privileges = privileges & ACL_OPTION_TO_PRIVS(avail_goptions);
	
	 /*
	 * GPDB: don't do this if we're an execute node. Let the QD handle the
	 * WARNING.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
		return this_privileges;

	if (is_grant)
	{
		if (this_privileges == 0)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
				  errmsg("no privileges were granted for \"%s\"", objname)));
		else if (!all_privs && this_privileges != privileges)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
			 errmsg("not all privileges were granted for \"%s\"", objname)));
	}
	else
	{
		if (this_privileges == 0)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
			  errmsg("no privileges could be revoked for \"%s\"", objname)));
		else if (!all_privs && this_privileges != privileges)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
					 errmsg("not all privileges could be revoked for \"%s\"", objname)));
	}

	return this_privileges;
}

/*
 * Called to execute the utility commands GRANT and REVOKE
 */
void
ExecuteGrantStmt(GrantStmt *stmt)
{
	InternalGrant istmt;
	ListCell   *cell;
	const char *errormsg;
	AclMode		all_privileges;
	List	   *objs = NIL;
	bool		added_objs = false;

	/*
	 * Turn the regular GrantStmt into the InternalGrant form.
	 */
	istmt.is_grant = stmt->is_grant;
	istmt.objtype = stmt->objtype;
	istmt.objects = objectNamesToOids(stmt->objtype, stmt->objects);

	/* If this is a GRANT/REVOKE on a table, expand partition references */
	if (istmt.objtype == ACL_OBJECT_RELATION)
	{
		foreach(cell, istmt.objects)
		{
			Oid relid = lfirst_oid(cell);
			Relation rel = heap_open(relid, AccessShareLock);
			bool add_self = true;

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				List *a;
				if (rel_is_partitioned(relid))
				{
					PartitionNode *pn = RelationBuildPartitionDesc(rel, false);

					a = all_partition_relids(pn);
					if (a)
						added_objs = true;

					objs = list_concat(objs, a);
				}
				else if (rel_is_child_partition(relid))
				{
					/* get my children */
					a = find_all_inheritors(relid);
					if (a)
						added_objs = true;

					objs = list_concat(objs, a);
	
					/* find_all_inheritors() adds me, don't do it twice */
					add_self = false;
				}
			}

			heap_close(rel, NoLock);

			if (add_self)
				objs = lappend_oid(objs, relid);
		}
		istmt.objects = objs;
	}

	/* If we're dispatching, put the objects back in into the parse tree */
	if (Gp_role == GP_ROLE_DISPATCH && added_objs)
	{
		List *n = NIL;

		foreach(cell, istmt.objects)
		{
			Oid rid = lfirst_oid(cell);
			RangeVar *rv;
			char *nspname = get_namespace_name(get_rel_namespace(rid));
			char *relname = get_rel_name(rid);

			rv = makeRangeVar(NULL /*catalogname*/, nspname, relname, -1);
			n = lappend(n, rv);
		}

		stmt->objects = n;
	}

	if (stmt->cooked_privs)
	{
		istmt.all_privs = false;
		istmt.privileges = 0;
		istmt.grantees = NIL;
		istmt.grant_option = stmt->grant_option;
		istmt.behavior = stmt->behavior;
		istmt.cooked_privs = stmt->cooked_privs;
	}
	else
	{
		/* all_privs to be filled below */
		/* privileges to be filled below */
		istmt.grantees = NIL;
		/* filled below */
		istmt.grant_option = stmt->grant_option;
		istmt.behavior = stmt->behavior;
	
	
		/*
		 * Convert the PrivGrantee list into an Oid list.  Note that at this point
		 * we insert an ACL_ID_PUBLIC into the list if an empty role name is
		 * detected (which is what the grammar uses if PUBLIC is found), so
		 * downstream there shouldn't be any additional work needed to support
		 * this case.
		 */
		foreach(cell, stmt->grantees)
		{
			PrivGrantee *grantee = (PrivGrantee *) lfirst(cell);
	
			if (grantee->rolname == NULL)
				istmt.grantees = lappend_oid(istmt.grantees, ACL_ID_PUBLIC);
			else
				istmt.grantees =
					lappend_oid(istmt.grantees,
								get_roleid_checked(grantee->rolname));
		}
	
		/*
		 * Convert stmt->privileges, a textual list, into an AclMode bitmask.
		 */
		switch (stmt->objtype)
		{
				/*
				 * Because this might be a sequence, we test both relation and
				 * sequence bits, and later do a more limited test when we know
				 * the object type.
				 */
			case ACL_OBJECT_RELATION:
				all_privileges = ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE;
				errormsg = _("invalid privilege type %s for relation");
				break;
			case ACL_OBJECT_SEQUENCE:
				all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
				errormsg = _("invalid privilege type %s for sequence");
				break;
			case ACL_OBJECT_DATABASE:
				all_privileges = ACL_ALL_RIGHTS_DATABASE;
				errormsg = _("invalid privilege type %s for database");
				break;
			case ACL_OBJECT_FUNCTION:
				all_privileges = ACL_ALL_RIGHTS_FUNCTION;
				errormsg = _("invalid privilege type %s for function");
				break;
			case ACL_OBJECT_LANGUAGE:
				all_privileges = ACL_ALL_RIGHTS_LANGUAGE;
				errormsg = _("invalid privilege type %s for language");
				break;
			case ACL_OBJECT_NAMESPACE:
				all_privileges = ACL_ALL_RIGHTS_NAMESPACE;
				errormsg = _("invalid privilege type %s for schema");
				break;
			case ACL_OBJECT_TABLESPACE:
				all_privileges = ACL_ALL_RIGHTS_TABLESPACE;
				errormsg = _("invalid privilege type %s for tablespace");
				break;
			case ACL_OBJECT_FDW:
				all_privileges = ACL_ALL_RIGHTS_FDW;
				errormsg = _("invalid privilege type %s for foreign-data wrapper");
				break;
			case ACL_OBJECT_FOREIGN_SERVER:
				all_privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;
				errormsg = _("invalid privilege type %s for foreign server");
				break;
			case ACL_OBJECT_EXTPROTOCOL:
				all_privileges = ACL_ALL_RIGHTS_EXTPROTOCOL;
				errormsg = _("invalid privilege type %s for external protocol");
				break;
			default:
				/* keep compiler quiet */
				all_privileges = ACL_NO_RIGHTS;
				errormsg = NULL;
				elog(ERROR, "unrecognized GrantStmt.objtype: %d",
					 (int) stmt->objtype);
		}
	
		if (stmt->privileges == NIL)
		{
			istmt.all_privs = true;
	
			/*
			 * will be turned into ACL_ALL_RIGHTS_* by the internal routines
			 * depending on the object type
			 */
			istmt.privileges = ACL_NO_RIGHTS;
		}
		else
		{
			istmt.all_privs = false;
			istmt.privileges = ACL_NO_RIGHTS;
	
			foreach(cell, stmt->privileges)
			{
				char	   *privname = strVal(lfirst(cell));
				AclMode		priv = string_to_privilege(privname);
	
				if (priv & ~((AclMode) all_privileges))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_GRANT_OPERATION),
							 errmsg(errormsg,
									privilege_to_string(priv))));
	
				istmt.privileges |= priv;
			}
		}
	
		istmt.cooked_privs = NIL;
	}

	ExecGrantStmt_oids(&istmt);
}

/*
 * ExecGrantStmt_oids
 *
 * "Internal" entrypoint for granting and revoking privileges.
 */
void
ExecGrantStmt_oids(InternalGrant *istmt)
{
	switch (istmt->objtype)
	{
		case ACL_OBJECT_RELATION:
		case ACL_OBJECT_SEQUENCE:
			ExecGrant_Relation(istmt);
			break;
		case ACL_OBJECT_DATABASE:
			ExecGrant_Database(istmt);
			break;
		case ACL_OBJECT_FDW:
			if (!(IsBootstrapProcessingMode() || (Gp_role == GP_ROLE_UTILITY) 
				|| gp_called_by_pgdump))
			{
                        	ereport(ERROR,
                                	(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support GRANT/REVOKE on FOREIGN DATA WRAPPER statement") ));
			}
			ExecGrant_Fdw(istmt);
			break;
		case ACL_OBJECT_FOREIGN_SERVER:
			if (!(IsBootstrapProcessingMode() || (Gp_role == GP_ROLE_UTILITY) 
				|| gp_called_by_pgdump))
			{
                        	ereport(ERROR,
                                	(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support GRANT/REVOKE on FOREIGN SERVER statement") ));
			}
			ExecGrant_ForeignServer(istmt);
			break;
		case ACL_OBJECT_FUNCTION:
			ExecGrant_Function(istmt);
			break;
		case ACL_OBJECT_LANGUAGE:
			if (!(IsBootstrapProcessingMode() || (Gp_role == GP_ROLE_UTILITY) 
				|| gp_called_by_pgdump))
			{
                        	ereport(ERROR,
                                	(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support GRANT/REVOKE on LANGUAGE statement") ));
			}
			ExecGrant_Language(istmt);
			break;
		case ACL_OBJECT_NAMESPACE:
			ExecGrant_Namespace(istmt);
			break;
		case ACL_OBJECT_TABLESPACE:
			if (!(IsBootstrapProcessingMode() || (Gp_role == GP_ROLE_UTILITY) 
				|| gp_called_by_pgdump))
			{
                        	ereport(ERROR,
                                	(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support GRANT/REVOKE on TABLESPACE statement") ));
			}
			ExecGrant_Tablespace(istmt);
			break;
		case ACL_OBJECT_EXTPROTOCOL:
			ExecGrant_ExtProtocol(istmt);
			break;
		default:
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) istmt->objtype);
	}
}

/*
 * objectNamesToOids
 *
 * Turn a list of object names of a given type into an Oid list.
 */
static List *
objectNamesToOids(GrantObjectType objtype, List *objnames)
{
	List	   *objects = NIL;
	ListCell   *cell;

	Assert(objnames != NIL);

	switch (objtype)
	{
		case ACL_OBJECT_RELATION:
		case ACL_OBJECT_SEQUENCE:
			foreach(cell, objnames)
			{
				RangeVar   *relvar = (RangeVar *) lfirst(cell);
				Oid			relOid;

				relOid = RangeVarGetRelid(relvar, false, false /*allowHcatalog*/);
				objects = lappend_oid(objects, relOid);
			}
			break;
		case ACL_OBJECT_DATABASE:
			foreach(cell, objnames)
			{
				char	   *dbname = strVal(lfirst(cell));
				Oid			dbid;

				dbid = get_database_oid(dbname);
				if (!OidIsValid(dbid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_DATABASE),
							 errmsg("database \"%s\" does not exist",
									dbname)));
				objects = lappend_oid(objects, dbid);
			}
			break;
		case ACL_OBJECT_FUNCTION:
			foreach(cell, objnames)
			{
				FuncWithArgs *func = (FuncWithArgs *) lfirst(cell);
				Oid			funcid;

				funcid = LookupFuncNameTypeNames(func->funcname,
												 func->funcargs, false);
				objects = lappend_oid(objects, funcid);
			}
			break;
		case ACL_OBJECT_LANGUAGE:
			foreach(cell, objnames)
			{
				int			fetchCount = 0;
				Oid			objId;
				char	   *langname = strVal(lfirst(cell));

				objId = caql_getoid_plus(
						NULL,
						&fetchCount,
						NULL,
						cql("SELECT oid FROM pg_language "
							" WHERE lanname = :1 ",
							PointerGetDatum(langname)));

				if (0 == fetchCount)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("language \"%s\" does not exist",
									langname)));

				objects = lappend_oid(objects, objId);
			}
			break;
		case ACL_OBJECT_NAMESPACE:
			foreach(cell, objnames)
			{
				char	   *nspname = strVal(lfirst(cell));

				Oid objId = LookupInternalNamespaceId(nspname);

				if (InvalidOid == objId)
				{
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_SCHEMA),
							 errmsg("schema \"%s\" does not exist",
									nspname)));
				}
				
				objects = lappend_oid(objects, objId);
			}
			break;
		case ACL_OBJECT_TABLESPACE:
			foreach(cell, objnames)
			{
				char	   *spcname = strVal(lfirst(cell));
				int			fetchCount = 0;
				Oid			objId;

				objId = caql_getoid_plus(
						NULL,
						&fetchCount,
						NULL,
						cql("SELECT oid FROM pg_tablespace "
							" WHERE spcname = :1",
							CStringGetDatum(spcname)));

				if (0 == fetchCount)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
					   errmsg("tablespace \"%s\" does not exist", spcname)));

				objects = lappend_oid(objects, objId);
			}
			break;
		case ACL_OBJECT_FDW:
			foreach(cell, objnames)
			{
				char	   *fdwname = strVal(lfirst(cell));
				Oid			fdwid = GetForeignDataWrapperOidByName(fdwname, false);

				objects = lappend_oid(objects, fdwid);
			}
			break;
		case ACL_OBJECT_FOREIGN_SERVER:
			foreach(cell, objnames)
			{
				char	   *srvname = strVal(lfirst(cell));
				Oid			srvid = GetForeignServerOidByName(srvname, false);

				objects = lappend_oid(objects, srvid);
			}
			break;
		case ACL_OBJECT_EXTPROTOCOL:
			foreach(cell, objnames)
			{
				char	   *ptcname = strVal(lfirst(cell));
				Oid			ptcid = LookupExtProtocolOid(ptcname, false);

				objects = lappend_oid(objects, ptcid);
			}
			break;			
		default:
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) objtype);
	}

	return objects;
}

/*
 *	This processes both sequences and non-sequences.
 */
static void
ExecGrant_Relation(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	relation = heap_open(RelationRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid				 relOid		 = lfirst_oid(cell);
		Datum			 aclDatum;
		Form_pg_class	 pg_class_tuple;
		bool			 isNull;
		AclMode			 avail_goptions;
		AclMode			 this_privileges;
		Acl				*old_acl;
		Acl				*new_acl;
		Oid				 grantorId;
		Oid				 ownerId	 = InvalidOid;
		HeapTuple		 tuple;
		HeapTuple		 newtuple;
		Datum			 values[Natts_pg_class];
		bool			 nulls[Natts_pg_class];
		bool			 replaces[Natts_pg_class];
		int				 nnewmembers;
		Oid				*newmembers;
		int				 noldmembers = 0;
		Oid				*oldmembers;
		bool			 bTemp;
		cqContext		 cqc;
		cqContext		*pcqCtx;

		bTemp = false;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_class "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(relOid)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", relOid);

		pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);

		/* Not sensible to grant on an index */
		if (pg_class_tuple->relkind == RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is an index",
							NameStr(pg_class_tuple->relname))));

		/* Composite types aren't tables either */
		if (pg_class_tuple->relkind == RELKIND_COMPOSITE_TYPE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a composite type",
							NameStr(pg_class_tuple->relname))));

		/* Used GRANT SEQUENCE on a non-sequence? */
		if (istmt->objtype == ACL_OBJECT_SEQUENCE &&
			pg_class_tuple->relkind != RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a sequence",
							NameStr(pg_class_tuple->relname))));

		/* pre-cooked privileges -- probably from ADD PARTITION */
		if (istmt->cooked_privs)
		{
			ListCell *lc;
			AclItem *aip;
			int size = ACL_N_SIZE(list_length(istmt->cooked_privs));

			new_acl = (Acl *) palloc0(size);
			SET_VARSIZE(new_acl, size);
			new_acl->ndim = 1;
			new_acl->dataoffset = 0;	/* we never put in any nulls */
			new_acl->elemtype = ACLITEMOID;
			ARR_LBOUND(new_acl)[0] = 1;
			ARR_DIMS(new_acl)[0] = list_length(istmt->cooked_privs);
			aip = ACL_DAT(new_acl);

			foreach(lc, istmt->cooked_privs)
			{
				char *aclstr = strVal(lfirst(lc));
				AclItem *newai;

				newai = DatumGetPointer(DirectFunctionCall1(aclitemin,
											CStringGetDatum(aclstr)));

				aip->ai_grantee = newai->ai_grantee;
				aip->ai_grantor = newai->ai_grantor;
				aip->ai_privs = newai->ai_privs;

				aip++;
			}
		}
		else
		{

			/* 
			 * Adjust the default permissions based on whether it is a 
			 * sequence
			 */
			if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
			{
				if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
					this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
				else
					this_privileges = ACL_ALL_RIGHTS_RELATION;
			}
			else
				this_privileges = istmt->privileges;
	
			/*
			 * The GRANT TABLE syntax can be used for sequences and
			 * non-sequences, so we have to look at the relkind to determine
			 * the supported permissions.  The OR of table and sequence
			 * permissions were already checked.
			 */
			if (istmt->objtype == ACL_OBJECT_RELATION)
			{
				if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
				{
					/*
					 * For backward compatibility, throw just a warning for
					 * invalid sequence permissions when using the non-sequence
					 * GRANT syntax is used.
					 */
					if (this_privileges & ~((AclMode) ACL_ALL_RIGHTS_SEQUENCE))
					{
						/*
						 * Mention the object name because the user
						 * needs to know which operations
						 * succeeded. This is required because WARNING
						 * allows the command to continue.
						 */
						ereport(WARNING,
								(errcode(ERRCODE_INVALID_GRANT_OPERATION),
								 errmsg("sequence \"%s\" only supports USAGE, SELECT, and UPDATE",
										NameStr(pg_class_tuple->relname))));
						this_privileges &= (AclMode) ACL_ALL_RIGHTS_SEQUENCE;
					}
				}
				else
				{
					if (this_privileges & ~((AclMode) ACL_ALL_RIGHTS_RELATION))
	
						/*
						 * USAGE is the only permission supported by
						 * sequences but not by non-sequences.  Don't
						 * mention the object name because we didn't
						 * in the combined TABLE | SEQUENCE check.
						 */
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_GRANT_OPERATION),
							  errmsg("invalid privilege type USAGE for table")));
				}
			}
	
			/*
			 * Get owner ID and working copy of existing ACL. If
			 * there's no ACL, substitute the proper default.
			 */
			ownerId = pg_class_tuple->relowner;
			aclDatum = caql_getattr(pcqCtx, Anum_pg_class_relacl,
									&isNull);
			if (isNull)
				old_acl = acldefault(pg_class_tuple->relkind == RELKIND_SEQUENCE ?
									 ACL_OBJECT_SEQUENCE : ACL_OBJECT_RELATION,
									 ownerId);
			else
				old_acl = DatumGetAclPCopy(aclDatum);
	
			/* Determine ID to do the grant as, and available grant options */
			select_best_grantor(GetUserId(), this_privileges,
								old_acl, ownerId,
								&grantorId, &avail_goptions);

			/*
			 * Restrict the privileges to what we can actually grant,
			 * and emit the standards-mandated warning and error
			 * messages.
			 */
			this_privileges =
				restrict_and_check_grant(istmt->is_grant, avail_goptions,
										 istmt->all_privs, this_privileges,
										 relOid, grantorId,
									  pg_class_tuple->relkind == RELKIND_SEQUENCE
										 ? ACL_KIND_SEQUENCE : ACL_KIND_CLASS,
										 NameStr(pg_class_tuple->relname));
	
			/*
			 * Generate new ACL.
			 *
			 * We need the members of both old and new ACLs so we can
			 * correct the shared dependency information.
			 */
			noldmembers = aclmembers(old_acl, &oldmembers);
	
			new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
										   istmt->grant_option, istmt->behavior,
										   istmt->grantees, this_privileges,
										   grantorId, ownerId);
		}

		nnewmembers = aclmembers(new_acl, &newmembers);

			/* finished building new ACL value, now insert it */
			MemSet(values, 0, sizeof(values));
			MemSet(nulls, false, sizeof(nulls));
			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pg_class_relacl - 1] = true;
			values[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);

			newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */
		
		/* MPP-7572: Don't track metadata if table in any
		 * temporary namespace
		 */
		bTemp = isAnyTempNamespace(pg_class_tuple->relnamespace);

		/* MPP-6929: metadata tracking */
		if (!bTemp && 
			(Gp_role == GP_ROLE_DISPATCH)
			&& (
				(pg_class_tuple->relkind == RELKIND_INDEX) ||
				(pg_class_tuple->relkind == RELKIND_RELATION) ||
				(pg_class_tuple->relkind == RELKIND_SEQUENCE) ||
				(pg_class_tuple->relkind == RELKIND_VIEW)))
			MetaTrackUpdObject(RelationRelationId,
							   relOid,
							   GetUserId(), /* not grantorId, */
							   "PRIVILEGE", 
							   (istmt->is_grant) ? "GRANT" : "REVOKE"
					);


		if (!istmt->cooked_privs)
		{
			/* Update the shared dependency ACL info */
			updateAclDependencies(RelationRelationId, relOid,
								  ownerId, istmt->is_grant,
								  noldmembers, oldmembers,
								  nnewmembers, newmembers);
		}

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Database(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_DATABASE;

	relation = heap_open(DatabaseRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			datId = lfirst_oid(cell);
		Form_pg_database pg_database_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_database];
		bool		nulls[Natts_pg_database];
		bool		replaces[Natts_pg_database];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		HeapTuple	tuple;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_database "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(datId)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for database %u", datId);

		pg_database_tuple = (Form_pg_database) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_database_tuple->datdba;
		aclDatum = heap_getattr(tuple, Anum_pg_database_datacl,
								RelationGetDescr(relation), &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 datId, grantorId, ACL_KIND_DATABASE,
									 NameStr(pg_database_tuple->datname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_database_datacl - 1] = true;
		values[Anum_pg_database_datacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackUpdObject(DatabaseRelationId,
							   datId,
							   GetUserId(), /* not grantorId, */
							   "PRIVILEGE", 
							   (istmt->is_grant) ? "GRANT" : "REVOKE"
					);

		/* Update the shared dependency ACL info */
		updateAclDependencies(DatabaseRelationId, HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Fdw(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FDW;

	relation = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			fdwid = lfirst_oid(cell);
		Form_pg_foreign_data_wrapper pg_fdw_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_foreign_data_wrapper];
		bool		nulls[Natts_pg_foreign_data_wrapper];
		bool		replaces[Natts_pg_foreign_data_wrapper];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_foreign_data_wrapper "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(fdwid)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for foreign-data wrapper %u", fdwid);

		pg_fdw_tuple = (Form_pg_foreign_data_wrapper) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_fdw_tuple->fdwowner;
		aclDatum = caql_getattr(pcqCtx,
								Anum_pg_foreign_data_wrapper_fdwacl,
								&isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FDW, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 fdwid, grantorId, ACL_KIND_FDW,
									 NameStr(pg_fdw_tuple->fdwname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;
		values[Anum_pg_foreign_data_wrapper_fdwacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* Update the shared dependency ACL info */
		updateAclDependencies(ForeignDataWrapperRelationId,
							  HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_ForeignServer(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;

	relation = heap_open(ForeignServerRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			srvid = lfirst_oid(cell);
		Form_pg_foreign_server pg_server_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_foreign_server];
		bool		nulls[Natts_pg_foreign_server];
		bool		replaces[Natts_pg_foreign_server];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_foreign_server "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(srvid)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for foreign server %u", srvid);

		pg_server_tuple = (Form_pg_foreign_server) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_server_tuple->srvowner;
		aclDatum = caql_getattr(pcqCtx,
								Anum_pg_foreign_server_srvacl,
								&isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 srvid, grantorId, ACL_KIND_FOREIGN_SERVER,
									 NameStr(pg_server_tuple->srvname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_foreign_server_srvacl - 1] = true;
		values[Anum_pg_foreign_server_srvacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* Update the shared dependency ACL info */
		updateAclDependencies(ForeignServerRelationId,
							  HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Function(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FUNCTION;

	relation = heap_open(ProcedureRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			funcId = lfirst_oid(cell);
		Form_pg_proc pg_proc_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_proc];
		bool		nulls[Natts_pg_proc];
		bool		replaces[Natts_pg_proc];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_proc "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(funcId)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", funcId);

		pg_proc_tuple = (Form_pg_proc) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_proc_tuple->proowner;
		aclDatum = caql_getattr(pcqCtx, Anum_pg_proc_proacl,
								&isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 funcId, grantorId, ACL_KIND_PROC,
									 NameStr(pg_proc_tuple->proname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_proc_proacl - 1] = true;
		values[Anum_pg_proc_proacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* Update the shared dependency ACL info */
		updateAclDependencies(ProcedureRelationId, funcId,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Language(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_LANGUAGE;

	relation = heap_open(LanguageRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			langId = lfirst_oid(cell);
		Form_pg_language pg_language_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_language];
		bool		nulls[Natts_pg_language];
		bool		replaces[Natts_pg_language];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_language "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(langId)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for language %u", langId);

		pg_language_tuple = (Form_pg_language) GETSTRUCT(tuple);

		if (!pg_language_tuple->lanpltrusted)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("language \"%s\" is not trusted",
							NameStr(pg_language_tuple->lanname)),
				   errhint("Only superusers may use untrusted languages.")));

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 *
		 * Note: for now, languages are treated as owned by the bootstrap
		 * user. We should add an owner column to pg_language instead.
		 */
		ownerId = BOOTSTRAP_SUPERUSERID;
		aclDatum = caql_getattr(pcqCtx, Anum_pg_language_lanacl,
								&isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 langId, grantorId, ACL_KIND_LANGUAGE,
									 NameStr(pg_language_tuple->lanname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_language_lanacl - 1] = true;
		values[Anum_pg_language_lanacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* Update the shared dependency ACL info */
		updateAclDependencies(LanguageRelationId, HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Namespace(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_NAMESPACE;

	relation = heap_open(NamespaceRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			nspid = lfirst_oid(cell);
		Form_pg_namespace pg_namespace_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_namespace];
		bool		nulls[Natts_pg_namespace];
		bool		replaces[Natts_pg_namespace];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_namespace "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(nspid)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for namespace %u", nspid);

		pg_namespace_tuple = (Form_pg_namespace) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_namespace_tuple->nspowner;
		aclDatum = caql_getattr(pcqCtx,
								Anum_pg_namespace_nspacl,
								&isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 nspid, grantorId, ACL_KIND_NAMESPACE,
									 NameStr(pg_namespace_tuple->nspname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_namespace_nspacl - 1] = true;
		values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackUpdObject(NamespaceRelationId,
							   nspid,
							   GetUserId(), /* not grantorId, */
							   "PRIVILEGE", 
							   (istmt->is_grant) ? "GRANT" : "REVOKE"
					);

		/* Update the shared dependency ACL info */
		updateAclDependencies(NamespaceRelationId, HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Tablespace(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_TABLESPACE;

	relation = heap_open(TableSpaceRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			tblId = lfirst_oid(cell);
		Form_pg_tablespace pg_tablespace_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_tablespace];
		bool		nulls[Natts_pg_tablespace];
		bool		replaces[Natts_pg_tablespace];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		HeapTuple	tuple;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_tablespace "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(tblId)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for tablespace %u", tblId);

		pg_tablespace_tuple = (Form_pg_tablespace) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_tablespace_tuple->spcowner;
		aclDatum = heap_getattr(tuple, Anum_pg_tablespace_spcacl,
								RelationGetDescr(relation), &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 tblId, grantorId, ACL_KIND_TABLESPACE,
									 NameStr(pg_tablespace_tuple->spcname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_tablespace_spcacl - 1] = true;
		values[Anum_pg_tablespace_spcacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackUpdObject(TableSpaceRelationId,
							   tblId,
							   GetUserId(), /* not grantorId, */
							   "PRIVILEGE", 
							   (istmt->is_grant) ? "GRANT" : "REVOKE"
					);

		/* Update the shared dependency ACL info */
		updateAclDependencies(TableSpaceRelationId, tblId,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_ExtProtocol(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_EXTPROTOCOL;

	relation = heap_open(ExtprotocolRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			ptcid = lfirst_oid(cell);
		bool		isNull;
		bool		isTrusted;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		Name	    ptcname;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_extprotocol];
		bool		nulls[Natts_pg_extprotocol];
		bool		replaces[Natts_pg_extprotocol];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		Datum		ownerDatum;
		Datum		aclDatum;
		Datum		trustedDatum;
		Datum		ptcnameDatum;
		cqContext	cqc;
		cqContext  *pcqCtx;
		TupleDesc	reldsc = RelationGetDescr(relation);

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), relation),
				cql("SELECT * FROM pg_extprotocol "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(ptcid)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "lookup failed for external protocol %u", ptcid);
		
		ownerDatum = heap_getattr(tuple, 
								  Anum_pg_extprotocol_ptcowner,
								  reldsc, 
								  &isNull);
		
		if(isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("protocol '%u' has no owner defined",
							 ptcid)));	

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = DatumGetObjectId(ownerDatum);
		
		aclDatum = heap_getattr(tuple, 
								Anum_pg_extprotocol_ptcacl,
								reldsc, 
								&isNull);
		
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_EXTPROTOCOL, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		ptcnameDatum = heap_getattr(tuple, 
									Anum_pg_extprotocol_ptcname,
									reldsc, 
									&isNull);

		ptcname = DatumGetName(ptcnameDatum);
		
		if(isNull)
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: protocol '%u' has no name defined",
						 ptcid)));
		
		trustedDatum = heap_getattr(tuple, 
									Anum_pg_extprotocol_ptctrusted,
									reldsc, 
									&isNull);
			
		isTrusted = DatumGetBool(trustedDatum);
		
		if (!isTrusted)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("protocol \"%s\" is not trusted",
							NameStr(*ptcname)),
				   errhint("Only superusers may use untrusted protocols.")));

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 ptcid, grantorId, ACL_KIND_EXTPROTOCOL,
									 NameStr(*ptcname));

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_extprotocol_ptcacl - 1] = true;
		values[Anum_pg_extprotocol_ptcacl - 1] = PointerGetDatum(new_acl);

		newtuple = caql_modify_current(pcqCtx, values, nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		/* Update the shared dependency ACL info */
		updateAclDependencies(ExtprotocolRelationId,
							  HeapTupleGetOid(tuple),
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		caql_endscan(pcqCtx);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static AclMode
string_to_privilege(const char *privname)
{
	if (strcmp(privname, "insert") == 0)
		return ACL_INSERT;
	if (strcmp(privname, "select") == 0)
		return ACL_SELECT;
	if (strcmp(privname, "update") == 0)
		return ACL_UPDATE;
	if (strcmp(privname, "delete") == 0)
		return ACL_DELETE;
	if (strcmp(privname, "references") == 0)
		return ACL_REFERENCES;
	if (strcmp(privname, "trigger") == 0)
		return ACL_TRIGGER;
	if (strcmp(privname, "execute") == 0)
		return ACL_EXECUTE;
	if (strcmp(privname, "usage") == 0)
		return ACL_USAGE;
	if (strcmp(privname, "create") == 0)
		return ACL_CREATE;
	if (strcmp(privname, "temporary") == 0)
		return ACL_CREATE_TEMP;
	if (strcmp(privname, "temp") == 0)
		return ACL_CREATE_TEMP;
	if (strcmp(privname, "connect") == 0)
		return ACL_CONNECT;
	if (strcmp(privname, "rule") == 0)
		return 0;				/* ignore old RULE privileges */
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unrecognized privilege type \"%s\"", privname)));
	return 0;					/* appease compiler */
}

static const char *
privilege_to_string(AclMode privilege)
{
	switch (privilege)
	{
		case ACL_INSERT:
			return "INSERT";
		case ACL_SELECT:
			return "SELECT";
		case ACL_UPDATE:
			return "UPDATE";
		case ACL_DELETE:
			return "DELETE";
		case ACL_REFERENCES:
			return "REFERENCES";
		case ACL_TRIGGER:
			return "TRIGGER";
		case ACL_EXECUTE:
			return "EXECUTE";
		case ACL_USAGE:
			return "USAGE";
		case ACL_CREATE:
			return "CREATE";
		case ACL_CREATE_TEMP:
			return "TEMP";
		case ACL_CONNECT:
			return "CONNECT";
		default:
			elog(ERROR, "unrecognized privilege: %d", (int) privilege);
	}
	return NULL;				/* appease compiler */
}

/*
 * Standardized reporting of aclcheck permissions failures.
 *
 * Note: we do not double-quote the %s's below, because many callers
 * supply strings that might be already quoted.
 */

static const char *const no_priv_msg[MAX_ACL_KIND] =
{
	/* ACL_KIND_CLASS */
	gettext_noop("permission denied for relation %s"),
	/* ACL_KIND_SEQUENCE */
	gettext_noop("permission denied for sequence %s"),
	/* ACL_KIND_DATABASE */
	gettext_noop("permission denied for database %s"),
	/* ACL_KIND_PROC */
	gettext_noop("permission denied for function %s"),
	/* ACL_KIND_OPER */
	gettext_noop("permission denied for operator %s"),
	/* ACL_KIND_TYPE */
	gettext_noop("permission denied for type %s"),
	/* ACL_KIND_LANGUAGE */
	gettext_noop("permission denied for language %s"),
	/* ACL_KIND_NAMESPACE */
	gettext_noop("permission denied for schema %s"),
	/* ACL_KIND_OPCLASS */
	gettext_noop("permission denied for operator class %s"),
	/* ACL_KIND_CONVERSION */
	gettext_noop("permission denied for conversion %s"),
	/* ACL_KIND_TABLESPACE */
	gettext_noop("permission denied for tablespace %s"),
	/* ACL_KIND_FILESPACE */
	gettext_noop("permission denied for filespace %s"),	
	/* ACL_KIND_FILESYSTEM */
	gettext_noop("permission denied for filesystem %s"),	
	/* ACL_KIND_FDW */
	gettext_noop("permission denied for foreign-data wrapper %s"),
	/* ACL_KIND_FOREIGN_SERVER */
	gettext_noop("permission denied for foreign server %s"),
	/* ACL_KIND_EXTPROTOCOL */
	gettext_noop("permission denied for external protocol %s")	
};

static const char *const not_owner_msg[MAX_ACL_KIND] =
{
	/* ACL_KIND_CLASS */
	gettext_noop("must be owner of relation %s"),
	/* ACL_KIND_SEQUENCE */
	gettext_noop("must be owner of sequence %s"),
	/* ACL_KIND_DATABASE */
	gettext_noop("must be owner of database %s"),
	/* ACL_KIND_PROC */
	gettext_noop("must be owner of function %s"),
	/* ACL_KIND_OPER */
	gettext_noop("must be owner of operator %s"),
	/* ACL_KIND_TYPE */
	gettext_noop("must be owner of type %s"),
	/* ACL_KIND_LANGUAGE */
	gettext_noop("must be owner of language %s"),
	/* ACL_KIND_NAMESPACE */
	gettext_noop("must be owner of schema %s"),
	/* ACL_KIND_OPCLASS */
	gettext_noop("must be owner of operator class %s"),
	/* ACL_KIND_CONVERSION */
	gettext_noop("must be owner of conversion %s"),
	/* ACL_KIND_TABLESPACE */
	gettext_noop("must be owner of tablespace %s"),
	/* ACL_KIND_FILESPACE */
	gettext_noop("must be owner of filespace %s"),
	/* ACL_KIND_FILESYSTEM */
	gettext_noop("must be owner of filesystem %s"),
	/* ACL_KIND_FDW */
	gettext_noop("must be owner of foreign-data wrapper %s"),
	/* ACL_KIND_FOREIGN_SERVER */
	gettext_noop("must be owner of foreign server %s"),
	/* ACL_KIND_EXTPROTOCOL */
	gettext_noop("must be owner of external protocol %s")
};


void
aclcheck_error(AclResult aclerr, AclObjectKind objectkind,
			   const char *objectname)
{
	switch (aclerr)
	{
		case ACLCHECK_OK:
			/* no error, so return to caller */
			break;
		case ACLCHECK_NO_PRIV:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg(no_priv_msg[objectkind], objectname),
					 errOmitLocation(true)));
			break;
		case ACLCHECK_NOT_OWNER:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg(not_owner_msg[objectkind], objectname),
					 errOmitLocation(true)));
			break;
		default:
			elog(ERROR, "unrecognized AclResult: %d", (int) aclerr);
			break;
	}
}


/* Check if given user has rolcatupdate privilege according to pg_authid */
static bool
has_rolcatupdate(Oid roleid)
{
	bool		rolcatupdate;
	HeapTuple	tuple;
	cqContext  *pcqCtx;

	/* XXX XXX: SELECT rolcatupdate */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_authid "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(roleid)));
	
	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role with OID %u does not exist", roleid)));

	rolcatupdate = ((Form_pg_authid) GETSTRUCT(tuple))->rolcatupdate;

	caql_endscan(pcqCtx);

	return rolcatupdate;
}

char *getRoleName(Oid role_id)
{
  Assert(OidIsValid(role_id));
  int fetchCount;
  char* role_name = caql_getcstring_plus(
            NULL,
            &fetchCount,
            NULL,
            cql("SELECT rolname FROM pg_authid "
              " WHERE oid = :1 ",
              ObjectIdGetDatum(role_id)));

 if (role_name == NULL)
    elog(ERROR, "oid [%u] not found in table pg_authid", role_id);
  return role_name;
}

char *getClassNameFromOid(Oid object_oid)
{
  StringInfoData tname;
  initStringInfo(&tname);

  Assert(OidIsValid(object_oid));
  char* rel_name = caql_getcstring(
                   NULL,
                   cql("SELECT relname FROM pg_class "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (rel_name == NULL)
    elog(ERROR, "oid [%u] not found in table pg_class", object_oid);

  int fetchCount=0;
  Oid schema_name_oid = caql_getoid_plus(
                     NULL,
                     &fetchCount,
                     NULL,
                     cql("SELECT relnamespace FROM pg_class "
                       " WHERE oid = :1",
                       ObjectIdGetDatum(object_oid)));
  if (schema_name_oid == InvalidOid)
      elog(ERROR, "oid [%u] not found in table pg_class", object_oid);

  char* schema_name= caql_getcstring(
      NULL,
      cql("select nspname from pg_namespace "
        " WHERE oid = :1",
        ObjectIdGetDatum(schema_name_oid)));
  if (schema_name == NULL)
      elog(ERROR, "oid [%u] not found in table pg_namespace", object_oid);

  char* database_name = get_database_name(MyDatabaseId);
  if (database_name == NULL)
       elog(ERROR, "oid [%u] not found current database", object_oid);

  appendStringInfo(&tname, "%s", database_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", schema_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", rel_name);
  pfree(rel_name);
  pfree(schema_name);
  pfree(database_name);

  return tname.data;
}

char *getSequenceNameFromOid(Oid object_oid)
{
  StringInfoData tname;
  initStringInfo(&tname);

  Assert(OidIsValid(object_oid));
  char* seq_name = caql_getcstring(
                  NULL,
                  cql("SELECT relname FROM pg_class "
                    " WHERE oid = :1",
                    ObjectIdGetDatum(object_oid)));
  if (seq_name == NULL)
   elog(ERROR, "oid [%u] not found in table pg_class", object_oid);

  int fetchCount=0;
  Oid schema_name_oid = caql_getoid_plus(
                    NULL,
                    &fetchCount,
                    NULL,
                    cql("SELECT relnamespace FROM pg_class "
                      " WHERE oid = :1",
                      ObjectIdGetDatum(object_oid)));
  if (schema_name_oid == InvalidOid)
     elog(ERROR, "oid [%u] not found in table pg_class", object_oid);

  char* schema_name= caql_getcstring(
     NULL,
     cql("select nspname from pg_namespace "
       " WHERE oid = :1",
       ObjectIdGetDatum(schema_name_oid)));
  if (schema_name == NULL)
     elog(ERROR, "oid [%u] not found in table pg_namespace", object_oid);

  char* database_name = get_database_name(MyDatabaseId);
  if (database_name == NULL)
      elog(ERROR, "oid [%u] not found current database", object_oid);

  appendStringInfo(&tname, "%s", database_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", schema_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", seq_name);
  pfree(seq_name);
  pfree(schema_name);
  pfree(database_name);

  return tname.data;
}
char *getDatabaseNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* dbname = caql_getcstring(
                   NULL,
                   cql("SELECT datname FROM pg_database "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (dbname == NULL)
    elog(ERROR, "oid [%u] not found in table pg_database", object_oid);

  return dbname;
}
char *getProcNameFromOid(Oid object_oid)
{
  StringInfoData tname;
  initStringInfo(&tname);

  Assert(OidIsValid(object_oid));
  char* proc_name = caql_getcstring(
                   NULL,
                   cql("SELECT proname FROM pg_proc "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (proc_name == NULL)
    elog(ERROR, "oid [%u] not found in table pg_proc", object_oid);

  int fetchCount=0;
  Oid schema_name_oid = caql_getoid_plus(
                    NULL,
                    &fetchCount,
                    NULL,
                    cql("SELECT pronamespace FROM pg_proc "
                      " WHERE oid = :1",
                      ObjectIdGetDatum(object_oid)));
  if (schema_name_oid == InvalidOid)
     elog(ERROR, "oid [%u] not found in table pg_class", object_oid);

  char* schema_name= caql_getcstring(
     NULL,
     cql("select nspname from pg_namespace "
       " WHERE oid = :1",
       ObjectIdGetDatum(schema_name_oid)));
  if (schema_name == NULL)
     elog(ERROR, "oid [%u] not found in table pg_namespace", object_oid);

  char* database_name = get_database_name(MyDatabaseId);
  if (database_name == NULL)
      elog(ERROR, "oid [%u] not found current database", object_oid);

  appendStringInfo(&tname, "%s", database_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", schema_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", proc_name);
  pfree(proc_name);
  pfree(schema_name);
  pfree(database_name);

  return tname.data;
}
char *getOperNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT oprname FROM pg_operator "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_operator", object_oid);

  return typename;
}
char *getTypeNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT typname FROM pg_type "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_type", object_oid);

  return typename;
}
char *getLanguageNameFromOid(Oid object_oid)
{
  StringInfoData tname;
  initStringInfo(&tname);

  Assert(OidIsValid(object_oid));
  char* lang_name = caql_getcstring(
                   NULL,
                   cql("SELECT lanname FROM pg_language "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (lang_name == NULL)
    elog(ERROR, "oid [%u] not found in table pg_language", object_oid);



  char* database_name = get_database_name(MyDatabaseId);
  if (database_name == NULL)
      elog(ERROR, "oid [%u] not found current database", object_oid);

  appendStringInfo(&tname, "%s", database_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", lang_name);

  pfree(lang_name);
  pfree(database_name);

  return tname.data;
}
char *getNamespaceNameFromOid(Oid object_oid)
{

  StringInfoData tname;
  initStringInfo(&tname);

  Assert(OidIsValid(object_oid));
  char* schema_name = caql_getcstring(
                   NULL,
                   cql("SELECT nspname FROM pg_namespace "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (schema_name == NULL)
    elog(ERROR, "oid [%u] not found in table pg_namespace", object_oid);


  char* database_name = get_database_name(MyDatabaseId);
  if (database_name == NULL)
      elog(ERROR, "oid [%u] not found current database", object_oid);

  appendStringInfo(&tname, "%s", database_name);
  appendStringInfoChar(&tname, '.');
  appendStringInfo(&tname, "%s", schema_name);

  pfree(schema_name);
  pfree(database_name);

  return tname.data;
}
char *getConversionNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT conname FROM pg_conversion "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_conversion", object_oid);

  return typename;
}
char *getTablespaceNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT spcname FROM pg_tablespace "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_tablespace", object_oid);

  return typename;
}
char *getFilespaceNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT fsname FROM pg_filespace "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_filespace", object_oid);

  return typename;
}
char *getFilesystemNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT fsysname FROM pg_filesystem "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_filesystem", object_oid);

  return typename;
}
char *getFDWNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT fdwname FROM pg_foreign_data_wrapper "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_foreign_data_wrapper", object_oid);

  return typename;
}
char *getForeignServerNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT srvname FROM pg_foreign_server "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_foreign_server", object_oid);

  return typename;
}
char *getExtprotocolNameFromOid(Oid object_oid)
{
  Assert(OidIsValid(object_oid));
  char* typename = caql_getcstring(
                   NULL,
                   cql("SELECT ptcname FROM pg_extprotocol "
                     " WHERE oid = :1",
                     ObjectIdGetDatum(object_oid)));
  if (typename == NULL)
    elog(ERROR, "oid [%u] not found in table pg_extprotocol", object_oid);

  return typename;
}

char *getNameFromOid(AclObjectKind objkind, Oid object_oid)
{
  switch (objkind)
  {
    case ACL_KIND_CLASS:
      return getClassNameFromOid(object_oid);
    case ACL_KIND_SEQUENCE:
      return getSequenceNameFromOid(object_oid);
    case ACL_KIND_DATABASE:
      return getDatabaseNameFromOid(object_oid);
    case ACL_KIND_PROC:
      return getProcNameFromOid(object_oid);
    case ACL_KIND_OPER:
      return getOperNameFromOid(object_oid);
    case ACL_KIND_TYPE:
      return getTypeNameFromOid(object_oid);
    case ACL_KIND_CONVERSION:
      return getConversionNameFromOid(object_oid);
    case ACL_KIND_LANGUAGE:
      return getLanguageNameFromOid(object_oid);
    case ACL_KIND_NAMESPACE:
      return getNamespaceNameFromOid(object_oid);
    case ACL_KIND_TABLESPACE:
      return getTablespaceNameFromOid(object_oid);
    case ACL_KIND_FILESPACE:
      return getFilespaceNameFromOid(object_oid);
    case ACL_KIND_FILESYSTEM:
      return getFilesystemNameFromOid(object_oid);
    case ACL_KIND_FDW:
      return getFDWNameFromOid(object_oid);
    case ACL_KIND_FOREIGN_SERVER:
      return getForeignServerNameFromOid(object_oid);
    case ACL_KIND_EXTPROTOCOL:
      return getExtprotocolNameFromOid(object_oid);
    default:
      elog(ERROR, "unrecognized objkind: %d",
         (int) objkind);
      /* not reached, but keep compiler quiet */
      return NULL;
  }
}

char actionName[12][12] = {"INSERT","SELECT","UPDATE", "DELETE",
    "TRUNCATE", "REFERENCES", "TRIGGER", "EXECUTE", "USAGE",
    "CREATE", "CREATE_TEMP", "CONNECT"};

List *getActionName(AclMode mask)
{
  List* actions = NIL;
  int i = 0;
  while(mask > 0)
  {
    if((mask & 1) > 0)
    {
      char* action = palloc(sizeof(char) * ACTION_LENGTH);
      strncpy(action, actionName[i], ACTION_LENGTH);
      actions = lappend(actions, action);
    }
    mask = mask >> 1;
    i++;
  }
  return actions;
}


bool fallBackToNativeCheck(AclObjectKind objkind, Oid obj_oid, Oid roleid)
{
  //for heap table, we fall back to native check.
  if(objkind == ACL_KIND_CLASS)
  {
    char relstorage = get_rel_relstorage(obj_oid);
    if(relstorage == 'h')
    {
      return true;
    }
  }
  return false;
}

bool fallBackToNativeChecks(AclObjectKind objkind, List* table_list, Oid roleid)
{
  //for heap table, we fall back to native check.
  if(objkind == ACL_KIND_CLASS)
  {
    ListCell   *l;
    foreach(l, table_list)
    {
      RangeTblEntry *rte=(RangeTblEntry *) lfirst(l);
      char relstorage = get_rel_relstorage(rte->relid);
      if(relstorage == 'h')
      {
        return true;
      }
    }

  }
  return false;
}

/*
 * return: List of RangerPrivilegeResults 
 * arg_list: List of RangerPrivilegeArgs
 */
List *pg_rangercheck_batch(List *arg_list)
{
  List *aclresults = NIL;
  List *requestargs = NIL;
  ListCell *arg;
  foreach(arg, arg_list) {
    RangerPrivilegeArgs *arg_ptr = (RangerPrivilegeArgs *) lfirst(arg);
    AclObjectKind objkind = arg_ptr->objkind;
    Oid object_oid = arg_ptr->object_oid;
    char *objectname = getNameFromOid(objkind, object_oid);
    char *rolename = getRoleName(arg_ptr->roleid);
    List* actions = getActionName(arg_ptr->mask);
    bool isAll = (arg_ptr->how == ACLMASK_ALL) ? true: false;
    RangerPrivilegeResults *aclresult = (RangerPrivilegeResults *) palloc(sizeof(RangerPrivilegeResults));
    aclresult->result = -1;
    aclresult->relOid = object_oid;
    aclresults = lappend(aclresults, aclresult);

    RangerRequestJsonArgs *requestarg = (RangerRequestJsonArgs *) palloc(sizeof(RangerRequestJsonArgs));
    requestarg->user = rolename;
    requestarg->kind = objkind;
    requestarg->object = objectname;
    requestarg->actions = actions;
    requestarg->isAll = isAll;
    requestargs = lappend(requestargs, requestarg);

  } // foreach

  RangerACLResult ret = check_privilege_from_ranger(requestargs);

  ListCell *result;
  int k = 0;
  foreach(result, aclresults) {
    RangerPrivilegeResults *result_ptr = (RangerPrivilegeResults *) lfirst(result);
    result_ptr->result = ret;
    ++k;
  }

  if(requestargs) {
    ListCell   *cell = list_head(requestargs);
    while (cell != NULL)
    {
      ListCell   *tmp = cell;
      cell = lnext(cell);
      RangerRequestJsonArgs* requestarg =
          (RangerRequestJsonArgs*)lfirst(tmp);
      pfree(requestarg->user);
      pfree(requestarg->object);
      pfree(requestarg->actions);
    }

    list_free_deep(requestargs);
    requestargs = NULL;
  }

  if(ret != RANGERCHECK_OK){
    elog(ERROR, "ACL check failed\n");
  }
  elog(LOG, "oids%d\n", arg_list->length);
  return aclresults;
}

AclResult
pg_rangercheck(AclObjectKind objkind, Oid object_oid, Oid roleid,
         AclMode mask, AclMaskHow how)
{
	char* objectname = getNameFromOid(objkind, object_oid);
	char* rolename = getRoleName(roleid);
	List* actions = getActionName(mask);
	bool isAll = (how == ACLMASK_ALL) ? true: false;

	elog(LOG, "rangeraclcheck kind:%d,objectname:%s,role:%s,mask:%u\n",objkind,objectname,rolename,mask);
	List *requestargs = NIL;
	RangerRequestJsonArgs *requestarg = (RangerRequestJsonArgs *) palloc(sizeof(RangerRequestJsonArgs));
	requestarg->user = rolename;
	requestarg->kind = objkind;
	requestarg->object = objectname;
	requestarg->actions = actions;
	requestarg->isAll = isAll;
	requestargs = lappend(requestargs, requestarg);
	int ret = check_privilege_from_ranger(requestargs);

	if (requestargs)
	{
		ListCell *cell = list_head(requestargs);
		while (cell != NULL)
		{
			ListCell *tmp = cell;
			cell = lnext(cell);
			RangerRequestJsonArgs* requestarg = (RangerRequestJsonArgs*) lfirst(tmp);
			pfree(requestarg->user);
			pfree(requestarg->object);
			pfree(requestarg->actions);
		}
		list_free_deep(requestargs);
		requestargs = NULL;
	}
	return ret;
}

/*
 * Relay for the various pg_*_mask routines depending on object kind
 */
static AclMode
pg_aclmask(AclObjectKind objkind, Oid table_oid, Oid roleid,
		   AclMode mask, AclMaskHow how)
{
	switch (objkind)
	{
		case ACL_KIND_CLASS:
		case ACL_KIND_SEQUENCE:
			return pg_class_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_DATABASE:
			return pg_database_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_PROC:
			return pg_proc_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_LANGUAGE:
			return pg_language_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_NAMESPACE:
			return pg_namespace_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_TABLESPACE:
			return pg_tablespace_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_FDW:
			return pg_foreign_data_wrapper_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_FOREIGN_SERVER:
			return pg_foreign_server_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_EXTPROTOCOL:
			return pg_extprotocol_aclmask(table_oid, roleid, mask, how);
		default:
			elog(ERROR, "unrecognized objkind: %d",
				 (int) objkind);
			/* not reached, but keep compiler quiet */
			return ACL_NO_RIGHTS;
	}
}

/*
 * Exported routine for examining a user's privileges for a table
 *
 * See aclmask() for a description of the API.
 *
 * Note: we give lookup failure the full ereport treatment because the
 * has_table_privilege() family of functions allow users to pass
 * any random OID to this function.  Likewise for the sibling functions
 * below.
 */
AclMode
pg_class_aclmask(Oid table_oid, Oid roleid,
				 AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Form_pg_class classForm;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	bool		updating;
	cqContext  *pcqCtx;

	/*
	 * Must get the relation's tuple from pg_class
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(table_oid)));
	
	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist",
						table_oid)));
	classForm = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * Deny anyone permission to update a system catalog unless
	 * pg_authid.rolcatupdate is set.	(This is to let superusers protect
	 * themselves from themselves.)  Also allow it if allowSystemTableMods.
	 *
	 * As of 7.4 we have some updatable system views; those shouldn't be
	 * protected in this way.  Assume the view rules can take care of
	 * themselves.	ACL_USAGE is if we ever have system sequences.
	 */

	updating = ((mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_USAGE)) != 0);

	if (updating)
	{
		if (IsSystemClass(classForm) &&
			classForm->relkind != RELKIND_VIEW &&
			!has_rolcatupdate(roleid) &&
			!allowSystemTableModsDDL)
		{
#ifdef ACLDEBUG
			elog(DEBUG2, "permission denied for system catalog update");
#endif
			mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_USAGE);
		}

		/* 
		 * Deny even superusers with rolcatupdate permissions to modify the
		 * persistent tables.  These tables are special and rely on precise
		 * settings of the ctids within the tables, attempting to modify these
		 * tables via INSERT/UPDATE/DELETE is a mistake.
		 */
		if (GpPersistent_IsPersistentRelation(table_oid))
		{
			if ((mask & ACL_UPDATE) &&
				gp_permit_persistent_metadata_update)
			{
				// Let this UPDATE through.
			}
			else
			{
#ifdef ACLDEBUG
				elog(DEBUG2, "permission denied for persistent system catalog update");
#endif
				mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_USAGE);
			}
		}

		/*
		 * And, gp_relfile_node, too.
		 */
		if (table_oid == GpRelfileNodeRelationId)
		{
			if (gp_permit_relation_node_change)
			{
				// Let this change through.
			}
			else
			{
#ifdef ACLDEBUG
				elog(DEBUG2, "permission denied for gp_relation_node system catalog update");
#endif
				mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_USAGE);
			}
		}
	}
	/*
	 * Otherwise, superusers or on QE bypass all permission-checking.
	 */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
	{
#ifdef ACLDEBUG
		elog(DEBUG2, "OID %u is superuser, home free", roleid);
#endif
		caql_endscan(pcqCtx);
		return mask;
	}

	/*
	 * Normal case: get the relation's ACL from pg_class
	 */
	ownerId = classForm->relowner;

	aclDatum = caql_getattr(pcqCtx, Anum_pg_class_relacl,
							&isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(classForm->relkind == RELKIND_SEQUENCE ?
						 ACL_OBJECT_SEQUENCE : ACL_OBJECT_RELATION,
						 ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a database
 */
AclMode
pg_database_aclmask(Oid db_oid, Oid roleid,
					AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	/* Superusers or on QE bypass all permission checking. */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Get the database's ACL from pg_database
	 */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_database "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(db_oid)));
	
	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database with OID %u does not exist", db_oid)));

	ownerId = ((Form_pg_database) GETSTRUCT(tuple))->datdba;

	aclDatum = caql_getattr(pcqCtx, Anum_pg_database_datacl,
							&isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a function
 */
AclMode
pg_proc_aclmask(Oid proc_oid, Oid roleid,
				AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	/* Superusers or on QE bypass all permission checking. */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Get the function's ACL from pg_proc
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(proc_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function with OID %u does not exist", proc_oid)));

	ownerId = ((Form_pg_proc) GETSTRUCT(tuple))->proowner;

	aclDatum = caql_getattr(pcqCtx, Anum_pg_proc_proacl,
							&isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a language
 */
AclMode
pg_language_aclmask(Oid lang_oid, Oid roleid,
					AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	/* Superusers or on QE bypass all permission checking. */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Get the language's ACL from pg_language
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_language "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(lang_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("language with OID %u does not exist", lang_oid)));

	/* XXX pg_language should have an owner column, but doesn't */
	ownerId = BOOTSTRAP_SUPERUSERID;

	aclDatum = caql_getattr(pcqCtx, Anum_pg_language_lanacl,
							&isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a namespace
 */
AclMode
pg_namespace_aclmask(Oid nsp_oid, Oid roleid,
					 AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	/* Superusers or on QE bypass all permission checking. */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * If we have been assigned this namespace as a temp namespace, check to
	 * make sure we have CREATE TEMP permission on the database, and if so act
	 * as though we have all standard (but not GRANT OPTION) permissions on
	 * the namespace.  If we don't have CREATE TEMP, act as though we have
	 * only USAGE (and not CREATE) rights.
	 *
	 * This may seem redundant given the check in InitTempTableNamespace, but
	 * it really isn't since current user ID may have changed since then. The
	 * upshot of this behavior is that a SECURITY DEFINER function can create
	 * temp tables that can then be accessed (if permission is granted) by
	 * code in the same session that doesn't have permissions to create temp
	 * tables.
	 *
	 * XXX Would it be safe to ereport a special error message as
	 * InitTempTableNamespace does?  Returning zero here means we'll get a
	 * generic "permission denied for schema pg_temp_N" message, which is not
	 * remarkably user-friendly.
	 */
	if (isTempNamespace(nsp_oid))
	{
		if (pg_database_aclcheck(MyDatabaseId, roleid,
								 ACL_CREATE_TEMP) == ACLCHECK_OK)
			return mask & ACL_ALL_RIGHTS_NAMESPACE;
		else
			return mask & ACL_USAGE;
	}

	/*
	 * Get the schema's ACL from pg_namespace
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_namespace "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(nsp_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

	aclDatum = caql_getattr(pcqCtx, Anum_pg_namespace_nspacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a tablespace
 */
AclMode
pg_tablespace_aclmask(Oid spc_oid, Oid roleid,
					  AclMode mask, AclMaskHow how)
{
	AclMode		result;
	Relation	pg_tablespace;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/*
	 * Only shared relations can be stored in global space; don't let even
	 * superusers override this, except during bootstrap and upgrade.
	 */
	if (spc_oid == GLOBALTABLESPACE_OID && !(IsBootstrapProcessingMode()||gp_upgrade_mode))
		return 0;

	/* Otherwise, superusers or on QE bypass all permission checking. */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Get the tablespace's ACL from pg_tablespace
	 *
	 * There's no syscache for pg_tablespace, so must look the hard way
	 */
	pg_tablespace = heap_open(TableSpaceRelationId, AccessShareLock);

	/* XXX XXX select spcowner, spcacl */

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_tablespace), 
			cql("SELECT * FROM pg_tablespace "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(spc_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist", spc_oid)));

	ownerId = ((Form_pg_tablespace) GETSTRUCT(tuple))->spcowner;

	aclDatum = heap_getattr(tuple, Anum_pg_tablespace_spcacl,
							RelationGetDescr(pg_tablespace), &isNull);

	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);
	heap_close(pg_tablespace, AccessShareLock);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * data wrapper
 */
AclMode
pg_foreign_data_wrapper_aclmask(Oid fdw_oid, Oid roleid,
								AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	Form_pg_foreign_data_wrapper fdwForm;

	/* Bypass permission checks for superusers or on QE */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Must get the FDW's tuple from pg_foreign_data_wrapper
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_foreign_data_wrapper "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(fdw_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errmsg("foreign-data wrapper with OID %u does not exist",
						fdw_oid)));
	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(tuple);

	/*
	 * Normal case: get the FDW's ACL from pg_foreign_data_wrapper
	 */
	ownerId = fdwForm->fdwowner;

	aclDatum = caql_getattr(pcqCtx,
							Anum_pg_foreign_data_wrapper_fdwacl, &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FDW, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * server.
 */
AclMode
pg_foreign_server_aclmask(Oid srv_oid, Oid roleid,
						  AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	cqContext  *pcqCtx;

	Form_pg_foreign_server srvForm;

	/* Bypass permission checks for superusers or on QE */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;

	/*
	 * Must get the FDW's tuple from pg_foreign_data_wrapper
	 */
	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_foreign_server "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(srv_oid)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errmsg("foreign server with OID %u does not exist",
						srv_oid)));
	srvForm = (Form_pg_foreign_server) GETSTRUCT(tuple);

	/*
	 * Normal case: get the foreign server's ACL from pg_foreign_server
	 */
	ownerId = srvForm->srvowner;

	aclDatum = caql_getattr(pcqCtx,
							Anum_pg_foreign_server_srvacl, &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	caql_endscan(pcqCtx);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * server.
 */
AclMode
pg_extprotocol_aclmask(Oid ptcOid, Oid roleid,
					   AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	Datum		ownerDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	Relation	rel;
	cqContext	cqc;
	cqContext  *pcqCtx;

	/* Bypass permission checks for superusers or on QE */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;
	
	rel = heap_open(ExtprotocolRelationId, AccessShareLock);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel), 
			cql("SELECT * FROM pg_extprotocol "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(ptcOid)));

	tuple = caql_getnext(pcqCtx);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "protocol %u could not be found", ptcOid);

	ownerDatum = heap_getattr(tuple, 
							  Anum_pg_extprotocol_ptcowner, 
							  RelationGetDescr(rel),
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid extprotocol owner value: NULL")));	

	ownerId = DatumGetObjectId(ownerDatum);

	aclDatum = heap_getattr(tuple, 
							Anum_pg_extprotocol_ptcacl, 
							RelationGetDescr(rel),
						    &isNull);
	
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_EXTPROTOCOL, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	/* Finish up scan and close pg_extprotocol catalog. */
	caql_endscan(pcqCtx);	
	heap_close(rel, AccessShareLock);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a file
 * system.
 */
AclMode
pg_filesystem_aclmask(Oid fsysOid, Oid roleid,
					   AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	Datum		ownerDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;
	Relation	rel;
	HeapScanDesc scandesc;
	ScanKeyData entry[1];


	/* Bypass permission checks for superusers or on QE */
	if (GP_ROLE_EXECUTE == Gp_role || superuser_arg(roleid))
		return mask;
	
	/*
	 * Search pg_filesystem.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filesystem will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSystemRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fsysOid));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "filesystem %u could not be found", fsysOid);

	ownerDatum = heap_getattr(tuple, 
							  Anum_pg_filesystem_fsysowner, 
							  rel->rd_att, 
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid filesystem owner value: NULL")));	

	ownerId = DatumGetObjectId(ownerDatum);

	aclDatum = heap_getattr(tuple, 
							Anum_pg_filesystem_fsysacl, 
							rel->rd_att, 
						    &isNull);
	
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FILESYSTEM, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	/* Finish up scan and close pg_filesystem catalog. */
	heap_endscan(scandesc);
	
	heap_close(rel, AccessShareLock);

	return result;
}


/*
 * Exported routine for checking a user's access privileges to a table
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 */
AclResult
pg_class_nativecheck(Oid table_oid, Oid roleid, AclMode mode)
{
  if (pg_class_aclmask(table_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a database
 */
AclResult
pg_database_nativecheck(Oid db_oid, Oid roleid, AclMode mode)
{
  if (pg_database_aclmask(db_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a function
 */
AclResult
pg_proc_nativecheck(Oid proc_oid, Oid roleid, AclMode mode)
{
  if (pg_proc_aclmask(proc_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a language
 */
AclResult
pg_language_nativecheck(Oid lang_oid, Oid roleid, AclMode mode)
{
  if (pg_language_aclmask(lang_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a namespace
 */
AclResult
pg_namespace_nativecheck(Oid nsp_oid, Oid roleid, AclMode mode)
{
  if (pg_namespace_aclmask(nsp_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a tablespace
 */
AclResult
pg_tablespace_nativecheck(Oid spc_oid, Oid roleid, AclMode mode)
{
  if (pg_tablespace_aclmask(spc_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * data wrapper
 */
AclResult
pg_foreign_data_wrapper_nativecheck(Oid fdw_oid, Oid roleid, AclMode mode)
{
  if (pg_foreign_data_wrapper_aclmask(fdw_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * server
 */
AclResult
pg_foreign_server_nativecheck(Oid srv_oid, Oid roleid, AclMode mode)
{
  if (pg_foreign_server_aclmask(srv_oid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to an
 * external protocol
 */
AclResult
pg_extprotocol_nativecheck(Oid ptcid, Oid roleid, AclMode mode)
{
  if (pg_extprotocol_aclmask(ptcid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a filesystem
 */
AclResult
pg_filesystem_nativecheck(Oid fsysid, Oid roleid, AclMode mode)
{
  if (pg_filesystem_aclmask(fsysid, roleid, mode, ACLMASK_ANY) != 0)
    return ACLCHECK_OK;
  else
    return ACLCHECK_NO_PRIV;
}


/*
 * Exported routine for checking a user's access privileges to a table
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 */
AclResult
pg_class_aclcheck(Oid table_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_CLASS, table_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_CLASS, table_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_class_nativecheck(table_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a database
 */
AclResult
pg_database_aclcheck(Oid db_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_DATABASE, db_oid, roleid))
   {
     return pg_rangercheck(ACL_KIND_DATABASE, db_oid, roleid, mode, ACLMASK_ANY);
   }
   else
   {
     return pg_database_nativecheck(db_oid, roleid, mode);
   }
}

/*
 * Exported routine for checking a user's access privileges to a function
 */
AclResult
pg_proc_aclcheck(Oid proc_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_PROC, proc_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_PROC, proc_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_proc_nativecheck(proc_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a language
 */
AclResult
pg_language_aclcheck(Oid lang_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_LANGUAGE, lang_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_LANGUAGE, lang_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_language_nativecheck(lang_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a namespace
 */
AclResult
pg_namespace_aclcheck(Oid nsp_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_NAMESPACE, nsp_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_NAMESPACE, nsp_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_namespace_nativecheck(nsp_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a tablespace
 */
AclResult
pg_tablespace_aclcheck(Oid spc_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_TABLESPACE, spc_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_TABLESPACE, spc_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_tablespace_nativecheck(spc_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * data wrapper
 */
AclResult
pg_foreign_data_wrapper_aclcheck(Oid fdw_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_FDW, fdw_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_FDW, fdw_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_foreign_data_wrapper_nativecheck(fdw_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * server
 */
AclResult
pg_foreign_server_aclcheck(Oid srv_oid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_FOREIGN_SERVER, srv_oid, roleid))
  {
    return pg_rangercheck(ACL_KIND_FOREIGN_SERVER, srv_oid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_foreign_server_nativecheck(srv_oid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to an
 * external protocol
 */
AclResult
pg_extprotocol_aclcheck(Oid ptcid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_EXTPROTOCOL, ptcid, roleid))
  {
    return pg_rangercheck(ACL_KIND_EXTPROTOCOL, ptcid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_extprotocol_nativecheck(ptcid, roleid, mode);
  }
}

/*
 * Exported routine for checking a user's access privileges to a filesystem
 */
AclResult
pg_filesystem_aclcheck(Oid fsysid, Oid roleid, AclMode mode)
{
  if(enable_ranger && !fallBackToNativeCheck(ACL_KIND_FILESYSTEM, fsysid, roleid))
  {
    return pg_rangercheck(ACL_KIND_FILESYSTEM, fsysid, roleid, mode, ACLMASK_ANY);
  }
  else
  {
    return pg_filesystem_nativecheck(fsysid, roleid, mode);
  }
}

/*
 * Ownership check for a relation (specified by OID).
 */
bool
pg_class_ownercheck(Oid class_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT relowner FROM pg_class WHERE oid = :1 ",
				ObjectIdGetDatum(class_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", class_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a type (specified by OID).
 */
bool
pg_type_ownercheck(Oid type_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT typowner FROM pg_type WHERE oid = :1 ",
				ObjectIdGetDatum(type_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type with OID %u does not exist", type_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator (specified by OID).
 */
bool
pg_oper_ownercheck(Oid oper_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT oprowner FROM pg_operator WHERE oid = :1 ",
				ObjectIdGetDatum(oper_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator with OID %u does not exist", oper_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a function (specified by OID).
 */
bool
pg_proc_ownercheck(Oid proc_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT proowner FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(proc_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function with OID %u does not exist", proc_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a namespace (specified by OID).
 */
bool
pg_namespace_ownercheck(Oid nsp_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT nspowner FROM pg_namespace WHERE oid = :1 ",
				ObjectIdGetDatum(nsp_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a tablespace (specified by OID).
 */
bool
pg_tablespace_ownercheck(Oid spc_oid, Oid roleid)
{
	Oid			spcowner;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	spcowner = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT spcowner FROM pg_tablespace WHERE oid = :1 ",
				ObjectIdGetDatum(spc_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist", spc_oid)));

	return has_privs_of_role(roleid, spcowner);
}

/*
 * Ownership check for a filespace (specified by OID).
 */
bool
pg_filespace_ownercheck(Oid fsoid, Oid roleid)
{
	Oid			owner;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	owner = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT fsowner FROM pg_filespace WHERE oid = :1 ",
				ObjectIdGetDatum(fsoid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("filepace with OID %u does not exist", fsoid)));

	return has_privs_of_role(roleid, owner);
}


/*
 * Ownership check for a filesystem (specified by OID).
 */
bool
pg_filesystem_ownercheck(Oid fsysOid, Oid roleid)
{
	Relation	rel;
	HeapTuple	tuple;
	Oid			ownerId;
	Datum		ownerDatum;
	bool		isNull;
	HeapScanDesc scandesc;
	ScanKeyData entry[1];

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	/*
	 * Search pg_filesystem.  We use a heapscan here even though there is an
	 * index on oid, on the theory that pg_filesystem will usually have just a
	 * few entries and so an indexed lookup is a waste of effort.
	 */
	rel = heap_open(FileSystemRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(fsysOid));
	scandesc = heap_beginscan(rel, SnapshotNow, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	/* We assume that there can be at most one matching tuple */
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "filesystem %u could not be found", fsysOid);

	ownerDatum = heap_getattr(tuple, 
							  Anum_pg_filesystem_fsysowner,
							  rel->rd_att, 
							  &isNull);

	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid filesystem owner value: NULL")));	

	ownerId = DatumGetObjectId(ownerDatum);

	/* Finish up scan and close pg_filesystem catalog. */
	heap_endscan(scandesc);
	
	heap_close(rel, AccessShareLock);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator class (specified by OID).
 */
bool
pg_opclass_ownercheck(Oid opc_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT opcowner FROM pg_opclass WHERE oid = :1 ",
				ObjectIdGetDatum(opc_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator class with OID %u does not exist",
						opc_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a foreign server (specified by OID).
 */
bool
pg_foreign_server_ownercheck(Oid srv_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT srvowner FROM pg_foreign_server WHERE oid = :1 ",
				ObjectIdGetDatum(srv_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign server with OID %u does not exist",
						srv_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a database (specified by OID).
 */
bool
pg_database_ownercheck(Oid db_oid, Oid roleid)
{
	Oid			dba;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	dba = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT datdba FROM pg_database WHERE oid = :1 ",
				ObjectIdGetDatum(db_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database with OID %u does not exist", db_oid)));

	return has_privs_of_role(roleid, dba);
}

/*
 * Ownership check for a conversion (specified by OID).
 */
bool
pg_conversion_ownercheck(Oid conv_oid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			NULL,
			cql("SELECT conowner FROM pg_conversion WHERE oid = :1 ",
				ObjectIdGetDatum(conv_oid)));

	if (0 == fetchCount)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("conversion with OID %u does not exist", conv_oid)));

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a foreign server (specified by OID).
 */
bool
pg_extprotocol_ownercheck(Oid protOid, Oid roleid)
{
	Oid			ownerId;
	int			fetchCount = 0;
	bool		isNull;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	ownerId = caql_getoid_plus(
			NULL,
			&fetchCount,
			&isNull,
			cql("SELECT ptcowner FROM pg_extprotocol WHERE oid = :1 ",
				ObjectIdGetDatum(protOid)));

	/* We assume that there can be at most one matching tuple */
	if (0 == fetchCount)
		elog(ERROR, "protocol %u could not be found", protOid);

	/* XXX XXX: this column not null (?) */
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid extprotocol owner value: NULL")));	

	return has_privs_of_role(roleid, ownerId);
}
