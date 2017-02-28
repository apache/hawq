/*-------------------------------------------------------------------------
 *
 * namespace.c
 *	  code to support accessing and searching namespaces
 *
 * This is separate from pg_namespace.c, which contains the routines that
 * directly manipulate the pg_namespace system catalog.  This module
 * provides routines associated with defining a "namespace search path"
 * and implementing search-path-controlled searches.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/namespace.c,v 1.88.2.1 2007/04/20 02:37:48 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/xact.h"
#include "access/hd_work_mgr.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/schemacmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "cdb/cdbvars.h"
#include "tcop/utility.h"


/*
 * The namespace search path is a possibly-empty list of namespace OIDs.
 * In addition to the explicit list, several implicitly-searched namespaces
 * may be included:
 *
 * 1. If a "special" namespace has been set by PushSpecialNamespace, it is
 * always searched first.  (This is a hack for CREATE SCHEMA.)
 *
 * 2. If a TEMP table namespace has been initialized in this session, it
 * is always searched just after any special namespace.
 *
 * 3. The system catalog namespace is always searched.	If the system
 * namespace is present in the explicit path then it will be searched in
 * the specified order; otherwise it will be searched after TEMP tables and
 * *before* the explicit list.	(It might seem that the system namespace
 * should be implicitly last, but this behavior appears to be required by
 * SQL99.  Also, this provides a way to search the system namespace first
 * without thereby making it the default creation target namespace.)
 *
 * For security reasons, searches using the search path will ignore the temp
 * namespace when searching for any object type other than relations and
 * types.  (We must allow types since temp tables have rowtypes.)
 *
 * The default creation target namespace is normally equal to the first
 * element of the explicit list, but is the "special" namespace when one
 * has been set.  If the explicit list is empty and there is no special
 * namespace, there is no default target.
 *
 * The textual specification of search_path can include "$user" to refer to
 * the namespace named the same as the current user, if any.  (This is just
 * ignored if there is no such namespace.)  Also, it can include "pg_temp"
 * to refer to the current backend's temp namespace.  This is usually also
 * ignorable if the temp namespace hasn't been set up, but there's a special
 * case: if "pg_temp" appears first then it should be the default creation
 * target.  We kluge this case a little bit so that the temp namespace isn't
 * set up until the first attempt to create something in it.  (The reason for
 * klugery is that we can't create the temp namespace outside a transaction,
 * but initial GUC processing of search_path happens outside a transaction.)
 * tempCreationPending is TRUE if "pg_temp" appears first in the string but
 * is not reflected in defaultCreationNamespace because the namespace isn't
 * set up yet.
 *
 * In bootstrap mode, the search path is set equal to "pg_catalog", so that
 * the system namespace is the only one searched or inserted into.
 * The initdb script is also careful to set search_path to "pg_catalog" for
 * its post-bootstrap standalone backend runs.	Otherwise the default search
 * path is determined by GUC.  The factory default path contains the PUBLIC
 * namespace (if it exists), preceded by the user's personal namespace
 * (if one exists).
 *
 * If namespaceSearchPathValid is false, then namespaceSearchPath (and other
 * derived variables) need to be recomputed from namespace_search_path.
 * We mark it invalid upon an assignment to namespace_search_path or receipt
 * of a syscache invalidation event for pg_namespace.  The recomputation
 * is done during the next lookup attempt.
 *
 * Any namespaces mentioned in namespace_search_path that are not readable
 * by the current user ID are simply left out of namespaceSearchPath; so
 * we have to be willing to recompute the path when current userid changes.
 * namespaceUser is the userid the path has been computed for.
 */

extern const char *debug_query_string;

static List *namespaceSearchPath = NIL;

static Oid	namespaceUser = InvalidOid;

/* default place to create stuff; if InvalidOid, no default */
static Oid	defaultCreationNamespace = InvalidOid;

/* first explicit member of list; usually same as defaultCreationNamespace */
static Oid	firstExplicitNamespace = InvalidOid;

/* if TRUE, defaultCreationNamespace is wrong, it should be temp namespace */
static bool tempCreationPending = false;

/* The above five values are valid only if namespaceSearchPathValid */
static bool namespaceSearchPathValid = true;

/* store the query sign on the last call of recomputeNamespacePath(), and used the sign to judge cache invalidation */
static uint32 last_query_sign = 0;

/*
 * myTempNamespace is InvalidOid until and unless a TEMP namespace is set up
 * in a particular backend session (this happens when a CREATE TEMP TABLE
 * command is first executed).	Thereafter it's the OID of the temp namespace.
 *
 * myTempNamespaceSubID shows whether we've created the TEMP namespace in the
 * current subtransaction.	The flag propagates up the subtransaction tree,
 * so the main transaction will correctly recognize the flag if all
 * intermediate subtransactions commit.  When it is InvalidSubTransactionId,
 * we either haven't made the TEMP namespace yet, or have successfully
 * committed its creation, depending on whether myTempNamespace is valid.
 */
static Oid	myTempNamespace = InvalidOid;

static SubTransactionId myTempNamespaceSubID = InvalidSubTransactionId;

/*
 * "Special" namespace for CREATE SCHEMA.  If set, it's the first search
 * path element, and also the default creation namespace.
 */
static Oid	mySpecialNamespace = InvalidOid;

/*
 * This is the text equivalent of the search path --- it's the value
 * of the GUC variable 'search_path'.
 */
char	   *namespace_search_path = NULL;


/* Local functions */
static void recomputeNamespacePath(void);
static void checkNamespaceInternal(List **oidlist, Oid namespaceId, Oid roleid);
static void InitTempTableNamespace(void);
static void RemoveTempRelations(Oid tempNamespaceId);
static void RemoveTempRelationsCallback(int code, Datum arg);
static void NamespaceCallback(Datum arg __attribute__((unused)) , Oid relid __attribute__((unused)) );
static bool TempNamespaceValid(bool error_if_removed);
static bool RelationExists(const RangeVar *relation, Oid dboid);

/* These don't really need to appear in any header file */
Datum		pg_table_is_visible(PG_FUNCTION_ARGS);
Datum		pg_type_is_visible(PG_FUNCTION_ARGS);
Datum		pg_function_is_visible(PG_FUNCTION_ARGS);
Datum		pg_operator_is_visible(PG_FUNCTION_ARGS);
Datum		pg_opclass_is_visible(PG_FUNCTION_ARGS);
Datum		pg_conversion_is_visible(PG_FUNCTION_ARGS);
Datum		pg_my_temp_schema(PG_FUNCTION_ARGS);
Datum		pg_is_other_temp_schema(PG_FUNCTION_ARGS);
Datum       pg_objname_to_oid(PG_FUNCTION_ARGS);

void
reset_query_sign()
{
	last_query_sign = 0;
}

/*
 * GetCatalogId
 *	Given a catalogname, return 0 if the current database is specified, or the oid from pg_database, if hcatalog is specified. 
 *	Error out if a catalog name is different from the current one or hcatalog
 */
Oid
GetCatalogId(const char *catalogname)
{
	if (NULL == catalogname || 0 == strcmp(catalogname, get_database_name(MyDatabaseId))) 
	{
		return NSPDBOID_CURRENT;
	}

	if (0 == strcmp(catalogname, "hcatalog"))
	{
		return HcatalogDbOid;
	}	

	ereport(ERROR,	(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cross-database references are not implemented: \"%s\"",
							catalogname)));
	return 0;
}

/*
 * RelationExists
 *		Given a RangeVar describing an existing relation,
 *		check if the relation exists in the pg_namespace and pg_class tables
 *
 */
bool RelationExists(const RangeVar *relation, Oid dboid)
{
	Assert(relation != NULL);
	Oid namespaceId = LookupExplicitNamespace(relation->schemaname, dboid);
	Oid relId = get_relname_relid(relation->relname, namespaceId);
	return OidIsValid(namespaceId) && OidIsValid(relId);
}

/*
 * RangeVarGetRelid
 *		Given a RangeVar describing an existing relation,
 *		select the proper namespace and look up the relation OID.
 *
 * If the relation is not found, return InvalidOid if failOK = true,
 * otherwise raise an error.
 */
Oid
RangeVarGetRelid(const RangeVar *relation, bool failOK, bool allowHcatalog)
{
	Oid			namespaceId;
	Oid			relId;

	/* check if database name is specified: the only allowed options are the current
	 * database name and the external "hcatalog" specifier
	 */
	Oid dboid = GetCatalogId(relation->catalogname);
	if (HcatalogDbOid == dboid)
	{
		if (!allowHcatalog)
		{
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("reference to hcatalog table \"%s.%s.%s\" is not allowed in this context",
						relation->catalogname, relation->schemaname,
						relation->relname)));
		}

		/* Pull relation metadata via the hcat proxy(pxf) only if relation doesn't exist*/
		if(!RelationExists(relation, dboid))
		{
			StringInfoData location;
			initStringInfo(&location);

			appendStringInfo(&location, "%s.%s", relation->schemaname, relation->relname);

			// TODO: May 29, 2015 - shivram: revisit returning the hcat tables here
			List *hcat_tables = get_pxf_item_metadata(HiveProfileName, location.data, HcatalogDbOid);
			Assert(hcat_tables != NIL);
			elog(DEBUG2, "Retrieved %d tables from HCatalog for \"%s.%s\"", 
				list_length(hcat_tables), relation->schemaname, relation->relname);
			pfree(location.data);
		}
	}
	
	if (relation->schemaname)
	{
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(relation->schemaname, dboid);
		relId = get_relname_relid(relation->relname, namespaceId);
	}
	else
	{
		/* external references must be fully specified */
		Assert(NSPDBOID_CURRENT == dboid);
		
		/* search the namespace path */
		relId = RelnameGetRelid(relation->relname);
	}

	if (!OidIsValid(relId) && !failOK)
	{
		if (relation->schemaname)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							relation->schemaname, relation->relname),
					 errOmitLocation(true)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s\" does not exist",
							relation->relname),
					 errOmitLocation(true)));
		}
	}
	return relId;
}

/*
 * RangeVarGetCreationNamespace
 *		Given a RangeVar describing a to-be-created relation,
 *		choose which namespace to create it in.
 *
 * Note: calling this may result in a CommandCounterIncrement operation.
 * That will happen on the first request for a temp table in any particular
 * backend run; we will need to either create or clean out the temp schema.
 */
Oid
RangeVarGetCreationNamespace(const RangeVar *newRelation)
{
	Oid			namespaceId;

	/*
	 * We check the catalog name and then ignore it.
	 */
	if (NULL != newRelation->catalogname)
	{
		if (strcmp(newRelation->catalogname, "hcatalog") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("creating hcatalog tables is not supported: \"%s.%s.%s\"",
							newRelation->catalogname, newRelation->schemaname,
							newRelation->relname)));
		}
		
		if (strcmp(newRelation->catalogname, get_database_name(MyDatabaseId)) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cross-database references are not implemented: \"%s.%s.%s\"",
							newRelation->catalogname, newRelation->schemaname,
							newRelation->relname)));
		}
	}

	if (newRelation->istemp)
	{
		/* TEMP tables are created in our backend-local temp namespace */
		if (Gp_role != GP_ROLE_EXECUTE && newRelation->schemaname)
		{
			char		namespaceName[NAMEDATALEN];
			snprintf(namespaceName, sizeof(namespaceName), "pg_temp_%d", gp_session_id);
			if (strcmp(newRelation->schemaname,namespaceName)!=0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					  errmsg("temporary tables may not specify a schema name"),
							   errOmitLocation(true)));
		}
		/* Initialize temp namespace if first time through */
		if (!TempNamespaceValid(false))
			InitTempTableNamespace();
		return myTempNamespace;
	}

	if (newRelation->schemaname)
	{
		/* check for pg_temp alias */
		if (strcmp(newRelation->schemaname, "pg_temp") == 0)
		{
			/* Initialize temp namespace if first time through */
			if (!TempNamespaceValid(false))
				InitTempTableNamespace();
			return myTempNamespace;
		}
		/* use exact schema given */
		namespaceId = LookupInternalNamespaceId(newRelation->schemaname);

		if (!OidIsValid(namespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("schema \"%s\" does not exist",
							newRelation->schemaname),
									   errOmitLocation(true)));
		/* we do not check for USAGE rights here! */
	}
	else
	{
		if (gp_upgrade_mode)
		{
            namespaceId = PG_CATALOG_NAMESPACE;
		}
		else
		{
			/* use the default creation namespace */
			recomputeNamespacePath();
			if (tempCreationPending)
			{
				/* Need to initialize temp namespace */
				InitTempTableNamespace();
				return myTempNamespace;
			}
			namespaceId = defaultCreationNamespace;
		}

		if (!OidIsValid(namespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("no schema has been selected to create in"),
							   errOmitLocation(true)));
	}

	/* Note: callers will check for CREATE rights when appropriate */

	return namespaceId;
}

/*
 * RelnameGetRelid
 *		Try to resolve an unqualified relation name.
 *		Returns OID if relation found in search path, else InvalidOid.
 */
Oid
RelnameGetRelid(const char *relname)
{
	Oid			relid;
	ListCell   *l;

	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);

		relid = get_relname_relid(relname, namespaceId);
		if (OidIsValid(relid))
			return relid;
	}

	/* Not found in path */
	return InvalidOid;
}


/*
 * RelationIsVisible
 *		Determine whether a relation (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified relation name".
 */
bool
RelationIsVisible(Oid relid)
{
	HeapTuple	reltup;
	Form_pg_class relform;
	Oid			relnamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_class "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(relid)));

	reltup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(reltup))
	{
		/* 
		 * MPP-6982:
		 * Note that the caller may not have gotten a lock on the relation.
		 * Therefore, it is possible that the relation may have been dropped
		 * by the time this method is called. Therefore, we simply return false
		 * when we cannot find the relation in syscache instead of erroring out.
		 */
		return false;
	}

	relform = (Form_pg_class) GETSTRUCT(reltup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	relnamespace = relform->relnamespace;
	if (relnamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, relnamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another relation of the same name earlier in the path. So
		 * we must do a slow check for conflicting relations.
		 */
		char	   *relname = NameStr(relform->relname);
		ListCell   *l;

		visible = false;
		foreach(l, namespaceSearchPath)
		{
			Oid			namespaceId = lfirst_oid(l);

			if (namespaceId == relnamespace)
			{
				/* Found it first in path */
				visible = true;
				break;
			}
			if (OidIsValid(get_relname_relid(relname, namespaceId)))
			{
				/* Found something else first in path */
				break;
			}
		}
	}

	caql_endscan(pcqCtx);

	return visible;
}


/*
 * TypenameGetTypid
 *		Try to resolve an unqualified datatype name.
 *		Returns OID if type found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
Oid
TypenameGetTypid(const char *typname)
{
	Oid			typid;
	ListCell   *l;

	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);

		typid = caql_getoid(
				NULL,
				cql("SELECT oid FROM pg_type "
					" WHERE typname = :1 "
					" AND typnamespace = :2 ",
					PointerGetDatum((char *) typname),
					ObjectIdGetDatum(namespaceId)));

		if (OidIsValid(typid))
			return typid;
	}

	/* Not found in path */
	return InvalidOid;
}

/*
 * TypeOidGetTypename
 * Get the name of the type, given the OID
 */
char*
TypeOidGetTypename(Oid typeid)
{
	StringInfoData tname;
	initStringInfo(&tname);
	
	Assert(OidIsValid(typeid));	
	char* typename = caql_getcstring(
									 NULL,
									 cql("SELECT typname FROM pg_type "
										 " WHERE oid = :1",
										 ObjectIdGetDatum(typeid)));
	if (typename == NULL)
		elog(ERROR, "oid [%u] not found in table pg_type", typeid);
	
	appendStringInfo(&tname, "%s", typename);
	return tname.data;
}

/*
 * TypeIsVisible
 *		Determine whether a type (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified type name".
 */
bool
TypeIsVisible(Oid typid)
{
	HeapTuple	typtup;
	Form_pg_type typform;
	Oid			typnamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_type "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(typid)));

	typtup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", typid);
	typform = (Form_pg_type) GETSTRUCT(typtup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	typnamespace = typform->typnamespace;
	if (typnamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, typnamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another type of the same name earlier in the path. So we
		 * must do a slow check for conflicting types.
		 */
		char	   *typname = NameStr(typform->typname);
		ListCell   *l;

		visible = false;
		foreach(l, namespaceSearchPath)
		{
			Oid			namespaceId = lfirst_oid(l);

			if (namespaceId == typnamespace)
			{
				/* Found it first in path */
				visible = true;
				break;
			}
			
			if (caql_getcount(
						NULL,
						cql("SELECT COUNT(*) FROM pg_type "
							" WHERE typname = :1 "
							" AND typnamespace = :2 ",
							PointerGetDatum((char *) typname),
							ObjectIdGetDatum(namespaceId))))
			{
				/* Found something else first in path */
				break;
			}
		}
	}

	caql_endscan(pcqCtx);

	return visible;
}


/*
 * FuncnameGetCandidates
 *		Given a possibly-qualified function name and argument count,
 *		retrieve a list of the possible matches.
 *
 * If nargs is -1, we return all functions matching the given name,
 * regardless of argument count.
 *
 * We search a single namespace if the function name is qualified, else
 * all namespaces in the search path.  The return list will never contain
 * multiple entries with identical argument lists --- in the multiple-
 * namespace case, we arrange for entries in earlier namespaces to mask
 * identical entries in later namespaces.
 */
FuncCandidateList
FuncnameGetCandidates(List *names, int nargs)
{
	FuncCandidateList resultList = NULL;
	char	   *schemaname;
	char	   *funcname;
	Oid			namespaceId;
	CatCList   *catlist;
	int			i;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &funcname);

	if (schemaname)
	{
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);
	}
	else
	{
		/* flag to indicate we need namespace search */
		namespaceId = InvalidOid;
		recomputeNamespacePath();
	}

	/* Search syscache by name only */

	catlist = caql_begin_CacheList(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE proname = :1 "
				" ORDER BY proname, "
				" proargtypes, "
				" pronamespace ",
				CStringGetDatum(funcname)));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	proctup = &catlist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);
		int			pronargs = procform->pronargs;
		int			pathpos = 0;
		FuncCandidateList newResult;

		/* Ignore if it doesn't match requested argument count */
		if (nargs >= 0 && pronargs != nargs)
			continue;

		if (OidIsValid(namespaceId))
		{
			/* Consider only procs in specified namespace */
			if (procform->pronamespace != namespaceId)
				continue;
			/* No need to check args, they must all be different */
		}
		else
		{
			/*
			 * Consider only procs that are in the search path and are not
			 * in the temp namespace.
			 */
			ListCell   *nsp;

			foreach(nsp, namespaceSearchPath)
			{
				if (procform->pronamespace == lfirst_oid(nsp) &&
					procform->pronamespace != myTempNamespace)
					break;
				pathpos++;
			}
			if (nsp == NULL)
				continue;		/* proc is not in search path */

			/*
			 * Okay, it's in the search path, but does it have the same
			 * arguments as something we already accepted?	If so, keep only
			 * the one that appears earlier in the search path.
			 *
			 * If we have an ordered list from caql_begin_CacheList (the normal
			 * case), then any conflicting proc must immediately adjoin this
			 * one in the list, so we only need to look at the newest result
			 * item.  If we have an unordered list, we have to scan the whole
			 * result list.
			 */
			if (resultList)
			{
				FuncCandidateList prevResult;

				if (catlist->ordered)
				{
					if (pronargs == resultList->nargs &&
						memcmp(procform->proargtypes.values,
							   resultList->args,
							   pronargs * sizeof(Oid)) == 0)
						prevResult = resultList;
					else
						prevResult = NULL;
				}
				else
				{
					for (prevResult = resultList;
						 prevResult;
						 prevResult = prevResult->next)
					{
						if (pronargs == prevResult->nargs &&
							memcmp(procform->proargtypes.values,
								   prevResult->args,
								   pronargs * sizeof(Oid)) == 0)
							break;
					}
				}
				if (prevResult)
				{
					/* We have a match with a previous result */
					Assert(pathpos != prevResult->pathpos);
					if (pathpos > prevResult->pathpos)
						continue;		/* keep previous result */
					/* replace previous result */
					prevResult->pathpos = pathpos;
					prevResult->oid = HeapTupleGetOid(proctup);
					continue;	/* args are same, of course */
				}
			}
		}

		/*
		 * Okay to add it to result list
		 */
		newResult = (FuncCandidateList)
			palloc(sizeof(struct _FuncCandidateList) - sizeof(Oid)
				   + pronargs * sizeof(Oid));
		newResult->pathpos = pathpos;
		newResult->oid = HeapTupleGetOid(proctup);
		newResult->nargs = pronargs;
		memcpy(newResult->args, procform->proargtypes.values,
			   pronargs * sizeof(Oid));

		newResult->next = resultList;
		resultList = newResult;
	}

	caql_end_CacheList(catlist);

	return resultList;
}

/*
 * FunctionIsVisible
 *		Determine whether a function (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified function name with exact argument matches".
 */
bool
FunctionIsVisible(Oid funcid)
{
	HeapTuple	proctup;
	Form_pg_proc procform;
	Oid			pronamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(funcid)));

	proctup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	pronamespace = procform->pronamespace;
	if (pronamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, pronamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another proc of the same name and arguments earlier in
		 * the path.  So we must do a slow check to see if this is the same
		 * proc that would be found by FuncnameGetCandidates.
		 */
		char	   *proname = NameStr(procform->proname);
		int			nargs = procform->pronargs;
		FuncCandidateList clist;

		visible = false;

		clist = FuncnameGetCandidates(list_make1(makeString(proname)), nargs);

		for (; clist; clist = clist->next)
		{
			if (memcmp(clist->args, procform->proargtypes.values,
					   nargs * sizeof(Oid)) == 0)
			{
				/* Found the expected entry; is it the right proc? */
				visible = (clist->oid == funcid);
				break;
			}
		}
	}

	caql_endscan(pcqCtx);

	return visible;
}


/*
 * OpernameGetOprid
 *		Given a possibly-qualified operator name and exact input datatypes,
 *		look up the operator.  Returns InvalidOid if not found.
 *
 * Pass oprleft = InvalidOid for a prefix op, oprright = InvalidOid for
 * a postfix op.
 *
 * If the operator name is not schema-qualified, it is sought in the current
 * namespace search path.
 */
Oid
OpernameGetOprid(List *names, Oid oprleft, Oid oprright)
{
	char	   *schemaname;
	char	   *opername;
	CatCList   *catlist;
	ListCell   *l;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &opername);

	if (schemaname)
	{
		/* search only in exact schema given */
		Oid			namespaceId;
		Oid			operoid;

		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);

		operoid = caql_getoid(
				NULL,
				cql("SELECT oid FROM pg_operator "
					" WHERE oprname = :1 "
					" AND oprleft = :2 "
					" AND oprright = :3 "
					" AND oprnamespace = :4 ",
					CStringGetDatum(opername),
					ObjectIdGetDatum(oprleft),
					ObjectIdGetDatum(oprright),
					ObjectIdGetDatum(namespaceId)));

		return operoid;
	}

	/* Search syscache by name and argument types */
	catlist = caql_begin_CacheList(
			NULL,
			cql("SELECT * FROM pg_operator "
				" WHERE oprname = :1 "
				" AND oprleft = :2 "
				" AND oprright = :3 "
				" ORDER BY oprname, "
				" oprleft, "
				" oprright, "
				" oprnamespace ",
				CStringGetDatum(opername),
				ObjectIdGetDatum(oprleft),
				ObjectIdGetDatum(oprright)));

	if (catlist->n_members == 0)
	{
		/* no hope, fall out early */
		caql_end_CacheList(catlist);
		return InvalidOid;
	}

	/*
	 * We have to find the list member that is first in the search path, if
	 * there's more than one.  This doubly-nested loop looks ugly, but in
	 * practice there should usually be few catlist members.
	 */
	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);
		int			i;

		if (namespaceId == myTempNamespace)
			continue;			/* do not look in temp namespace */

		for (i = 0; i < catlist->n_members; i++)
		{
			HeapTuple	opertup = &catlist->members[i]->tuple;
			Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);

			if (operform->oprnamespace == namespaceId)
			{
				Oid			result = HeapTupleGetOid(opertup);

				caql_end_CacheList(catlist);
				return result;
			}
		}
	}

	caql_end_CacheList(catlist);
	return InvalidOid;
}

/*
 * OpernameGetCandidates
 *		Given a possibly-qualified operator name and operator kind,
 *		retrieve a list of the possible matches.
 *
 * If oprkind is '\0', we return all operators matching the given name,
 * regardless of arguments.
 *
 * We search a single namespace if the operator name is qualified, else
 * all namespaces in the search path.  The return list will never contain
 * multiple entries with identical argument lists --- in the multiple-
 * namespace case, we arrange for entries in earlier namespaces to mask
 * identical entries in later namespaces.
 *
 * The returned items always have two args[] entries --- one or the other
 * will be InvalidOid for a prefix or postfix oprkind.	nargs is 2, too.
 */
FuncCandidateList
OpernameGetCandidates(List *names, char oprkind)
{
	FuncCandidateList resultList = NULL;
	char	   *resultSpace = NULL;
	int			nextResult = 0;
	char	   *schemaname;
	char	   *opername;
	Oid			namespaceId;
	CatCList   *catlist;
	int			i;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &opername);

	if (schemaname)
	{
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);
	}
	else
	{
		/* flag to indicate we need namespace search */
		namespaceId = InvalidOid;
		recomputeNamespacePath();
	}

	/* Search syscache by name only */
	catlist = caql_begin_CacheList(
			NULL, 
			cql("SELECT * FROM pg_operator "
				" WHERE oprname = :1 "
				" ORDER BY oprname, "
				" oprleft, "
				" oprright, "
				" oprnamespace ",
				CStringGetDatum(opername)));

	/*
	 * In typical scenarios, most if not all of the operators found by the
	 * catcache search will end up getting returned; and there can be quite a
	 * few, for common operator names such as '=' or '+'.  To reduce the time
	 * spent in palloc, we allocate the result space as an array large enough
	 * to hold all the operators.  The original coding of this routine did a
	 * separate palloc for each operator, but profiling revealed that the
	 * pallocs used an unreasonably large fraction of parsing time.
	 */
#define SPACE_PER_OP MAXALIGN(sizeof(struct _FuncCandidateList) + sizeof(Oid))

	if (catlist->n_members > 0)
		resultSpace = palloc(catlist->n_members * SPACE_PER_OP);

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	opertup = &catlist->members[i]->tuple;
		Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
		int			pathpos = 0;
		FuncCandidateList newResult;

		/* Ignore operators of wrong kind, if specific kind requested */
		if (oprkind && operform->oprkind != oprkind)
			continue;

		if (OidIsValid(namespaceId))
		{
			/* Consider only opers in specified namespace */
			if (operform->oprnamespace != namespaceId)
				continue;
			/* No need to check args, they must all be different */
		}
		else
		{
			/*
			 * Consider only opers that are in the search path and are not
			 * in the temp namespace.
			 */
			ListCell   *nsp;

			foreach(nsp, namespaceSearchPath)
			{
				if (operform->oprnamespace == lfirst_oid(nsp) &&
					operform->oprnamespace != myTempNamespace)
					break;
				pathpos++;
			}
			if (nsp == NULL)
				continue;		/* oper is not in search path */

			/*
			 * Okay, it's in the search path, but does it have the same
			 * arguments as something we already accepted?	If so, keep only
			 * the one that appears earlier in the search path.
			 *
			 * If we have an ordered list from caql_begin_CacheList (the normal
			 * case), then any conflicting oper must immediately adjoin this
			 * one in the list, so we only need to look at the newest result
			 * item.  If we have an unordered list, we have to scan the whole
			 * result list.
			 */
			if (resultList)
			{
				FuncCandidateList prevResult;

				if (catlist->ordered)
				{
					if (operform->oprleft == resultList->args[0] &&
						operform->oprright == resultList->args[1])
						prevResult = resultList;
					else
						prevResult = NULL;
				}
				else
				{
					for (prevResult = resultList;
						 prevResult;
						 prevResult = prevResult->next)
					{
						if (operform->oprleft == prevResult->args[0] &&
							operform->oprright == prevResult->args[1])
							break;
					}
				}
				if (prevResult)
				{
					/* We have a match with a previous result */
					Assert(pathpos != prevResult->pathpos);
					if (pathpos > prevResult->pathpos)
						continue;		/* keep previous result */
					/* replace previous result */
					prevResult->pathpos = pathpos;
					prevResult->oid = HeapTupleGetOid(opertup);
					continue;	/* args are same, of course */
				}
			}
		}

		/*
		 * Okay to add it to result list
		 */
		newResult = (FuncCandidateList) (resultSpace + nextResult);
		nextResult += SPACE_PER_OP;

		newResult->pathpos = pathpos;
		newResult->oid = HeapTupleGetOid(opertup);
		newResult->nargs = 2;
		newResult->args[0] = operform->oprleft;
		newResult->args[1] = operform->oprright;
		newResult->next = resultList;
		resultList = newResult;
	}

	caql_end_CacheList(catlist);

	return resultList;
}

/*
 * OperatorIsVisible
 *		Determine whether an operator (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified operator name with exact argument matches".
 */
bool
OperatorIsVisible(Oid oprid)
{
	HeapTuple	oprtup;
	Form_pg_operator oprform;
	Oid			oprnamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_operator "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(oprid)));

	oprtup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(oprtup))
		elog(ERROR, "cache lookup failed for operator %u", oprid);
	oprform = (Form_pg_operator) GETSTRUCT(oprtup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	oprnamespace = oprform->oprnamespace;
	if (oprnamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, oprnamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another operator of the same name and arguments earlier
		 * in the path.  So we must do a slow check to see if this is the same
		 * operator that would be found by OpernameGetOprId.
		 */
		char	   *oprname = NameStr(oprform->oprname);

		visible = (OpernameGetOprid(list_make1(makeString(oprname)),
									oprform->oprleft, oprform->oprright)
				   == oprid);
	}

	caql_endscan(pcqCtx);

	return visible;
}


/*
 * OpclassnameGetOpcid
 *		Try to resolve an unqualified index opclass name.
 *		Returns OID if opclass found in search path, else InvalidOid.
 *
 * This is essentially the same as TypenameGetTypid, but we have to have
 * an extra argument for the index AM OID.
 */
Oid
OpclassnameGetOpcid(Oid amid, const char *opcname)
{
	Oid			opcid;
	ListCell   *l;

	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);

		if (namespaceId == myTempNamespace)
			continue;			/* do not look in temp namespace */

		opcid = caql_getoid(
				NULL,
				cql("SELECT oid FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(amid),
					PointerGetDatum((char *) opcname),
					ObjectIdGetDatum(namespaceId)));

		if (OidIsValid(opcid))
			return opcid;
	}

	/* Not found in path */
	return InvalidOid;
}

/*
 * OpclassIsVisible
 *		Determine whether an opclass (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified opclass name".
 */
bool
OpclassIsVisible(Oid opcid)
{
	HeapTuple	opctup;
	Form_pg_opclass opcform;
	Oid			opcnamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_opclass "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(opcid)));

	opctup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(opctup))
		elog(ERROR, "cache lookup failed for opclass %u", opcid);
	opcform = (Form_pg_opclass) GETSTRUCT(opctup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	opcnamespace = opcform->opcnamespace;
	if (opcnamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, opcnamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another opclass of the same name earlier in the path. So
		 * we must do a slow check to see if this opclass would be found by
		 * OpclassnameGetOpcid.
		 */
		char	   *opcname = NameStr(opcform->opcname);

		visible = (OpclassnameGetOpcid(opcform->opcamid, opcname) == opcid);
	}

	caql_endscan(pcqCtx);

	return visible;
}

/*
 * ConversionGetConid
 *		Try to resolve an unqualified conversion name.
 *		Returns OID if conversion found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
Oid
ConversionGetConid(const char *conname)
{
	Oid			conid;
	ListCell   *l;

	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);

		if (namespaceId == myTempNamespace)
			continue;			/* do not look in temp namespace */

		conid = caql_getoid(
				NULL,
				cql("SELECT oid FROM pg_conversion "
					" WHERE conname = :1 "
					" AND connamespace = :2 ",
					PointerGetDatum((char *) conname),
					ObjectIdGetDatum(namespaceId)));

		if (OidIsValid(conid))
			return conid;
	}

	/* Not found in path */
	return InvalidOid;
}

/*
 * ConversionIsVisible
 *		Determine whether a conversion (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified conversion name".
 */
bool
ConversionIsVisible(Oid conid)
{
	HeapTuple	contup;
	Form_pg_conversion conform;
	Oid			connamespace;
	bool		visible;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_conversion "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(conid)));

	contup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(contup))
		elog(ERROR, "cache lookup failed for conversion %u", conid);
	conform = (Form_pg_conversion) GETSTRUCT(contup);

	recomputeNamespacePath();

	/*
	 * Quick check: if it ain't in the path at all, it ain't visible. Items in
	 * the system namespace are surely in the path and so we needn't even do
	 * list_member_oid() for them.
	 */
	connamespace = conform->connamespace;
	if (connamespace != PG_CATALOG_NAMESPACE &&
		!list_member_oid(namespaceSearchPath, connamespace))
		visible = false;
	else
	{
		/*
		 * If it is in the path, it might still not be visible; it could be
		 * hidden by another conversion of the same name earlier in the path.
		 * So we must do a slow check to see if this conversion would be found
		 * by ConversionGetConid.
		 */
		char	   *conname = NameStr(conform->conname);

		visible = (ConversionGetConid(conname) == conid);
	}

	caql_endscan(pcqCtx);

	return visible;
}

/*
 * DeconstructQualifiedName
 *		Given a possibly-qualified name expressed as a list of String nodes,
 *		extract the schema name and object name.
 *
 * *nspname_p is set to NULL if there is no explicit schema name.
 */
void
DeconstructQualifiedName(List *names,
						 char **nspname_p,
						 char **objname_p)
{
	char	   *catalogname;
	char	   *schemaname = NULL;
	char	   *objname = NULL;

	switch (list_length(names))
	{
		case 1:
			objname = strVal(linitial(names));
			break;
		case 2:
			schemaname = strVal(linitial(names));
			objname = strVal(lsecond(names));
			break;
		case 3:
			catalogname = strVal(linitial(names));
			schemaname = strVal(lsecond(names));
			objname = strVal(lthird(names));

			/*
			 * We check the catalog name and then ignore it.
			 */
			if (strcmp(catalogname, get_database_name(MyDatabaseId)) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("cross-database references are not implemented: %s",
						 NameListToString(names))));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("improper qualified name (too many dotted names): %s",
					   NameListToString(names))));
			break;
	}

	*nspname_p = schemaname;
	*objname_p = objname;
}


/*
 * LookupInternalNamespaceId
 *		Look up oid of the specified internal schema .
 *
 *		Returns the namespace OID or InvalidOid if not found.
 */
Oid
LookupInternalNamespaceId(const char *nspname)
{
	return LookupNamespaceId(nspname, NSPDBOID_CURRENT);
}

/*
 * LookupNamespaceId
 *		Process an explicitly-specified schema name and database oid: 
 *		look up the schema id from the catalog
 *
 *		Returns the namespace OID or InvalidOid if not found.
 */
Oid
LookupNamespaceId(const char *nspname, Oid dboid)
{

    /* check for pg_temp alias */
    if (NSPDBOID_CURRENT == dboid && strcmp(nspname, "pg_temp") == 0)
    {
        if (TempNamespaceValid(true))
            return myTempNamespace;
        /*
         * Since this is used only for looking up existing objects, there
         * is no point in trying to initialize the temp namespace here;
         * and doing so might create problems for some callers.
         * Just fall through and give the "does not exist" error.
         */
    }

    if(gp_upgrade_mode)
    {
        // This code is only need at hawq2.0 upgrade, in this case the old pg_namespace doesn't 
        // have column nspdboid, so we report error in else clause
        if(ObjectIdGetDatum(dboid)==NSPDBOID_CURRENT)
        {
            return caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_namespace "
				" WHERE nspname = :1",
				CStringGetDatum((char *) nspname)));
        }
        else
        	elog(ERROR, "Upgrade cannot process the namespace: %s in dbid: %i", nspname, dboid);
    }
	return caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_namespace "
				" WHERE nspname = :1 and nspdboid = :2",
				CStringGetDatum((char *) nspname), ObjectIdGetDatum(dboid)));
}

/*
 * LookupExplicitNamespace
 *		Process an explicitly-specified schema name: look up the schema
 *		and verify we have USAGE (lookup) rights in it.
 *
 * Returns the namespace OID.  Raises ereport if any problem.
 */
Oid
LookupExplicitNamespace(const char *nspname, Oid dboid)
{
	Assert(NSPDBOID_CURRENT == dboid || HcatalogDbOid == dboid);
	
	Oid			namespaceId;
	AclResult	aclresult;

	namespaceId = LookupNamespaceId(nspname, dboid);

	if (!OidIsValid(namespaceId))
	{
		if (HcatalogDbOid == dboid)
		{
			/* Return invalid namespaceId if in hcatalog */
			return namespaceId;
		}
		StringInfoData qualifiedSchemaName;
		initStringInfo(&qualifiedSchemaName);

		appendStringInfoString(&qualifiedSchemaName, nspname);
		
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", qualifiedSchemaName.data),
				 errOmitLocation(true)));
	}

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   nspname);

	return namespaceId;
}

/*
 * LookupCreationNamespace
 *		Look up the schema and verify we have CREATE rights on it.
 *
 * This is just like LookupExplicitNamespace except for the permission check,
 * and that we are willing to create pg_temp if needed.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
Oid
LookupCreationNamespace(const char *nspname)
{
	Oid			namespaceId;
	AclResult	aclresult;

	/* check for pg_temp alias */
	if (strcmp(nspname, "pg_temp") == 0)
	{
		/* Initialize temp namespace if first time through */
		if (!TempNamespaceValid(false))
			InitTempTableNamespace();
		return myTempNamespace;
	}

	namespaceId = LookupInternalNamespaceId((char *) nspname);

	if (!OidIsValid(namespaceId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", nspname),
				 errOmitLocation(true)));

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   nspname);

	return namespaceId;
}

/*
 * QualifiedNameGetCreationNamespace
 *		Given a possibly-qualified name for an object (in List-of-Values
 *		format), determine what namespace the object should be created in.
 *		Also extract and return the object name (last component of list).
 *
 * Note: this does not apply any permissions check.  Callers must check
 * for CREATE rights on the selected namespace when appropriate.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
Oid
QualifiedNameGetCreationNamespace(List *names, char **objname_p)
{
	char	   *schemaname;
	Oid			namespaceId;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, objname_p);

	if (schemaname)
	{
		/* check for pg_temp alias */
		if (strcmp(schemaname, "pg_temp") == 0)
		{
			/* Initialize temp namespace if first time through */
			if (!TempNamespaceValid(false))
				InitTempTableNamespace();
			return myTempNamespace;
		}
		/* use exact schema given */
		namespaceId = LookupInternalNamespaceId(schemaname);

		if (!OidIsValid(namespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("schema \"%s\" does not exist", schemaname),
				     errOmitLocation(true)));
		/* we do not check for USAGE rights here! */
	}
	else
	{
		/* use the default creation namespace */
		recomputeNamespacePath();
		if (tempCreationPending)
		{
			/* Need to initialize temp namespace */
			InitTempTableNamespace();
			return myTempNamespace;
		}
		namespaceId = defaultCreationNamespace;
		if (!OidIsValid(namespaceId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("no schema has been selected to create in")));
	}

	return namespaceId;
}

/*
 * makeRangeVarFromNameList
 *		Utility routine to convert a qualified-name list into RangeVar form.
 */
RangeVar *
makeRangeVarFromNameList(List *names)
{
	RangeVar   *rel = makeRangeVar(NULL, NULL, NULL, -1);

	switch (list_length(names))
	{
		case 1:
			rel->relname = strVal(linitial(names));
			break;
		case 2:
			rel->schemaname = strVal(linitial(names));
			rel->relname = strVal(lsecond(names));
			break;
		case 3:
			rel->catalogname = strVal(linitial(names));
			rel->schemaname = strVal(lsecond(names));
			rel->relname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("improper relation name (too many dotted names): %s",
						NameListToString(names))));
			break;
	}

	return rel;
}

/*
 * NameListToString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * This is used primarily to form error messages, and so we do not quote
 * the list elements, for the sake of legibility.
 */
char *
NameListToString(List *names)
{
	StringInfoData string;
	ListCell   *l;

	initStringInfo(&string);

	foreach(l, names)
	{
		if (l != list_head(names))
			appendStringInfoChar(&string, '.');
		appendStringInfoString(&string, strVal(lfirst(l)));
	}

	return string.data;
}

/*
 * NameListToQuotedString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * Same as above except that names will be double-quoted where necessary,
 * so the string could be re-parsed (eg, by textToQualifiedNameList).
 */
char *
NameListToQuotedString(List *names)
{
	StringInfoData string;
	ListCell   *l;

	initStringInfo(&string);

	foreach(l, names)
	{
		if (l != list_head(names))
			appendStringInfoChar(&string, '.');
		appendStringInfoString(&string, quote_identifier(strVal(lfirst(l))));
	}

	return string.data;
}

/*
 * isTempNamespace - is the given namespace my temporary-table namespace?
 */
bool
isTempNamespace(Oid namespaceId)
{
	/* 
	 * We know these namespaces aren't temporary. We need this bootstrapping to
	 * avoid complex situations where we're actively trying to rebuild
	 * pg_namespace's catalog cache but continue to recurse because
	 * TempNamespaceValid() wants to rebuild the catalog cache for us. Chicken
	 * and egg...
	 */
	if (IsBuiltInNameSpace(namespaceId))
		return false;

	if (TempNamespaceValid(false) && myTempNamespace == namespaceId)
		return true;

	return false;
}

/*
 * isAnyTempNamespace - is the given namespace a temporary-table namespace
 * (either my own, or another backend's)?
 */
bool
isAnyTempNamespace(Oid namespaceId)
{
	bool		result;
	char	   *nspname;

	/* Metadata tracking: don't check at bootstrap (before
	 * pg_namespace is loaded 
	 */
	if (IsBootstrapProcessingMode())
		return false;

	/* If the namespace name starts with "pg_temp_", say "true" */
	nspname = get_namespace_name(namespaceId);
	if (!nspname)
		return false;			/* no such namespace? */
	result = (strncmp(nspname, "pg_temp_", 8) == 0);
	pfree(nspname);
	return result;
}

/*
 * isOtherTempNamespace - is the given namespace some other backend's
 * temporary-table namespace?
 */
bool
isOtherTempNamespace(Oid namespaceId)
{
	/* If it's my own temp namespace, say "false" */
	if (isTempNamespace(namespaceId))
		return false;
	/* Else, if the namespace name starts with "pg_temp_", say "true" */
	return isAnyTempNamespace(namespaceId);
}

/*
 * PushSpecialNamespace - push a "special" namespace onto the front of the
 * search path.
 *
 * This is a slightly messy hack intended only for support of CREATE SCHEMA.
 * Although the API is defined to allow a stack of pushed namespaces, we
 * presently only support one at a time.
 *
 * The pushed namespace will be removed from the search path at end of
 * transaction, whether commit or abort.
 */
void
PushSpecialNamespace(Oid namespaceId)
{
	Assert(!OidIsValid(mySpecialNamespace));
	mySpecialNamespace = namespaceId;
	namespaceSearchPathValid = false;
}

/*
 * PopSpecialNamespace - remove previously pushed special namespace.
 */
void
PopSpecialNamespace(Oid namespaceId)
{
	Assert(mySpecialNamespace == namespaceId);
	mySpecialNamespace = InvalidOid;
	namespaceSearchPathValid = false;
}

/*
 * FindConversionByName - find a conversion by possibly qualified name
 */
Oid
FindConversionByName(List *name)
{
	char	   *schemaname;
	char	   *conversion_name;
	Oid			namespaceId;
	Oid			conoid;
	ListCell   *l;

	/* deconstruct the name list */
	DeconstructQualifiedName(name, &schemaname, &conversion_name);

	if (schemaname)
	{
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(schemaname, NSPDBOID_CURRENT);
		return FindConversion(conversion_name, namespaceId);
	}
	else
	{
		/* search for it in search path */
		recomputeNamespacePath();

		foreach(l, namespaceSearchPath)
		{
			namespaceId = lfirst_oid(l);

			if (namespaceId == myTempNamespace)
				continue;			/* do not look in temp namespace */

			conoid = FindConversion(conversion_name, namespaceId);
			if (OidIsValid(conoid))
				return conoid;
		}
	}

	/* Not found in path */
	return InvalidOid;
}

/*
 * FindDefaultConversionProc - find default encoding conversion proc
 */
Oid
FindDefaultConversionProc(int4 for_encoding, int4 to_encoding)
{
	Oid			proc;
	ListCell   *l;

	recomputeNamespacePath();

	foreach(l, namespaceSearchPath)
	{
		Oid			namespaceId = lfirst_oid(l);

		if (namespaceId == myTempNamespace)
			continue;			/* do not look in temp namespace */

		proc = FindDefaultConversion(namespaceId, for_encoding, to_encoding);
		if (OidIsValid(proc))
			return proc;
	}

	/* Not found in path */
	return InvalidOid;
}



/*
 *
 */
static void checkNamespaceInternal(List **oidlist, Oid namespaceId, Oid roleid)
{
	if (pg_namespace_aclcheck(namespaceId, roleid,
			ACL_USAGE) == ACLCHECK_OK)
	{
		*oidlist = lappend_oid(*oidlist, namespaceId);
	}
	else {
		if (OidIsValid(namespaceId) && aclType == HAWQ_ACL_RANGER) {
			elog(WARNING, "usage privilege of namespace %s is required.",
					getNamespaceNameByOid(namespaceId));
		}
	}
}
/*
 * recomputeNamespacePath - recompute path derived variables if needed.
 */
static void
recomputeNamespacePath(void)
{
	Oid			roleid = GetUserId();
	char	   *rawname;
	List	   *namelist;
	List	   *oidlist;
	List	   *newpath;
	ListCell   *l;
	bool		temp_missing;
	Oid			firstNS;
	MemoryContext oldcxt;

	/*
	 * Do nothing if path is already valid.
	 */
	if (namespaceSearchPathValid && namespaceUser == roleid)
	{
		if (aclType != HAWQ_ACL_RANGER)
		{
			return;
		}
		else
		{
			uint32 current_query_sign = 0;
			if (debug_query_string != NULL)
				current_query_sign = string_hash(debug_query_string, strlen(debug_query_string));

			if (current_query_sign == last_query_sign)
				return;
			last_query_sign = current_query_sign;
			elog(DEBUG3, "recompute search_path[%s] when acl_type is ranger", namespace_search_path);
		}
	}
	else 
	{
		if (aclType == HAWQ_ACL_RANGER && debug_query_string != NULL)
		{
			last_query_sign = string_hash(debug_query_string, strlen(debug_query_string));
		}
	}

	/* Need a modifiable copy of namespace_search_path string */
	rawname = pstrdup(namespace_search_path);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawname, ',', &namelist))
	{
		/* syntax error in name list */
		/* this should not happen if GUC checked check_search_path */
		elog(ERROR, "invalid list syntax");
	}

	/*
	 * Convert the list of names to a list of OIDs.  If any names are not
	 * recognizable or we don't have read access, just leave them out of the
	 * list.  (We can't raise an error, since the search_path setting has
	 * already been accepted.)	Don't make duplicate entries, either.
	 */
	oidlist = NIL;
	temp_missing = false;
	foreach(l, namelist)
	{
		char	   *curname = (char *) lfirst(l);
		Oid			namespaceId;

		if (strcmp(curname, "$user") == 0)
		{
			/* $user --- substitute namespace matching user name, if any */
			char	   *rname = NULL;
			int			fetchCount;

			rname = caql_getcstring_plus(
					NULL,
					&fetchCount,
					NULL,
					cql("SELECT rolname FROM pg_authid "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(roleid)));

			if (fetchCount)
			{
				namespaceId = LookupInternalNamespaceId(rname);

				if (OidIsValid(namespaceId) &&
						!list_member_oid(oidlist, namespaceId))
				{
					checkNamespaceInternal(&oidlist, namespaceId, roleid);
				}
			}
		}
		else if (strcmp(curname, "pg_temp") == 0)
		{
			/* pg_temp --- substitute temp namespace, if any */

			if (TempNamespaceValid(true))
			{
				if (!list_member_oid(oidlist, myTempNamespace))
					oidlist = lappend_oid(oidlist, myTempNamespace);
			}
			else
			{
				/* If it ought to be the creation namespace, set flag */
				if (oidlist == NIL)
					temp_missing = true;
			}
		}
		else
		{
			/* normal namespace reference */
			namespaceId = LookupInternalNamespaceId(curname);

			if (OidIsValid(namespaceId) &&
				!list_member_oid(oidlist, namespaceId))
			{
				checkNamespaceInternal(&oidlist, namespaceId, roleid);
			}
		}
	}

	/*
	 * Remember the first member of the explicit list.  (Note: this is
	 * nominally wrong if temp_missing, but we need it anyway to distinguish
	 * explicit from implicit mention of pg_catalog.)
	 */
	if (oidlist == NIL)
		firstNS = InvalidOid;
	else
		firstNS = linitial_oid(oidlist);

	/*
	 * Add any implicitly-searched namespaces to the list.	Note these go on
	 * the front, not the back; also notice that we do not check USAGE
	 * permissions for these.
	 */
	if (!list_member_oid(oidlist, PG_CATALOG_NAMESPACE))
		oidlist = lcons_oid(PG_CATALOG_NAMESPACE, oidlist);

	if (TempNamespaceValid(false) &&
		!list_member_oid(oidlist, myTempNamespace))
		oidlist = lcons_oid(myTempNamespace, oidlist);

	if (OidIsValid(mySpecialNamespace) &&
		!list_member_oid(oidlist, mySpecialNamespace))
		oidlist = lcons_oid(mySpecialNamespace, oidlist);

	/*
	 * Now that we've successfully built the new list of namespace OIDs, save
	 * it in permanent storage.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	newpath = list_copy(oidlist);
	MemoryContextSwitchTo(oldcxt);

	/* Now safe to assign to state variable. */
	list_free(namespaceSearchPath);
	namespaceSearchPath = newpath;

	/*
	 * Update info derived from search path.
	 */
	firstExplicitNamespace = firstNS;
	if (OidIsValid(mySpecialNamespace))
	{
		defaultCreationNamespace = mySpecialNamespace;
		/* don't have to create temp in this state */
		tempCreationPending = false;
	}
	else
	{
		defaultCreationNamespace = firstNS;
		tempCreationPending = temp_missing;
	}

	/* Mark the path valid. */
	namespaceSearchPathValid = true;
	namespaceUser = roleid;

	/* Clean up. */
	pfree(rawname);
	list_free(namelist);
	list_free(oidlist);
}

/*
 * InitTempTableNamespace
 *		Initialize temp table namespace on first use in a particular backend
 */
static void
InitTempTableNamespace(void)
{
	char				 namespaceName[NAMEDATALEN];
	int					 fetchCount;
	char				*rolname;
	CreateSchemaStmt   	*stmt;

	Assert(!OidIsValid(myTempNamespace));

	/*
	 * First, do permission check to see if we are authorized to make temp
	 * tables.	We use a nonstandard error message here since "databasename:
	 * permission denied" might be a tad cryptic.
	 *
	 * Note that ACL_CREATE_TEMP rights are rechecked in pg_namespace_aclmask;
	 * that's necessary since current user ID could change during the session.
	 * But there's no need to make the namespace in the first place until a
	 * temp table creation request is made by someone with appropriate rights.
	 */
	if (pg_database_aclcheck(MyDatabaseId, GetUserId(),
							 ACL_CREATE_TEMP) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create temporary tables in database \"%s\"",
						get_database_name(MyDatabaseId))));

	/* 
	 * TempNamespace name creation rules are different depending on the
	 * nature of the current connection role.
	 */
	switch (Gp_role)
	{
		case GP_ROLE_DISPATCH:
			snprintf(namespaceName, sizeof(namespaceName), "pg_temp_%d", 
					 gp_session_id);
			break;

		case GP_ROLE_UTILITY:
			snprintf(namespaceName, sizeof(namespaceName), "pg_temp_%d", 
					 MyBackendId);
			break;

		default:
			/* Should never hit this */
			elog(ERROR, "invalid backend temp schema creation");
			break;
	}

	/*
	 * First use of this temp namespace in this database; create it. The
	 * temp namespaces are always owned by the superuser.  We leave their
	 * permissions at default --- i.e., no access except to superuser ---
	 * to ensure that unprivileged users can't peek at other backends'
	 * temp tables.  This works because the places that access the temp
	 * namespace for my own backend skip permissions checks on it.
	 */

	/* 
	 * CDB: Dispatch CREATE SCHEMA command.
	 *
	 * We need to keep the OID of temp schemas synchronized across the
	 * cluster which means that we must go through regular dispatch
	 * logic rather than letting every backend manage the 
	 */
		
	/* Lookup the name of the superuser */

	rolname = caql_getcstring_plus(
					NULL,
					&fetchCount,
					NULL,
					cql("SELECT rolname FROM pg_authid "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID)));

	Assert(fetchCount);  /* bootstrap user MUST exist */

	/* Execute the internal DDL */
	stmt = makeNode(CreateSchemaStmt);
	stmt->schemaname = namespaceName;
	stmt->istemp	 = true;
	stmt->authid	 = rolname;
	ProcessUtility((Node*) stmt, "(internal create temp schema command)",
				   NULL, false, None_Receiver, NULL);
}

/*
 * Drop temp relations for session reset.
 */
void
DropTempTableNamespaceForResetSession(Oid namespaceOid)
{
  if (IsTransactionOrTransactionBlock())
    elog(ERROR, "Called within a transation");

  StartTransactionCommand();

  RemoveTempRelations(namespaceOid);

  CommitTransactionCommand();
}

/*
 * Called by CreateSchemaCommand when creating a temporary schema 
 */
void
SetTempNamespace(Oid namespaceOid)
{
	if (TempNamespaceValid(false))
		elog(ERROR, "temporary namespace already exists");

	/*
	 * Okay, we've prepared the temp namespace ... but it's not committed yet,
	 * so all our work could be undone by transaction rollback.  Set flag for
	 * AtEOXact_Namespace to know what to do.
	 */
	myTempNamespace = namespaceOid;

	/* It should not be done already. */
	AssertState(myTempNamespaceSubID == InvalidSubTransactionId);
	myTempNamespaceSubID = GetCurrentSubTransactionId();

	namespaceSearchPathValid = false;	/* need to rebuild list */
}

/*
 * End-of-transaction cleanup for namespaces.
 */
void
AtEOXact_Namespace(bool isCommit)
{
	/*
	 * If we abort the transaction in which a temp namespace was selected,
	 * we'll have to do any creation or cleanout work over again.  So, just
	 * forget the namespace entirely until next time.  On the other hand, if
	 * we commit then register an exit callback to clean out the temp tables
	 * at backend shutdown.  (We only want to register the callback once per
	 * session, so this is a good place to do it.)
	 */
	if (myTempNamespaceSubID != InvalidSubTransactionId)
	{
		if (isCommit)
			on_shmem_exit(RemoveTempRelationsCallback, 0);
		else
		{
			myTempNamespace = InvalidOid;
			namespaceSearchPathValid = false;	/* need to rebuild list */
		}
		myTempNamespaceSubID = InvalidSubTransactionId;
	}

	/*
	 * Clean up if someone failed to do PopSpecialNamespace
	 */
	if (OidIsValid(mySpecialNamespace))
	{
		mySpecialNamespace = InvalidOid;
		namespaceSearchPathValid = false;		/* need to rebuild list */
	}
}

/*
 * AtEOSubXact_Namespace
 *
 * At subtransaction commit, propagate the temp-namespace-creation
 * flag to the parent subtransaction.
 *
 * At subtransaction abort, forget the flag if we set it up.
 */
void
AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid,
					  SubTransactionId parentSubid)
{

    if (Gp_role == GP_ROLE_EXECUTE)
        return;

	if (myTempNamespaceSubID == mySubid)
	{
		if (isCommit)
			myTempNamespaceSubID = parentSubid;
		else
		{
			myTempNamespaceSubID = InvalidSubTransactionId;
			/* TEMP namespace creation failed, so reset state */
			myTempNamespace = InvalidOid;
			namespaceSearchPathValid = false;	/* need to rebuild list */
		}
	}
}

/*
 * Remove all relations in the specified temp namespace.
 *
 * This is called at backend shutdown (if we made any temp relations).
 * It is also called when we begin using a pre-existing temp namespace,
 * in order to clean out any relations that might have been created by
 * a crashed backend.
 */
static void
RemoveTempRelations(Oid tempNamespaceId)
{
	ObjectAddress object;

	/*
	 * We want to get rid of everything in the target namespace, but not the
	 * namespace itself (deleting it only to recreate it later would be a
	 * waste of cycles).  We do this by finding everything that has a
	 * dependency on the namespace.
	 */
	object.classId = NamespaceRelationId;
	object.objectId = tempNamespaceId;
	object.objectSubId = 0;

	deleteWhatDependsOn(&object, false);
}

/*
 * Callback to remove temp relations at backend exit.
 */
static void
RemoveTempRelationsCallback(int code, Datum arg)
{
	/*
	 * in hawq, we do not create any object on QE,
	 * therefore we do not drop anything.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
        return;

	if (OidIsValid(myTempNamespace))
	{
		/* Need to ensure we have a usable transaction. */
		AbortOutOfAnyTransaction();
		StartTransactionCommand();

		/* 
		 * Make sure that the schema hasn't been removed. We must do this after
		 * we start a new transaction (see previous two lines), otherwise we
		 * wont have a valid CurrentResourceOwner.
		 */
		if (TempNamespaceValid(false))
		{
			RemoveTempRelations(myTempNamespace);

			/* MPP-3390: drop pg_temp_N schema entry from pg_namespace */
			RemoveSchemaById(myTempNamespace);
			elog(DEBUG1, "Remove schema entry %u from pg_namespace", 
				 myTempNamespace); 
		}

		CommitTransactionCommand();
	}
}


/*
 * Routines for handling the GUC variable 'search_path'.
 */

/* assign_hook: validate new search_path, do extra actions as needed */
const char *
assign_search_path(const char *newval, bool doit, GucSource source)
{
	char	   *rawname;
	List	   *namelist;
	ListCell   *l;

	/* Need a modifiable copy of string */
	rawname = pstrdup(newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawname, ',', &namelist))
	{
		/* syntax error in name list */
		pfree(rawname);
		list_free(namelist);
		return NULL;
	}

	/*
	 * If we aren't inside a transaction, we cannot do database access so
	 * cannot verify the individual names.	Must accept the list on faith.
	 */
	if (source >= PGC_S_INTERACTIVE && IsTransactionState())
	{
		/*
		 * Verify that all the names are either valid namespace names or
		 * "$user" or "pg_temp".  We do not require $user to correspond to a
		 * valid namespace, and pg_temp might not exist yet.  We do not check
		 * for USAGE rights, either; should we?
		 *
		 * When source == PGC_S_TEST, we are checking the argument of an ALTER
		 * DATABASE SET or ALTER USER SET command.	It could be that the
		 * intended use of the search path is for some other database, so we
		 * should not error out if it mentions schemas not present in the
		 * current database.  We reduce the message to NOTICE instead.
		 */
		foreach(l, namelist)
		{
			char	   *curname = (char *) lfirst(l);

			if (strcmp(curname, "$user") == 0)
				continue;
			if (strcmp(curname, "pg_temp") == 0)
				continue;

			if (0 == LookupInternalNamespaceId(curname))
			{
				if (Gp_role != GP_ROLE_EXECUTE)
					ereport((source == PGC_S_TEST) ? NOTICE : ERROR,
							(errcode(ERRCODE_UNDEFINED_SCHEMA),
							 errmsg("schema \"%s\" does not exist", curname),
							 errOmitLocation(true)));
			}
		}
	}

	pfree(rawname);
	list_free(namelist);

	/*
	 * We mark the path as needing recomputation, but don't do anything until
	 * it's needed.  This avoids trying to do database access during GUC
	 * initialization.
	 */
	if (doit)
		namespaceSearchPathValid = false;

	return newval;
}

/*
 * InitializeSearchPath: initialize module during InitPostgres.
 *
 * This is called after we are up enough to be able to do catalog lookups.
 */
void
InitializeSearchPath(void)
{
	if (IsBootstrapProcessingMode())
	{
		/*
		 * In bootstrap mode, the search path must be 'pg_catalog' so that
		 * tables are created in the proper namespace; ignore the GUC setting.
		 */
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		namespaceSearchPath = list_make1_oid(PG_CATALOG_NAMESPACE);
		MemoryContextSwitchTo(oldcxt);
		defaultCreationNamespace = PG_CATALOG_NAMESPACE;
		firstExplicitNamespace = PG_CATALOG_NAMESPACE;
		tempCreationPending = false;
		namespaceSearchPathValid = true;
		namespaceUser = GetUserId();
	}
	else
	{
		/*
		 * In normal mode, arrange for a callback on any syscache invalidation
		 * of pg_namespace rows.
		 */
		CacheRegisterSyscacheCallback(NAMESPACEOID,
									  NamespaceCallback,
									  (Datum) 0);
		/* Force search path to be recomputed on next use */
		namespaceSearchPathValid = false;
	}
}

/*
 * NamespaceCallback
 *		Syscache inval callback function
 */
static void
NamespaceCallback(Datum arg, Oid relid)
{
	/* Force search path to be recomputed on next use */
	namespaceSearchPathValid = false;
}

/* double check that temp name space is valid. */
static bool
TempNamespaceValid(bool error_if_removed)
{
	if (!OidIsValid(myTempNamespace))
		return false;
	else
	{
		/*
		 * Warning:  To use the syscache, there must be a valid ResourceOwner.
		 * This implies we must be in a Portal, and if we are in a
		 * Portal, we are in a transaction.  So you can't use this if
		 * we are currently idle.
		 */
		AcceptInvalidationMessages();  /* minimize race conditions */

		/* NOTE: use of syscache with caql ! */
		/* XXX XXX: jic 20120430: is this correct - check if oid exists? */
		myTempNamespace = caql_getoid(
				NULL,
				cql("SELECT oid FROM pg_namespace "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(myTempNamespace)));

		if (OidIsValid(myTempNamespace))
			return true;
		else if (Gp_role != GP_ROLE_EXECUTE && error_if_removed) 
			/*
			 * We might call this on QEs if we're dropping our own
			 * session's temp table schema. However, we want the
			 * QD to be the one to find it not the QE.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_SCHEMA),
					 errmsg("temporary table schema removed while session "
							"still in progress")));
	}
	return false;
}

/*
 * GPDB: Special just for cdbgang use
 */
bool
TempNamespaceOidIsValid(void)
{
	return OidIsValid(myTempNamespace);
}

/*
 * Fetch the active search path. The return value is a palloc'ed list
 * of OIDs; the caller is responsible for freeing this storage as
 * appropriate.
 *
 * The returned list includes the implicitly-prepended namespaces only if
 * includeImplicit is true.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
List *
fetch_search_path(bool includeImplicit)
{
	List	   *result;

	recomputeNamespacePath();

	/*
	 * If the temp namespace should be first, force it to exist.  This is
	 * so that callers can trust the result to reflect the actual default
	 * creation namespace.  It's a bit bogus to do this here, since
	 * current_schema() is supposedly a stable function without side-effects,
	 * but the alternatives seem worse.
	 */
	if (tempCreationPending)
	{
		InitTempTableNamespace();
		recomputeNamespacePath();
	}

	result = list_copy(namespaceSearchPath);
	if (!includeImplicit)
	{
		while (result && linitial_oid(result) != firstExplicitNamespace)
			result = list_delete_first(result);
	}

	return result;
}

/*
 * Export the FooIsVisible functions as SQL-callable functions.
 *
 * Note: as of Postgres 8.4, these will silently return NULL if called on
 * a nonexistent object OID, rather than failing.  This is to avoid race
 * condition errors when a query that's scanning a catalog using an MVCC
 * snapshot uses one of these functions.  The underlying IsVisible functions
 * operate on SnapshotNow semantics and so might see the object as already
 * gone when it's still visible to the MVCC snapshot.  (There is no race
 * condition in the current coding because we don't accept sinval messages
 * between the searchsyscacheexists/getcount test and the subsequent lookup.)
 */

Datum
pg_table_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_class "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(RelationIsVisible(oid));
}

Datum
pg_type_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_type "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(TypeIsVisible(oid));
}

Datum
pg_function_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_proc "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(FunctionIsVisible(oid));
}

Datum
pg_operator_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_operator "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(OperatorIsVisible(oid));
}

Datum
pg_opclass_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_opclass "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(OpclassIsVisible(oid));
}

Datum
pg_conversion_is_visible(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	if (0 == caql_getcount(
				NULL,
				cql("SELECT COUNT(*) FROM pg_conversion "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(oid))))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(ConversionIsVisible(oid));
}

Datum
pg_my_temp_schema(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(myTempNamespace);
}

Datum
pg_is_other_temp_schema(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);

	PG_RETURN_BOOL(isOtherTempNamespace(oid));
}

Datum
pg_objname_to_oid(PG_FUNCTION_ARGS)
{
    text *s = PG_GETARG_TEXT_P(0); 
    RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(s));
    Oid relid = RangeVarGetRelid(rv, true, false /*allowHcatalog*/);

    PG_RETURN_OID(relid);
}

    
    
