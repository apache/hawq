/*-------------------------------------------------------------------------
 *
 * catalog.c
 *		routines concerned with catalog naming conventions and other
 *		bits of hard-wired knowledge
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/catalog/catalog.c,v 1.68 2006/10/04 00:29:50 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_filespace.h"
#include "catalog/pg_filespace_entry.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_resqueue.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_tidycat.h"

#include "catalog/gp_configuration.h"
#include "catalog/gp_configuration.h"
#include "catalog/gp_segment_config.h"
#include "catalog/gp_san_config.h"
#include "catalog/gp_verification_history.h"

#include "catalog/gp_master_mirroring.h"
#include "catalog/gp_persistent.h"
#include "catalog/gp_global_sequence.h"
#include "catalog/gp_version.h"
#include "catalog/toasting.h"
#include "catalog/gp_policy.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/fmgroids.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"

#include "cdb/cdbpersistenttablespace.h"
#include "cdb/cdbdispatchedtablespaceinfo.h"
#include "cdb/cdbvars.h"

#include "commands/dbcommands.h"

#define OIDCHARS	10			/* max chars printed by %u */

void GetFilespacePathPrefix(Oid tablespaceOid, char **pathPrefix)
{
	char *path = NULL;
	GetFilespacePathForTablespace(tablespaceOid, &path);

	if (path == NULL)
		elog(ERROR, "cannot get table space location for content 0 table space %u", tablespaceOid);

	int len = strlen(path);

	if (len < 1 || path[len - 1] == '/')
	{
		path[len - 1] = 0;
		len -= 1;
	}

	*pathPrefix = path;
}

void GetFilespacePathForTablespace(
	Oid tablespaceOid,
	char **filespacePath)
{
	RC4PersistentTablespaceGetFilespaces tablespaceGetFilespaces;
	Oid filespaceOid;

	/* All other tablespaces are accessed via filespace locations */
	char *ret_path;

	Assert(tablespaceOid != GLOBALTABLESPACE_OID);
	Assert(tablespaceOid != DEFAULTTABLESPACE_OID);
	
	if (Gp_role != GP_ROLE_EXECUTE || IsBootstrapProcessingMode())
	{
		/* Lookup filespace location from the persistent object layer. */
		tablespaceGetFilespaces = 
			PersistentTablespace_TryGetFilespacePath(
														tablespaceOid, 
														&ret_path, 
														&filespaceOid);
	}
	else
	{
		bool found;
		DispatchedFilespace_GetPathForTablespace(tablespaceOid, &ret_path, &found);
		if (!found)
			tablespaceGetFilespaces = RC4PersistentTablespaceGetFilespaces_TablespaceNotFound;
		else
			tablespaceGetFilespaces = RC4PersistentTablespaceGetFilespaces_Ok;
	}

	switch (tablespaceGetFilespaces)
	{
	case RC4PersistentTablespaceGetFilespaces_TablespaceNotFound:
		ereport(ERROR, 
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("Unable to find entry for tablespace OID = %u when forming file-system path",
						tablespaceOid)));
		break;
			
	case RC4PersistentTablespaceGetFilespaces_FilespaceNotFound:
		ereport(ERROR, 
				(errcode(ERRCODE_CDB_INTERNAL_ERROR),
				 errmsg("Unable to find entry for filespace OID = %u when forming file-system path for tablespace OID = %u",
				 		filespaceOid,
						tablespaceOid)));
		break;
					
	case RC4PersistentTablespaceGetFilespaces_Ok:
		// Go below and pass back the result.
		break;
		
	default:
		elog(ERROR, "Unexpected tablespace filespace fetch result: %d",
			 tablespaceGetFilespaces);
	}
	

	Assert(ret_path != NULL);

	*filespacePath = ret_path;
}

/*
 * relpath			- construct path to a relation's file
 *
 * Result is a palloc'd string.
 */
char *
relpath(RelFileNode rnode)
{
	int			pathlen;
	char	   *path;
	int 		snprintfResult;

	if (rnode.spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(rnode.dbNode == 0);
		pathlen = 7 + OIDCHARS + 1;
		path = (char *) palloc(pathlen);
		snprintfResult =
			snprintf(path, pathlen, "global/%u",
					 rnode.relNode);
		
	}
	else if (rnode.spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		pathlen = 5 + OIDCHARS + 1 + OIDCHARS + 1;
		path = (char *) palloc(pathlen);
		snprintfResult =
			snprintf(path, pathlen, "base/%u/%u",
					 rnode.dbNode, rnode.relNode);
	}
	else
	{
		char *primary_path;

		/* All other tablespaces are accessed via filespace locations */
		GetFilespacePathForTablespace(
								rnode.spcNode,
								&primary_path);

		/* 
		 * We should develop an interface for the above that doesn't
		 * require reallocating to a slightly larger size...
		 */
		pathlen = strlen(primary_path)+1+OIDCHARS+1+OIDCHARS+1+OIDCHARS+1;
		path = (char *) palloc(pathlen);
		snprintfResult =
			snprintf(path, pathlen, "%s/%u/%u/%u",
					 primary_path, rnode.spcNode, rnode.dbNode, rnode.relNode);

		/* Throw away the allocation we got from persistent layer */
		pfree(primary_path);
	}
	
	Assert(snprintfResult >= 0);
	Assert(snprintfResult < pathlen);

	return path;
}

void
CopyRelPath(char *target, int targetMaxLen, RelFileNode rnode)
{
	int 		snprintfResult;

	if (rnode.spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(rnode.dbNode == 0);
		snprintfResult =
			snprintf(target, targetMaxLen, "global/%u",
					 rnode.relNode);
	}
	else if (rnode.spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		snprintfResult =
			snprintf(target, targetMaxLen, "base/%u/%u",
					 rnode.dbNode, rnode.relNode);
	}
	else
	{
		char *primary_path;

		/* All other tablespaces are accessed via filespace locations */
		GetFilespacePathForTablespace(
								rnode.spcNode,
								&primary_path);

		/* Copy path into the passed in target location */
		snprintfResult =
			snprintf(target, targetMaxLen, "%s/%u/%u/%u",
					 primary_path, rnode.spcNode, rnode.dbNode, rnode.relNode);

		/* Throw away the allocation we got from persistent layer */
		pfree(primary_path);
	}

	if (snprintfResult < 0)
		elog(ERROR, "CopyRelPath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "CopyRelPath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}

/*
 * GetDatabasePath			- construct path to a database dir
 *
 * Result is a palloc'd string.
 *
 * XXX this must agree with relpath()!
 */
char *
GetDatabasePath(Oid dbNode, Oid spcNode)
{
	int 		pathlen;
	char	   *path;
	int 		snprintfResult;

	if (spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbNode == 0);
		pathlen = 6 + 1;
		path = (char *) palloc(pathlen);

		// Using strncpy is error prone.
		snprintfResult =
			snprintf(path, pathlen, "global");
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		pathlen = 5 + OIDCHARS + 1;
		path = (char *) palloc(pathlen);
		snprintfResult =
			snprintf(path, pathlen, "base/%u",
					 dbNode);
	}
	else
	{
		char *primary_path;

		/* All other tablespaces are accessed via filespace locations */
		GetFilespacePathForTablespace(
								spcNode,
								&primary_path);

		/* 
		 * We should develop an interface for the above that doesn't
		 * require reallocating to a slightly larger size...
		 */
		pathlen = strlen(primary_path)+1+OIDCHARS+1+OIDCHARS+1;
		path = (char *) palloc(pathlen);
		snprintfResult =
			snprintf(path, pathlen, "%s/%u/%u",
					 primary_path, spcNode, dbNode);

		/* Throw away the allocation we got from persistent layer */
		pfree(primary_path);
	}
	
	Assert(snprintfResult >= 0);
	Assert(snprintfResult < pathlen);

	return path;
}

void
CopyDatabasePath(char *target, int targetMaxLen, Oid dbNode, Oid spcNode)
{
	int 		snprintfResult;

	if (spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(dbNode == 0);

		// Using strncpy is error prone.
		snprintfResult =
			snprintf(target, targetMaxLen, "global");
	}
	else if (spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		snprintfResult =
			snprintf(target, targetMaxLen, "base/%u",
					 dbNode);
	}
	else
	{
		char *primary_path;

		/* All other tablespaces are accessed via filespace locations */
		GetFilespacePathForTablespace(
								spcNode,
								&primary_path);

		/* Copy path into the passed in target location */
		snprintfResult =
			snprintf(target, targetMaxLen, "%s/%u/%u",
					 primary_path, spcNode, dbNode);

		/* Throw away the allocation we got from persistent layer */
		pfree(primary_path);
	}

	if (snprintfResult < 0)
		elog(ERROR, "CopyDatabasePath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "CopyDatabasePath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}


void FormDatabasePath(
	char *databasePath,

	char *filespaceLocation,

	Oid tablespaceOid,

	Oid databaseOid)
{
	int			targetMaxLen = MAXPGPATH + 1;
	int 		snprintfResult;

	if (tablespaceOid == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(databaseOid == 0);
		Assert(filespaceLocation == NULL);

		// Using strncpy is error prone.
		snprintfResult =
			snprintf(databasePath, targetMaxLen, "global");
	}
	else if (tablespaceOid == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		Assert(filespaceLocation == NULL);

		snprintfResult =
			snprintf(databasePath, targetMaxLen, "base/%u",
					 databaseOid);
	}
	else
	{
		/* All other tablespaces are in filespace locations */
		Assert(filespaceLocation != NULL);

		snprintfResult =
			snprintf(databasePath, targetMaxLen, "%s/%u/%u",
					 filespaceLocation, tablespaceOid, databaseOid);
	}

	if (snprintfResult < 0)
		elog(ERROR, "CopyDatabasePath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "CopyDatabasePath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}

void FormTablespacePath(
	char *tablespacePath,

	char *filespaceLocation,

	Oid tablespaceOid)
{
	int			targetMaxLen = MAXPGPATH + 1;
	int 		snprintfResult;

	if (tablespaceOid == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(filespaceLocation == NULL);

		// Using strncpy is error prone.
		snprintfResult =
			snprintf(tablespacePath, targetMaxLen, "global");
	}
	else if (tablespaceOid == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		Assert(filespaceLocation == NULL);

		// Using strncpy is error prone.
		snprintfResult =
			snprintf(tablespacePath, targetMaxLen, "base");
	}
	else
	{
		/* All other tablespaces are in filespace locations */
		Assert(filespaceLocation != NULL);
		snprintfResult =
			snprintf(tablespacePath, targetMaxLen, "%s/%u",
					 filespaceLocation, tablespaceOid);
	}

	if (snprintfResult < 0)
		elog(ERROR, "FormTablespacePath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "FormTablespacePath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}


void 
FormRelationPath(char *relationPath, char *filespaceLocation, RelFileNode rnode)
{
	int			targetMaxLen = MAXPGPATH + 1;
	int 		snprintfResult;

	if (rnode.spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		Assert(rnode.dbNode == 0);
		
		snprintfResult =
			snprintf(relationPath, targetMaxLen, "global/%u",
					 rnode.relNode);
	}
	else if (rnode.spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		
		snprintfResult =
			snprintf(relationPath, targetMaxLen, "base/%u/%u",
					 rnode.dbNode, rnode.relNode);
	}
	else
	{
		snprintfResult =
			snprintf(relationPath, targetMaxLen, "%s/%u/%u/%u",
				filespaceLocation,
				rnode.spcNode,
				rnode.dbNode,
				rnode.relNode);
	}	

	if (snprintfResult < 0)
		elog(ERROR, "FormRelationPath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "FormRelationPath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}

/**
 * Form path for relfile, it support not only catalog relfile but also relfile on hdfs
 */
void
FormRelfilePath(char *relfilePath, char *filespaceLocation, RelFileNode *rnode, int32 segmentFileNum)
{
	char		dbPath[MAXPGPATH + 1];
	int			targetMaxLen = MAXPGPATH + 1;
	int 		snprintfResult;

	FormDatabasePath(dbPath,
					 filespaceLocation,
					 rnode->spcNode,
					 rnode->dbNode);

	if (IsLocalPath(dbPath))
	{
		/* catalog relfile rule same as postgres: all relfile at same level */
		if (segmentFileNum == 0)
			snprintfResult =
						snprintf(relfilePath, targetMaxLen, "%s/%u", dbPath, rnode->relNode);
		else
			snprintfResult =
						snprintf(relfilePath, targetMaxLen, "%s/%u.%u", dbPath, rnode->relNode, segmentFileNum);

	}else
	{
		/* hdfs relfile rule change from file to directory at relation level, and  segnum */
		snprintfResult =
					snprintf(relfilePath, targetMaxLen, "%s/%u/%u", dbPath, rnode->relNode, segmentFileNum);
	}

	if (snprintfResult < 0)
		elog(ERROR, "FormRelfilePath formatting error");

	/*
	 * Magically truncating the result to fit in the target string is unacceptable here
	 * because it can result in the wrong file-system object being referenced.
	 */
	if (snprintfResult >= targetMaxLen)
		elog(ERROR, "FormRelfilePath formatting result length %d exceeded the maximum length %d",
					snprintfResult,
					targetMaxLen);
}

/*
 * IsSystemRelation
 *		True iff the relation is a system catalog relation.
 *
 *		NB: TOAST relations are considered system relations by this test
 *		for compatibility with the old IsSystemRelationName function.
 *		This is appropriate in many places but not all.  Where it's not,
 *		also check IsToastRelation.
 *
 *		We now just test if the relation is in the system catalog namespace;
 *		so it's no longer necessary to forbid user relations from having
 *		names starting with pg_.
 */
bool
IsSystemRelation(Relation relation)
{
	return IsSystemNamespace(RelationGetNamespace(relation)) ||
		   IsToastNamespace(RelationGetNamespace(relation)) ||
		   IsAoSegmentNamespace(RelationGetNamespace(relation));
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsSystemClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsSystemNamespace(relnamespace) ||
		IsToastNamespace(relnamespace) ||
		IsAoSegmentNamespace(relnamespace);
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 */
bool
IsToastRelation(Relation relation)
{
	return IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsToastClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;

	return IsToastNamespace(relnamespace);
}

/*
 * IsAoSegmentRelation
 *		True iff relation is an AO segment support relation (or index).
 */
bool
IsAoSegmentRelation(Relation relation)
{
	return IsAoSegmentNamespace(RelationGetNamespace(relation));
}

/*
 * IsAoSegmentClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool
IsAoSegmentClass(Form_pg_class reltuple)
{
	Oid			relnamespace = reltuple->relnamespace;
	
	return IsAoSegmentNamespace(relnamespace);
}

/*
 * IsSystemNamespace
 *		True iff namespace is pg_catalog.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsSystemNamespace(Oid namespaceId)
{
	return namespaceId == PG_CATALOG_NAMESPACE;
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsToastNamespace(Oid namespaceId)
{
	return namespaceId == PG_TOAST_NAMESPACE;
}

/*
 * IsAoSegmentNamespace
 *		True iff namespace is pg_aoseg.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool
IsAoSegmentNamespace(Oid namespaceId)
{
	return namespaceId == PG_AOSEGMENT_NAMESPACE;
}

/**
 * Method determines if a relation is master-only or distributed among segments.
 * Input:
 * 	relationOid
 * Output:
 * 	true if masteronly
 */
bool
isMasterOnly(Oid relationOid)
{
	Assert(relationOid != InvalidOid);
	Oid				schemaOid = get_rel_namespace(relationOid);
	GpPolicy		*distributionPolicy = GpPolicyFetch(CurrentMemoryContext, relationOid);
	
	bool masterOnly = (Gp_role == GP_ROLE_UTILITY 
			|| IsSystemNamespace(schemaOid)
			|| IsToastNamespace(schemaOid)
			|| IsAoSegmentNamespace(schemaOid)
			|| (distributionPolicy == NULL)
			|| (distributionPolicy->ptype == POLICYTYPE_ENTRY));
	
	return masterOnly;
}
/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.  As of 8.0, this is only true for
 *		schema and tablespace names.
 *
 *      As of Greenplum 4.0 we also reserve the prefix gp_
 */
bool
IsReservedName(const char *name)
{
	/* ugly coding for speed */
	return ((name[0] == 'p' && name[1] == 'g' && name[2] == '_') ||
			(name[0] == 'g' && name[1] == 'p' && name[2] == '_'));
}

/*
 * GetReservedPrefix
 *		Given a string that is a reserved name return the portion of
 *      the name that makes it reserved - the reserved prefix.
 *
 *      Current return values include "pg_" and "gp_"
 */
char *
GetReservedPrefix(const char *name)
{
	char		*prefix = NULL;

	if (IsReservedName(name))
	{
		prefix = palloc(4);
		memcpy(prefix, name, 3);
		prefix[3] = '\0';
	}

	return prefix;
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * Hard-wiring this list is pretty grotty, but we really need it so that
 * we can compute the locktag for a relation (and then lock it) without
 * having already read its pg_class entry.	If we try to retrieve relisshared
 * from pg_class with no pre-existing lock, there is a race condition against
 * anyone who is concurrently committing a change to the pg_class entry:
 * since we read system catalog entries under SnapshotNow, it's possible
 * that both the old and new versions of the row are invalid at the instants
 * we scan them.  We fix this by insisting that updaters of a pg_class
 * row must hold exclusive lock on the corresponding rel, and that users
 * of a relation must hold at least AccessShareLock on the rel *before*
 * trying to open its relcache entry.  But to lock a rel, you have to
 * know if it's shared.  Fortunately, the set of shared relations is
 * fairly static, so a hand-maintained list of their OIDs isn't completely
 * impractical.
 */
bool
IsSharedRelation(Oid relationId)
{
	/* These are the shared catalogs (look for BKI_SHARED_RELATION) */
	if (
		relationId == AuthMemRelationId ||
		relationId == DatabaseRelationId ||
		relationId == PLTemplateRelationId ||
		relationId == SharedDescriptionRelationId ||
		relationId == SharedDependRelationId ||
		relationId == FileSpaceRelationId ||
		relationId == TableSpaceRelationId ||

		relationId == GpVersionRelationId ||

		relationId == GpPersistentRelfileNodeRelationId ||
		relationId == GpPersistentRelationNodeRelationId ||
		relationId == GpPersistentDatabaseNodeRelationId ||
		relationId == GpPersistentTablespaceNodeRelationId ||
		relationId == GpPersistentFilespaceNodeRelationId ||
		relationId == GpGlobalSequenceRelationId ||

		/* MPP-6929: metadata tracking */
		relationId == StatLastShOpRelationId ||

/* TIDYCAT_BEGIN_CODEGEN 
*/
/*
   WARNING: DO NOT MODIFY THE FOLLOWING SECTION: 
   Generated by ./tidycat.pl version 29
   on Mon Aug  1 17:08:37 2011
 */
/* relation id: 1260 - pg_authid 20100129 */
relationId == AuthIdRelationId || 
/* relation id: 5008 - gp_master_mirroring 20101104 */
relationId == GpMasterMirroringRelationId || 
/* relation id: 5035 - gp_san_configuration 20101104 */
relationId == GpSanConfigRelationId || 
/* relation id: 5000 - gp_configuration 20101104 */
relationId == GpConfigurationRelationId || 
/* relation id: 5006 - gp_configuration_history 20101104 */
relationId == GpConfigHistoryRelationId || 
/* relation id: 5029 - gp_db_interfaces 20101104 */
relationId == GpDbInterfacesRelationId || 
/* relation id: 5030 - gp_interfaces 20101104 */
relationId == GpInterfacesRelationId || 
/* relation id: 5033 - pg_filespace_entry 20101122 */
relationId == FileSpaceEntryRelationId || 
/* relation id: 6429 - gp_verification_history 20110609 */
relationId == GpVerificationHistoryRelationId || 
/* relation id: 2914 - pg_auth_time_constraint 20110908 */
relationId == AuthTimeConstraintRelationId ||
/* relation id: 6112 - pg_filesystem 20130123 */
relationId == FileSystemRelationId || 
/* relation id: 5036 - gp_segment_configuration 20150204 */
relationId == GpSegmentConfigRelationId || 

/* relation id: 6026 - pg_resqueue 20151014 */
relationId == ResQueueRelationId || 
/* TIDYCAT_END_CODEGEN */
		0 /* OR ZERO */
			)

		return true;
	/* These are their indexes (see indexing.h) */
	if (
		relationId == AuthMemRoleMemIndexId ||
		relationId == AuthMemMemRoleIndexId ||
		relationId == DatabaseNameIndexId ||
		relationId == DatabaseOidIndexId ||
		relationId == PLTemplateNameIndexId ||
		relationId == SharedDescriptionObjIndexId ||
		relationId == SharedDependDependerIndexId ||
		relationId == SharedDependReferenceIndexId ||
		relationId == FilespaceOidIndexId ||
		relationId == FilespaceNameIndexId ||
		relationId == TablespaceOidIndexId ||
		relationId == TablespaceNameIndexId ||

		/* MPP-6929: metadata tracking */
		relationId == StatLastShOpClassidObjidIndexId ||
		relationId == StatLastShOpClassidObjidStaactionnameIndexId ||

/* TIDYCAT_BEGIN_CODEGEN 
*/
/*
   WARNING: DO NOT MODIFY THE FOLLOWING SECTION: 
   Generated by ./tidycat.pl version 31
   on Thu May 24 14:25:58 2012
 */
/* relation id: 1260 - pg_authid 20100129 */
relationId == AuthIdRolnameIndexId || 
/* relation id: 1260 - pg_authid 20100129 */
relationId == AuthIdOidIndexId || 
/* relation id: 1260 - pg_authid 20100129 */
relationId == AuthIdRolResQueueIndexId || 
/* relation id: 5035 - gp_san_configuration 20101104 */
relationId == GpSanConfigMountidIndexId || 
/* relation id: 5000 - gp_configuration 20101104 */
relationId == GpConfigurationContentDefinedprimaryIndexId || 
/* relation id: 5000 - gp_configuration 20101104 */
relationId == GpConfigurationDbidIndexId || 
/* relation id: 5029 - gp_db_interfaces 20101104 */
relationId == GpDbInterfacesDbidIndexId || 
/* relation id: 5030 - gp_interfaces 20101104 */
relationId == GpInterfacesInterfaceidIndexId || 
/* relation id: 5033 - pg_filespace_entry 20101122 */
relationId == FileSpaceEntryFsefsoidIndexId || 
/* relation id: 5033 - pg_filespace_entry 20101122 */
relationId == FileSpaceEntryFsefsoidFsedbidIndexId || 
/* relation id: 6429 - gp_verification_history 20110609 */
relationId == GpVerificationHistoryVertokenIndexId || 
/* relation id: 6112 - pg_filesystem 20130123 */
relationId == FileSystemOidIndexId || 
/* relation id: 6112 - pg_filesystem 20130123 */
relationId == FileSystemFsysnameIndexId || 
/* relation id: 5036 - gp_segment_configuration 20150207 */
relationId == GpSegmentConfigRegistration_orderIndexId || 
/* relation id: 5036 - gp_segment_configuration 20150207 */
relationId == GpSegmentConfigRoleIndexId || 

/* relation id: 6026 - pg_resqueue 20151014 */
relationId == ResQueueOidIndexId || 
/* relation id: 6026 - pg_resqueue 20151014 */
relationId == ResQueueRsqnameIndexId || 

/* TIDYCAT_END_CODEGEN */
		0 /* OR ZERO */

			)
		return true;
	/* These are their toast tables and toast indexes (see toasting.h) */
	if (relationId == PgAuthidToastTable ||
		relationId == PgAuthidToastIndex ||
		relationId == PgDatabaseToastTable ||
		relationId == PgDatabaseToastIndex ||
		relationId == PgShdescriptionToastTable ||
		relationId == PgShdescriptionToastIndex ||

/* TIDYCAT_BEGIN_CODEGEN 
*/
/* relation id: 5033 - pg_filespace_entry 20101122 */
relationId == PgFileSpaceEntryToastTable || 
relationId == PgFileSpaceEntryToastIndex || 
/* relation id: 6112 - pg_filesystem 20130123 */
relationId == PgFileSystemToastTable || 
relationId == PgFileSystemToastIndex || 
/* relation id: 5036 - gp_segment_configuration 20150204 */
relationId == GpSegmentConfigToastTable || 
relationId == GpSegmentConfigToastIndex || 

/* relation id: 6026 - pg_resqueue 20151014 */
relationId == PgResQueueToastTable || 
relationId == PgResQueueToastIndex || 
/* TIDYCAT_END_CODEGEN */
		0 /* OR ZERO */
			)
		return true;
	return false;
}


/*
 * GetNewOid
 *		Generate a new OID that is unique within the given relation.
 *
 * Caller must have a suitable lock on the relation.
 *
 * Uniqueness is promised only if the relation has a unique index on OID.
 * This is true for all system catalogs that have OIDs, but might not be
 * true for user tables.  Note that we are effectively assuming that the
 * table has a relatively small number of entries (much less than 2^32)
 * and there aren't very long runs of consecutive existing OIDs.  Again,
 * this is reasonable for system catalogs but less so for user tables.
 *
 * Since the OID is not immediately inserted into the table, there is a
 * race condition here; but a problem could occur only if someone else
 * managed to cycle through 2^32 OIDs and generate the same OID before we
 * finish inserting our row.  This seems unlikely to be a problem.	Note
 * that if we had to *commit* the row to end the race condition, the risk
 * would be rather higher; therefore we use SnapshotDirty in the test,
 * so that we will see uncommitted rows.
 */
Oid
GetNewOid(Relation relation)
{
	Oid			newOid;
	Oid			oidIndex;
	Relation	indexrel;

	/* If relation doesn't have OIDs at all, caller is confused */
	Assert(relation->rd_rel->relhasoids);

	/* In bootstrap mode, we don't have any indexes to use */
	if (IsBootstrapProcessingMode())
		return GetNewObjectId();

	/* The relcache will cache the identity of the OID index for us */
	oidIndex = RelationGetOidIndex(relation);

	/* If no OID index, just hand back the next OID counter value */
	if (!OidIsValid(oidIndex))
	{
		Oid result;
		/*
		 * System catalogs that have OIDs should *always* have a unique OID
		 * index; we should only take this path for user tables. Give a
		 * warning if it looks like somebody forgot an index.
		 */
		if (IsSystemRelation(relation))
			elog(WARNING, "generating possibly-non-unique OID for \"%s\"",
				 RelationGetRelationName(relation));

		result=  GetNewObjectId();
		
		if (IsSystemNamespace(RelationGetNamespace(relation)))
		{
			if (Gp_role == GP_ROLE_EXECUTE)
			{
				elog(DEBUG1,"Allocating Oid %u on relid %u %s in EXECUTE mode",result,relation->rd_id,RelationGetRelationName(relation));
			}
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				elog(DEBUG5,"Allocating Oid %u on relid %u %s in DISPATCH mode",result,relation->rd_id,RelationGetRelationName(relation));
			}
		}
		return result;
	}

	/* Otherwise, use the index to find a nonconflicting OID */
	indexrel = index_open(oidIndex, AccessShareLock);
	newOid = GetNewOidWithIndex(relation, indexrel);
	index_close(indexrel, AccessShareLock);

	return newOid;
}

/*
 * GetNewOidWithIndex
 *		Guts of GetNewOid: use the supplied index
 *
 * This is exported separately because there are cases where we want to use
 * an index that will not be recognized by RelationGetOidIndex: TOAST tables
 * and pg_largeobject have indexes that are usable, but have multiple columns
 * and are on ordinary columns rather than a true OID column.  This code
 * will work anyway, so long as the OID is the index's first column.
 *
 * Caller must have a suitable lock on the relation.
 */
Oid
GetNewOidWithIndex(Relation relation, Relation indexrel)
{
	Oid			newOid;
	IndexScanDesc scan;
	ScanKeyData key;
	bool		collides;

	/* Generate new OIDs until we find one not in the table */
	do
	{
		CHECK_FOR_INTERRUPTS();

		newOid = GetNewObjectId();

		ScanKeyInit(&key,
					(AttrNumber) 1,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		/* see notes above about using SnapshotDirty */
		scan = index_beginscan(relation, indexrel,
							   SnapshotDirty, 1, &key);

		collides = HeapTupleIsValid(index_getnext(scan, ForwardScanDirection));

		index_endscan(scan);
	} while (collides);
	
	if (IsSystemNamespace(RelationGetNamespace(relation)))
	{
		if (Gp_role == GP_ROLE_EXECUTE)
		{
			if (relation->rd_id != 2604 /* pg_attrdef */ && relation->rd_id != 2606 /* pg_constraint */ && relation->rd_id != 2615 /* pg_namespace */) 
				elog(DEBUG1,"Allocating Oid %u with index on relid %u %s in EXECUTE mode",newOid,relation->rd_id, RelationGetRelationName(relation));
			else
				elog(DEBUG4,"Allocating Oid %u with index on relid %u %s in EXECUTE mode",newOid,relation->rd_id, RelationGetRelationName(relation));
		}
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			elog(DEBUG5,"Allocating Oid %u with index on relid %u %s in DISPATCH mode",newOid,relation->rd_id,  RelationGetRelationName(relation));
		}
	}

	return newOid;
}

/*
 * GetNewRelFileNode
 *		Generate a new relfilenode number that is unique within the given
 *		tablespace.
 *
 * If the relfilenode will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine will guarantee that the result
 * is also an unused OID within pg_class.  If the result is to be used only
 * as a relfilenode for an existing relation, pass NULL for pg_class.
 *
 * As with GetNewOid, there is some theoretical risk of a race condition,
 * but it doesn't seem worth worrying about.
 *
 * Note: we don't support using this in bootstrap mode.  All relations
 * created by bootstrap have preassigned OIDs, so there's no need.
 */
Oid
GetNewRelFileNode(Oid reltablespace, bool relisshared, Relation pg_class, bool isAo)
{
	RelFileNode rnode;
	char	   *rpath;
	bool		exist = false;

	/* This should match RelationInitPhysicalAddr */
	if (isAo && reltablespace == InvalidOid)
		rnode.spcNode = get_database_dts(MyDatabaseId);
	else if (isAo)
		rnode.spcNode = reltablespace;
	else
		rnode.spcNode = MyDatabaseTableSpace;

	rnode.dbNode = relisshared ? InvalidOid : MyDatabaseId;

	do
	{
		CHECK_FOR_INTERRUPTS();

		/* Generate the OID */
		if (pg_class)
			rnode.relNode = GetNewOid(pg_class);
		else
			rnode.relNode = GetNewObjectId();

		/* Check for existing file of same name */
		rpath = relpath(rnode);
		exist = PathExist(rpath);

		pfree(rpath);
	} while (exist);
	
	if (!gp_upgrade_mode && Gp_role == GP_ROLE_EXECUTE)
		Insist(!PointerIsValid(pg_class));

	elog(DEBUG1, "Calling GetNewRelFileNode in %s mode %s pg_class. "
		 "New relOid = %d", 
		 (Gp_role == GP_ROLE_EXECUTE ? "execute" :
		  Gp_role == GP_ROLE_UTILITY ? "utility" :
		  "dispatch"), pg_class ? "with" : "without",
		 rnode.relNode );

	return rnode.relNode;
}


bool
CheckNewRelFileNodeIsOk(Oid newOid, Oid reltablespace, bool relisshared, 
						Relation pg_class, bool isAo)
{
	RelFileNode rnode;
	char	   *rpath;
	int			fd;
	bool		collides;
	
	
	if (pg_class)
	{
		Oid			oidIndex;
		Relation	indexrel;
		IndexScanDesc scan;
		ScanKeyData key;
	
		Assert(!IsBootstrapProcessingMode());
		Assert(pg_class->rd_rel->relhasoids);
	
		/* The relcache will cache the identity of the OID index for us */
		oidIndex = RelationGetOidIndex(pg_class);
	
		Assert(OidIsValid(oidIndex));
		
		indexrel = index_open(oidIndex, AccessShareLock);
		
		ScanKeyInit(&key,
					(AttrNumber) 1,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(newOid));

		scan = index_beginscan(pg_class, indexrel, SnapshotDirty, 1, &key);

		collides = HeapTupleIsValid(index_getnext(scan, ForwardScanDirection));

		index_endscan(scan);
		
		index_close(indexrel, AccessShareLock);
		
		if (collides)
			elog(ERROR, "relfilenode %d already in use in \"pg_class\"",
				 newOid);	
		
	}

	/* This should match RelationInitPhysicalAddr */
	if (isAo && reltablespace == InvalidOid)
		rnode.spcNode = get_database_dts(MyDatabaseId);
	else if (isAo)
		rnode.spcNode = reltablespace;
	else
		rnode.spcNode = MyDatabaseTableSpace;

	rnode.dbNode = relisshared ? InvalidOid : MyDatabaseId;
	
	rnode.relNode = newOid;
	
	/* Check for existing file of same name */
	rpath = relpath(rnode);
	fd = BasicOpenFile(rpath, O_RDONLY | PG_BINARY, 0);

	if (fd >= 0)
	{
		/* definite collision */
		gp_retry_close(fd);
		collides = true;
	}
	else
		collides = false;

	pfree(rpath);
	
	if (collides && !relisshared)
		elog(ERROR, "oid %d already in use", newOid);	

	while(GetNewObjectId() < newOid);

	return !collides;
}

