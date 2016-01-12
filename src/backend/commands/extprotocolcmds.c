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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*-------------------------------------------------------------------------
 *
 * extprotocolcmds.c
 *
 *	  Routines for external protocol-manipulation commands
 *
 * Portions Copyright (c) 2011, Greenplum/EMC.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extprotocolcmds.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "cdb/dispatcher.h"


/*
 *	DefineExtprotocol
 */
void
DefineExtProtocol(List *name, List *parameters, Oid newOid, bool trusted)
{
	char	   *protName;
	List	   *readfuncName = NIL;
	List	   *writefuncName = NIL;
	List	   *validatorfuncName = NIL;
	ListCell   *pl;
	Oid			protOid;

	protName = strVal(linitial(name));

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(defel->defname, "readfunc") == 0)
			readfuncName = defGetQualifiedName(defel);
		else if (pg_strcasecmp(defel->defname, "writefunc") == 0)
			writefuncName = defGetQualifiedName(defel);
		else if (pg_strcasecmp(defel->defname, "validatorfunc") == 0)
			validatorfuncName = defGetQualifiedName(defel);
		else if (gp_upgrade_mode && pg_strcasecmp(defel->defname, "oid") == 0) /* OID */
		{
			int64 oid = defGetInt64(defel);
			Assert(oid < FirstBootstrapObjectId);
			newOid = (Oid)oid;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("protocol attribute \"%s\" not recognized",
							defel->defname)));
	}

	/*
	 * make sure we have our required definitions
	 */
	if (readfuncName == NULL && writefuncName == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("protocol must be specify at least a readfunc or a writefunc")));


	/*
	 * Most of the argument-checking is done inside of ExtProtocolCreate
	 */
	protOid = ExtProtocolCreateWithOid(protName,			/* protocol name */
									   readfuncName,		/* read function name */
									   writefuncName,		/* write function name */
									   validatorfuncName, 	/* validator function name */
									   newOid,
									   trusted);
					
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		DefineStmt * stmt = makeNode(DefineStmt);
		stmt->kind = OBJECT_EXTPROTOCOL;
		stmt->oldstyle = false;  
		stmt->defnames = name;
		stmt->args = NIL;
		stmt->definition = parameters;
		stmt->newOid = protOid;
		stmt->shadowOid = 0;
		stmt->ordered = false;
		stmt->trusted = trusted;
		dispatch_statement_node((Node *) stmt, NULL, NULL, NULL);
	}
}


/*
 * RemoveExtProtocol
 *		Deletes an external protocol.
 */
void
RemoveExtProtocol(List *names, DropBehavior behavior, bool missing_ok)
{
	char	   *protocolName;
	Oid			protocolOid = InvalidOid;
	ObjectAddress object;

	/* 
	 * General DROP (object) syntax allows fully qualified names, but
	 * protocols are global objects that do not live in schemas, so
	 * it is a syntax error if a fully qualified name was given.
	 */
	if (list_length(names) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("protocol name may not be qualified")));
	protocolName = strVal(linitial(names));

	/* find protocol Oid. error inline if doesn't exist */
	protocolOid = LookupExtProtocolOid(protocolName, missing_ok);

	if (!OidIsValid(protocolOid))
	{
		if (!missing_ok)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("protocol \"%s\" does not exist",
							protocolName)));
		}
		else
		{
			if (Gp_role != GP_ROLE_EXECUTE)
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("protocol \"%s\" does not exist, skipping",
								protocolName)));
		}

		return;
	}

	/* Permission check: must own protocol */
	if (!pg_extprotocol_ownercheck(protocolOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EXTPROTOCOL, protocolName);

	/*
	 * Do the deletion
	 */
	object.classId = ExtprotocolRelationId;
	object.objectId = protocolOid;
	object.objectSubId = 0;

	performDeletion(&object, behavior);
	
}

/*
 * Drop PROTOCOL by OID. This is the guts of deletion.
 * This is called to clean up dependencies.
 */
void
RemoveExtProtocolById(Oid protOid)
{
	ExtProtocolDeleteByOid(protOid);
}

/*
 * Change external protocol owner
 */
void
AlterExtProtocolOwner(const char *name, Oid newOwnerId)
{
	HeapTuple	tup;
	Relation	rel;
	Oid			ptcId;
	Oid			ownerId;
	AclResult	aclresult;
	bool		isNull;
	bool		isTrusted;
	Datum		ownerDatum;
	Datum		trustedDatum;
	cqContext	cqc;	
	cqContext  *pcqCtx;
	
	/*
	 * Check the pg_extprotocol relation to be certain the protocol
	 * is there. 
	 */
	rel = heap_open(ExtprotocolRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);
	
	tup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_extprotocol "
				" WHERE ptcname = :1 "
				" FOR UPDATE ",
				CStringGetDatum(name)));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("protocol \"%s\" does not exist",
						name)));
	
	ptcId = HeapTupleGetOid(tup);

	ownerDatum = caql_getattr(pcqCtx,
							  Anum_pg_extprotocol_ptcowner,
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: protocol \"%s\"  has no owner defined",
						 name)));

	ownerId = DatumGetObjectId(ownerDatum);

	trustedDatum = caql_getattr(pcqCtx,
								Anum_pg_extprotocol_ptctrusted,
								&isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: protocol \"%s\" has no trust attribute defined",
						 name)));

	isTrusted = DatumGetBool(trustedDatum);
	
	if (ownerId != newOwnerId)
	{
		Acl		   *newAcl;
		Datum		values[Natts_pg_extprotocol];
		bool		nulls[Natts_pg_extprotocol];
		bool		replaces[Natts_pg_extprotocol];
		HeapTuple	newtuple;
		Datum		aclDatum;

		/* Superusers can always do it */
		if (!superuser())
		{
			/* Must be owner */
			if (!pg_extprotocol_ownercheck(ptcId, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EXTPROTOCOL,
							   name);

			/* Must be able to become new owner */
			check_is_member_of_role(GetUserId(), newOwnerId);

			/* New owner must have USAGE privilege on protocol */
			aclresult = pg_extprotocol_aclcheck(ptcId, newOwnerId, ACL_USAGE);

			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_EXTPROTOCOL, name);
		}

		/* MPP-14592: untrusted? don't allow ALTER OWNER to non-super user */
		if(!isTrusted && !superuser_arg(newOwnerId))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("untrusted protocol \"%s\" can't be owned by non superuser", 
							 name)));

		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_extprotocol_ptcowner - 1] = true;
		values[Anum_pg_extprotocol_ptcowner - 1] = ObjectIdGetDatum(newOwnerId);

		aclDatum = caql_getattr(pcqCtx,
								Anum_pg_extprotocol_ptcacl,
								&isNull);
		
		if (!isNull)
		{
			newAcl = aclnewowner(DatumGetAclP(aclDatum),
								 ownerId, newOwnerId);
			replaces[Anum_pg_extprotocol_ptcacl - 1] = true;
			values[Anum_pg_extprotocol_ptcacl - 1] = PointerGetDatum(newAcl);
		}

		newtuple = caql_modify_current(pcqCtx, values,
									   nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		heap_freetuple(newtuple);
		
		/* Update owner dependency reference */
		changeDependencyOnOwner(ExtprotocolRelationId, ptcId, newOwnerId);
	}

	heap_close(rel, NoLock);
}

/*
 * Change external protocol owner
 */
void
RenameExtProtocol(const char *oldname, const char *newname)
{
	HeapTuple	tup;
	Relation	rel;
	Oid			ptcId;
	Oid			ownerId;
	bool		isNull;
	cqContext	cqc;		
	cqContext  *pcqCtx;
	Datum		ownerDatum;
	
	/*
	 * Check the pg_extprotocol relation to be certain the protocol 
	 * is there. 
	 */
	rel = heap_open(ExtprotocolRelationId, RowExclusiveLock);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);
	
	tup = caql_getfirst(
			pcqCtx,
			cql("SELECT * FROM pg_extprotocol "
				" WHERE ptcname = :1 "
				" FOR UPDATE ",
				CStringGetDatum(oldname)));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("protocol \"%s\" does not exist",
						oldname)));
	
	ptcId = HeapTupleGetOid(tup);

	ownerDatum = caql_getattr(pcqCtx,
							  Anum_pg_extprotocol_ptcowner,
							  &isNull);
	
	if(isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("internal error: protocol \"%s\"  has no owner defined",
						 oldname)));	

	ownerId = DatumGetObjectId(ownerDatum);

	if (strcmp(oldname, newname) != 0)
	{
		Datum		values[Natts_pg_extprotocol];
		bool		nulls[Natts_pg_extprotocol];
		bool		replaces[Natts_pg_extprotocol];
		HeapTuple	newtuple;

		/* Superusers can always do it */
		if (!superuser())
		{
			/* Must be owner */
			if (!pg_extprotocol_ownercheck(ptcId, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EXTPROTOCOL,
							   oldname);
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_extprotocol_ptcname - 1] = true;
		values[Anum_pg_extprotocol_ptcname - 1] = CStringGetDatum(newname);
		
		newtuple = caql_modify_current(pcqCtx, values,
									   nulls, replaces);

		caql_update_current(pcqCtx, newtuple);
		/* and Update indexes (implicit) */

		heap_freetuple(newtuple);		
	}

	heap_close(rel, NoLock);
}
