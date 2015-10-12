/*-------------------------------------------------------------------------
 *
 * user.c
 *	  Commands for manipulating roles (formerly called users).
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/backend/commands/user.c,v 1.174.2.1 2010/03/25 14:45:21 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_auth_time_constraint.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resqueue.h"
#include "commands/comment.h"
#include "commands/user.h"
#include "libpq/auth.h"
#include "libpq/password_hash.h"
#include "libpq/md5.h"
#include "libpq/pg_sha2.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "executor/execdesc.h"
#include "utils/resscheduler.h"
#include "utils/syscache.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbsrlz.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbcat.h"
#include "cdb/dispatcher.h"

#include "resourcemanager/communication/rmcomm_QD2RM.h"
#include "resourcemanager/errorcode.h"

typedef struct genericPair
{
	char* key1;
	char* val1;
	char* key2;
	char* val2;
	
} genericPair;

typedef struct extAuthPair
{
	char* protocol;
	char* type;

} extAuthPair;

extern bool Password_encryption;

static List *roleNamesToIds(List *memberNames);
static void AddRoleMems(const char *rolename, Oid roleid,
			List *memberNames, List *memberIds,
			Oid grantorId, bool admin_opt);
static void DelRoleMems(const char *rolename, Oid roleid,
			List *memberNames, List *memberIds,
			bool admin_opt);
static void TransformExttabAuthClause(DefElem *defel, 
			extAuthPair *extauth);
static void SetCreateExtTableForRole(List* allow, 
			List* disallow, bool* createrextgpfd,
			bool* createrexthttp, bool* createwextgpfd,
			bool* createrexthdfs, bool* createwexthdfs);

static char *daysofweek[] = {"Sunday", "Monday", "Tuesday", "Wednesday",
							 "Thursday", "Friday", "Saturday"};
static int16 ExtractAuthInterpretDay(Value * day);
static void ExtractAuthIntervalClause(DefElem *defel,
			authInterval *authInterval);
static void AddRoleDenials(const char *rolename, Oid roleid,
			List *addintervals); 
static void DelRoleDenials(const char *rolename, Oid roleid, 
			List *dropintervals);
static bool resourceQueueIsBranch(Oid queueid);

/* Check if current user has createrole privileges */
static bool
have_createrole_privilege(void)
{
	bool		result = false;
	cqContext  *pcqCtx, cqc;
	HeapTuple	utup;

	/* Superusers can always do everything */
	if (superuser())
		return true;

	pcqCtx = 
			caql_beginscan(
					cqclr(&cqc),
					cql("SELECT * FROM pg_authid "
						" WHERE oid = :1 ",
						ObjectIdGetDatum(GetUserId())));

	if (HeapTupleIsValid(utup = caql_getnext(pcqCtx)))
	{
		result = ((Form_pg_authid) GETSTRUCT(utup))->rolcreaterole;
	}
	caql_endscan(pcqCtx);

	return result;
}

/*
 * If a resource queue(oid) is a branch
 */
static bool resourceQueueIsBranch(Oid queueid)
{
	HeapTuple		tuple = NULL;
	cqContext		cqc;
	cqContext  		*pcqCtx;
	Relation		pg_resqueue_rel;
	bool			res = false;
	Datum           status;
	bool            isNull = false;

	Assert(queueid != InvalidOid);
	pg_resqueue_rel = heap_open(ResQueueRelationId, RowExclusiveLock);
	pcqCtx = caql_addrel(cqclr(&cqc), pg_resqueue_rel);

	tuple = caql_getfirst(pcqCtx,
				  cql("SELECT * FROM pg_resqueue WHERE oid = :1",
					   ObjectIdGetDatum(queueid)));

	Assert(tuple != NULL);

	status = heap_getattr(tuple, Anum_pg_resqueue_status, RelationGetDescr(pg_resqueue_rel), &isNull);
	if (!isNull && strncmp(TextDatumGetCString(status), "branch", strlen("branch")) == 0)
		res = true;

	heap_close(pg_resqueue_rel, NoLock);
	return res;
}

/*
 * CREATE ROLE
 */
void
CreateRole(CreateRoleStmt *stmt)
{
	Relation	pg_authid_rel;
	HeapTuple	tuple;
	Datum		new_record[Natts_pg_authid];
	bool		new_record_nulls[Natts_pg_authid];
	Oid			roleid;
	ListCell   *item;
	ListCell   *option;
	char	   *password = NULL;	/* user password */
	bool		encrypt_password = Password_encryption; /* encrypt password? */
	char		encrypted_password[MAX_PASSWD_HASH_LEN + 1];
	bool		issuper = false;	/* Make the user a superuser? */
	bool		inherit = true; /* Auto inherit privileges? */
	bool		createrole = false;		/* Can this user create roles? */
	bool		createdb = false;		/* Can the user create databases? */
	bool		canlogin = false;		/* Can this user login? */
	bool		createrextgpfd = false; /* Can create readable gpfdist exttab? */
	bool		createrexthttp = false; /* Can create readable http exttab? */
	bool		createwextgpfd = false; /* Can create writable gpfdist exttab? */
	bool		createrexthdfs = false; /* Can create readable gphdfs exttab? */
	bool		createwexthdfs = false; /* Can create writable gphdfs exttab? */
	int			connlimit = -1; /* maximum connections allowed */
	List	   *addroleto = NIL;	/* roles to make this a member of */
	List	   *rolemembers = NIL;		/* roles to be members of this role */
	List	   *adminmembers = NIL;		/* roles to be admins of this role */
	List	   *exttabcreate = NIL;		/* external table create privileges being added  */
	List	   *exttabnocreate = NIL;	/* external table create privileges being removed */
	char	   *validUntil = NULL;		/* time the login is valid until */
	char	   *resqueue = NULL;		/* resource queue for this role */
	List	   *addintervals = NIL;	/* list of time intervals for which login should be denied */
	DefElem    *dpassword = NULL;
	DefElem    *dresqueue = NULL;
	DefElem    *dissuper = NULL;
	DefElem    *dinherit = NULL;
	DefElem    *dcreaterole = NULL;
	DefElem    *dcreatedb = NULL;
	DefElem    *dcanlogin = NULL;
	DefElem    *dconnlimit = NULL;
	DefElem    *daddroleto = NULL;
	DefElem    *drolemembers = NULL;
	DefElem    *dadminmembers = NULL;
	DefElem    *dvalidUntil = NULL;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pcqCtx;
	Oid		queueid = InvalidOid;

	int  		res 		= FUNC_RETURN_OK;
	static char errorbuf[1024] = "";

	/* The defaults can vary depending on the original statement type */
	switch (stmt->stmt_type)
	{
		case ROLESTMT_ROLE:
			break;
		case ROLESTMT_USER:
			canlogin = true;
			/* may eventually want inherit to default to false here */
			break;
		case ROLESTMT_GROUP:
			break;
	}

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "password") == 0 ||
			strcmp(defel->defname, "encryptedPassword") == 0 ||
			strcmp(defel->defname, "unencryptedPassword") == 0)
		{
			if (dpassword)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dpassword = defel;
			if (strcmp(defel->defname, "encryptedPassword") == 0)
				encrypt_password = true;
			else if (strcmp(defel->defname, "unencryptedPassword") == 0)
				encrypt_password = false;
		}
		else if (strcmp(defel->defname, "sysid") == 0)
		{
			if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
					(errmsg("SYSID can no longer be specified")));
		}
		else if (strcmp(defel->defname, "superuser") == 0)
		{
			if (dissuper)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dissuper = defel;
		}
		else if (strcmp(defel->defname, "inherit") == 0)
		{
			if (dinherit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dinherit = defel;
		}
		else if (strcmp(defel->defname, "createrole") == 0)
		{
			if (dcreaterole)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcreaterole = defel;
		}
		else if (strcmp(defel->defname, "createdb") == 0)
		{
			if (dcreatedb)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcreatedb = defel;
		}
		else if (strcmp(defel->defname, "canlogin") == 0)
		{
			if (dcanlogin)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcanlogin = defel;
		}
		else if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dconnlimit = defel;
		}
		else if (strcmp(defel->defname, "addroleto") == 0)
		{
			if (daddroleto)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			daddroleto = defel;
		}
		else if (strcmp(defel->defname, "rolemembers") == 0)
		{
			if (drolemembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			drolemembers = defel;
		}
		else if (strcmp(defel->defname, "adminmembers") == 0)
		{
			if (dadminmembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dadminmembers = defel;
		}
		else if (strcmp(defel->defname, "validUntil") == 0)
		{
			if (dvalidUntil)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dvalidUntil = defel;
		}
		else if (strcmp(defel->defname, "resourceQueue") == 0)
		{
			if (dresqueue)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dresqueue = defel;
		}
		else if (strcmp(defel->defname, "exttabauth") == 0)
		{
			extAuthPair *extauth = (extAuthPair *) palloc0 (2 * sizeof(char *));
			
			TransformExttabAuthClause(defel, extauth);
			
			/* now actually append our transformed key value pairs to the list */
			exttabcreate = lappend(exttabcreate, extauth);			
		}			  
		else if (strcmp(defel->defname, "exttabnoauth") == 0)
		{
			extAuthPair *extauth = (extAuthPair *) palloc0 (2 * sizeof(char *));
			
			TransformExttabAuthClause(defel, extauth);
			
			/* now actually append our transformed key value pairs to the list */
			exttabnocreate = lappend(exttabnocreate, extauth);
		}
		else if (strcmp(defel->defname, "deny") == 0) 
		{
			authInterval *interval = (authInterval *) palloc0(sizeof(authInterval));

			ExtractAuthIntervalClause(defel, interval);

			addintervals = lappend(addintervals, interval);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (dpassword && dpassword->arg)
		password = strVal(dpassword->arg);
	if (dissuper)
		issuper = intVal(dissuper->arg) != 0;
	if (dinherit)
		inherit = intVal(dinherit->arg) != 0;
	if (dcreaterole)
		createrole = intVal(dcreaterole->arg) != 0;
	if (dcreatedb)
		createdb = intVal(dcreatedb->arg) != 0;
	if (dcanlogin)
		canlogin = intVal(dcanlogin->arg) != 0;
	if (dconnlimit)
		connlimit = intVal(dconnlimit->arg);
	if (daddroleto)
		addroleto = (List *) daddroleto->arg;
	if (drolemembers)
		rolemembers = (List *) drolemembers->arg;
	if (dadminmembers)
		adminmembers = (List *) dadminmembers->arg;
	if (dvalidUntil)
		validUntil = strVal(dvalidUntil->arg);
	if (dresqueue)
		resqueue = strVal(linitial((List *) dresqueue->arg));
	else
	{
		/* MPP-6926: resource queue required -- use default queue  */
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			/* MPP-7587: don't complain if you CREATE a superuser,
			 * who doesn't use the queue 
			 */
			if (!issuper)
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("resource queue required -- "
								"using default resource queue \"%s\"",
								GP_DEFAULT_RESOURCE_QUEUE_NAME)));
		}

		resqueue = pstrdup(GP_DEFAULT_RESOURCE_QUEUE_NAME);
	}

	/* Check some permissions first */
	if (issuper)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create superusers")));
	}
	else
	{
		if (!have_createrole_privilege())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to create role")));
	}

	if (strcmp(stmt->role, "public") == 0 ||
		strcmp(stmt->role, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved",
						stmt->role)));

	/* Create a new resource context to manipulate role in resource manager. */
	int resourceid = 0;
	res = createNewResourceContext(&resourceid);
	if (res != FUNC_RETURN_OK) {
		Assert( res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT );
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Can not apply CREATE ROLE. "
								"Because too many resource contexts were created.")));
	}

	/* Here, using user oid is more convenient. */
	res = registerConnectionInRMByOID(resourceid,
									  GetUserId(),
									  errorbuf,
									  sizeof(errorbuf));
	if (res != FUNC_RETURN_OK)
	{
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
	}

	/*
	 * Check the pg_authid relation to be certain the role doesn't already
	 * exist.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);

	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), pg_authid_rel),
					cql("INSERT INTO pg_authid ",
						NULL));

	if (caql_getcount(
				caql_addrel(cqclr(&cqc2), pg_authid_rel),
				cql("SELECT COUNT(*) FROM pg_authid "
					" WHERE rolname = :1 ",
					PointerGetDatum(stmt->role)))) {
		unregisterConnectionInRMWithErrorReport(resourceid);
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role \"%s\" already exists",
						stmt->role)));
	}

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_authid_rolname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->role));

	new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper);
	new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit);
	new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole);
	new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb);
	/* superuser gets catupdate right by default */
	new_record[Anum_pg_authid_rolcatupdate - 1] = BoolGetDatum(issuper);
	new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin);
	new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);

	/* Set the CREATE EXTERNAL TABLE permissions for this role */
	if (exttabcreate || exttabnocreate)
		SetCreateExtTableForRole(exttabcreate, exttabnocreate, &createrextgpfd,
								 &createrexthttp, &createwextgpfd,
								 &createrexthdfs, &createwexthdfs);

	new_record[Anum_pg_authid_rolcreaterextgpfd - 1] = BoolGetDatum(createrextgpfd);
	new_record[Anum_pg_authid_rolcreaterexthttp - 1] = BoolGetDatum(createrexthttp);
	new_record[Anum_pg_authid_rolcreatewextgpfd - 1] = BoolGetDatum(createwextgpfd);
	new_record[Anum_pg_authid_rolcreaterexthdfs - 1] = BoolGetDatum(createrexthdfs);
	new_record[Anum_pg_authid_rolcreatewexthdfs - 1] = BoolGetDatum(createwexthdfs);
	
	if (password)
	{
		if (!encrypt_password || isHashedPasswd(password))
			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(password);
		else
		{
			if (!hash_password(password, stmt->role, strlen(stmt->role),
							   encrypted_password))
			{
				elog(ERROR, "password encryption failed");
			}

			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(encrypted_password);
		}
	}
	else
		new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;

	if (validUntil)
		new_record[Anum_pg_authid_rolvaliduntil - 1] =
			DirectFunctionCall3(timestamptz_in,
								CStringGetDatum(validUntil),
								ObjectIdGetDatum(InvalidOid),
								Int32GetDatum(-1));

	else
		new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = true;

	if (resqueue)
	{
		if (strcmp(resqueue, "none") == 0)
		{
			unregisterConnectionInRMWithErrorReport(resourceid);
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR,
					(errcode(ERRCODE_RESERVED_NAME),
					 errmsg("Resource queue name \"%s\" is reserved",
							resqueue), errOmitLocation(true)));
		}

		queueid = GetResQueueIdForName(resqueue);
		if (queueid == InvalidOid)
		{
			unregisterConnectionInRMWithErrorReport(resourceid);
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("Resource queue \"%s\" does not exist",
							resqueue), errOmitLocation(true)));
		}

		if(resourceQueueIsBranch(queueid))
		{
			unregisterConnectionInRMWithErrorReport(resourceid);
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("Can't assign a branch resource queue \"%s\" to role",
							resqueue), errOmitLocation(true)));
		}

		new_record[Anum_pg_authid_rolresqueue - 1] = 
		ObjectIdGetDatum(queueid);
	}
	else
		new_record_nulls[Anum_pg_authid_rolresqueue - 1] = true;

	new_record_nulls[Anum_pg_authid_rolconfig - 1] = true;

	tuple = caql_form_tuple(pcqCtx, new_record, new_record_nulls);


	if (stmt->roleOid != InvalidOid)
		/* force tuple to have the desired OID */
		HeapTupleSetOid(tuple, stmt->roleOid);
	
	/*
	 * Insert new record in the pg_authid table
	 */
	roleid = caql_insert(pcqCtx, tuple); /* implicit update of index as well */
	stmt->roleOid = roleid;

	/*
	 * send RPC: notify RM to update
	 */
	if (resqueue && queueid != InvalidOid)
	{
		res = manipulateRoleForResourceQueue(resourceid,
											 roleid,
											 queueid,
											 MANIPULATE_ROLE_RESQUEUE_CREATE,
											 issuper,
											 stmt->role,
											 errorbuf,
											 sizeof(errorbuf));
	}

	/* We always unregister connection. */
	unregisterConnectionInRMWithErrorReport(resourceid);

	/* We always release resource context. */
	releaseResourceContextWithErrorReport(resourceid);

	if (resqueue && queueid != InvalidOid)
	{
		if ( res != FUNC_RETURN_OK )
		{
			ereport(ERROR,
					(errcode(IS_TO_RM_RPC_ERROR(res) ?
							 ERRCODE_INTERNAL_ERROR :
							 ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("Can not apply CREATE ROLE because of %s", errorbuf)));
		}
	}

	/*
	 * Advance command counter so we can see new record; else tests in
	 * AddRoleMems may fail.
	 */
	if (addroleto || adminmembers || rolemembers)
		CommandCounterIncrement();

	/*
	 * Add the new role to the specified existing roles.
	 */
	foreach(item, addroleto)
	{
		char	   *oldrolename = strVal(lfirst(item));
		Oid			oldroleid = get_roleid_checked(oldrolename);

		AddRoleMems(oldrolename, oldroleid,
					list_make1(makeString(stmt->role)),
					list_make1_oid(roleid),
					GetUserId(), false);
	}

	/*
	 * Add the specified members to this new role. adminmembers get the admin
	 * option, rolemembers don't.
	 */
	AddRoleMems(stmt->role, roleid,
				adminmembers, roleNamesToIds(adminmembers),
				GetUserId(), true);
	AddRoleMems(stmt->role, roleid,
				rolemembers, roleNamesToIds(rolemembers),
				GetUserId(), false);

	/*
	 * Populate pg_auth_time_constraint with intervals for which this 
	 * particular role should be denied access.
	 */
	if (addintervals)
	{
		if (issuper)
			ereport(ERROR,
					(errmsg("cannot create superuser with DENY rules")));
		AddRoleDenials(stmt->role, roleid, addintervals);
	}

	/*
	 * Close pg_authid, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	caql_endscan(pcqCtx);
	heap_close(pg_authid_rel, NoLock);

	/*
	 * Set flag to update flat auth file at commit.
	 */
	auth_file_update_needed();

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		Assert(stmt->type == T_CreateRoleStmt);
		Assert(stmt->type < 1000);
		/* GPSQL: no dispatch to segments */
		/* CdbDispatchUtilityStatement((Node *) stmt, "CreateRole"); */

		/* MPP-6929: metadata tracking */
		MetaTrackAddObject(AuthIdRelationId,
						   roleid,
						   GetUserId(),
						   "CREATE", "ROLE"
				);
	}
}


/*
 * ALTER ROLE
 *
 * Note: the rolemembers option accepted here is intended to support the
 * backwards-compatible ALTER GROUP syntax.  Although it will work to say
 * "ALTER ROLE role ROLE rolenames", we don't document it.
 */
void
AlterRole(AlterRoleStmt *stmt)
{
	Datum		 new_record[Natts_pg_authid];
	bool		 new_record_nulls[Natts_pg_authid];
	bool		 new_record_repl[Natts_pg_authid];
	Relation	 pg_authid_rel;
	TupleDesc	 pg_authid_dsc;
	HeapTuple	 tuple,
				 new_tuple;
	ListCell	*option;
	char		*password	   = NULL;	/* user password */
	bool		 encrypt_password 
							   = Password_encryption;	/* encrypt password? */
	char		 encrypted_password[MAX_PASSWD_HASH_LEN + 1];
	int			 issuper	   = -1;	/* Make the user a superuser? */
	int			 inherit	   = -1;	/* Auto inherit privileges? */
	int			 createrole	   = -1;	/* Can this user create roles? */
	int			 createdb	   = -1;	/* Can the user create databases? */
	int			 canlogin	   = -1;	/* Can this user login? */
	int			 connlimit	   = -1;	/* maximum connections allowed */
	char		*resqueue	   = NULL;	/* resource queue for this role */
	List		*rolemembers   = NIL;	/* roles to be added/removed */
	List	    *exttabcreate  = NIL;	/* external table create privileges being added  */
	List	    *exttabnocreate = NIL;	/* external table create privileges being removed */
	char		*validUntil	   = NULL;	/* time the login is valid until */
	DefElem		*dpassword	   = NULL;
	DefElem		*dresqueue	   = NULL;
	DefElem		*dissuper	   = NULL;
	DefElem		*dinherit	   = NULL;
	DefElem		*dcreaterole   = NULL;
	DefElem		*dcreatedb	   = NULL;
	DefElem		*dcanlogin	   = NULL;
	DefElem		*dconnlimit	   = NULL;
	DefElem		*drolemembers  = NULL;
	DefElem		*dvalidUntil   = NULL;
	Oid			 roleid;
	bool		 bWas_super	   = false;	/* Was the user a superuser? */
	int			 numopts	   = 0;
	char		*alter_subtype = "";	/* metadata tracking: kind of
										   redundant to say "role" */
	bool		 createrextgpfd;
	bool 		 createrexthttp;
	bool		 createwextgpfd;
	bool 		 createrexthdfs;
	bool		 createwexthdfs;
	List		*addintervals = NIL;    /* list of time intervals for which login should be denied */
	List		*dropintervals = NIL;    /* list of time intervals for which matching rules should be dropped */

	cqContext	 cqc;
	cqContext	*pcqCtx;
	Oid			 queueid = InvalidOid;

	int  		 res 			= FUNC_RETURN_OK;
	static char  errorbuf[1024] = "";

	numopts = list_length(stmt->options);

	if (numopts > 1)
	{
		char allopts[NAMEDATALEN];

		sprintf(allopts, "%d OPTIONS", numopts);

		alter_subtype = pstrdup(allopts);
	}
	else if (0 == numopts)
	{
		alter_subtype = "0 OPTIONS";
	}

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "password") == 0 ||
			strcmp(defel->defname, "encryptedPassword") == 0 ||
			strcmp(defel->defname, "unencryptedPassword") == 0)
		{
			if (dpassword)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dpassword = defel;
			if (strcmp(defel->defname, "encryptedPassword") == 0)
				encrypt_password = true;
			else if (strcmp(defel->defname, "unencryptedPassword") == 0)
				encrypt_password = false;

			if (1 == numopts) alter_subtype = "PASSWORD";
		}
		else if (strcmp(defel->defname, "superuser") == 0)
		{
			if (dissuper)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dissuper = defel;
			if (1 == numopts) alter_subtype = "SUPERUSER";
		}
		else if (strcmp(defel->defname, "inherit") == 0)
		{
			if (dinherit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dinherit = defel;
			if (1 == numopts) alter_subtype = "INHERIT";
		}
		else if (strcmp(defel->defname, "createrole") == 0)
		{
			if (dcreaterole)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dcreaterole = defel;
			if (1 == numopts) alter_subtype = "CREATEROLE";
		}
		else if (strcmp(defel->defname, "createdb") == 0)
		{
			if (dcreatedb)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dcreatedb = defel;
			if (1 == numopts) alter_subtype = "CREATEDB";
		}
		else if (strcmp(defel->defname, "canlogin") == 0)
		{
			if (dcanlogin)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dcanlogin = defel;
			if (1 == numopts) alter_subtype = "LOGIN";
		}
		else if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dconnlimit = defel;
			if (1 == numopts) alter_subtype = "CONNECTION LIMIT";
		}
		else if (strcmp(defel->defname, "rolemembers") == 0 &&
				 stmt->action != 0)
		{
			if (drolemembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			drolemembers = defel;
			if (1 == numopts) alter_subtype = "ROLE";
		}
		else if (strcmp(defel->defname, "validUntil") == 0)
		{
			if (dvalidUntil)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dvalidUntil = defel;
			if (1 == numopts) alter_subtype = "VALID UNTIL";
		}
		else if (strcmp(defel->defname, "resourceQueue") == 0)
		{
			if (dresqueue)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
								   errOmitLocation(true)));
			dresqueue = defel;
			if (1 == numopts) alter_subtype = "RESOURCE QUEUE";
		}
		else if (strcmp(defel->defname, "exttabauth") == 0)
		{
			extAuthPair *extauth = (extAuthPair *) palloc0 (2 * sizeof(char *));
			
			TransformExttabAuthClause(defel, extauth);
			
			/* now actually append our transformed key value pairs to the list */
			exttabcreate = lappend(exttabcreate, extauth);	
			
			if (1 == numopts) alter_subtype = "CREATEEXTTABLE";
		}			  
		else if (strcmp(defel->defname, "exttabnoauth") == 0)
		{
			extAuthPair *extauth = (extAuthPair *) palloc0 (2 * sizeof(char *));
			
			TransformExttabAuthClause(defel, extauth);
			
			/* now actually append our transformed key value pairs to the list */
			exttabnocreate = lappend(exttabnocreate, extauth);
			
			if (1 == numopts) alter_subtype = "NO CREATEEXTTABLE";
		}
		else if (strcmp(defel->defname, "deny") == 0)
		{
			authInterval *interval = (authInterval *) palloc0(sizeof(authInterval));

			ExtractAuthIntervalClause(defel, interval);

			addintervals = lappend(addintervals, interval);
		}
		else if (strcmp(defel->defname, "drop_deny") == 0)
		{
			authInterval *interval = (authInterval *) palloc0(sizeof(authInterval));
		
			ExtractAuthIntervalClause(defel, interval);
	
			dropintervals = lappend(dropintervals, interval);
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (dpassword && dpassword->arg)
		password = strVal(dpassword->arg);
	if (dissuper)
		issuper = intVal(dissuper->arg);
	if (dinherit)
		inherit = intVal(dinherit->arg);
	if (dcreaterole)
		createrole = intVal(dcreaterole->arg);
	if (dcreatedb)
		createdb = intVal(dcreatedb->arg);
	if (dcanlogin)
		canlogin = intVal(dcanlogin->arg);
	if (dconnlimit)
		connlimit = intVal(dconnlimit->arg);
	if (drolemembers)
		rolemembers = (List *) drolemembers->arg;
	if (dvalidUntil)
		validUntil = strVal(dvalidUntil->arg);
	if (dresqueue)
		resqueue = strVal(linitial((List *) dresqueue->arg));

	/*
	 * create a new context
	 */
	int resourceid = 0;
    res = createNewResourceContext(&resourceid);
    if ( res != FUNC_RETURN_OK )
    {
    	Assert( res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT );
    	ereport(ERROR,
    			(errcode(ERRCODE_INTERNAL_ERROR),
    			         errmsg("Too many existing resource context.")));
    }

	/* Here, using user oid is more convenient. */
	res = registerConnectionInRMByOID(resourceid,
									  GetUserId(),
									  errorbuf,
									  sizeof(errorbuf));
	if (res != FUNC_RETURN_OK)
	{
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
	}

	/*
	 * Scan the pg_authid relation to be certain the user exists.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	pg_authid_dsc = RelationGetDescr(pg_authid_rel);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), pg_authid_rel),
			cql("SELECT * FROM pg_authid "
				" WHERE rolname = :1 "
				" FOR UPDATE ",
				PointerGetDatum(stmt->role)));

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple)) {
		releaseResourceContextWithErrorReport(resourceid);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", stmt->role)));
	}

	roleid = HeapTupleGetOid(tuple);

	/*
	 * To mess with a superuser you gotta be superuser; else you need
	 * createrole, or just want to change your own password
	 */

	bWas_super = ((Form_pg_authid) GETSTRUCT(tuple))->rolsuper;

	if (((Form_pg_authid) GETSTRUCT(tuple))->rolsuper || issuper >= 0)
	{
		if (!superuser()) {
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
		}
	}
	else if (!have_createrole_privilege())
	{
		bool is_createrol_required = !(inherit < 0 &&
									   createrole < 0 &&
									   createdb < 0 &&
									   canlogin < 0 &&
									   !dconnlimit &&
									   !rolemembers &&
									   !validUntil &&
									   dpassword &&
									   !exttabcreate &&
									   !exttabnocreate &&
									   roleid == GetUserId());
		
		if (is_createrol_required) {
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied"),
							   errOmitLocation(true)));
		}
	}

	/*
	 * Build an updated tuple, perusing the information just obtained
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/*
	 * issuper/createrole/catupdate/etc
	 *
	 * XXX It's rather unclear how to handle catupdate.  It's probably best to
	 * keep it equal to the superuser status, otherwise you could end up with
	 * a situation where no existing superuser can alter the catalogs,
	 * including pg_authid!
	 */
	if (issuper >= 0)
	{
		new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper > 0);
		new_record_repl[Anum_pg_authid_rolsuper - 1] = true;

		new_record[Anum_pg_authid_rolcatupdate - 1] = BoolGetDatum(issuper > 0);
		new_record_repl[Anum_pg_authid_rolcatupdate - 1] = true;

		bWas_super = (issuper > 0); /* get current superuser status */
	}

	if (inherit >= 0)
	{
		new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit > 0);
		new_record_repl[Anum_pg_authid_rolinherit - 1] = true;
	}

	if (createrole >= 0)
	{
		new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole > 0);
		new_record_repl[Anum_pg_authid_rolcreaterole - 1] = true;
	}

	if (createdb >= 0)
	{
		new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb > 0);
		new_record_repl[Anum_pg_authid_rolcreatedb - 1] = true;
	}

	if (canlogin >= 0)
	{
		new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin > 0);
		new_record_repl[Anum_pg_authid_rolcanlogin - 1] = true;
	}

	if (dconnlimit)
	{
		new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);
		new_record_repl[Anum_pg_authid_rolconnlimit - 1] = true;
	}

	/* password */
	if (password)
	{
		if (!encrypt_password || isHashedPasswd(password))
			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(password);
		else
		{
			
			if (!hash_password(password, stmt->role, strlen(stmt->role),
							   encrypted_password))
				elog(ERROR, "password encryption failed");

			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(encrypted_password);
		}
		new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
	}

	/* unset password */
	if (dpassword && dpassword->arg == NULL)
	{
		new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
		new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
	}

	/* valid until */
	if (validUntil)
	{
		new_record[Anum_pg_authid_rolvaliduntil - 1] =
			DirectFunctionCall3(timestamptz_in,
								CStringGetDatum(validUntil),
								ObjectIdGetDatum(InvalidOid),
								Int32GetDatum(-1));
		new_record_repl[Anum_pg_authid_rolvaliduntil - 1] = true;
	}

	/* Set the CREATE EXTERNAL TABLE permissions for this role, if specified in ALTER */	
	if (exttabcreate || exttabnocreate)
	{
		bool	isnull;
		Datum 	dcreaterextgpfd;
		Datum 	dcreaterexthttp;
		Datum 	dcreatewextgpfd;
		Datum 	dcreaterexthdfs;
		Datum 	dcreatewexthdfs;

		/* 
		 * get bool values from catalog. we don't ever expect a NULL value, but just
		 * in case it is there (perhaps after an upgrade) we treat it as 'false'.
		 */
		dcreaterextgpfd = heap_getattr(tuple, Anum_pg_authid_rolcreaterextgpfd, pg_authid_dsc, &isnull);
		createrextgpfd = (isnull ? false : DatumGetBool(dcreaterextgpfd));
		dcreaterexthttp = heap_getattr(tuple, Anum_pg_authid_rolcreaterexthttp, pg_authid_dsc, &isnull);
		createrexthttp = (isnull ? false : DatumGetBool(dcreaterexthttp));
		dcreatewextgpfd = heap_getattr(tuple, Anum_pg_authid_rolcreatewextgpfd, pg_authid_dsc, &isnull);
		createwextgpfd = (isnull ? false : DatumGetBool(dcreatewextgpfd));
		dcreaterexthdfs = heap_getattr(tuple, Anum_pg_authid_rolcreaterexthdfs, pg_authid_dsc, &isnull);
		createrexthdfs = (isnull ? false : DatumGetBool(dcreaterexthdfs));
		dcreatewexthdfs = heap_getattr(tuple, Anum_pg_authid_rolcreatewexthdfs, pg_authid_dsc, &isnull);
		createwexthdfs = (isnull ? false : DatumGetBool(dcreatewexthdfs));
		
		SetCreateExtTableForRole(exttabcreate, exttabnocreate, &createrextgpfd,
								 &createrexthttp, &createwextgpfd,
								 &createrexthdfs, &createwexthdfs);

		new_record[Anum_pg_authid_rolcreaterextgpfd - 1] = BoolGetDatum(createrextgpfd);
		new_record_repl[Anum_pg_authid_rolcreaterextgpfd - 1] = true;
		new_record[Anum_pg_authid_rolcreaterexthttp - 1] = BoolGetDatum(createrexthttp);
		new_record_repl[Anum_pg_authid_rolcreaterexthttp - 1] = true;
		new_record[Anum_pg_authid_rolcreatewextgpfd - 1] = BoolGetDatum(createwextgpfd);
		new_record_repl[Anum_pg_authid_rolcreatewextgpfd - 1] = true;
		new_record[Anum_pg_authid_rolcreaterexthdfs - 1] = BoolGetDatum(createrexthdfs);
		new_record_repl[Anum_pg_authid_rolcreaterexthdfs - 1] = true;
		new_record[Anum_pg_authid_rolcreatewexthdfs - 1] = BoolGetDatum(createwexthdfs);
		new_record_repl[Anum_pg_authid_rolcreatewexthdfs - 1] = true;
	}

	/* resource queue */
	if (resqueue)
	{

		/* MPP-6926: NONE not supported -- use default queue  */
		if (
/*			( 0 == pg_strcasecmp(resqueue,"none"))) */
			( 0 == strcmp(resqueue,"none")))
		{
			/* MPP-7587: don't complain if you ALTER a superuser,
			 * who doesn't use the queue 
			 */
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				if (!bWas_super)
						ereport(NOTICE,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("resource queue required -- "
										"using default resource queue \"%s\"",
										GP_DEFAULT_RESOURCE_QUEUE_NAME)));
			}
			
			resqueue = pstrdup(GP_DEFAULT_RESOURCE_QUEUE_NAME);
		}

		if ( strcmp(resqueue, "none") == 0)
		{
			new_record_nulls[Anum_pg_authid_rolresqueue - 1] = true;
		}
		else
		{
			queueid = GetResQueueIdForName(resqueue);
			if (queueid == InvalidOid) {
				releaseResourceContextWithErrorReport(resourceid);
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("resource queue \"%s\" does not exist",
							resqueue),
									   errOmitLocation(true)));
			}

			if(resourceQueueIsBranch(queueid)) {
				releaseResourceContextWithErrorReport(resourceid);
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("can't assign a branch resource queue \"%s\" to role",
								resqueue),
										   errOmitLocation(true)));
			}
			new_record[Anum_pg_authid_rolresqueue - 1] = 
			ObjectIdGetDatum(GetResQueueIdForName(resqueue));
		}
		new_record_repl[Anum_pg_authid_rolresqueue - 1] = true;
	}


	new_tuple = caql_modify_current(pcqCtx, new_record,
									new_record_nulls, new_record_repl);
	caql_update_current(pcqCtx, new_tuple);
	/* and Update indexes (implicit) */

	caql_endscan(pcqCtx);
	heap_freetuple(new_tuple);

	/*
	 * send RPC: notify RM to update
	 */
	if (resqueue && queueid != InvalidOid)
	{
		res = manipulateRoleForResourceQueue (resourceid,
											  roleid,
											  queueid,
											  MANIPULATE_ROLE_RESQUEUE_ALTER,
											  issuper,
											  stmt->role,
											  errorbuf,
											  sizeof(errorbuf));
	}

	/* We always unregister connection. */
	unregisterConnectionInRMWithErrorReport(resourceid);

	/* We always release resource context. */
	releaseResourceContextWithErrorReport(resourceid);

	if (resqueue && queueid != InvalidOid)
	{
		if ( res != FUNC_RETURN_OK )
		{

			ereport(ERROR,
					(errcode(IS_TO_RM_RPC_ERROR(res) ?
							 ERRCODE_INTERNAL_ERROR :
							 ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("Can not apply ALTER ROLE because of %s", errorbuf)));
		}
	}

	/*
	 * Advance command counter so we can see new record; else tests in
	 * AddRoleMems may fail.
	 */
	if (rolemembers)
		CommandCounterIncrement();

	if (stmt->action == +1)		/* add members to role */
	{
		if (rolemembers)
			alter_subtype = "ADD USER";
		AddRoleMems(stmt->role, roleid,
					rolemembers, roleNamesToIds(rolemembers),
					GetUserId(), false);
	}
	else if (stmt->action == -1)	/* drop members from role */
	{
		if (rolemembers)
			alter_subtype = "DROP USER";
		DelRoleMems(stmt->role, roleid,
					rolemembers, roleNamesToIds(rolemembers),
					false);
	}

	if (bWas_super)
	{
		if (addintervals)
			ereport(ERROR,
					(errmsg("cannot alter superuser with DENY rules")));
		else
			DelRoleDenials(stmt->role, roleid, NIL);	/* drop all preexisting constraints, if any. */
	}

	/*
	 * Disallow the use of DENY and DROP DENY fragments in the same query.
	 *
	 * We do this to prevent commands with unusual behavior.
	 * e.g. consider "ALTER ROLE foo DENY DAY 0 DROP DENY FOR DAY 1 DENY DAY 1 DENY DAY 2"
	 * In the manner that this is currently coded, because all DENY fragments are interpreted
	 * first, this actually becomes equivalent to you "ALTER ROLE foo DENY DAY 0 DENY DAY 2".
	 * 
	 * Instead, we could honor the order in which the fragments are presented, but still that
	 * allows users to contradict themselves, as in the example given.
	 */
	if (addintervals && dropintervals)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("conflicting or redundant options"),
				 errhint("DENY and DROP DENY cannot be used in the same ALTER ROLE statement.")));

	/*
	 * Populate pg_auth_time_constraint with the new intervals for which this 
	 * particular role should be denied access.
	 */
	if (addintervals)
		AddRoleDenials(stmt->role, roleid, addintervals);

	/*
	 * Remove pg_auth_time_constraint entries that overlap with the 
	 * intervals given by the user.
	 */
	if (dropintervals)
		DelRoleDenials(stmt->role, roleid, dropintervals);

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(AuthIdRelationId,
						   roleid,
						   GetUserId(),
						   "ALTER", alter_subtype
				);

	/*
	 * Close pg_authid, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_authid_rel, NoLock);

	/*
	 * Set flag to update flat auth file at commit.
	 */
	auth_file_update_needed();

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* GPSQL: no dispatch to segments */
		/*CdbDispatchUtilityStatement((Node *) stmt, "AlterRole");*/
	}
}


/*
 * ALTER ROLE ... SET
 */
void
AlterRoleSet(AlterRoleSetStmt *stmt)
{
	char	   *valuestr;
	HeapTuple	oldtuple,
				newtuple;
	Datum		repl_val[Natts_pg_authid];
	bool		repl_null[Natts_pg_authid];
	bool		repl_repl[Natts_pg_authid];
	char	   *alter_subtype = "SET"; /* metadata tracking */
	cqContext  *pcqCtx;

	valuestr = flatten_set_variable_args(stmt->variable, stmt->value);

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_authid "
				" WHERE rolname = :1 "
				" FOR UPDATE ",
				PointerGetDatum(stmt->role)));

	oldtuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(oldtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", stmt->role),
						   errOmitLocation(true)));

	/*
	 * To mess with a superuser you gotta be superuser; else you need
	 * createrole, or just want to change your own settings
	 */
	if (((Form_pg_authid) GETSTRUCT(oldtuple))->rolsuper)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers"),
							   errOmitLocation(true)));
	}
	else
	{
		if (!have_createrole_privilege() &&
			HeapTupleGetOid(oldtuple) != GetUserId())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied"),
							   errOmitLocation(true)));
	}

	memset(repl_repl, false, sizeof(repl_repl));
	repl_repl[Anum_pg_authid_rolconfig - 1] = true;

	if (strcmp(stmt->variable, "all") == 0 && valuestr == NULL)
	{
		alter_subtype = "RESET ALL";

		ArrayType  *new = NULL;
		Datum		datum;
		bool		isnull;

		/*
		 * in RESET ALL, request GUC to reset the settings array; if none
		 * left, we can set rolconfig to null; otherwise use the returned
		 * array
		 */
		datum = caql_getattr(pcqCtx,
							 Anum_pg_authid_rolconfig, &isnull);
		if (!isnull)
			new = GUCArrayReset(DatumGetArrayTypeP(datum));
		if (new)
		{
			repl_val[Anum_pg_authid_rolconfig - 1] = PointerGetDatum(new);
			repl_null[Anum_pg_authid_rolconfig - 1] = false;
		}
		else
		{
			repl_null[Anum_pg_authid_rolconfig - 1] = true;
			repl_val[Anum_pg_authid_rolconfig - 1] = (Datum) 0;
		}
	}
	else
	{
		Datum		datum;
		bool		isnull;
		ArrayType  *array;

		repl_null[Anum_pg_authid_rolconfig - 1] = false;

		/* Extract old value of rolconfig */
		datum = caql_getattr(pcqCtx,
							 Anum_pg_authid_rolconfig, &isnull);
		array = isnull ? NULL : DatumGetArrayTypeP(datum);

		/* Update (valuestr is NULL in RESET cases) */
		if (valuestr)
			array = GUCArrayAdd(array, stmt->variable, valuestr);
		else
		{
			alter_subtype = "RESET";
			array = GUCArrayDelete(array, stmt->variable);
		}

		if (array)
			repl_val[Anum_pg_authid_rolconfig - 1] = PointerGetDatum(array);
		else
			repl_null[Anum_pg_authid_rolconfig - 1] = true;
	}

	newtuple = caql_modify_current(pcqCtx, 
								   repl_val, repl_null, repl_repl);

	caql_update_current(pcqCtx, newtuple);
	/* and Update indexes (implicit) */

	if (Gp_role == GP_ROLE_DISPATCH)
		/* MPP-6929: metadata tracking */
		MetaTrackUpdObject(AuthIdRelationId,
						   HeapTupleGetOid(oldtuple),
						   GetUserId(),
						   "ALTER", alter_subtype
				);

	caql_endscan(pcqCtx);
	/* needn't keep lock since we won't be updating the flat file */

}


/*
 * DROP ROLE
 */
void
DropRole(DropRoleStmt *stmt)
{
	Relation	pg_authid_rel,
				pg_auth_members_rel;
	ListCell   *item;

	int  		res			   = FUNC_RETURN_OK;
	static char errorbuf[1024] = "";

	if (!have_createrole_privilege())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to drop role"),
						   errOmitLocation(true)));
	/*
	 * Scan the pg_authid relation to find the Oid of the role(s) to be
	 * deleted.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	pg_auth_members_rel = heap_open(AuthMemRelationId, RowExclusiveLock);

	foreach(item, stmt->roles)
	{
		const char *role = strVal(lfirst(item));
		HeapTuple	tuple;
		char	   *detail;
		Oid			roleid;
		cqContext	cqc;
		cqContext  *pcqCtx;

		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), pg_authid_rel),
				cql("SELECT * FROM pg_authid "
					" WHERE rolname = :1 "
					" FOR UPDATE ",
					PointerGetDatum((char *) role)));

		tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(tuple))
		{
			if (!stmt->missing_ok)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("role \"%s\" does not exist", role),
								   errOmitLocation(true)));
			}
			else
			{
				ereport(NOTICE,
						(errmsg("role \"%s\" does not exist, skipping",
								role),
										   errOmitLocation(true)));
			}

			continue;
		}

		roleid = HeapTupleGetOid(tuple);

		if (roleid == GetUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("current user cannot be dropped"),
							   errOmitLocation(true)));
		if (roleid == GetOuterUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("current user cannot be dropped"),
							   errOmitLocation(true)));
		if (roleid == GetSessionUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("session user cannot be dropped"),
							   errOmitLocation(true)));

		/*
		 * For safety's sake, we allow createrole holders to drop ordinary
		 * roles but not superuser roles.  This is mainly to avoid the
		 * scenario where you accidentally drop the last superuser.
		 */
		if (((Form_pg_authid) GETSTRUCT(tuple))->rolsuper &&
			!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to drop superusers")));

		/*
		 * Lock the role, so nobody can add dependencies to her while we drop
		 * her.  We keep the lock until the end of transaction.
		 */
		LockSharedObject(AuthIdRelationId, roleid, 0, AccessExclusiveLock);

		/* Check for pg_shdepend entries depending on this role */
		if ((detail = checkSharedDependencies(AuthIdRelationId, roleid)) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("role \"%s\" cannot be dropped because some objects depend on it",
							role),
					 errdetail("%s", detail),
					 errOmitLocation(true)));

		/*
		 * Remove the role from the pg_authid table
		 */
		caql_delete_current(pcqCtx);

		caql_endscan(pcqCtx);

		/* Create a new resource context to manipulate role in resource manager. */
		int resourceid = 0;
		res = createNewResourceContext(&resourceid);
		if ( res != FUNC_RETURN_OK ) {
			Assert( res == COMM2RM_CLIENT_FULL_RESOURCECONTEXT );
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Can not apply DROP ROLE. "
									"Because too many resource contexts were created.")));
		}

		/* Here, using user oid is more convenient. */
		res = registerConnectionInRMByOID(resourceid,
										  GetUserId(),
										  errorbuf,
										  sizeof(errorbuf));
		if (res != FUNC_RETURN_OK)
		{
			releaseResourceContextWithErrorReport(resourceid);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", errorbuf)));
		}

		/* Notify resource manager to drop role. */
		res = manipulateRoleForResourceQueue (resourceid,
											  roleid,
											  0, 		// not used when drop role
											  MANIPULATE_ROLE_RESQUEUE_DROP,
											  0,		// not used when drop role
											  (char*)role,
											  errorbuf,
											  sizeof(errorbuf));
		/* We always unregister connection. */
		unregisterConnectionInRMWithErrorReport(resourceid);

		/* We always release resource context. */
		releaseResourceContextWithErrorReport(resourceid);

		if ( res != FUNC_RETURN_OK )
		{
			ereport(ERROR,
					(errcode(IS_TO_RM_RPC_ERROR(res) ?
							 ERRCODE_INTERNAL_ERROR :
							 ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("Can not apply DROP ROLE because of %s", errorbuf)));
		}

		/*
		 * Remove role from the pg_auth_members table.	We have to remove all
		 * tuples that show it as either a role or a member.
		 *
		 * XXX what about grantor entries?	Maybe we should do one heap scan.
		 */
		{
			int numDel;
			cqContext cqc2;

			numDel = 
					caql_getcount(
							caql_addrel(cqclr(&cqc2), pg_auth_members_rel),
							cql("DELETE FROM pg_auth_members "
								" WHERE roleid = :1 ",
								ObjectIdGetDatum(roleid)));

			numDel = 
					caql_getcount(
							caql_addrel(cqclr(&cqc2), pg_auth_members_rel),
							cql("DELETE FROM pg_auth_members "
								" WHERE member = :1 ",
								ObjectIdGetDatum(roleid)));

		}

		/*
		 * Remove any time constraints on this role.
		 */
		DelRoleDenials(role, roleid, NIL);

		/*
		 * Remove any comments on this role.
		 */
		DeleteSharedComments(roleid, AuthIdRelationId);

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
			MetaTrackDropObject(AuthIdRelationId,
								roleid);
		/*
		 * Advance command counter so that later iterations of this loop will
		 * see the changes already made.  This is essential if, for example,
		 * we are trying to drop both a role and one of its direct members ---
		 * we'll get an error if we try to delete the linking pg_auth_members
		 * tuple twice.  (We do not need a CCI between the two delete loops
		 * above, because it's not allowed for a role to directly contain
		 * itself.)
		 */
		CommandCounterIncrement();
	}

	/*
	 * Now we can clean up; but keep locks until commit (to avoid possible
	 * deadlock failure while updating flat file)
	 */
	heap_close(pg_auth_members_rel, NoLock);
	heap_close(pg_authid_rel, NoLock);

	/*
	 * Set flag to update flat auth file at commit.
	 */
	auth_file_update_needed();

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* GPSQL: no dispatch to segments */
		/* CdbDispatchUtilityStatement((Node *) stmt, "DropRole"); */

	}
}

/*
 * Rename role
 */
void
RenameRole(const char *oldname, const char *newname)
{
	HeapTuple	oldtuple,
				newtuple;
	TupleDesc	dsc;
	Relation	rel;
	Datum		datum;
	bool		isnull;
	Datum		repl_val[Natts_pg_authid];
	bool		repl_null[Natts_pg_authid];
	bool		repl_repl[Natts_pg_authid];
	int			i;
	Oid			roleid;
	cqContext	cqc;
	cqContext	cqc2;
	cqContext  *pcqCtx;

	rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	dsc = RelationGetDescr(rel);

	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel),
			cql("SELECT * FROM pg_authid "
				" WHERE rolname = :1 "
				" FOR UPDATE ",
				CStringGetDatum((char *) oldname)));

	oldtuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(oldtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", oldname),
				 errOmitLocation(true)));

	/*
	 * XXX Client applications probably store the session user somewhere, so
	 * renaming it could cause confusion.  On the other hand, there may not be
	 * an actual problem besides a little confusion, so think about this and
	 * decide.	Same for SET ROLE ... we don't restrict renaming the current
	 * effective userid, though.
	 */

	roleid = HeapTupleGetOid(oldtuple);

	if (roleid == GetSessionUserId())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("session user cannot be renamed")));
	if (roleid == GetOuterUserId())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("current user cannot be renamed")));

	/* make sure the new name doesn't exist */
	if (caql_getcount(
				caql_addrel(cqclr(&cqc2), rel),
				cql("SELECT COUNT(*) FROM pg_authid "
					" WHERE rolname = :1 ",
					CStringGetDatum((char *) newname))))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role \"%s\" already exists", newname)));

	if (strcmp(newname, "public") == 0 ||
		strcmp(newname, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved",
						newname)));

	/*
	 * createrole is enough privilege unless you want to mess with a superuser
	 */
	if (((Form_pg_authid) GETSTRUCT(oldtuple))->rolsuper)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to rename superusers")));
	}
	else
	{
		if (!have_createrole_privilege())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to rename role")));
	}

	/* OK, construct the modified tuple */
	for (i = 0; i < Natts_pg_authid; i++)
		repl_repl[i] = false;

	repl_repl[Anum_pg_authid_rolname - 1] = true;
	repl_val[Anum_pg_authid_rolname - 1] = DirectFunctionCall1(namein,
												   CStringGetDatum((char *) newname));
	repl_null[Anum_pg_authid_rolname - 1] = false;

	datum = heap_getattr(oldtuple, Anum_pg_authid_rolpassword, dsc, &isnull);

	if (!isnull && isMD5(TextDatumGetCString(datum)))
	{
		/* MD5 uses the username as salt, so just clear it on a rename */
		repl_repl[Anum_pg_authid_rolpassword - 1] = true;
		repl_null[Anum_pg_authid_rolpassword - 1] = true;

		if (Gp_role != GP_ROLE_EXECUTE)
		ereport(NOTICE,
				(errmsg("MD5 password cleared because of role rename")));
	}

	newtuple = caql_modify_current(pcqCtx, repl_val, repl_null, repl_repl);
	caql_update_current(pcqCtx, newtuple); 
	/* and Update indexes (implicit) */

	caql_endscan(pcqCtx);

	/*
	 * Close pg_authid, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(rel, NoLock);

	/*
	 * Set flag to update flat auth file at commit.
	 */
	auth_file_update_needed();

	/* MPP-6929: metadata tracking */
	if (Gp_role == GP_ROLE_DISPATCH)
		MetaTrackUpdObject(AuthIdRelationId,
						   roleid,
						   GetUserId(),
						   "ALTER", "RENAME"
				);

}

/*
 * GrantRoleStmt
 *
 * Grant/Revoke roles to/from roles
 */
void
GrantRole(GrantRoleStmt *stmt)
{
	Relation	 pg_authid_rel;
	Oid			 grantor;
	List		*grantee_ids;
	ListCell	*item;

	if (stmt->grantor)
		grantor = get_roleid_checked(stmt->grantor);
	else
		grantor = GetUserId();

	grantee_ids = roleNamesToIds(stmt->grantee_roles);

	/* AccessShareLock is enough since we aren't modifying pg_authid */
	pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);

	/*
	 * Step through all of the granted roles and add/remove entries for the
	 * grantees, or, if admin_opt is set, then just add/remove the admin
	 * option.
	 *
	 * Note: Permissions checking is done by AddRoleMems/DelRoleMems
	 */
	foreach(item, stmt->granted_roles)
	{
		char	   *rolename = strVal(lfirst(item));
		Oid			roleid = get_roleid_checked(rolename);

		if (stmt->is_grant)
		{
			AddRoleMems(rolename, roleid,
						stmt->grantee_roles, grantee_ids,
						grantor, stmt->admin_opt);
		}
		else
		{
			DelRoleMems(rolename, roleid,
						stmt->grantee_roles, grantee_ids,
						stmt->admin_opt);
		}

		/* MPP-6929: metadata tracking */
		if (Gp_role == GP_ROLE_DISPATCH)
				MetaTrackUpdObject(AuthIdRelationId,
								   roleid,
								   GetUserId(),
								   "PRIVILEGE", 
								   (stmt->is_grant) ? "GRANT" : "REVOKE"
						);

	}

	/*
	 * Close pg_authid, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_authid_rel, NoLock);

	/*
	 * Set flag to update flat auth file at commit.
	 */
	auth_file_update_needed();
}

/*
 * DropOwnedObjects
 *
 * Drop the objects owned by a given list of roles.
 */
void
DropOwnedObjects(DropOwnedStmt *stmt)
{
	List	   *role_ids = roleNamesToIds(stmt->roles);
	ListCell   *cell;

	/* Check privileges */
	foreach(cell, role_ids)
	{
		Oid			roleid = lfirst_oid(cell);

		if (!has_privs_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to drop objects"),
					 errOmitLocation(true)));
	}
	
	/*
	if (Gp_role == GP_ROLE_DISPATCH)
    {
        CdbDispatchUtilityStatement((Node *) stmt, "DropOwnedObjects");
    }
	*/
    
	/* Ok, do it */
	shdepDropOwned(role_ids, stmt->behavior);
}

/*
 * ReassignOwnedObjects
 *
 * Give the objects owned by a given list of roles away to another user.
 */
void
ReassignOwnedObjects(ReassignOwnedStmt *stmt)
{
	List	   *role_ids = roleNamesToIds(stmt->roles);
	ListCell   *cell;
	Oid			newrole;

	/* Check privileges */
	foreach(cell, role_ids)
	{
		Oid			roleid = lfirst_oid(cell);

		if (!has_privs_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to reassign objects"),
					 errOmitLocation(true)));
	}

	/* Must have privileges on the receiving side too */
	newrole = get_roleid_checked(stmt->newrole);

	if (!has_privs_of_role(GetUserId(), newrole))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to reassign objects"),
				 errOmitLocation(true)));
				 
	/*
	if (Gp_role == GP_ROLE_DISPATCH)
    {
        CdbDispatchUtilityStatement((Node *) stmt, "ReassignOwnedObjects");
    }
	*/

	/* Ok, do it */
	shdepReassignOwned(role_ids, newrole);
}

/*
 * roleNamesToIds
 *
 * Given a list of role names (as String nodes), generate a list of role OIDs
 * in the same order.
 */
static List *
roleNamesToIds(List *memberNames)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, memberNames)
	{
		char	   *rolename = strVal(lfirst(l));
		Oid			roleid = get_roleid_checked(rolename);

		result = lappend_oid(result, roleid);
	}
	return result;
}

/*
 * AddRoleMems -- Add given members to the specified role
 *
 * rolename: name of role to add to (used only for error messages)
 * roleid: OID of role to add to
 * memberNames: list of names of roles to add (used only for error messages)
 * memberIds: OIDs of roles to add
 * grantorId: who is granting the membership
 * admin_opt: granting admin option?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void
AddRoleMems(const char *rolename, Oid roleid,
			List *memberNames, List *memberIds,
			Oid grantorId, bool admin_opt)
{
	Relation	pg_authmem_rel;
	TupleDesc	pg_authmem_dsc;
	ListCell   *nameitem;
	ListCell   *iditem;

	Assert(list_length(memberNames) == list_length(memberIds));

	/* Skip permission check if nothing to do */
	if (!memberIds)
		return;

	/*
	 * Check permissions: must have createrole or admin option on the role to
	 * be changed.	To mess with a superuser role, you gotta be superuser.
	 */
	if (superuser_arg(roleid))
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
	}
	else
	{
		if (!have_createrole_privilege() &&
			!is_admin_of_role(grantorId, roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must have admin option on role \"%s\"",
							rolename)));
	}

	/* XXX not sure about this check */
	if (grantorId != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set grantor")));

	pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);
	pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

	forboth(nameitem, memberNames, iditem, memberIds)
	{
		const char *membername = strVal(lfirst(nameitem));
		Oid			memberid = lfirst_oid(iditem);
		HeapTuple	authmem_tuple;
		HeapTuple	tuple;
		Datum		new_record[Natts_pg_auth_members];
		bool		new_record_nulls[Natts_pg_auth_members];
		bool		new_record_repl[Natts_pg_auth_members];
		cqContext	cqc;
		cqContext  *pcqCtx;

		/*
		 * Refuse creation of membership loops, including the trivial case
		 * where a role is made a member of itself.  We do this by checking to
		 * see if the target role is already a member of the proposed member
		 * role.  We have to ignore possible superuserness, however, else we
		 * could never grant membership in a superuser-privileged role.
		 */
		if (is_member_of_role_nosuper(roleid, memberid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_GRANT_OPERATION),
					 (errmsg("role \"%s\" is a member of role \"%s\"",
							 rolename, membername))));

		/*
		 * Check if entry for this role/member already exists; if so, give
		 * warning unless we are adding admin option.
		 */
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), pg_authmem_rel),
				cql("SELECT * FROM pg_auth_members "
					" WHERE roleid = :1 "
					" AND member = :2 "
					" FOR UPDATE ",
					ObjectIdGetDatum(roleid),
					ObjectIdGetDatum(memberid)));

		authmem_tuple = caql_getnext(pcqCtx);

		if (HeapTupleIsValid(authmem_tuple) &&
			(!admin_opt ||
			 ((Form_pg_auth_members) GETSTRUCT(authmem_tuple))->admin_option))
		{
			if (Gp_role != GP_ROLE_EXECUTE)
			ereport(NOTICE,
					(errmsg("role \"%s\" is already a member of role \"%s\"",
							membername, rolename)));
			caql_endscan(pcqCtx);
			continue;
		}

		/* Build a tuple to insert or update */
		MemSet(new_record, 0, sizeof(new_record));
		MemSet(new_record_nulls, false, sizeof(new_record_nulls));
		MemSet(new_record_repl, false, sizeof(new_record_repl));

		new_record[Anum_pg_auth_members_roleid - 1] = ObjectIdGetDatum(roleid);
		new_record[Anum_pg_auth_members_member - 1] = ObjectIdGetDatum(memberid);
		new_record[Anum_pg_auth_members_grantor - 1] = ObjectIdGetDatum(grantorId);
		new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(admin_opt);

		if (HeapTupleIsValid(authmem_tuple))
		{
			new_record_repl[Anum_pg_auth_members_grantor - 1] = true;
			new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;
			tuple = caql_modify_current(pcqCtx,
										new_record,
										new_record_nulls, new_record_repl);
			caql_update_current(pcqCtx, tuple);
			/* and Update indexes (implicit) */

			caql_endscan(pcqCtx);
		}
		else
		{
			pcqCtx = caql_beginscan(
					caql_addrel(cqclr(&cqc), pg_authmem_rel),
					cql("INSERT INTO pg_auth_members ",
						NULL));

			tuple = caql_form_tuple(pcqCtx, new_record, new_record_nulls);

			/* Insert tuple into the relation */
			caql_insert(pcqCtx, tuple);  /* implicit update of index as well */

			caql_endscan(pcqCtx);
		}

		/* CCI after each change, in case there are duplicates in list */
		CommandCounterIncrement();
	}

	/*
	 * Close pg_authmem, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_authmem_rel, NoLock);

}

/*
 * CheckKeywordIsValid
 * 
 * check that string in 'keyword' is included in set of strings in 'arr'
 */
static void CheckKeywordIsValid(char *keyword, const char **arr, const int arrsize)
{
	int 	i = 0;
	bool	ok = false;
	
	for(i = 0 ; i < arrsize ; i++)
	{
		if(strcasecmp(keyword, arr[i]) == 0)
			ok = true;
	}
	
	if(!ok)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid [NO]CREATEEXTTABLE option \"%s\"", keyword)));				

}

/*
 * CheckValueBelongsToKey
 * 
 * check that value (e.g 'gpfdist') belogs to the key it was defined for (e.g 'protocol').
 * error out otherwise (for example, [protocol='writable'] includes valid keywords, but makes
 * no sense.
 */
static void CheckValueBelongsToKey(char *key, char *val, const char **keys, const char **vals)
{
	if(strcasecmp(key, keys[0]) == 0)
	{
		if(strcasecmp(val, vals[0]) != 0 && 
		   strcasecmp(val, vals[1]) != 0)
			
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid %s value \"%s\"", key, val)));	
	}
	else /* keys[1] */
	{
		if (strcasecmp(val, "gphdfs") == 0 && Gp_role == GP_ROLE_DISPATCH)
			ereport(WARNING,
					(errmsg("GRANT/REVOKE on gphdfs is deprecated"),
					 errhint("Issue the GRANT or REVOKE on the protocol itself"),
					 errOmitLocation(true)));

		if(strcasecmp(val, "gpfdist") != 0 && 
		   strcasecmp(val, "gpfdists") != 0 &&
		   strcasecmp(val, "http") != 0 &&
		   strcasecmp(val, "gphdfs") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("invalid %s value \"%s\"", key, val)));
	}
	
}

/*
 * TransformExttabAuthClause
 * 
 * Given a set of key value pairs, take them apart, fill in any default
 * values, and validate that pairs are legal and make sense.
 * 
 * defaults are: 
 *   - 'readable' when no type defined, 
 *   - 'gpfdist' when no protocol defined,
 *   - 'readable' + ' gpfdist' if both type and protocol aren't defined.
 * 
 */
static void TransformExttabAuthClause(DefElem *defel, extAuthPair *extauth)
{
	ListCell   	*lc;
	List	   	*l = (List *) defel->arg;
	DefElem 	*d1 = NULL, 
				*d2 = NULL;
	genericPair *genpair = (genericPair *) palloc0 (4 * sizeof(char *));
	
	const int	numkeys = 2;
	const int	numvals = 6;
	const char *keys[] = { "type", "protocol"};	 /* order matters for validation. don't change! */
	const char *vals[] = { /* types     */ "readable", "writable", 
						   /* protocols */ "gpfdist", "gpfdists" , "http", "gphdfs"};

	if(list_length(l) > 2)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid [NO]CREATEEXTTABLE specification. too many values")));				

		
	if(list_length(l) == 2)
	{
		/* both a protocol and type specification */
		
		lc = list_head(l); 
		d1 = (DefElem *) lfirst(lc);
		genpair->key1 = pstrdup(d1->defname);
		genpair->val1 = pstrdup(strVal(d1->arg));
		
		lc = lnext(lc);
		d2 = (DefElem *) lfirst(lc);
		genpair->key2 = pstrdup(d2->defname);
		genpair->val2 = pstrdup(strVal(d2->arg));
	}
	else if(list_length(l) == 1)
	{
		/* either a protocol or type specification */
		
		lc = list_head(l); 
		d1 = (DefElem *) lfirst(lc);
		genpair->key1 = pstrdup(d1->defname);
		genpair->val1 = pstrdup(strVal(d1->arg));
		
		if(strcasecmp(genpair->key1, "type") == 0)
		{
			/* default value for missing protocol */
			genpair->key2 = pstrdup("protocol");
			genpair->val2 = pstrdup("gpfdist");
		}
		else
		{
			/* default value for missing type */
			genpair->key2 = pstrdup("type");
			genpair->val2 = pstrdup("readable");
		}
	}
	else
	{
		/* none specified. use global default */
		
		genpair->key1 = pstrdup("protocol");
		genpair->val1 = pstrdup("gpfdist");
		genpair->key2 = pstrdup("type");
		genpair->val2 = pstrdup("readable");
	}
	
	/* check all keys and values are legal */
	CheckKeywordIsValid(genpair->key1, keys, numkeys);
	CheckKeywordIsValid(genpair->key2, keys, numkeys);
	CheckKeywordIsValid(genpair->val1, vals, numvals);
	CheckKeywordIsValid(genpair->val2, vals, numvals);
		
	/* check all values are of the proper key */
	CheckValueBelongsToKey(genpair->key1, genpair->val1, keys, vals);
	CheckValueBelongsToKey(genpair->key2, genpair->val2, keys, vals);

	if(strcasecmp(genpair->key1, genpair->key2) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("redundant option for \"%s\"", genpair->key1)));
	
	/* now set values in extauth, which is the result returned */
	if(strcasecmp(genpair->key1, "protocol") == 0)
	{
		extauth->protocol = pstrdup(genpair->val1);
		extauth->type = pstrdup(genpair->val2);
	}
	else
	{
		extauth->protocol = pstrdup(genpair->val2);
		extauth->type = pstrdup(genpair->val1);
	}
	
	pfree(genpair->key1);
	pfree(genpair->key2);
	pfree(genpair->val1);
	pfree(genpair->val2);
	pfree(genpair);
}

/*
 * SetCreateExtTableForRole
 * 
 * Given the allow list (permissions to add) and disallow (permissions
 * to take away) consolidate this information into the 3 catalog
 * boolean columns that will need to get updated. While at it we check
 * that all the options are valid and don't conflict with each other.
 * 
 */
static void SetCreateExtTableForRole(List* allow, 
									 List* disallow,
									 bool* createrextgpfd,
									 bool* createrexthttp, 
									 bool* createwextgpfd,
									 bool* createrexthdfs,
									 bool* createwexthdfs)
{
	ListCell*	lc;
	bool		createrextgpfd_specified = false;
	bool		createwextgpfd_specified = false;
	bool		createrexthttp_specified = false;
	bool		createrexthdfs_specified = false;
	bool		createwexthdfs_specified = false;
	
	if(list_length(allow) > 0)
	{
		/* examine key value pairs */
		foreach(lc, allow)
		{
			extAuthPair* extauth = (extAuthPair*) lfirst(lc);
			
			/* we use the same privilege for gpfdist and gpfdists */
			if ((strcasecmp(extauth->protocol, "gpfdist") == 0) ||
			    (strcasecmp(extauth->protocol, "gpfdists") == 0))
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					*createrextgpfd = true;
					createrextgpfd_specified = true; 
				}
				else
				{
					*createwextgpfd = true;
					createwextgpfd_specified = true;
				}
			}
			else if(strcasecmp(extauth->protocol, "gphdfs") == 0)
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					*createrexthdfs = true;
					createrexthdfs_specified = true;
				}
				else
				{
					*createwexthdfs = true;
					createwexthdfs_specified = true;
				}
			}
			else /* http */
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					*createrexthttp = true;
					createrexthttp_specified = true;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid CREATEEXTTABLE specification. writable http external tables do not exist")));
				}
			}			
		}
	}	
	
	/*
	 * go over the disallow list.
	 * if we're in CREATE ROLE, check that we don't negate something from the 
	 * allow list. error out with conflicting options if we do. 
	 * if we're in ALTER ROLE, just set the flags accordingly.
	 */
	if(list_length(disallow) > 0)
	{
		bool conflict = false;
		
		/* examine key value pairs */
		foreach(lc, disallow)
		{
			extAuthPair* extauth = (extAuthPair*) lfirst(lc);
			
			/* we use the same privilege for gpfdist and gpfdists */
			if ((strcasecmp(extauth->protocol, "gpfdist") == 0) ||
				(strcasecmp(extauth->protocol, "gpfdists") == 0))
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					if(createrextgpfd_specified)
						conflict = true;
						
					*createrextgpfd = false;
				}
				else
				{
					if(createwextgpfd_specified)
						conflict = true;

					*createwextgpfd = false;
				}
			}
			else if(strcasecmp(extauth->protocol, "gphdfs") == 0)
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					if(createrexthdfs_specified)
						conflict = true;

					*createrexthdfs = false;
				}
				else
				{
					if(createwexthdfs_specified)
						conflict = true;

					*createwexthdfs = false;
				}
			}
			else /* http */
			{
				if(strcasecmp(extauth->type, "readable") == 0)
				{
					if(createrexthttp_specified)
						conflict = true;

					*createrexthttp = false;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid NOCREATEEXTTABLE specification. writable http external tables do not exist")));
				}
			}			
		}
		
		if(conflict)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("conflicting specifications in CREATEEXTTABLE and NOCREATEEXTTABLE")));
			
	}	

}

/*
 * DelRoleMems -- Remove given members from the specified role
 *
 * rolename: name of role to del from (used only for error messages)
 * roleid: OID of role to del from
 * memberNames: list of names of roles to del (used only for error messages)
 * memberIds: OIDs of roles to del
 * admin_opt: remove admin option only?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void
DelRoleMems(const char *rolename, Oid roleid,
			List *memberNames, List *memberIds,
			bool admin_opt)
{
	Relation	pg_authmem_rel;
	ListCell   *nameitem;
	ListCell   *iditem;

	Assert(list_length(memberNames) == list_length(memberIds));

	/* Skip permission check if nothing to do */
	if (!memberIds)
		return;

	/*
	 * Check permissions: must have createrole or admin option on the role to
	 * be changed.	To mess with a superuser role, you gotta be superuser.
	 */
	if (superuser_arg(roleid))
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
	}
	else
	{
		if (!have_createrole_privilege() &&
			!is_admin_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must have admin option on role \"%s\"",
							rolename)));
	}

	pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);

	forboth(nameitem, memberNames, iditem, memberIds)
	{
		const char *membername = strVal(lfirst(nameitem));
		Oid			memberid = lfirst_oid(iditem);
		HeapTuple	authmem_tuple;
		cqContext	cqc;
		cqContext  *pcqCtx;

		/*
		 * Find entry for this role/member
		 */
		pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), pg_authmem_rel),
				cql("SELECT * FROM pg_auth_members "
					" WHERE roleid = :1 "
					" AND member = :2 "
					" FOR UPDATE ",
					ObjectIdGetDatum(roleid),
					ObjectIdGetDatum(memberid)));

		authmem_tuple = caql_getnext(pcqCtx);

		if (!HeapTupleIsValid(authmem_tuple))
		{
			ereport(WARNING,
					(errmsg("role \"%s\" is not a member of role \"%s\"",
							membername, rolename)));
			continue;
		}

		if (!admin_opt)
		{
			/* Remove the entry altogether */
			caql_delete_current(pcqCtx);
		}
		else
		{
			/* Just turn off the admin option */
			HeapTuple	tuple;
			Datum		new_record[Natts_pg_auth_members];
			bool		new_record_nulls[Natts_pg_auth_members];
			bool		new_record_repl[Natts_pg_auth_members];

			/* Build a tuple to update with */
			MemSet(new_record, 0, sizeof(new_record));
			MemSet(new_record_nulls, false, sizeof(new_record_nulls));
			MemSet(new_record_repl, false, sizeof(new_record_repl));

			new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(false);
			new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;

			tuple = caql_modify_current(pcqCtx,
										new_record,
										new_record_nulls, new_record_repl);
			caql_update_current(pcqCtx, tuple);
			/* and Update indexes (implicit) */
		}

		caql_endscan(pcqCtx);

		/* CCI after each change, in case there are duplicates in list */
		CommandCounterIncrement();
	}

	/*
	 * Close pg_authmem, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_authmem_rel, NoLock);
}

/*
 * ExtractAuthIntervalClause
 * 
 * Build an authInterval struct (defined above) from given input
 */
static void 
ExtractAuthIntervalClause(DefElem *defel, authInterval *interval)
{
	DenyLoginPoint *start = NULL, *end = NULL;
	char	*temp;
	if (IsA(defel->arg, DenyLoginInterval))
	{
		DenyLoginInterval *span = (DenyLoginInterval *)defel->arg;
		start = span->start;
		end = span->end;
	} 
	else 
	{
		Assert(IsA(defel->arg, DenyLoginPoint));
		start = (DenyLoginPoint *)defel->arg;
		end = start;
	}
	interval->start.day = ExtractAuthInterpretDay(start->day);
	temp = start->time != NULL ? strVal(start->time) : "00:00:00";
	interval->start.time = DatumGetTimeADT(DirectFunctionCall1(time_in, CStringGetDatum(temp)));
	interval->end.day = ExtractAuthInterpretDay(end->day);
	temp = end->time != NULL ? strVal(end->time) : "24:00:00";
	interval->end.time = DatumGetTimeADT(DirectFunctionCall1(time_in, CStringGetDatum(temp)));
	if (point_cmp(&interval->start, &interval->end) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("time interval must not wrap around"))); 
}

/*
 * TransferAuthInterpretDay -- Interpret day of week from parse node
 *
 * day: node which dictates a day of week;
 *		may be either an integer in [0, 6] 
 *		or a string giving name of day in English
 */
static int16 
ExtractAuthInterpretDay(Value * day) 
{
	int16   ret;
	if (day->type == T_Integer) 
	{
		ret = intVal(day);
		if (ret < 0 || ret > 6)
			ereport(ERROR,
					 (errcode(ERRCODE_SYNTAX_ERROR),
					  errmsg("numeric day of week must be between 0 and 6")));
	} 
	else 
	{
		int16		 elems = 7;
		char		*target = strVal(day);
		for (ret = 0; ret < elems; ret++) 
			if (strcasecmp(target, daysofweek[ret]) == 0)
				break;
		if (ret == elems)
			ereport(ERROR,
					 (errcode(ERRCODE_SYNTAX_ERROR),
					  errmsg("invalid weekday name \"%s\"", target),
					  errhint("Day of week must be one of 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'.")));
	}
	return ret;
}

/*
 * AddRoleDenials -- Populate pg_auth_time_constraint
 *
 * rolename: name of role to add to (used only for error messages)
 * roleid: OID of role to add to
 * addintervals: list of authInterval structs dictating when
 *				  this particular role should be denied access
 *
 * Note: caller is reponsible for checking permissions to edit the given role.
 */
static void
AddRoleDenials(const char *rolename, Oid roleid, List *addintervals) {
	Relation	pg_auth_time_rel;
	ListCell   *intervalitem;
	cqContext	cqc;
	cqContext  *pcqCtx;

	pg_auth_time_rel = heap_open(AuthTimeConstraintRelationId, RowExclusiveLock);

	pcqCtx = caql_beginscan(
				caql_addrel(cqclr(&cqc), pg_auth_time_rel),
				cql("INSERT INTO pg_auth_time_constraint ",
					NULL));

	foreach(intervalitem, addintervals)
	{
		authInterval 	*interval = (authInterval *)lfirst(intervalitem);
		HeapTuple   tuple;
		Datum		new_record[Natts_pg_auth_time_constraint];
		bool		new_record_nulls[Natts_pg_auth_time_constraint];

		/* Build a tuple to insert or update */
		MemSet(new_record, 0, sizeof(new_record));
		MemSet(new_record_nulls, false, sizeof(new_record_nulls));

		new_record[Anum_pg_auth_time_constraint_authid - 1] = ObjectIdGetDatum(roleid);
		new_record[Anum_pg_auth_time_constraint_start_day - 1] = Int16GetDatum(interval->start.day);
		new_record[Anum_pg_auth_time_constraint_start_time - 1] = TimeADTGetDatum(interval->start.time);
		new_record[Anum_pg_auth_time_constraint_end_day - 1] = Int16GetDatum(interval->end.day);
		new_record[Anum_pg_auth_time_constraint_end_time - 1] = TimeADTGetDatum(interval->end.time);

		tuple = caql_form_tuple(pcqCtx, new_record, new_record_nulls);
		
		/* Insert tuple into the relation */
		caql_insert(pcqCtx, tuple); /* implicit update of index as well */

	}

	CommandCounterIncrement();

	/*
	 * Close pg_auth_time_constraint, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	caql_endscan(pcqCtx);
	heap_close(pg_auth_time_rel, NoLock);

	/*
	 * Set flag to update flat auth time constraint file at commit.
	 */
	auth_time_file_update_needed();
}

/*
 * DelRoleDenials -- Trim pg_auth_time_constraint
 *
 * rolename: name of role to edit (used only for error messages)
 * roleid: OID of role to edit
 * dropintervals: list of authInterval structs dictating which
 *                existing rules should be dropped. Here, NIL will mean
 *                remove all constraints for the given role.
 *
 * Note: caller is reponsible for checking permissions to edit the given role.
 */
static void
DelRoleDenials(const char *rolename, Oid roleid, List *dropintervals) 
{
	Relation    pg_auth_time_rel;
	ListCell	*intervalitem;
	bool		dropped_matching_interval = false; 

	HeapTuple 	tmp_tuple;
	cqContext	cqc;
	cqContext  *pcqCtx;

	pg_auth_time_rel = heap_open(AuthTimeConstraintRelationId, RowExclusiveLock);

	pcqCtx = 
			caql_beginscan(
					caql_addrel(cqclr(&cqc), pg_auth_time_rel),
					cql("SELECT * FROM pg_auth_time_constraint "
						" WHERE authid = :1 "
						" FOR UPDATE ",
						ObjectIdGetDatum(roleid)));

	while (HeapTupleIsValid(tmp_tuple = caql_getnext(pcqCtx)))
	{
		if (dropintervals != NIL) 
		{
			Form_pg_auth_time_constraint obj = (Form_pg_auth_time_constraint) GETSTRUCT(tmp_tuple);
			authInterval *interval, *existing = (authInterval *) palloc0(sizeof(authInterval));
			existing->start.day = obj->start_day;
			existing->start.time = obj->start_time;
			existing->end.day = obj->end_day;
			existing->end.time = obj->end_time;
			foreach(intervalitem, dropintervals) 
			{
				interval = (authInterval *)lfirst(intervalitem);
				if (interval_overlap(existing, interval))
				{
					if (Gp_role == GP_ROLE_DISPATCH)
						ereport(NOTICE,
								(errmsg("dropping DENY rule for \"%s\" between %s %s and %s %s",
										rolename, 
										daysofweek[existing->start.day],
										DatumGetCString(DirectFunctionCall1(time_out, TimeADTGetDatum(existing->start.time))),
										daysofweek[existing->end.day],
										DatumGetCString(DirectFunctionCall1(time_out, TimeADTGetDatum(existing->end.time))))));
					caql_delete_current(pcqCtx);
					dropped_matching_interval = true;
					break;
				}
			}
		} 
		else
			caql_delete_current(pcqCtx);
	}

	/* if intervals were specified and none was found, raise error */
	if (dropintervals && !dropped_matching_interval)
		ereport(ERROR, 
				(errmsg("cannot find matching DENY rules for \"%s\"", rolename)));

	caql_endscan(pcqCtx);

	/*
	 * Close pg_auth_time_constraint, but keep lock till commit (this is important to
	 * prevent any risk of deadlock failure while updating flat file)
	 */
	heap_close(pg_auth_time_rel, NoLock);

	/*
	 * Set flag to update flat auth time constraint file at commit.
	 */
	auth_time_file_update_needed();
}
