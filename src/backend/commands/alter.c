/*-------------------------------------------------------------------------
 *
 * alter.c
 *	  Drivers for generic alter commands
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/alter.c,v 1.20 2006/07/14 14:52:18 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/alter.h"
#include "commands/conversioncmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extprotocolcmds.h"
#include "commands/filespace.h"
#include "commands/filesystemcmds.h"
#include "commands/proclang.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "commands/user.h"
#include "miscadmin.h"
#include "parser/parse_clause.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"


/*
 * Executes an ALTER OBJECT / RENAME TO statement.	Based on the object
 * type, the function appropriate to that type is executed.
 */
void
ExecRenameStmt(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_AGGREGATE:
			RenameAggregate(stmt->object, stmt->objarg, stmt->newname);
			break;

		case OBJECT_CONVERSION:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
					(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename conversion statement yet") ));
			}
			RenameConversion(stmt->object, stmt->newname);
			break;

		case OBJECT_DATABASE:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename database statement yet") ));
			}
			RenameDatabase(stmt->subname, stmt->newname);
			break;

		case OBJECT_EXTPROTOCOL:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename protocol statement yet") ));
			}
			RenameExtProtocol(stmt->subname, stmt->newname);
			break;

		case OBJECT_FUNCTION:
			RenameFunction(stmt->object, stmt->objarg, stmt->newname);
			break;

		case OBJECT_LANGUAGE:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename language statement yet") ));
			}
			RenameLanguage(stmt->subname, stmt->newname);
			break;

		case OBJECT_OPCLASS:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename operator class statement yet") ));
			}
			RenameOpClass(stmt->object, stmt->subname, stmt->newname);
			break;

		case OBJECT_ROLE:
			RenameRole(stmt->subname, stmt->newname);
			break;

		case OBJECT_SCHEMA:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename schema statement yet") ));
			}
			RenameSchema(stmt->subname, stmt->newname);
			break;

		case OBJECT_TABLESPACE:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename tablespace statement yet") ));
			}
			RenameTableSpace(stmt->subname, stmt->newname);
			break;

		case OBJECT_FILESPACE:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename filespace statement yet") ));
			}
			RenameFileSpace(stmt->subname, stmt->newname);
			break;

		case OBJECT_FILESYSTEM:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename filesystem statement yet") ));
			}
			RenameFileSystem(stmt->subname, stmt->newname);
			break;

		case OBJECT_INDEX:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename index statement yet") ));
			}

		case OBJECT_TABLE:
		{
			stmt->objid = RangeVarGetRelid(stmt->relation, false, false /*allowHcatalog*/);
			CheckRelationOwnership(stmt->objid, true);

			/*
			 * RENAME TABLE requires that we (still) hold
			 * CREATE rights on the containing namespace, as
			 * well as ownership of the table.
			 */
			Oid			namespaceId = get_rel_namespace(stmt->objid);
			AclResult	aclresult;

			aclresult = pg_namespace_aclcheck(namespaceId,
											  GetUserId(),
											  ACL_CREATE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
							   get_namespace_name(namespaceId));

			renamerel(stmt->objid, stmt->newname, stmt);
			break;
		}

		case OBJECT_TRIGGER:
			if (!gp_called_by_pgdump)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support rename trigger statement yet") ));
			}

		case OBJECT_COLUMN:
			{
				Oid			relid;

				relid = RangeVarGetRelid(stmt->relation, false, false /*allowHcatalog*/);
				CheckRelationOwnership(relid, true);

				switch (stmt->renameType)
				{
					case OBJECT_COLUMN:
						renameatt(relid,
								  stmt->subname,		/* old att name */
								  stmt->newname,		/* new att name */
								  interpretInhOption(stmt->relation->inhOpt),	/* recursive? */
								  false);		/* recursing already? */
						break;
					case OBJECT_TRIGGER:
						renametrig(relid,
								   stmt->subname,		/* old att name */
								   stmt->newname);		/* new att name */
						break;
					default:
						 /* can't happen */ ;
				}
				break;
			}

		default:
			elog(ERROR, "unrecognized rename stmt type: %d",
				 (int) stmt->renameType);
	}
}

/*
 * Executes an ALTER OBJECT / SET SCHEMA statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
void
ExecAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt)
{
	switch (stmt->objectType)
	{
		case OBJECT_AGGREGATE:
			AlterFunctionNamespace(stmt->object, stmt->objarg, true,
								   stmt->newschema);
			break;

		case OBJECT_FUNCTION:
			AlterFunctionNamespace(stmt->object, stmt->objarg, false,
								   stmt->newschema);
			break;

		case OBJECT_SEQUENCE:
			ereport(ERROR,
			                            (errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter sequence schema statement yet") ));
		case OBJECT_TABLE:
			AlterTableNamespace(stmt->relation, stmt->newschema);
			break;

		case OBJECT_TYPE:
		  /* Permissions are checked inside the function. */
			AlterTypeNamespace(stmt->object, stmt->newschema);
			break;

		case OBJECT_DOMAIN:
			ereport(ERROR,
                            (errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter domain schema statement yet") ));
			AlterTypeNamespace(stmt->object, stmt->newschema);
			break;

		default:
			elog(ERROR, "unrecognized AlterObjectSchemaStmt type: %d",
				 (int) stmt->objectType);
	}
}

/*
 * Executes an ALTER OBJECT / OWNER TO statement.  Based on the object
 * type, the function appropriate to that type is executed.
 */
void
ExecAlterOwnerStmt(AlterOwnerStmt *stmt)
{
	Oid			newowner = get_roleid_checked(stmt->newowner);

	switch (stmt->objectType)
	{
		case OBJECT_AGGREGATE:
			AlterAggregateOwner(stmt->object, stmt->objarg, newowner);
			break;

		case OBJECT_CONVERSION:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter conversion owner statement yet") ));
			AlterConversionOwner(stmt->object, newowner);
			break;

		case OBJECT_DATABASE:
			if (!gp_called_by_pgdump)
							ereport(ERROR,
									(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter database owner statement yet") ));
			AlterDatabaseOwner(strVal(linitial(stmt->object)), newowner);
			break;

		case OBJECT_FUNCTION:
			AlterFunctionOwner(stmt->object, stmt->objarg, newowner);
			break;

		case OBJECT_OPERATOR:
			Assert(list_length(stmt->objarg) == 2);
			AlterOperatorOwner(stmt->object,
							   (TypeName *) linitial(stmt->objarg),
							   (TypeName *) lsecond(stmt->objarg),
							   newowner);
			break;

		case OBJECT_OPCLASS:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter operator class owner statement yet") ));
			AlterOpClassOwner(stmt->object, stmt->addname, newowner);
			break;

		case OBJECT_SCHEMA:
			if (!gp_called_by_pgdump)
							ereport(ERROR,
									(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter schema owner statement yet") ));
			AlterSchemaOwner(strVal(linitial(stmt->object)), newowner);
			break;

		case OBJECT_TABLESPACE:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter tablespace owner statement yet") ));
			AlterTableSpaceOwner(strVal(linitial(stmt->object)), newowner);
			break;

		case OBJECT_FILESPACE:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter filespace owner statement yet") ));
			AlterFileSpaceOwner(stmt->object, newowner);
			break;

		case OBJECT_TYPE:
		case OBJECT_DOMAIN:		/* same as TYPE */
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter type owner statement yet") ));
			AlterTypeOwner(stmt->object, newowner);
			break;

		case OBJECT_FDW:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter foreign data wrapper owner statement yet") ));
			AlterForeignDataWrapperOwner(strVal(linitial(stmt->object)),
										 newowner);
			break;

		case OBJECT_FOREIGN_SERVER:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter foreign server owner statement yet") ));
			AlterForeignServerOwner(strVal(linitial(stmt->object)), newowner);
			break;
		
		case OBJECT_EXTPROTOCOL:
			if (!gp_called_by_pgdump)
				ereport(ERROR,
						(errcode(ERRCODE_CDB_FEATURE_NOT_YET), errmsg("Cannot support alter external protocol owner statement yet") ));
			AlterExtProtocolOwner(strVal(linitial(stmt->object)), newowner);
			break;
			
		default:
			elog(ERROR, "unrecognized AlterOwnerStmt type: %d",
				 (int) stmt->objectType);
	}
}
