/*-------------------------------------------------------------------------
 *
 * user.h
 *	  Commands for manipulating roles (formerly called users).
 *
 *
 * $PostgreSQL: pgsql/src/include/commands/user.h,v 1.30 2006/10/04 00:30:08 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef USER_H
#define USER_H

#include "catalog/catquery.h"
#include "nodes/parsenodes.h"


extern void CreateRole(CreateRoleStmt *stmt);
extern void AlterRole(AlterRoleStmt *stmt);
extern void AlterRoleSet(AlterRoleSetStmt *stmt);
extern void DropRole(DropRoleStmt *stmt);
extern void GrantRole(GrantRoleStmt *stmt);
extern void RenameRole(const char *oldname, const char *newname);
extern void DropOwnedObjects(DropOwnedStmt *stmt);
extern void ReassignOwnedObjects(ReassignOwnedStmt *stmt);
extern bool CheckUserExistOnCloudSimple(char *rolename, Oid *roleid);
extern bool CheckUserExistOnCloud(cqContext **pcqCtx, cqContext *cqc, Relation pg_authid_rel, char *rolename, HeapTuple *tuple, bool forUpdate);

#endif   /* USER_H */
