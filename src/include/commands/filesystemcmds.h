/*-------------------------------------------------------------------------
 *
 * filesystemcmds.h
 *	  prototypes for filesystemcmds.c.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FILESYSTEMCMDS_H
#define FILESYSTEMCMDS_H

#include "nodes/parsenodes.h"

extern void DefineFileSystem(List *name, List *parameters, Oid newOid, bool trusted);
extern void RemoveFileSystem(List *names, DropBehavior behavior, bool missing_ok);
extern void RemoveFileSystemById(Oid protOid);
extern void AlterFileSystemOwner(const char *name, Oid newOwnerId);
extern void RenameFileSystem(const char *oldname, const char *newname);


#endif   /* FILESYSTEMCMDS_H */
