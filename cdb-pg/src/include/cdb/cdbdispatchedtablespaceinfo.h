/*-------------------------------------------------------------------------
 *
 * cdbdispatchedtablespaceinfo.h
 *
 * Copyright (c) 2009-2012, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef _CDBDISPATCHEDTABLESPACEINFO_H_
#define _CDBDISPATCHEDTABLESPACEINFO_H_

#include "catalog/gp_persistent.h"

/*
 * in gpsql, dispatcher will dispatch the filespace info to all QE.
 * QE keep this info in hash table.
 */
struct DispatchedFilespaceDirEntryData
{
	Oid 	tablespace;
	char	location[FilespaceLocationBlankPaddedWithNullTermLen];
};

typedef struct DispatchedFilespaceDirEntryData DispatchedFilespaceDirEntryData;

typedef DispatchedFilespaceDirEntryData *DispatchedFilespaceDirEntry;

extern void DispatchedFilespace_GetPathForTablespace(Oid tablespace, char **filespacePath, bool * found);
extern void DispatchedFilespace_AddForTablespace(Oid tablespace, const char * path);
extern DispatchedFilespaceDirEntry DispatchedFilespace_SeqSearch_GetNext(void);
extern void DispatchedFilespace_SeqSearch_Term(void);

#endif /* _CDBDISPATCHEDTABLESPACEINFO_H_ */
