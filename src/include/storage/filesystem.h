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
 * filesystem.h
 *	  Expended file system interface
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/storage/filesystem.h,v 1.0 2012/05/25 15:58:59 Exp $
 *
 *-------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Seek, Tell, Sync}
 *	{File Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = FilePathOpenFile("foo", O_RDONLY, 0600);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*).
 */

#ifndef FILESYSTEM_H
#define FILESYSTEM_H

#include "fmgr.h"
#include "catalog/pg_filesystem.h"
#include "hdfs/hdfs.h"

typedef const char *FsysName;

/*
 * This struct is used to pass args between gpdb and gpfshdfs.so
 */
typedef struct FileSystemUdfData
{
	NodeTag			type;                 /* see T_FileSystemFunctionData */
	char*			fsys_host;
	int				fsys_port;
	void*			fsys_token;           /* filesystem token, can be NULL */
	char*			fsys_ccname;		  /* kerberos ticket chache path, can be NULL */
	hdfsFS			fsys_hdfs;
	hdfsFile		fsys_hfile;
	char*			fsys_filepath;
	int				fsys_fileflags;
	char*			fsys_databuf;
	int				fsys_maxbytes;
	int				fsys_filebufsize;
	short			fsys_replication;
	int64_t			fsys_fileblksize;
	int				fsys_recursive;
	short			fsys_mode;
	int64_t			fsys_pos;
	hdfsFileInfo*   fsys_fileinfo;
	int             fsys_fileinfonum;
	void*			fsys_user_ctx;
} FileSystemUdfData;

#define CALLED_AS_GPFILESYSTEM(fcinfo) \
	((fcinfo->context != NULL && IsA((fcinfo)->context, FileSystemFunctionData)))

#define FSYS_UDF_GET_HDFS(fcinfo)		   	(((FileSystemUdfData *) (fcinfo)->context)->fsys_hdfs)
#define FSYS_UDF_GET_HFILE(fcinfo)		   	(((FileSystemUdfData *) (fcinfo)->context)->fsys_hfile)
#define FSYS_UDF_GET_HOST(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_host)
#define FSYS_UDF_GET_PORT(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_port)
#define FSYS_UDF_GET_PATH(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_filepath)
#define FSYS_UDF_GET_FILEFLAGS(fcinfo)		(((FileSystemUdfData *) (fcinfo)->context)->fsys_fileflags)
#define FSYS_UDF_GET_FILEBUFSIZE(fcinfo)	(((FileSystemUdfData *) (fcinfo)->context)->fsys_filebufsize)
#define FSYS_UDF_GET_FILEREP(fcinfo)		(((FileSystemUdfData *) (fcinfo)->context)->fsys_replication)
#define FSYS_UDF_GET_FILEBLKSIZE(fcinfo)	(((FileSystemUdfData *) (fcinfo)->context)->fsys_fileblksize)
#define FSYS_UDF_GET_RECURSIVE(fcinfo)		(((FileSystemUdfData *) (fcinfo)->context)->fsys_recursive)
#define FSYS_UDF_GET_MODE(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_mode)
#define FSYS_UDF_GET_POS(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_pos)

#define FSYS_UDF_GET_DATABUF(fcinfo)		(((FileSystemUdfData *) (fcinfo)->context)->fsys_databuf)
#define FSYS_UDF_GET_BUFLEN(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_maxbytes)
#define FSYS_UDF_GET_FILEINFO(fcinfo)		(((FileSystemUdfData *) (fcinfo)->context)->fsys_fileinfo)
#define FSYS_UDF_GET_FILEINFONUM(fcinfo)	(((FileSystemUdfData *) (fcinfo)->context)->fsys_fileinfonum)

#define FSYS_UDF_SET_HDFS(fcinfo, hdfs)       (((FileSystemUdfData *) (fcinfo)->context)->fsys_hdfs=hdfs)
#define FSYS_UDF_SET_HFILE(fcinfo, hFile)     (((FileSystemUdfData *) (fcinfo)->context)->fsys_hfile=hFile)
#define FSYS_UDF_SET_FILEINFO(fcinfo, info)   (((FileSystemUdfData *) (fcinfo)->context)->fsys_fileinfo=info)

#define FSYS_UDF_GET_TOKEN(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_token)
#define FSYS_UDF_GET_CCNAME(fcinfo)			(((FileSystemUdfData *) (fcinfo)->context)->fsys_ccname)


/* wrapper for hdfs functions */
hdfsFS HdfsConnect(FsysName protocol, char * host, uint16_t port, char *ccname, void *token);
int HdfsDisconnect(FsysName protocol, hdfsFS fileSystem);
hdfsFile HdfsOpenFile(FsysName protocol, hdfsFS fileSystem, char * path, int flags,
					  int bufferSize, short replication, int64_t blocksize);
int HdfsSync(FsysName protocol, hdfsFS fileSystem, hdfsFile file);
int HdfsCloseFile(FsysName protocol, hdfsFS fileSystem, hdfsFile file);

int HdfsCreateDirectory(FsysName protocol, hdfsFS fileSystem, char * path);
int HdfsDelete(FsysName protocol, hdfsFS fileSystem, char * path, int recursive);
int HdfsChmod(FsysName protocol, hdfsFS fileSystem, char * path, short mode);

int HdfsRead(FsysName protocol, hdfsFS fileSystem, hdfsFile file, void * buffer, int length);
int HdfsWrite(FsysName protocol, hdfsFS fileSystem, hdfsFile file, const void * buffer, int length);
int HdfsSeek(FsysName protocol, hdfsFS fileSystem, hdfsFile file, int64_t desiredPos);
int64_t HdfsTell(FsysName protocol, hdfsFS fileSystem, hdfsFile file);

int HdfsTruncate(FsysName protocol, hdfsFS fileSystem, char * path, int64_t size);

hdfsFileInfo * HdfsGetPathInfo(FsysName protocol, hdfsFS fileSystem, char * path);
int HdfsFreeFileInfo(FsysName protocol, hdfsFileInfo * hdfsFileInfo, int numEntries);

/*
void HdfsLogLevel(LogLevel level);
void HdfsLogOutput(FILE * file);

int HdfsSetReplication(hdfsFS fileSystem, const char * path, int16_t replication);
*/
#endif
