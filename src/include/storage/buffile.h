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
 * buffile.h
 *	  Management of large buffered files, primarily temporary files.
 *
 * The BufFile routines provide a partial replacement for stdio atop
 * virtual file descriptors managed by fd.c.  Currently they only support
 * buffered access to a virtual file, without any of stdio's formatting
 * features.  That's enough for immediate needs, but the set of facilities
 * could be expanded if necessary.
 *
 * BufFile also supports working with temporary files that exceed the OS
 * file size limit and/or the largest offset representable in an int.
 * It might be better to split that out as a separately accessible module,
 * but currently we have no need for oversize temp files without buffered
 * access.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/storage/buffile.h,v 1.19 2006/03/05 15:58:59 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#ifndef BUFFILE_H
#define BUFFILE_H

/* BufFile is an opaque type whose details are not known outside buffile.c. */

typedef struct BufFile BufFile;

/*
 * prototypes for functions in buffile.c
 */

extern BufFile *BufFileCreateFile(const char * filePrefix, bool delOnClose, bool interXact);
extern BufFile *BufFileOpenFile(const char * fileName, bool create, bool delOnClose, bool interXact);
extern BufFile *BufFileCreateTemp(const char * filePrefix, bool interXact);
extern BufFile *BufFileCreateTemp_ReaderWriter(const char* fileName, bool isWriter);
extern void BufFileClose(BufFile *file);
extern void *BufFileSetBuf(BufFile *file, void *buf, Size bufsize);

extern Size BufFileRead(BufFile *file, void *ptr, Size size);
extern Size BufFileWrite(BufFile *file, const void *ptr, Size size);

extern int BufFileSeek(BufFile *file, int64 offset, int whence);
extern void BufFileTell(BufFile *file, int64 *offset);
extern int	BufFileSeekBlock(BufFile *file, int64 blknum);
extern void BufFileFlush(BufFile *file);
extern int64 BufFileGetSize(BufFile *buffile);
extern void BufFileSetWorkfile(BufFile *buffile);

#endif   /* BUFFILE_H */
