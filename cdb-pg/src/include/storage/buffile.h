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

extern BufFile *BufFileCreateTemp(const char * filePrefix, bool interXact);
extern BufFile *BufFileCreateTemp_ReaderWriter(const char* fileName, bool isWriter);
extern void BufFileClose(BufFile *file);
extern void *BufFileSetBuf(BufFile *file, void *buf, Size bufsize);

extern Size BufFileRead(BufFile *file, void *ptr, Size size);
extern Size BufFileWrite(BufFile *file, const void *ptr, Size size);

extern int	BufFileSeek(BufFile *file, int fileno, long offset, int whence);
extern void BufFileTell(BufFile *file, int *fileno, long *offset);
extern int  BufFileSeek64(BufFile *file, uint64 offset);
extern uint64 BufFileTell64(BufFile *file);
extern int	BufFileSeekBlock(BufFile *file, long blknum);
extern void BufFileFlush(BufFile *file);

extern char* BufFilePeek(BufFile *file, int size);


extern size_t sizeBufFile(BufFile * file); /* MPP */

extern int BufFileNumFiles(BufFile *file); 
#endif   /* BUFFILE_H */
