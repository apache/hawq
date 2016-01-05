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
 * buffile.c
 *	  Management of large buffered files, primarily temporary files.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/storage/file/buffile.c,v 1.24.2.1 2007/06/01 23:43:17 tgl Exp $
 *
 * NOTES:
 *
 * BufFiles provide a very incomplete emulation of stdio atop virtual Files
 * (as managed by fd.c).  Currently, we only support the buffered-I/O
 * aspect of stdio: a read or write of the low-level File occurs only
 * when the buffer is filled or emptied.  This is an even bigger win
 * for virtual Files than for ordinary kernel files, since reducing the
 * frequency with which a virtual File is touched reduces "thrashing"
 * of opening/closing file descriptors.
 *
 * Note that BufFile structs are allocated with palloc(), and therefore
 * will go away automatically at transaction end.  If the underlying
 * virtual File is made with OpenTemporaryFile, then all resources for
 * the file are certain to be cleaned up even if processing is aborted
 * by ereport(ERROR). To avoid confusion, the caller should take care that
 * all calls for a single BufFile are made in the same palloc context.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/fd.h"
#include "storage/buffile.h"
#include "postmaster/primary_mirror_mode.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "utils/workfile_mgr.h"

/*
 * This data structure represents a buffered file that consists of one
 * physical file (accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile {
	File file; /* palloc'd file*/

	bool isTemp; /* can only add files if this is TRUE */
	bool isWorkfile; /* true is file is managed by the workfile manager */
	bool pfreeAtClose; /* CDB: true => buffer is ours, we pfree it */
	bool dirty; /* does buffer need to be written? */

	int64 offset; /* offset part of current pos */
	int64 pos; /* next read/write position in buffer */
	int nbytes; /* total # of valid bytes in buffer */
	int64 maxoffset; /* maximum offset that this file has reached, for disk usage */

	Size bufsize; /* CDB: size of buffer (bytes) */
	char *buffer; /* CDB: -> buffer */
	MemoryContext context; /* CDB: where our memory comes from */

	struct {
		/* buffer that allows peeking */
		char* buf;
		int max, bot, top;

		/* "current pos" (see above) to ensure that the file did not
		   do any seeking behind BufFilePeek() */
		int xFile, xOffset, xPos;
	} peek;

	/*
	 * "common" prefix.  Used by shareinput reader/writer
	 * It takes form SIRW_%d_%d_%d, pid, address, fileSeqNum.
	 */
	char *file_prefix;
};

static BufFile *makeBufFile(File firstfile);
static void BufFileUpdateSize(BufFile *buffile);

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isTemp true if appropriate.
 */
static BufFile *
makeBufFile(File firstfile)
{
	BufFile *file = (BufFile *) palloc0(sizeof(BufFile));

	file->file = firstfile;

	file->isTemp = false;
	file->isWorkfile = false;
	file->pfreeAtClose = false;
	file->dirty = false;
	/*
	 * "current pos" is a position of start of buffer within the logical file.
	 * Position as seen by user of Buffile is (offset+pos).
	 * */
	file->offset = 0L;
	file->pos = 0;
	file->nbytes = 0;
	file->maxoffset = 0L;
	file->bufsize = BLCKSZ;
	file->buffer = NULL;
	file->context = CurrentMemoryContext;

	file->file_prefix = NULL;

	/* temporary space, we will repalloc later on first peek*/
	file->peek.buf = palloc(8);
	file->peek.max = 8;

	return file;
}


/*
 * Create a BufFile for a new temporary file.
 *
 * Adds the pgsql_tmp/ prefix to the file path before creating.
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context that will survive across transaction boundaries.
 */
BufFile *
BufFileCreateTemp(const char * filePrefix, bool interXact)
{
	BufFile *file;
	File pfile;
	bool closeAtEOXact = !interXact;

	pfile = OpenTemporaryFile(filePrefix, 1, /* extentseqnum */
							  true, /* makenameunique */
							  true, /* create */
							  true, /* delOnClose */
							  closeAtEOXact); /* closeAtEOXact */
	Assert(pfile >= 0);

	file = makeBufFile(pfile);
	file->isTemp = true;
	file->file_prefix = pstrdup(filePrefix);

	return file;
}

/*
 * Create a BufFile for a new file.
 *
 * Does not add the pgsql_tmp/ prefix to the file path before creating.
 *
 */
BufFile *
BufFileCreateFile(const char * fileName, bool delOnClose, bool interXact)
{
	return BufFileOpenFile(fileName,
			true, /* create */
			delOnClose,
			interXact);
}

/*
 * Opens an existing file as BufFile
 *
 * If create is true, the file is created if it doesn't exist.
 *
 * Does not add the pgsql_tmp/ prefix to the file path before opening.
 *
 */
BufFile *
BufFileOpenFile(const char * fileName, bool create, bool delOnClose, bool interXact)
{
	bool closeAtEOXact = !interXact;
	File pfile = OpenNamedFile(fileName,
							  create,
							  delOnClose,
							  closeAtEOXact); /* closeAtEOXact */
	/*
	 * If we are trying to open an existing file and it failed,
	 * signal this to the caller.
	 */
	if (!create && pfile <= 0)
	{
		return NULL;
	}

	Assert(pfile >= 0);

	BufFile *file = makeBufFile(pfile);
	file->isTemp = delOnClose;
	file->file_prefix = pstrdup(fileName);
	if (!create)
	{
		/* Open existing file, initialize its size */
		file->maxoffset = FileDiskSize(file->file);
	}

	return file;

}

/*
 * Create a BufFile for a new temporary file used for producer-consumer operators
 *
 * Adds the pgsql_tmp/ prefix to the file path before creating.
 *
 */
BufFile *
BufFileCreateTemp_ReaderWriter(const char* filePrefix, bool isWriter)
{
	File pfile = OpenTemporaryFile(filePrefix, 1, /* extentseqnum */
							  false, /* makenameunique */
							  isWriter, /* create */
							  isWriter, /* delOnClose */
							  false); /* closeAtEOXact */
	if (pfile < 0)
	{
		TemporaryDirectorySanityCheck(filePrefix, errno, false);
		elog(ERROR, "could not open temporary file \"%s\": %m", filePrefix);
	}

	BufFile *file = makeBufFile(pfile);
	file->isTemp = true;
	file->file_prefix = pstrdup(filePrefix);

	return file;
}

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
void
BufFileClose(BufFile *file)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(file->context);

	if (file->isWorkfile && WorkfileDiskspace_IsFull())
	{
		elog(gp_workfile_caching_loglevel, "closing workfile while workfile diskspace full, skipping flush");
	}
	else
	{
		/* flush any unwritten data */
		if (!file->isTemp)
		{
			/* This can thrown an exception */
			BufFileFlush(file);
		}
	}


	FileClose(file->file);

	/* release the buffer space */
	if (file->buffer && file->pfreeAtClose)
	{
		pfree(file->buffer);
	}

	if (file->file_prefix)
	{
		pfree(file->file_prefix);
	}

	pfree(file->peek.buf);
	pfree(file);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Assign or remove I/O buffer, and/or set buffer size.
 *
 * Logical file position remains unchanged.
 * A buffer assigned by the caller must be freed by the caller.
 * If 'bufsize' is set to 0, BufFileRead() and BufFileWrite() will use
 * their caller's buffer directly with no additional buffering.
 */
void *
BufFileSetBuf(BufFile *file, void *buf, Size bufsize)
{
	void *oldbuf = file->buffer;

	/* If bufsize is zero, buf must be NULL. */
	Assert(bufsize > 0 || buf == NULL);

	/* bufsize must fit in an int */
	Assert((int)bufsize >= 0);
	Assert(bufsize == (size_t)(int)bufsize);

	/* Just return if not changing anything. */
	if (file->buffer == buf && file->bufsize == bufsize)
	{
		return NULL;
	}

	/* Flush old buffer if dirty. */
	if (oldbuf)
	{
		BufFileFlush(file);
	}

	/* If we allocated the old buffer, free it. */
	if (oldbuf && file->pfreeAtClose)
	{
		file->buffer = NULL;
		pfree(oldbuf);
		oldbuf = NULL;
	}

	/* Install new buffer (or NULL). */
	file->buffer = (char *) buf;
	file->bufsize = bufsize;
	file->pfreeAtClose = false;

	/* The new buffer does not contain any data yet. */
	file->nbytes = 0;
	file->pos = 0;

	/* If old buffer belonged to caller, give it back. */
	return oldbuf;
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, pos and nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
static int BufFileLoadBuffer(BufFile *file, void* buffer, size_t bufsize)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(file->context);
	int nb;

	/*
	 * May need to reposition physical file.
	 */
	if (FileSeek(file->file, file->offset, SEEK_SET) != file->offset)
	{
		/* Do some sanity check and mark the path */
		TemporaryDirectorySanityCheck(FileGetName(file->file), errno, false);
		elog(ERROR, "could not seek in temporary file: %m");
	}


	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	nb = FileRead(file->file, buffer, (int)bufsize);
	if (nb < 0)
	{
		TemporaryDirectorySanityCheck(FileGetName(file->file), errno, false);
		elog(ERROR, "could not read from temporary file: %m");
	}

	/* we choose not to advance curOffset here */

	MemoryContextSwitchTo(oldcontext);
	return nb;
}

/* BufFileLoadBuffer */

/*
 * BufFileDumpBuffer
 *
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
static void BufFileDumpBuffer(BufFile *file, const void* buffer, Size nbytes)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(file->context);
	size_t wpos = 0;
	size_t bytestowrite;
	int wrote = 0;

	/*
	 * Unlike BufFileLoadBuffer, we must dump the whole buffer.
	 */
	while (wpos < nbytes)
	{
		bytestowrite = nbytes - wpos;

		if (FileSeek(file->file, file->offset, SEEK_SET) != file->offset)
		{
			elog(ERROR, "could not seek in temporary file: %m");
		}

		wrote = FileWrite(file->file, (char *)buffer + wpos, (int)bytestowrite);
		if (wrote != bytestowrite)
		{
			if (file->isWorkfile)
			{
				elog(gp_workfile_caching_loglevel, "FileWrite failed while writing to a workfile. Marking IO Error flag."
				     " offset=" INT64_FORMAT " pos=" INT64_FORMAT " maxoffset=" INT64_FORMAT " wpos=%d", 
				     file->offset, file->pos, file->maxoffset, (int) wpos);

				Assert(!WorkfileDiskspace_IsFull());
				WorkfileDiskspace_SetFull(true /* isFull */);
			}
			TemporaryDirectorySanityCheck(FileGetName(file->file), errno, false);
			elog(ERROR, "could not write %d bytes to temporary file: %m", (int)bytestowrite);
		}
		file->offset += wrote; 
		wpos += wrote;
	}
	file->dirty = false;

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	file->pos = 0;
	file->nbytes = 0;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * BufFileRead
 *
 * Like fread() except we assume 1-byte element size.
 */
Size
BufFileRead(BufFile *file, void *ptr, Size size)
{
	size_t nread = 0;
	int nthistime;

	if (file->dirty)
		BufFileFlush(file);

	while (size > 0)
	{
		if (file->pos >= file->nbytes)
		{
			Assert(file->pos == file->nbytes);

			file->offset += file->pos;
			file->pos = 0;
			file->nbytes = 0;

			/*
			 * Read full blocks directly into caller's buffer.  If bufsize
			 * has been set to 0, read all the data into caller's buffer.
			 */
			while (size >= file->bufsize)
			{
				size_t nwant = size;

				if (file->bufsize > 0)
				{
					nwant -= nwant % file->bufsize;
				}

				nthistime = BufFileLoadBuffer(file, ptr, nwant);
				file->offset += nthistime;
				ptr = (char *) ptr + nthistime;
				size -= nthistime;
				nread += nthistime;

				if (size == 0 || nthistime == 0)
				{
					return nread;
				}
			}

			/* Allocate buffer */
			if (!file->buffer)
			{
				file->pfreeAtClose = true;
				file->buffer = (char *)
					MemoryContextAlloc(file->context, file->bufsize);
			}

			/* Try to load more data into buffer. */
			file->nbytes = BufFileLoadBuffer(file, file->buffer, file->bufsize);
			if (file->nbytes == 0)
			{
				break; /* no more data available */
			}
		}

		nthistime = file->nbytes - file->pos;
		if (nthistime > size)
		{
			nthistime = (int) size;
		}

		Assert(nthistime > 0);

		memcpy(ptr, file->buffer + file->pos, nthistime);

		file->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}

	return nread;
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
Size
BufFileWrite(BufFile *file, const void *ptr, Size size)
{
	size_t nwritten = 0;
	size_t nthistime;

	while (size > 0)
	{
		if ((size_t) file->pos >= file->bufsize)
		{
			Assert((size_t)file->pos == file->bufsize);

			/* Buffer full, dump it out */
			if (file->dirty)
			{
				/* This can throw an exception, but it correctly updates the size when that happens */
				BufFileDumpBuffer(file, file->buffer, file->nbytes);
			}
			else
			{
				/* Hmm, went directly from reading to writing? */
				file->offset += file->pos;
				file->pos = 0;
				file->nbytes = 0;
			}
		}

		/*
		 * Write full blocks directly from caller's buffer.  If bufsize has
		 * been set to 0, write whole requested amount from caller's buffer.
		 */
		if (size >= file->bufsize && file->pos == 0)
		{
			nthistime = size;
			if (file->bufsize > 0)
			{
				nthistime -= nthistime % file->bufsize;
			}

			/* This can throw an exception, but it correctly updates the size when that happens */
			BufFileDumpBuffer(file, ptr, nthistime);

			ptr = (void *) ((char *) ptr + nthistime);
			size -= nthistime;
			nwritten += nthistime;

			BufFileUpdateSize(file);

			if (size == 0)
			{
				return nwritten;
			}
		}

		/* Allocate buffer */
		if (!file->buffer)
		{
			file->pfreeAtClose = true;
			file->buffer
				= (char *) MemoryContextAlloc(file->context, file->bufsize);
		}

		nthistime = file->bufsize - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(file->buffer + file->pos, ptr, nthistime);

		file->dirty = true;
		file->pos += (int) nthistime;
		if (file->nbytes < file->pos)
			file->nbytes = file->pos;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nwritten += nthistime;
	}

	BufFileUpdateSize(file);
	return nwritten;
}

/*
 * BufFileFlush
 *
 * Like fflush()
 */
void
BufFileFlush(BufFile *file)
{
	if (file->dirty)
	{
		int nbytes = file->nbytes;
		int pos = file->pos;

		BufFileDumpBuffer(file, file->buffer, nbytes);

		/*
		 * At this point, curOffset has been advanced to the end of the buffer,
		 * ie, its original value + nbytes.  We need to make it point to the
		 * logical file position, ie, original value + pos, in case that is less
		 * (as could happen due to a small backwards seek in a dirty buffer!)
		 */
		file->offset -= (nbytes - pos);
		BufFileUpdateSize(file);
	}
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by long.
 * We do not support relative seeks across more than LONG_MAX, however.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeek(BufFile *file, int64 offset, int whence)
{
	int64 newOffset;

	switch (whence)
	{
		case SEEK_SET:
			newOffset = offset;
			break;

		case SEEK_CUR:
			newOffset = (file->offset + file->pos) + offset;
			break;

		default:
			elog(LOG, "invalid whence: %d", whence);
			Assert(false);
			return EOF;
	}

	if (newOffset < 0)
	{
		newOffset = file->offset - newOffset;
		return EOF;
	}

	if (newOffset >= file->offset &&
		newOffset <= file->offset + file->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.	Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		file->pos = (newOffset - file->offset);
		BufFileUpdateSize(file);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	BufFileFlush(file);

	/* Seek is OK! */
	file->offset = newOffset;
	file->pos = 0;
	file->nbytes = 0;
	BufFileUpdateSize(file);
	return 0;
}

void BufFileTell(BufFile *file, int64 *offset)
{
	*offset = file->offset + file->pos;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * LONG_MAX bytes, but that is quite a lot; we don't work
 * with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeekBlock(BufFile *file, int64 blknum)
{
	return BufFileSeek(file, blknum * BLCKSZ, SEEK_SET);
}

/*
 * BufFileUpdateSize
 *
 * Updates the total disk size of a BufFile after a write
 *
 * Some of the data might still be in the buffer and not on disk, but we count
 * it here regardless
 */
static void
BufFileUpdateSize(BufFile *buffile)
{
	Assert(NULL != buffile);

	if (buffile->offset + buffile->pos > buffile->maxoffset)
	{
		buffile->maxoffset = buffile->offset + buffile->pos;
	}
}

/*
 * Returns the size of this file according to current accounting
 */
int64
BufFileGetSize(BufFile *buffile)
{
	Assert(NULL != buffile);

	BufFileUpdateSize(buffile);
	return buffile->maxoffset;
}

/*
 * Mark this file as being managed by the workfile manager
 */
void
BufFileSetWorkfile(BufFile *buffile)
{
	Assert(NULL != buffile);
	buffile->isWorkfile = true;
}
