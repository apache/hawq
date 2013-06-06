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
 * by ereport(ERROR).	To avoid confusion, the caller should take care that
 * all calls for a single BufFile are made in the same palloc context.
 *
 * BufFile also supports temporary files that exceed the OS file size limit
 * (by opening multiple fd.c temporary files).	This is an essential feature
 * for sorts and hashjoins on large amounts of data.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/fd.h"
#include "storage/buffile.h"
#include "postmaster/primary_mirror_mode.h"

/*
 * The maximum safe file size is presumed to be RELSEG_SIZE * BLCKSZ.
 * Note we adhere to this limit whether or not LET_OS_MANAGE_FILESIZE
 * is defined, although md.c ignores it when that symbol is defined.
 */
#define MAX_PHYSICAL_FILESIZE  (RELSEG_SIZE * BLCKSZ)

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile {
	int numFiles; /* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File *files; /* palloc'd array with numFiles entries */
	long *offsets; /* palloc'd array with numFiles entries */

	/*
	 * offsets[i] is the current seek position of files[i].  We use this to
	 * avoid making redundant FileSeek calls.
	 */

	bool isTemp; /* can only add files if this is TRUE */
	bool closeAtEOXact; /* CDB: true => close at end of transaction */
	bool pfreeAtClose; /* CDB: true => buffer is ours, we pfree it */
	bool dirty; /* does buffer need to be written? */

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int curFile; /* file index (0..n) part of current pos */
	int curOffset; /* offset part of current pos */
	int pos; /* next read/write position in buffer */
	int nbytes; /* total # of valid bytes in buffer */

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
	bool is_rwfile;
	bool rwfile_iswriter;
	char *file_prefix;
};

static BufFile *makeBufFile(File firstfile);
static File extendBufFile(BufFile *file);

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isTemp true if appropriate.
 */
static BufFile *
makeBufFile(File firstfile)
{
	BufFile *file = (BufFile *) palloc0(sizeof(BufFile));

	file->numFiles = 1;
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = firstfile;
	file->offsets = (long *) palloc(sizeof(long));
	file->offsets[0] = 0L;
	file->isTemp = false;
	file->pfreeAtClose = false;
	file->dirty = false;

	file->curFile = 0;
	file->curOffset = 0L;
	file->pos = 0;
	file->nbytes = 0;
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
 * MPP: How much memory does a BufFile use?  (Just an estimate. NULL
 *      argument specifies a default BufFile.)  Must agree with
 *      makeBufFile.
 *
 *      TODO: Should we account for palloc overhead?
 */
size_t
sizeBufFile(BufFile * file)
{
	int n = (file == NULL) ? 1 : file->numFiles;

	return sizeof(BufFile) + n * (sizeof(File) + sizeof(long)) + BLCKSZ;
}

int
BufFileNumFiles(BufFile *file)
{
	return file->numFiles;
}

/*
 * Add another component temp file.
 */
static File
extendBufFile(BufFile *file)
{
	File pfile;
	bool create = !file->is_rwfile || file->rwfile_iswriter;

	Assert(file->isTemp);

	pfile = OpenTemporaryFile(file->file_prefix, file->numFiles + 1, /* extentseqnum */
							  !file->is_rwfile, /* makenameunique */
							  create, /* create */
							  create, /* delOnClose */
							  file->closeAtEOXact); /* closeAtEOXact */
	if (pfile < 0)
	{
		return pfile;
	}

	file->files = (File *)
		repalloc(file->files, (file->numFiles + 1) * sizeof(File));
	file->offsets = (long *)
		repalloc(file->offsets, (file->numFiles + 1) * sizeof(long));

	file->files[file->numFiles] = pfile;
	file->offsets[file->numFiles] = 0L;
	file->numFiles++;

	return pfile;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than MAX_PHYSICAL_FILESIZE bytes are
 * written to it).
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context that will survive across transaction boundaries.
 */
BufFile *
BufFileCreateTemp(const char * fileName, bool interXact)
{
	BufFile *file;
	File pfile;
	bool closeAtEOXact = !interXact;

	pfile = OpenTemporaryFile(fileName, 1, /* extentseqnum */
							  true, /* makenameunique */
							  true, /* create */
							  true, /* delOnClose */
							  closeAtEOXact); /* closeAtEOXact */
	Assert(pfile >= 0);

	file = makeBufFile(pfile);
	file->isTemp = true;
	file->closeAtEOXact = closeAtEOXact;
	file->is_rwfile = false;
	file->file_prefix = pstrdup(fileName);

	return file;
}

BufFile *
BufFileCreateTemp_ReaderWriter(const char* fileName, bool isWriter)
{
	BufFile *file;
	File pfile;

	pfile = OpenTemporaryFile(fileName, 1, /* extentseqnum */
							  false, /* makenameunique */
							  isWriter, /* create */
							  isWriter, /* delOnClose */
							  false); /* closeAtEOXact */
	if (pfile < 0)
	{
		TemporaryDirectorySanityCheck(fileName, errno, false);
		elog(ERROR, "could not open temporary file \"%s\": %m", fileName);
	}

	file = makeBufFile(pfile);
	file->isTemp = true;
	file->closeAtEOXact = false;
	file->is_rwfile = true;
	file->rwfile_iswriter = isWriter;
	file->file_prefix = pstrdup(fileName);

	if(!isWriter)
	{
		/* For readers, we need to call extendBufFile to all the files */
		while(extendBufFile(file) >= 0)
			;
	}

	return file;
}

#ifdef NOT_USED
/*
 * Create a BufFile and attach it to an already-opened virtual File.
 *
 * This is comparable to fdopen() in stdio.  This is the only way at present
 * to attach a BufFile to a non-temporary file.  Note that BufFiles created
 * in this way CANNOT be expanded into multiple files.
 */
BufFile *
BufFileCreate(File file)
{
	return makeBufFile(file);
}
#endif

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
void
BufFileClose(BufFile *file)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(file->context);
	int i;

	/* flush any unwritten data */
	if (!file->isTemp)
	{
		BufFileFlush(file);
	}

	/* close the underlying file(s) (with delete if it's a temp file) */
	for (i = 0; i < file->numFiles; i++)
	{
		FileClose(file->files[i]);
	}

	/* release the buffer space */
	if (file->buffer && file->pfreeAtClose)
	{
		pfree(file->buffer);
	}

	if (file->file_prefix)
	{
		pfree(file->file_prefix);
	}

	pfree(file->files);
	pfree(file->offsets);
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
	File thisfile;
	int nb;

	/*
	 * Advance to next component file if necessary and possible.
	 *
	 * This path can only be taken if there is more than one component, so it
	 * won't interfere with reading a non-temp file that is over
	 * MAX_PHYSICAL_FILESIZE.
	 */
	if (file->curOffset >= MAX_PHYSICAL_FILESIZE &&
		file->curFile + 1 < file->numFiles)
	{
		file->curFile++;
		file->curOffset = 0L;
	}

	/*
	 * Quit reading at end of physical file.  Caller will call again to
	 * proceed to the next file.
	 */
	if (file->curOffset + bufsize > MAX_PHYSICAL_FILESIZE && file->isTemp)
	{
		bufsize = MAX_PHYSICAL_FILESIZE - file->curOffset;
	}

	/*
	 * May need to reposition physical file.
	 */
	thisfile = file->files[file->curFile];
	if (file->curOffset != file->offsets[file->curFile])
	{
		if (FileSeek(thisfile, file->curOffset, SEEK_SET) != file->curOffset)
		{
			/* Do some sanity check and mark the path */
			TemporaryDirectorySanityCheck(FileGetName(thisfile), errno, false);
			elog(ERROR, "could not seek in temporary file: %m");
		}
		file->offsets[file->curFile] = file->curOffset;
	}

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	nb = FileRead(thisfile, buffer, (int)bufsize);
	if (nb < 0)
	{
		TemporaryDirectorySanityCheck(FileGetName(thisfile), errno, false);
		elog(ERROR, "could not read from temporary file: %m");
	}

	file->offsets[file->curFile] += nb;
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
	int wrote;
	File thisfile;

	/*
	 * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
	 * crosses a component-file boundary; so we need a loop.
	 */
	while (wpos < nbytes)
	{
		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->isTemp)
		{
			while (file->curFile + 1 >= file->numFiles)
			{
				File newTmpFile = extendBufFile(file);
				if (newTmpFile < 0)
				{
					TemporaryDirectorySanityCheck(FileGetName(newTmpFile), errno, false);
					elog(ERROR, "could not create (or extend) temporary file: %m");
					return;
				}
			}

			file->curFile++;
			file->curOffset = 0L;
		}

		/*
		 * Enforce per-file size limit only for temp files, else just try to
		 * write as much as asked...
		 */
		bytestowrite = nbytes - wpos;
		if (file->curOffset + bytestowrite > MAX_PHYSICAL_FILESIZE &&
			file->isTemp)
		{
			bytestowrite = MAX_PHYSICAL_FILESIZE - file->curOffset;
		}

		/*
		 * May need to reposition physical file.
		 */
		thisfile = file->files[file->curFile];
		if (file->curOffset != file->offsets[file->curFile])
		{
			if (FileSeek(thisfile, file->curOffset, SEEK_SET) != file->curOffset)
			{
				TemporaryDirectorySanityCheck(FileGetName(thisfile), errno, false);
				elog(ERROR, "could not seek in temporary file: %m");
			}

			file->offsets[file->curFile] = file->curOffset;
		}
		wrote = FileWrite(thisfile, (char *)buffer + wpos, (int)bytestowrite);
		if (wrote != bytestowrite)
		{
			TemporaryDirectorySanityCheck(FileGetName(thisfile), errno, false);
			elog(ERROR, "could not write %d bytes to temporary file: %m", (int)bytestowrite);
		}
		file->offsets[file->curFile] += wrote;
		file->curOffset += wrote;
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

			file->curOffset += file->pos;
			file->pos = 0;
			file->nbytes = 0;

			/*
			 * Read full blocks directly into caller's buffer.  If bufsize
			 * has been set to 0, read all the data into caller's buffer.
			 * Loop can be executed more than once if we read across a
			 * MAX_PHYSICAL_FILESIZE boundary.
			 */
			while (size >= file->bufsize)
			{
				size_t nwant = size;

				if (file->bufsize > 0)
				{
					nwant -= nwant % file->bufsize;
				}

				nthistime = BufFileLoadBuffer(file, ptr, nwant);
				file->curOffset += nthistime;
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
 *  BufFileReadRaw
 *
 *  Like BufFileRead() except read directly into user buffer
 */
static int
BufFileReadRaw(BufFile *file, void* ptr_, int size)
{
	char *ptr = (char*) ptr_;
	char *orig = (char*) ptr_;
	int n = 0;

	if (file->dirty)
		BufFileFlush(file);

	if (size <= 0)
		return 0;

	if (file->nbytes)
	{
		if (file->pos >= file->nbytes)
		{
			file->curOffset += file->pos;
			file->pos = file->nbytes = 0;
		}

		/* if there are bytes cached ... */
		if (file->pos < file->nbytes)
		{
			n = file->nbytes - file->pos;
			if (n > size)
				n = size;
			memcpy(ptr, file->buffer + file->pos, n);
			file->pos += n;
			ptr += n, size -= n;
		}
	}

	while (size > 0)
	{
		n = BufFileLoadBuffer(file, ptr, size);
		if (n <= 0)
			break;

		file->curOffset += n;
		ptr += n, size -= n;
	}

	return ptr - orig;
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
				BufFileDumpBuffer(file, file->buffer, file->nbytes);
			}
			else
			{
				/* Hmm, went directly from reading to writing? */
				file->curOffset += file->pos;
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

			BufFileDumpBuffer(file, ptr, nthistime);

			ptr = (void *) ((char *) ptr + nthistime);
			size -= nthistime;
			nwritten += nthistime;

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
		file->curOffset -= (nbytes - pos);
		if (file->curOffset < 0) /* handle possible segment crossing */
		{
			Assert(file->curFile > 0);
			file->curFile--;
			file->curOffset += MAX_PHYSICAL_FILESIZE;
		}
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
BufFileSeek(BufFile *file, int fileno, long offset, int whence)
{
	int newFile;
	long newOffset;

	switch (whence)
	{
		case SEEK_SET:
			if (fileno < 0)
				return EOF;
			newFile = fileno;
			newOffset = offset;
			break;

		case SEEK_CUR:
			/*
			 * Relative seek considers only the signed offset, ignoring
			 * fileno. Note that large offsets (> 1 gig) risk overflow in this
			 * add...
			 */
			newFile = file->curFile;
			newOffset = (file->curOffset + file->pos) + offset;
			break;
#ifdef NOT_USED
		case SEEK_END:
			/* could be implemented, not needed currently */
			break;
#endif
		default:
			elog(LOG, "invalid whence: %d", whence);
			Assert(false);
			return EOF;
	}
	while (newOffset < 0)
	{
		if (--newFile < 0)
			return EOF;
		newOffset += MAX_PHYSICAL_FILESIZE;
	}

	if (newFile == file->curFile &&
		newOffset >= file->curOffset &&
		newOffset <= file->curOffset + file->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.	Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */
		file->pos = (int) (newOffset - file->curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	BufFileFlush(file);

	/*
	 * If the value of newFile still indicates that new segment(s) are needed,
	 * we extend them here.
	 */
	while (newFile >= file->numFiles && file->isTemp)
	{
		File newTmpFile = extendBufFile(file);

		if (newTmpFile < 0)
		{
			return EOF;
		}
	}

	/*
	 * At this point and no sooner, check for seek past last segment. The
	 * above flush could have created a new segment, so checking sooner would
	 * not work (at least not with this code).
	 */
	if (file->isTemp)
	{
		/* convert seek to "start of next seg" to "end of last seg" */
		if (newFile == file->numFiles && newOffset == 0)
		{
			newFile--;
			newOffset = MAX_PHYSICAL_FILESIZE;
		}

		while (newOffset > MAX_PHYSICAL_FILESIZE)
		{
			if (++newFile >= file->numFiles)
			{
				return EOF;
			}
			newOffset -= MAX_PHYSICAL_FILESIZE;
		}
	}
	if (newFile >= file->numFiles)
	{
		return EOF;
	}
	/* Seek is OK! */
	file->curFile = newFile;
	file->curOffset = newOffset;
	file->pos = 0;
	file->nbytes = 0;
	return 0;
}

void BufFileTell(BufFile *file, int *fileno, long *offset)
{
	*fileno = file->curFile;
	*offset = file->curOffset + file->pos;
}

/*
 * BufFileSeek64
 *
 * Set position within virtual file, using 64-bit offset instead of
 * fileno/offset.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
int
BufFileSeek64(BufFile *file, uint64 offset)
{
	return BufFileSeek(file, (int) (offset / MAX_PHYSICAL_FILESIZE),
			(long) (offset % MAX_PHYSICAL_FILESIZE), SEEK_SET);
} /* BufFileSeek64 */

/*
 * BufFileTell64 --- return current 64-bit offset from beginning of virtual file
 */
uint64
BufFileTell64(BufFile *file)
{
	return file->curOffset + file->pos + MAX_PHYSICAL_FILESIZE
			* (uint64) file->curFile;
} /* BufFileTell64 */

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
BufFileSeekBlock(BufFile *file, long blknum)
{
	return BufFileSeek(file, (int) (blknum / RELSEG_SIZE), (blknum
			% RELSEG_SIZE) * BLCKSZ, SEEK_SET);
}

#ifdef NOT_USED
/*
 * BufFileTellBlock --- block-oriented tell
 *
 * Any fractional part of a block in the current seek position is ignored.
 */
long
BufFileTellBlock(BufFile *file)
{
	long blknum;

	blknum = (file->curOffset + file->pos) / BLCKSZ;
	blknum += file->curFile * RELSEG_SIZE;
	return blknum;
}

#endif

/*
 *  Peek into a file for sequential scans only. No seeking allowed.
 */
char *
BufFilePeek(BufFile *file, int size)
{
	int n;

	if (file->dirty)
		BufFileFlush(file);

	if (file->peek.max < BLCKSZ || file->peek.max < size)
	{
		/* get a reasonable buffer */
		file->peek.max = size + 2 * BLCKSZ;
		file->peek.buf = repalloc(file->peek.buf, file->peek.max);
	}

	if (!(file->peek.xFile == file->curFile &&
		  file->peek.xOffset == file->curOffset &&
		  file->peek.xPos == file->pos))
	{
		/* reset */
		file->peek.bot = file->peek.top = 0;
	}

	n = file->peek.top - file->peek.bot;
	if (n < size)
	{
		if (n > 0)
		{
			memmove(file->peek.buf, file->peek.buf + file->peek.bot, n);
		}

		file->peek.bot = 0;
		file->peek.top = n;

		/*
		  elog(NOTICE, "reading %d bytes (bot %d, top %d, max %d), size %d",
		  file->peek.max - file->peek.top,
		  file->peek.bot, file->peek.top, file->peek.max,
		  size);
		*/
		n = BufFileReadRaw(file, file->peek.buf + file->peek.top,
						   file->peek.max - file->peek.top);
		if (n < 0)
		{
			n = 0;
		}
		file->peek.top += n;

		/* adjust position if it moved */
		file->peek.xFile = file->curFile;
		file->peek.xOffset = file->curOffset;
		file->peek.xPos = file->pos;

		n = file->peek.top - file->peek.bot;
		if (n < size)
		{
			return 0;
		}
	}

	/* we have enough in the buffer:
	   advance, but send the ptr into the past */
	n = file->peek.bot;
	file->peek.bot += size;
	return file->peek.buf + n;
}

