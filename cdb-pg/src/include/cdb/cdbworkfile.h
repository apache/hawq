/*-------------------------------------------------------------------------
 *
 * cdbworkfile.h
 *
 * Wraps a BufFile object for workfile I/O.  Multiple streams ("subfiles")
 * can be stored in the same workfile.
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBWORKFILE_H
#define CDBWORKFILE_H

#include "cdb/cdbcellbuf.h"             /* CdbCellBuf */

struct BufFile;                         /* #include "storage/buffile.h" */


/*-------------------------------------------------------------------------
 *                               CdbWorkfile
 *-------------------------------------------------------------------------
 * Wraps a BufFile object for workfile I/O.
 *
 * To write and read, callers can use BufFileWrite() and BufFileRead().
 *
 * Instead of BufFileWrite/Read, or in addition, callers also can write
 * and read via CdbWorkfile_Subfile objects which can be layered atop a
 * CdbWorkfile in order to multiplex any number of sequential streams
 * into one workfile.
 */
typedef struct CdbWorkfile
{
    struct BufFile     *buffile;
    uint64              curseek;        /* current read/write position (offset
                                         *  in bytes from beginning of file)
                                         */
    uint64              len;            /* file size in bytes */
    MemoryContext       context;        /* where to get memory for structs */
} CdbWorkfile;


/*
 * CdbWorkfile_Create
 *
 * Returns a CdbWorkfile object, newly allocated from the given memory context.
 *
 * The CdbWorkfile object owns a BufFile object, which in turn owns a
 * newly created temporary file.
 *
 * NB. The BufFile object and associated structures need to stick around
 * until released by BufFileClose().  A sufficiently long-lived context
 * must be used.
 */
CdbWorkfile *
CdbWorkfile_Create(const char *filename, MemoryContext context);

/*
 * CdbWorkfile_Destroy
 *
 * Close the workfile; close and remove the underlying file; and free the
 * BufFile and CdbWorkfile objects.
 */
void
CdbWorkfile_Destroy(CdbWorkfile *workfile);

/*
 * CdbWorkfile_Seek
 *
 * Position the workfile so that the next BufFileRead() or BufFileWrite()
 * will begin reading or writing at the specified offset (in bytes) from
 * the beginning of the file.
 *
 * This function assumes that the current position is cached in
 * workfile->curseek.  Callers intending to use this function must
 * cooperate by keeping the workfile->curseek field up to date when
 * reading or writing.
 */
/* made static inline
void
CdbWorkfile_Seek(CdbWorkfile *workfile, uint64 seekto);*/

/*
 * CdbWorkfile_Read
 *
 * Read from workfile, starting at the given offset.
 *
 * This function assumes that the current position is cached in
 * workfile->curseek.
 *
 * NB. Depending on your needs, other ways of reading from a CdbWorkfile
 * include BufFileRead(workfile->buffile) or CdbWorkfile_Subfile_Iterator.
 */
void
CdbWorkfile_Read(CdbWorkfile *workfile, uint64 seekto, void *buf, Size len);

/*
 * CdbWorkfile_Write
 *
 * Write to workfile, starting at the given offset.
 *
 * This function assumes that the current position is cached in
 * workfile->curseek.
 *
 * NB. Depending on your needs, other ways of writing to a CdbWorkfile include
 * BufFileWrite(workfile->buffile) or CdbWorkfile_Subfile_Append().
 */
void
CdbWorkfile_Write(CdbWorkfile *workfile, uint64 seekto, const void *src, Size len);


/*-------------------------------------------------------------------------
 *                            CdbWorkfile_Extent
 *-------------------------------------------------------------------------
 * Low and high bounds of a subrange of a workfile, as byte offsets
 * relative to the beginning of the workfile.
 */
typedef struct CdbWorkfile_Extent
{
    uint64              seekbeg;        /* beginning offset in workfile */
    uint64              physlen;        /* num of bytes reserved in workfile:
                                         *  ending offset minus seekbeg.
                                         */
    uint64              datalen;        /* number of bytes of data contained in
                                         *  the extent.  0 < datalen <= physlen
                                         */
} CdbWorkfile_Extent;


/*-------------------------------------------------------------------------
 *                           CdbWorkfile_Subfile
 *-------------------------------------------------------------------------
 * Storing multiple data streams in the same workfile can be more
 * efficient than creating a workfile per stream.
 *
 * A CdbWorkfile_Subfile object holds a collection of CdbWorkfile_Extent
 * entries representing subranges of a workfile which are to be treated
 * logically as a single file.
 *
 * A subfile object doesn't own any data buffers.
 */
typedef struct CdbWorkfile_Subfile
{
    CdbWorkfile        *workfile;       /* workfile which holds subfile data */
    uint64              len;            /* amount of data in subfile (bytes):
                                         *  datalen summed over all extents
                                         */
    CdbCellBuf          extents;        /* collection of Extent entries */
    int                 npin;           /* pin count: number of objects (such
                                         *  as a CdbChunkBuf_Filebuf) holding
                                         *  references to this subfile.
                                         */

    /* The first few 'extents' entries immediately follow the struct. */
} CdbWorkfile_Subfile;


/*
 * CdbWorkfile_Subfile_Create
 *
 * Returns a new empty subfile object which can be used for writing data
 * to 'workfile' and reading it back in.
 *
 * The subfile and associated (small) structures are allocated from 'context'.
 *
 * The subfile's pin count is initialized to 1.
 */
CdbWorkfile_Subfile *
CdbWorkfile_Subfile_Create(CdbWorkfile *workfile, MemoryContext context);

/*
 * CdbWorkfile_Subfile_Pin
 *
 * Increments the subfile's pin count.  The caller should later balance this
 * with a matching call to CdbWorkfile_Subfile_Unpin().
 */
static inline void
CdbWorkfile_Subfile_Pin(CdbWorkfile_Subfile *subfile)
{
    subfile->npin++;
}

/*
 * CdbWorkfile_Subfile_Unpin
 *
 * Decrements the pin count.  Frees the subfile object when the pin count
 * reaches zero.  Note that the underlying workfile isn't affected by this.
 */
void
CdbWorkfile_Subfile_Unpin(CdbWorkfile_Subfile *subfile);


/*
 * CdbWorkfile_Subfile_Append
 *
 * Write data from 'src' to workfile for 'len' bytes.  Updates the subfile
 * object to remember which portion of the workfile contains this data.
 */
void
CdbWorkfile_Subfile_Append(CdbWorkfile_Subfile *subfile, const void *src, Size len);

/*
 * CdbWorkfile_Subfile_Truncate
 *
 * Reset logical length of subfile to a value 'newlen' which is not greater
 * than the current length.  Contents of the subfile beyond 'newlen' become
 * undefined.  Does not affect the size of the underlying physical file.
 *
 * Has an undefined effect on any CdbWorkfile_Subfile_Iterator object whose
 * current position is 'newlen' or greater.
 */
void
CdbWorkfile_Subfile_Truncate(CdbWorkfile_Subfile *subfile, uint64 newlen);


/*-------------------------------------------------------------------------
 *                       CdbWorkfile_Subfile_Iterator
 *-------------------------------------------------------------------------
 * For reading from a CdbWorkfile_Subfile.
 */
typedef struct CdbWorkfile_Subfile_Iterator
{
    uint64              extoffset;      /* subfile offset at beginning of extent
                                         *  (datalen sum over subfile's extents
                                         *  which precede the current extent)
                                         */
    uint64              curseek;        /* offset of current position in workfile
                                         *  (in bytes from beginning of workfile)
                                         */
    CdbWorkfile_Extent *extent;         /* -> subfile extent entry whose bounds
                                         *  encompass the 'curseek' position
                                         */
    CdbWorkfile        *workfile;       /* file to read from */
    CdbCellBuf_Iterator extentit;       /* iterator over extents of subfile */
} CdbWorkfile_Subfile_Iterator;


/* Helpers for inline functions */
void
CdbWorkfile_Subfile_Iterator_SetPosHelper(CdbWorkfile_Subfile_Iterator *it,
                                          uint64                        newoffset);


/*
 * CdbWorkfile_Subfile_Iterator_Init
 *
 * Prepare to read from 'subfile', starting at the beginning.
 */
static inline void
CdbWorkfile_Subfile_Iterator_Init(CdbWorkfile_Subfile_Iterator *it,
                                  CdbWorkfile_Subfile          *subfile)
{
    Assert(subfile &&
           subfile->npin > 0 &&
           subfile->workfile);
    it->extoffset = 0;
    it->curseek = 0;
    it->extent = NULL;
    it->workfile = subfile->workfile;
    CdbCellBuf_Iterator_Init(&it->extentit, &subfile->extents);
}                               /* CdbWorkfile_Subfile_Iterator_Init */


/*
 * CdbWorkfile_Subfile_Iterator_GetSubfile
 *
 * Returns the subfile ptr.
 */
static inline CdbWorkfile_Subfile *
CdbWorkfile_Subfile_Iterator_GetSubfile(CdbWorkfile_Subfile_Iterator *it)
{
    CdbWorkfile_Subfile    *subfile;

    Assert(it &&
           it->workfile &&
           it->extentit.cellbuf &&
           it->extentit.cellbuf->cellbytes == sizeof(CdbWorkfile_Extent));
    subfile = (CdbWorkfile_Subfile *)((char *)it->extentit.cellbuf -
                                      offsetof(CdbWorkfile_Subfile, extents));
    Assert(subfile->workfile == it->workfile &&
           subfile->len <= subfile->workfile->len);
    return subfile;
}                               /* CdbWorkfile_Subfile_Iterator_GetSubfile */


/*
 * CdbWorkfile_Subfile_Iterator_Length
 *
 * Returns the subfile length.
 */
static inline uint64
CdbWorkfile_Subfile_Iterator_Length(CdbWorkfile_Subfile_Iterator *it)
{
    return CdbWorkfile_Subfile_Iterator_GetSubfile(it)->len;
}                               /* CdbWorkfile_Subfile_Iterator_Length */


/*
 * CdbWorkfile_Subfile_Iterator_GetPos
 *
 * Returns the subfile offset of the current position.  That is, the number
 * of bytes of data in the subfile from the beginning to the current position.
 */
static inline uint64
CdbWorkfile_Subfile_Iterator_GetPos(CdbWorkfile_Subfile_Iterator *it)
{
    return it->extent ? (it->curseek - it->extent->seekbeg + it->extoffset)
                      : 0;
}                               /* CdbWorkfile_Subfile_Iterator_GetPos */


/*
 * CdbWorkfile_Subfile_Iterator_SetPos
 *
 * Positions the iterator so that the next CdbWorkfile_Subfile_Iterator_Read()
 * will read starting at the given offset from the beginning of the subfile.
 */
static inline void
CdbWorkfile_Subfile_Iterator_SetPos(CdbWorkfile_Subfile_Iterator   *it,
                                    uint64                          newoffset)
{
    /* Fast path if the new position is within the current extent. */
    if (it->extent &&
        newoffset >= it->extoffset &&
        newoffset - it->extoffset <= it->extent->datalen)
        it->curseek = newoffset - it->extoffset + it->extent->seekbeg;
    else
        CdbWorkfile_Subfile_Iterator_SetPosHelper(it, newoffset);
}                               /* CdbWorkfile_Subfile_Iterator_SetPos */


/*
 * CdbWorkfile_Subfile_Iterator_Read
 *
 * Read from subfile into 'buf' for 'size' bytes, starting from the current
 * position of the iterator.
 *
 * Returns 0 if 'size' bytes were read successfully.  If end of subfile is
 * encountered, returns the original value of 'size' minus the number of
 * bytes read.
 *
 * Advances the iterator to the end of the data that was read.
 */
Size
CdbWorkfile_Subfile_Iterator_Read(CdbWorkfile_Subfile_Iterator *it,
                                  void                         *buf,
                                  Size                          len);

#endif   /* CDBWORKFILE_H */
