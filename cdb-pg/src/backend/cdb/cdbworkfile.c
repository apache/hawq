/*-------------------------------------------------------------------------
 *
 * cdbworkfile.c
 *
 * Wraps a BufFile object for workfile I/O.  Multiple streams ("subfiles")
 * can be stored in the same workfile.
 *
 * Copyright (c) 2007-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buffile.h"                /* BufFile */

#include "cdb/cdbworkfile.h"                /* me */


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

/*
 * CdbWorkfile_Create
 *
 * Returns a CdbWorkfile object, newly allocated from the given
 * memory context.
 *
 * The CdbWorkfile object owns a BufFile object, which in turn
 * owns a newly created temporary file.
 *
 * NB. The BufFile object and associated structures need to stick around
 * until released by BufFileClose().  A sufficiently long-lived context
 * must be used.
 */
CdbWorkfile *
CdbWorkfile_Create(const char *filename, MemoryContext context)
{
    CdbWorkfile    *workfile;
    MemoryContext   oldcontext = MemoryContextSwitchTo(context);

    /* Allocate our workfile struct. */
    workfile = (CdbWorkfile *)palloc0(sizeof(*workfile));

    workfile->context = context;

    /* Create next lower level workfile object (and the file itself). */
    workfile->buffile = BufFileCreateTemp(filename, false);

    /*
     * Tell BufFile not to interpose an extra layer of buffering.
     *
     * If caller intends to do small reads and writes which would benefit
     * from the extra buffering, caller can override this via another call
     * to BufFileSetBuf() upon return or anytime before the first I/O.
     */
    BufFileSetBuf(workfile->buffile, NULL, 0);

    /* Switch back to caller's memory context. */
    MemoryContextSwitchTo(oldcontext);
    return workfile;
}                               /* CdbWorkfile_Create */


/*
 * CdbWorkfile_Destroy
 *
 * Close the BufFile; close and remove the underlying file; and free the
 * BufFile and CdbWorkfile objects.
 */
void
CdbWorkfile_Destroy(CdbWorkfile *workfile)
{
    if (workfile)
    {
        if (workfile->buffile)
            BufFileClose(workfile->buffile);
        pfree(workfile);
    }
}                               /* CdbWorkfile_Destroy */


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
static inline void
CdbWorkfile_Seek(CdbWorkfile *workfile, uint64 seekto)
{
    Assert(workfile &&
           workfile->buffile);

    if (workfile->curseek != seekto)
    {
        if (BufFileSeek64(workfile->buffile, seekto))
            ereport(ERROR, (errmsg("Workfile I/O error: %m."),
                            errdetail("Failed to seek to offset " UINT64_FORMAT
                                      "in file of size " UINT64_FORMAT " bytes.",
                                      seekto,
                                      workfile->len)
                            ));
        workfile->curseek = seekto;
    }

    Assert(workfile->curseek == BufFileTell64(workfile->buffile));
}                               /* CdbWorkfile_Seek */


/*
 * CdbWorkfile_Read
 *
 * Read from workfile, starting at the given offset.
 *
 * This function assumes that the current position is cached in
 * workfile->curseek.
 *
 * NB. Depending on your needs, other ways you could read from a CdbWorkfile
 * include BufFileRead(workfile->buffile) or CdbWorkfile_Subfile_Iterator.
 */
void
CdbWorkfile_Read(CdbWorkfile *workfile, uint64 seekto, void *buf, Size len)
{
    Size    nread;

    CdbWorkfile_Seek(workfile, seekto);

    nread = BufFileRead(workfile->buffile, buf, len);
    if (nread != len)
        ereport(ERROR, (errmsg("Workfile I/O error: Unexpected eof "
                               "at offset " UINT64_FORMAT ".",
                               seekto + nread),
                        errdetail("Failed to read " UINT64_FORMAT " bytes "
                                  "starting at offset " UINT64_FORMAT ".  "
                                  "File size " UINT64_FORMAT "bytes.",
                                  (uint64)len,
                                  seekto,
                                  workfile->len)
                        ));

    workfile->curseek += nread;
}                               /* CdbWorkfile_Read */


/*
 * CdbWorkfile_Write
 *
 * Write to workfile, starting at the given offset.
 *
 * This function assumes that the current position is cached in
 * workfile->curseek.
 *
 * Caller should update workfile->len if needed after writing beyond the end.
 *
 * NB. Depending on your needs, other ways you could write to a CdbWorkfile
 * include BufFileWrite(workfile->buffile) or CdbWorkfile_Subfile_Append().
 */
void
CdbWorkfile_Write(CdbWorkfile *workfile, uint64 seekto, const void *src, Size len)
{
    Size    written;

    CdbWorkfile_Seek(workfile, seekto);

    written = BufFileWrite(workfile->buffile, src, len);
    if (written != len)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("Workfile I/O error: %m."),
                        errdetail("Failed to write " UINT64_FORMAT " bytes "
                                  "starting at offset " UINT64_FORMAT ".  "
                                  "File size " UINT64_FORMAT "bytes.",
                                  (uint64)len,
                                  seekto,
                                  workfile->len)
                        ));

    workfile->curseek += written;
}                               /* CdbWorkfile_Write */


/*-------------------------------------------------------------------------
 *                           CdbWorkfile_Subfile
 *-------------------------------------------------------------------------
 * A CdbWorkfile_Subfile object holds a collection of CdbWorkfile_Extent
 * entries representing subranges of a workfile which are to be treated
 * logically as a single file.
 *
 * Storing multiple data streams in the same workfile can be more
 * efficient than creating a workfile per stream.
 */

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
CdbWorkfile_Subfile_Create(CdbWorkfile *workfile, MemoryContext context)
{
    CdbWorkfile_Subfile    *subfile;

    Assert(workfile);

    /*
     * Allocate CdbWorkfile_Subfile struct, zero it, and initialize its
     * 'extents' field as an empty CdbCellBuf for the extent list.  Reserve
     * space for the initial cells of the extent list immediately following
     * the CdbWorkfile_Subfile struct.
     */
    subfile = CdbCellBuf_CreateWrap(sizeof(*subfile),
                                    offsetof(CdbWorkfile_Subfile, extents),
                                    sizeof(CdbWorkfile_Extent),
                                    10,     /* num_initial_cells */
                                    50,     /* num_cells_expand */
                                    context);
    subfile->workfile = workfile;
    subfile->npin = 1;

    return subfile;
}                               /* CdbWorkfile_Subfile_Create */


/*
 * CdbWorkfile_Subfile_Unpin
 *
 * Decrements the pin count.  Frees the subfile object when the pin count
 * reaches zero.  Note that the underlying workfile isn't affected by this.
 */
void
CdbWorkfile_Subfile_Unpin(CdbWorkfile_Subfile *subfile)
{
    if (subfile)
    {
        Insist(subfile->npin > 0);
        if (--subfile->npin == 0)
        {
            subfile->workfile = NULL;
            CdbCellBuf_Reset(&subfile->extents);
            pfree(subfile);
        }
    }
}                               /* CdbWorkfile_Subfile_Destroy */


/*
 * CdbWorkfile_Subfile_Append
 *
 * Write data from 'src' to workfile for 'len' bytes.  Updates the subfile
 * object to remember which portion of the workfile contains this data.
 */
void
CdbWorkfile_Subfile_Append(CdbWorkfile_Subfile *subfile, const void *src, Size len)
{
    CdbWorkfile        *workfile = subfile->workfile;
    CdbWorkfile_Extent *extent;

    Assert(subfile->npin > 0);
    Insist(workfile->len < workfile->len + len);    /* mustn't overflow */

    /* Find subfile's last extent. */
    extent = (CdbWorkfile_Extent *)CdbCellBuf_LastCell(&subfile->extents);

    Assert(!extent ||
           extent->seekbeg + extent->physlen <= workfile->len);

    /* If extent has some extra space due to truncation, start writing there. */
    if (extent &&
        extent->datalen < extent->physlen)
    {
        /* Can write all we want if the extent is at the end of the workfile. */
        if (extent->seekbeg + extent->physlen == workfile->len)
        {
            /* Chop off the unused portion of extent, then append as usual. */
            extent->physlen = extent->datalen;
            workfile->len = extent->seekbeg + extent->physlen;
        }

        /* Extent is followed by some other data; don't overwrite that. */
        else
        {
            Size    bytestowrite;

            /* Write to hole in workfile. */
            bytestowrite = (Size)Min(len, extent->physlen - extent->datalen);
            CdbWorkfile_Write(workfile,
                              extent->seekbeg + extent->datalen,
                              src,
                              bytestowrite);

            /* Update amount of data in extent and subfile. */
            extent->datalen += bytestowrite;
            subfile->len += bytestowrite;

            /* Return if hole holds whole requested amount. */
            if (bytestowrite == len)
                return;

            /* The rest will be written to a new extent. */
            len -= bytestowrite;
            src = (const char *)src + bytestowrite;
        }
    }

    /* Add new extent if current extent doesn't end at end of workfile. */
    if (!extent ||
        extent->seekbeg + extent->physlen != workfile->len)
    {
        extent = (CdbWorkfile_Extent *)CdbCellBuf_AppendCell(&subfile->extents);
        extent->seekbeg = workfile->len;
        extent->physlen = 0;
        extent->datalen = 0;
    }

    /* Write to end of workfile. */
    CdbWorkfile_Write(workfile, workfile->len, src, len);

    /* Update sizes of workfile, subfile and extent. */
    workfile->len += len;
    subfile->len += len;
    extent->physlen += len;
    extent->datalen += len;
}                               /* CdbWorkfile_Subfile_Append */


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
CdbWorkfile_Subfile_Truncate(CdbWorkfile_Subfile *subfile, uint64 newlen)
{
    CdbWorkfile_Extent *extent;

    Insist(newlen <= subfile->len &&
           subfile->len <= subfile->workfile->len);

    /* Get last extent of subfile. */
    extent = (CdbWorkfile_Extent *)CdbCellBuf_LastCell(&subfile->extents);
    Assert(extent ? (extent->datalen > 0 &&
                     extent->datalen <= subfile->len &&
                     extent->physlen <= subfile->workfile->len)
                  : subfile->len == 0);

    /* Drop one or more whole extents. */
    while (extent &&
           newlen <= subfile->len - extent->datalen)
    {
        /*
         * When we drop extents at end of workfile, adjust length so new extents
         * can be created in that space.  We could truncate the physical file to
         * actually free the space, but it's unclear whether that would be a win.
         */
        if (subfile->workfile->len == extent->seekbeg + extent->physlen)
            subfile->workfile->len = extent->seekbeg;

        subfile->len -= extent->datalen;
        CdbCellBuf_PopCell(&subfile->extents);
        extent = (CdbWorkfile_Extent *)CdbCellBuf_LastCell(&subfile->extents);
        Assert(extent ? (extent->datalen > 0 &&
                         extent->datalen <= subfile->len &&
                         extent->physlen <= subfile->workfile->len)
                      : subfile->len == 0);
    }

    /*
     * Shorten subfile's last extent.  We leave extent->physlen unchanged,
     * so as not to interfere with direct I/O.  (We hope the host file
     * system will schedule I/O transfers directly to/from our caller's
     * buffers; but usually this can occur only if the request size is a
     * multiple of some host-dependent block or page size.)
     */
    if (extent)
        extent->datalen -= subfile->len - newlen;

    subfile->len = newlen;
}                               /* CdbWorkfile_Subfile_Truncate */


/*-------------------------------------------------------------------------
 *                       CdbWorkfile_Subfile_Iterator
 *-------------------------------------------------------------------------
 * For reading from a CdbWorkfile_Subfile.
 */

/*
 * CdbWorkfile_Subfile_Iterator_SetPosHelper
 *
 * Helper called by inline function CdbWorkfile_Subfile_Iterator_SetPos() when
 * 'newoffset' is not within the current extent.
 *
 * Positions the iterator so that the next CdbWorkfile_Subfile_Iterator_Read()
 * will read starting at the given offset from the beginning of the subfile.
 */
void
CdbWorkfile_Subfile_Iterator_SetPosHelper(CdbWorkfile_Subfile_Iterator *it,
                                          uint64                        newoffset)
{
    CdbWorkfile_Extent *ex = it->extent;

    /* At beginning, get first extent. */
    if (!ex)
    {
        Assert(CdbCellBuf_Iterator_AtBegin(&it->extentit));
        ex = (CdbWorkfile_Extent *)CdbCellBuf_Iterator_NextCell(&it->extentit);
        if (!ex)
        {
            Assert(newoffset == 0 &&
                   it->extoffset == 0 &&
                   it->curseek == 0);
            return;
        }
    }

    /* For backwards repositioning, work forward from beginning of subfile. */
    if (newoffset < it->extoffset)
    {
        CdbCellBuf_Iterator_SetPos(&it->extentit, 0);
        ex = (CdbWorkfile_Extent *)CdbCellBuf_Iterator_NextCell(&it->extentit);
        it->extoffset = 0;
    }

    /* Advance to the extent which contains the newoffset. */
    while (ex)
    {
        Assert(ex->datalen > 0 &&
               ex->datalen <= ex->physlen &&
               ex->seekbeg + ex->physlen <= it->workfile->len);

        /* Finished if newoffset is in this extent. */
        if (newoffset <= it->extoffset + ex->datalen)
        {
            it->curseek = newoffset - it->extoffset + ex->seekbeg;
            it->extent = ex;
            return;
        }

        /* Onward to next extent. */
        it->extoffset += ex->datalen;
        ex = (CdbWorkfile_Extent *)CdbCellBuf_Iterator_NextCell(&it->extentit);
    }

    /* 'newoffset' must not be past end of subfile's data. */
    elog(ERROR, "Workfile I/O error: SetPos offset " UINT64_FORMAT
         " exceeds subfile size " UINT64_FORMAT,
         newoffset,
         it->extoffset);
}                               /* CdbWorkfile_Subfile_Iterator_SetPosHelper */


/*
 * CdbWorkfile_Subfile_Iterator_Read
 *
 * Read from subfile into 'buf' for 'len' bytes, starting from the current
 * position of the iterator 'it'.
 *
 * Returns 0 if 'len' bytes were read successfully.
 *
 * If end of subfile is encountered, returns the original value of 'len'
 * minus the number of bytes read.  The contents of the unused portion of
 * the buffer are undefined.  The iterator remains validly positioned
 * at the end of the subfile, so the caller could append more data and
 * continue reading.
 *
 * Advances the iterator to the end of the data that was read.
 */
Size
CdbWorkfile_Subfile_Iterator_Read(CdbWorkfile_Subfile_Iterator *it,
                                  void                         *buf,
                                  Size                          len)
{
    Assert(it->workfile);

    while (len > 0)
    {
        CdbWorkfile_Extent *ex = it->extent;
        Size                nwant;
        Size                nread;

        /* At end of extent, advance to next. */
        if (!ex ||
            it->curseek - ex->seekbeg == ex->datalen)
        {
            /* Quit if no more extents.  Iterator position remains at end. */
            ex = (CdbWorkfile_Extent *)CdbCellBuf_Iterator_NextCell(&it->extentit);
            if (!ex)
                break;

            /* Maintain total of sizes of extents before the current one. */
            if (it->extent)
                it->extoffset += it->extent->datalen;
            else
                Assert(it->extoffset == 0);

            it->extent = ex;
            it->curseek = ex->seekbeg;
        }

        Assert(it->curseek >= ex->seekbeg &&
               it->curseek < ex->seekbeg + ex->datalen &&
               ex->seekbeg + ex->physlen <= it->workfile->len);

        /*
         * Don't read past end of extent (ex->seekbeg + ex->physlen).
         *
         * Some host file systems can read directly from disk into the
         * caller's buffer, provided the buffer address and length are
         * multiples of a host-dependent block or page size.  Hoping to
         * facilitate direct I/O, we prefer to write and read whole
         * blocks rather than short ones.
         *
         * ex->datalen (valid content length) could be less than ex->physlen
         * (amount written) if the last block of the extent was filled out
         * with uninitialized padding to avoid writing a short block and then
         * was truncated to chop off the padding.  Read all the way to physlen
         * if the caller's buffer is big enough.
         */
        nwant = (Size)(ex->seekbeg + ex->physlen - it->curseek);
        if (nwant > len)
            nwant = len;

        /* Read into caller's buffer. */
        CdbWorkfile_Read(it->workfile, it->curseek, buf, nwant);
        nread = nwant;

        /* If we read past datalen in a truncated extent, ignore the excess. */
        if (ex->datalen < ex->physlen &&
            it->curseek - ex->seekbeg + nread > ex->datalen)
            nread -= (Size)(it->curseek - ex->seekbeg + nread - ex->datalen);

        /* Store new position relative to beginning of workfile. */
        it->curseek += nread;
        it->workfile->curseek = it->curseek;

        /* Advance thru caller's buffer. */
        buf = (char *)buf + nread;
        len -= nread;

        /*
         * Return to notify caller if read past datalen in a truncated extent.
         * Caller can call again to proceed to the next extent, hopefully with
         * a fresh buffer thus maintaining proper alignment for direct I/O.
         */
        if (nread < nwant)
            break;
    }
    return len;
}                               /* CdbWorkfile_Subfile_Iterator_Read */
