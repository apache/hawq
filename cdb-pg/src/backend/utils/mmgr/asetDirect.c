/*-------------------------------------------------------------------------
 *
 * asetdirect.c
 *    A specialized implementation of the abstract MemoryContext type,
 *    which allocates directly from malloc() and does not support pfree().
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memutils.h"

#include "cdb/cdbptrbuf.h"              /* CdbPtrBuf */

/* Define this to detail debug alloc information */
/* #define HAVE_ALLOCINFO */


#ifdef CDB_PALLOC_CALLER_ID
#define CDB_MCXT_WHERE(context) (context)->callerFile, (context)->callerLine
#else
#define CDB_MCXT_WHERE(context) __FILE__, __LINE__
#endif


/*
 * AsetDirectContext
 */
typedef struct AsetDirectContext
{
    MemoryContextData   header;         /* standard memory-context fields */
    Size                size_total;     /* total size of all allocated areas */
    unsigned            narea_total;    /* number of allocated areas */
    CdbPtrBuf           areas;          /* collection of allocated area ptrs */

    /* variably-sized array, must be last */
    CdbPtrBuf_Ptr       areaspace[10];
} AsetDirectContext;

#define ASETDIRECTCONTEXT_BYTES(nareaspace) \
            (MAXALIGN(SIZEOF_VARSTRUCT(nareaspace, AsetDirectContext, areaspace)))


/*
 * These functions implement the MemoryContext API for AsetDirect contexts.
 */
static void *AsetDirectAlloc(MemoryContext context, Size size);
static void AsetDirectInit(MemoryContext context);
static void AsetDirectReset(MemoryContext context);
static void AsetDirectDelete(MemoryContext context);
static bool AsetDirectIsEmpty(MemoryContext context);
static void AsetDirectStats(MemoryContext context, const char* contextName);

#ifdef MEMORY_CONTEXT_CHECKING
static void AsetDirectCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for AsetDirect contexts.
 */
static MemoryContextMethods AsetDirectMethods = {
    AsetDirectAlloc,
    NULL,                               /* pfree */
    NULL,                               /* repalloc */
    AsetDirectInit,
    AsetDirectReset,
    AsetDirectDelete,
    NULL,                               /* GetChunkSpace */
    AsetDirectIsEmpty,
    AsetDirectStats,
#ifdef MEMORY_CONTEXT_CHECKING
    AsetDirectCheck
#endif
};


/* ----------
 * Debug macros
 * ----------
 */
#ifdef HAVE_ALLOCINFO
#define AllocAllocInfo(_cxt, _chunk) \
            fprintf(stderr, "AsetDirectAlloc: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_cxt)->chunksize)
#else
#define AllocAllocInfo(_cxt, _chunk)
#endif


/*
 * Public routines
 */


/*
 * AsetDirectContextCreate
 *      Create a new AsetDirect context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 */
MemoryContext
AsetDirectContextCreate(MemoryContext parent, const char *name)
{
    AsetDirectContext  *set;
    Size                namesize = MAXALIGN(strlen(name) + 1);
    Size                allocsize;
    Size                setsize;        /* #bytes to request for new context */
    int                 nareaspace;     /* num of slots in areaspace array */

    /*
     * Determine amount of memory to request for the AsetDirectContext struct.
     *
     * Assume the total allocation will be rounded up to a power of 2, and
     * will include the AsetDirectContext with variably sized 'areaspace' array
     * and the context 'name' string.  Size the 'areaspace' array to use up any
     * extra space in the expected allocation.
     */
    allocsize = 1 << ceil_log2_Size(MAXALIGN(sizeof(AsetDirectContext)) + namesize);
    nareaspace = VARELEMENTS_TO_FIT(allocsize - namesize, AsetDirectContext, areaspace);
    setsize = ASETDIRECTCONTEXT_BYTES(nareaspace);

    /*
     * Create the new memory context and hook up to parent context.
     */
    set = (AsetDirectContext *)MemoryContextCreate(T_AsetDirectContext,
                                                   setsize,
                                                   &AsetDirectMethods,
                                                   parent,
                                                   name);

    /*
     * Initialize empty collection of ptrs to allocated areas.
     */
    CdbPtrBuf_Init(&set->areas,
                   set->areaspace,
                   nareaspace,
                   50,                      /* num_cells_expand */
                   set->header.parent);
    return (MemoryContext)set;
}                               /* AsetDirectContextCreate */


/*
 * AsetDirectInit
 *      Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AsetDirectContextCreate can allocate memory when it gets control
 * back, however.)
 */
static void
AsetDirectInit(MemoryContext context)
{
	/*
	 * Since MemoryContextCreate already zeroed the context node, we don't
	 * have to do anything here: it's already OK.
	 */
}                               /* AsetDirectInit */


/*
 * AsetDirectReset
 *      Frees all memory which is allocated in the given set.
 */
static void
AsetDirectReset(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    CdbPtrBuf_Iterator  it;
    CdbPtrBuf_Ptr      *pp;
    CdbPtrBuf_Ptr       p;

    Assert(set && IsA(set, AsetDirectContext));
    Assert(CdbPtrBuf_IsOk(&set->areas));

    /* Free allocated areas. */
    CdbPtrBuf_Iterator_Init(&it, &set->areas);
    while (NULL != (pp = CdbPtrBuf_Iterator_NextCell(&it)))
    {
        p = *pp;
        *pp = NULL;
        if (p)
        {
#ifdef CLOBBER_FREED_MEMORY
            /* Wipe first few bytes of freed memory for debugging purposes */
            memset(p, 0x7F, MAXALIGN(1));   /* don't know actual size of area */
#endif
            free(p);
        }
    }

    /* Empty the 'areas' collection. */
    CdbPtrBuf_Reset(&set->areas);

    /* Update statistics. */
    MemoryContextNoteFree(&set->header, set->size_total);
    set->narea_total = 0;
    set->size_total = 0;
}                               /* AsetDirectReset */


/*
 * AsetDirectDelete
 *      Frees all memory which is allocated in the given set,
 *      in preparation for deletion of the set.
 *
 * Unlike AsetDirectReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void
AsetDirectDelete(MemoryContext context)
{
    AsetDirectReset(context);
}                               /* AsetDirectDelete */


/*
 * AsetDirectAlloc
 *      Returns pointer to allocated memory of given size; memory is added
 *      to the set.
 */
static void *
AsetDirectAlloc(MemoryContext context, Size size)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    CdbPtrBuf_Ptr      *pp;

    Assert(set && IsA(set, AsetDirectContext));

    if (size < MAXALIGN(1))
        size = MAXALIGN(1);

    /* Obtain a slot in 'areas' collection to point to the new allocation. */
    pp = CdbPtrBuf_Append(&set->areas, NULL);

    /* Allocate the memory. */
    *pp = malloc(size);
    if (!*pp)
        MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                           &set->header, CDB_MCXT_WHERE(&set->header),
                           "Out of memory.  Failed on request of size %lu bytes.",
                           (unsigned long)size);

    /* Update statistics. */
    set->size_total += size;
    set->narea_total++;
    MemoryContextNoteAlloc(&set->header, size);
    AllocAllocInfo(set, chunk);
    return *pp;
}                               /* AsetDirectAlloc */


/*
 * AsetDirectIsEmpty
 *      Is an allocset empty of any allocated space?
 */
static bool
AsetDirectIsEmpty(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;

    return set->narea_total == 0;
}                               /* AsetDirectIsEmpty */


/*
 * AsetDirectStats
 *      Displays stats about memory consumption of an allocset.
 */

/* Helper to portably right-justify an int64 in a field of specified width. */
static char *
aset_rjfmt_int64(int64 v, int width, char **inout_bufpos, char *bufend)
{
    char       *bp = *inout_bufpos;
    char        fmtbuf[32];
    int         len;
    int         pad;

    snprintf(fmtbuf, sizeof(fmtbuf), INT64_FORMAT, v);
    len = strlen(fmtbuf);
    pad = Max(width - len, 0);
    if (pad + len >= bufend - bp)
        return "***";
    memset(bp, ' ', pad);
    memcpy(bp+pad, fmtbuf, len);
    bp[pad+len] = '\0';
   *inout_bufpos += pad+len+1;
    return bp;
}                               /* aset_rjfmt_int64 */


static void
AsetDirectStats(MemoryContext context, const char* contextName)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    int64       held;
    char       *cfp;
    char       *efp;
    char        fmtbuf[200];

    Assert(set && IsA(set, AsetDirectContext));

    /* Display the totals. */
    efp = fmtbuf + sizeof(fmtbuf);
    cfp = fmtbuf;
    held = set->header.allBytesAlloc - set->header.allBytesFreed;
    write_stderr("  Subtree: %s KB held; %s KB peak; %s KB lifetime sum."
                 "   Node: %s KB held in %3u blocks."
                 "  %s\n",
                 aset_rjfmt_int64((held + 1023) / 1024, 8, &cfp, efp),
                 aset_rjfmt_int64((set->header.maxBytesHeld + 1023) / 1024, 8, &cfp, efp),
                 aset_rjfmt_int64((set->header.allBytesAlloc + 1023) / 1024, 9, &cfp, efp),
                 aset_rjfmt_int64((set->size_total + 1023) / 1024, 8, &cfp, efp),
                 set->narea_total,
                 contextName);
}


#ifdef MEMORY_CONTEXT_CHECKING
/*
 * AsetDirectCheck
 *      Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
AsetDirectCheck(MemoryContext context)
{
    AsetDirectContext  *set = (AsetDirectContext *)context;
    const char         *name = set->header.name;

    if (!IsA(set, AsetDirectContext))
        elog(WARNING, "problem in alloc set %s: type=%d",
             name, set->header.type);

    else if (!CdbPtrBuf_IsOk(&set->areas))
        elog(WARNING, "problem in alloc set %s: CdbPtrBuf error",
             name);

    else if (set->narea_total < 0 ||
             set->narea_total > CdbPtrBuf_Length(&set->areas))
        elog(WARNING, "problem in alloc set %s: narea=%d",
             name, set->narea_total);

}                               /* AsetDirectCheck */
#endif   /* MEMORY_CONTEXT_CHECKING */
