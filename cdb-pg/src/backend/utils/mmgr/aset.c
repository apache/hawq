/*-------------------------------------------------------------------------
 *
 * aset.c
 *	  Allocation set definitions.
 *
 * AllocSet is our standard implementation of the abstract MemoryContext
 * type.
 *
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/utils/mmgr/aset.c,v 1.69 2006/11/08 19:27:24 tgl Exp $
 *
 * NOTE:
 *	This is a new (Feb. 05, 1999) implementation of the allocation set
 *	routines. AllocSet...() does not use OrderedSet...() any more.
 *	Instead it manages allocations in a block pool by itself, combining
 *	many small allocations in a few bigger blocks. AllocSetFree() normally
 *	doesn't free() memory really. It just add's the free'd area to some
 *	list for later reuse by AllocSetAlloc(). All memory blocks are free()'d
 *	at once on AllocSetReset(), which happens when the memory context gets
 *	destroyed.
 *				Jan Wieck
 *
 *	Performance improvement from Tom Lane, 8/99: for extremely large request
 *	sizes, we do want to be able to give the memory back to free() as soon
 *	as it is pfree()'d.  Otherwise we risk tying up a lot of memory in
 *	freelist entries that might never be usable.  This is specially needed
 *	when the caller is repeatedly repalloc()'ing a block bigger and bigger;
 *	the previous instances of the block were guaranteed to be wasted until
 *	AllocSetReset() under the old way.
 *
 *	Further improvement 12/00: as the code stood, request sizes in the
 *	midrange between "small" and "large" were handled very inefficiently,
 *	because any sufficiently large free chunk would be used to satisfy a
 *	request, even if it was much larger than necessary.  This led to more
 *	and more wasted space in allocated chunks over time.  To fix, get rid
 *	of the midrange behavior: we now handle only "small" power-of-2-size
 *	chunks as chunks.  Anything "large" is passed off to malloc().	Change
 *	the number of freelists to change the small/large boundary.
 *
 *
 *	About CLOBBER_FREED_MEMORY:
 *
 *	If this symbol is defined, all freed memory is overwritten with 0x7F's.
 *	This is useful for catching places that reference already-freed memory.
 *
 *	About MEMORY_CONTEXT_CHECKING:
 *
 *	Since we usually round request sizes up to the next power of 2, there
 *	is often some unused space immediately after a requested data area.
 *	Thus, if someone makes the common error of writing past what they've
 *	requested, the problem is likely to go unnoticed ... until the day when
 *	there *isn't* any wasted space, perhaps because of different memory
 *	alignment on a new platform, or some other effect.	To catch this sort
 *	of problem, the MEMORY_CONTEXT_CHECKING option stores 0x7E just beyond
 *	the requested space whenever the request is less than the actual chunk
 *	size, and verifies that the byte is undamaged when the chunk is freed.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/memutils.h"

#include "miscadmin.h"

/* Define this to detail debug alloc information */
/* #define HAVE_ALLOCINFO */

#ifdef CDB_PALLOC_CALLER_ID
#define CDB_MCXT_WHERE(context) (context)->callerFile, (context)->callerLine
#else
#define CDB_MCXT_WHERE(context) __FILE__, __LINE__
#endif

#if defined(CDB_PALLOC_TAGS) && !defined(CDB_PALLOC_CALLER_ID)
#error "If CDB_PALLOC_TAGS is defined, CDB_PALLOC_CALLER_ID must be defined too"
#endif


/*--------------------
 * Chunk freelist k holds chunks of size 1 << (k + ALLOC_MINBITS),
 * for k = 0 .. ALLOCSET_NUM_FREELISTS-1.
 *
 * Note that all chunks in the freelists have power-of-2 sizes.  This
 * improves recyclability: we may waste some space, but the wasted space
 * should stay pretty constant as requests are made and released.
 *
 * A request too large for the last freelist is handled by allocating a
 * dedicated block from malloc().  The block still has a block header and
 * chunk header, but when the chunk is freed we'll return the whole block
 * to malloc(), not put it on our freelists.
 *
 * CAUTION: ALLOC_MINBITS must be large enough so that
 * 1<<ALLOC_MINBITS is at least MAXALIGN,
 * or we may fail to align the smallest chunks adequately.
 * 8-byte alignment is enough on all currently known machines.
 *
 * With the current parameters, request sizes up to 8K are treated as chunks,
 * larger requests go into dedicated blocks.  Change ALLOCSET_NUM_FREELISTS
 * to adjust the boundary point.
 *--------------------
 */

#define ALLOC_MINBITS		3	/* smallest chunk size is 8 bytes */
#define ALLOCSET_NUM_FREELISTS	11
#define ALLOC_CHUNK_LIMIT	(1 << (ALLOCSET_NUM_FREELISTS-1+ALLOC_MINBITS))
/* Size of largest chunk that we use a fixed size for */

/*--------------------
 * The first block allocated for an allocset has size initBlockSize.
 * Each time we have to allocate another block, we double the block size
 * (if possible, and without exceeding maxBlockSize), so as to reduce
 * the bookkeeping load on malloc().
 *
 * Blocks allocated to hold oversize chunks do not follow this rule, however;
 * they are just however big they need to be to hold that single chunk.
 *--------------------
 */

#define ALLOC_BLOCKHDRSZ	MAXALIGN(sizeof(AllocBlockData))
#define ALLOC_CHUNKHDRSZ	MAXALIGN(sizeof(AllocChunkData))

typedef struct AllocBlockData *AllocBlock;		/* forward reference */
typedef struct AllocChunkData *AllocChunk;

/*
 * AllocPointer
 *		Aligned pointer which may be a member of an allocation set.
 */
typedef void *AllocPointer;

/*
 * AllocSetContext is our standard implementation of MemoryContext.
 *
 * Note: isReset means there is nothing for AllocSetReset to do.  This is
 * different from the aset being physically empty (empty blocks list) because
 * we may still have a keeper block.  It's also different from the set being
 * logically empty, because we don't attempt to detect pfree'ing the last
 * active chunk.
 */
typedef struct AllocSetContext
{
	MemoryContextData header;	/* Standard memory-context fields */
	/* Info about storage allocated in this context: */
	AllocBlock	blocks;			/* head of list of blocks in this set */
	AllocChunk	freelist[ALLOCSET_NUM_FREELISTS];		/* free chunk lists */
	bool		isReset;		/* T = no space alloced since last reset */
	/* Allocation parameters for this context: */
	Size		initBlockSize;	/* initial block size */
	Size		maxBlockSize;	/* maximum block size */
	Size		nextBlockSize;	/* next block size to allocate */
	AllocBlock	keeper;			/* if not NULL, keep this block over resets */
#ifdef CDB_PALLOC_TAGS
	AllocChunk  allocList;
#endif
} AllocSetContext;

typedef AllocSetContext *AllocSet;

/*
 * AllocBlock
 *		An AllocBlock is the unit of memory that is obtained by aset.c
 *		from malloc().	It contains one or more AllocChunks, which are
 *		the units requested by palloc() and freed by pfree().  AllocChunks
 *		cannot be returned to malloc() individually, instead they are put
 *		on freelists by pfree() and re-used by the next palloc() that has
 *		a matching request size.
 *
 *		AllocBlockData is the header data for a block --- the usable space
 *		within the block begins at the next alignment boundary.
 */
typedef struct AllocBlockData
{
	AllocSet	aset;			/* aset that owns this block */
	AllocBlock	next;			/* next block in aset's blocks list */
	char	   *freeptr;		/* start of free space in this block */
	char	   *endptr;			/* end of space in this block */
} AllocBlockData;

/*
 * AllocChunk
 *		The prefix of each piece of memory in an AllocBlock
 *
 * NB: this MUST match StandardChunkHeader as defined by utils/memutils.h.
 */
typedef struct AllocChunkData
{
	/* aset is the owning aset if allocated, or the freelist link if free */
	void	   *aset;
	/* size is always the size of the usable space in the chunk */
	Size		size;
#ifdef MEMORY_CONTEXT_CHECKING
	/* when debugging memory usage, also store actual requested size */
	/* this is zero in a free chunk */
	Size		requested_size;
#endif
#ifdef CDB_PALLOC_TAGS
	const char  *alloc_tag;
	int 		alloc_n;
	void *alloc_prev;
	void *alloc_next;
#endif
} AllocChunkData;

/*
 * AllocPointerIsValid
 *		True iff pointer is valid allocation pointer.
 */
#define AllocPointerIsValid(pointer) PointerIsValid(pointer)

/*
 * AllocSetIsValid
 *		True iff set is valid allocation set.
 */
#define AllocSetIsValid(set) PointerIsValid(set)

#define AllocPointerGetChunk(ptr)	\
					((AllocChunk)(((char *)(ptr)) - ALLOC_CHUNKHDRSZ))
#define AllocChunkGetPointer(chk)	\
					((AllocPointer)(((char *)(chk)) + ALLOC_CHUNKHDRSZ))

/*
 * These functions implement the MemoryContext API for AllocSet contexts.
 */
static void *AllocSetAlloc(MemoryContext context, Size size);
static void AllocSetFree(MemoryContext context, void *pointer);
static void *AllocSetRealloc(MemoryContext context, void *pointer, Size size);
static void AllocSetInit(MemoryContext context);
static void AllocSetReset(MemoryContext context);
static void AllocSetDelete(MemoryContext context);
static Size AllocSetGetChunkSpace(MemoryContext context, void *pointer);
static bool AllocSetIsEmpty(MemoryContext context);
static void AllocSetStats(MemoryContext context, const char* contextName);

#ifdef MEMORY_CONTEXT_CHECKING
static void AllocSetCheck(MemoryContext context);
#endif

/*
 * This is the virtual function table for AllocSet contexts.
 */
static MemoryContextMethods AllocSetMethods = {
	AllocSetAlloc,
	AllocSetFree,
	AllocSetRealloc,
	AllocSetInit,
	AllocSetReset,
	AllocSetDelete,
	AllocSetGetChunkSpace,
	AllocSetIsEmpty,
	AllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
	,AllocSetCheck
#endif
};


/* ----------
 * Debug macros
 * ----------
 */
#ifdef CDB_PALLOC_TAGS

void dump_memory_allocation(const char* fname)
{
	FILE *ofile = fopen(fname, "w+");
	dump_memory_allocation_ctxt(ofile, TopMemoryContext);
	fclose(ofile);
}

void dump_memory_allocation_ctxt(FILE *ofile, void *ctxt)
{
	AllocSet set = (AllocSet) ctxt;
	AllocSet next;
	AllocChunk chunk = set->allocList;

	while(chunk)
	{
#ifdef MEMORY_CONTEXT_CHECKING
        fprintf(ofile, "%ld|%s|%d|%d|%d\n", (long) ctxt, chunk->alloc_tag, chunk->alloc_n, (int) chunk->size, (int) chunk->requested_size);
#else
		fprintf(ofile, "%ld|%s|%d|%d\n", (long) ctxt, chunk->alloc_tag, chunk->alloc_n, (int) chunk->size);
#endif
		chunk = chunk->alloc_next;
	}

	next = (AllocSet) set->header.firstchild;
	while(next)
	{
		dump_memory_allocation_ctxt(ofile, next);
		next = (AllocSet) next->header.nextchild;
	}
}

static void AllocFreeInfo(AllocSet set, AllocChunk chunk)
{
	AllocChunk prev = chunk->alloc_prev;
	AllocChunk next = chunk->alloc_next;

	if(prev != NULL)
		prev->alloc_next = next;
	else
	{
		Assert(set->allocList == chunk);
		set->allocList = next;
	}

	if(next != NULL)
		next->alloc_prev = prev;

	chunk->alloc_tag = set->header.callerFile;
	chunk->alloc_n = set->header.callerLine;

	chunk->alloc_prev = NULL;
	chunk->alloc_next = NULL;
}

static void AllocAllocInfo(AllocSet set, AllocChunk chunk)
{
	chunk->alloc_tag = set->header.callerFile;
	chunk->alloc_n = set->header.callerLine;

	chunk->alloc_prev = NULL;
	chunk->alloc_next = set->allocList;

	if(set->allocList)
		set->allocList->alloc_prev = chunk;

	set->allocList = chunk;
}
#else
#ifdef HAVE_ALLOCINFO
#define AllocFreeInfo(_cxt, _chunk) \
			fprintf(stderr, "AllocFree: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#define AllocAllocInfo(_cxt, _chunk) \
			fprintf(stderr, "AllocAlloc: %s: %p, %d\n", \
				(_cxt)->header.name, (_chunk), (_chunk)->size)
#else
#define AllocFreeInfo(_cxt, _chunk)
#define AllocAllocInfo(_cxt, _chunk)
#endif
#endif

/* ----------
 * AllocSetFreeIndex -
 *
 *		Depending on the size of an allocation compute which freechunk
 *		list of the alloc set it belongs to.  Caller must have verified
 *		that size <= ALLOC_CHUNK_LIMIT.
 * ----------
 */
static inline int
AllocSetFreeIndex(Size size)
{
	int			idx = 0;

	if (size > 0)
	{
		size = (size - 1) >> ALLOC_MINBITS;
		while (size != 0)
		{
			idx++;
			size >>= 1;
		}
		Assert(idx < ALLOCSET_NUM_FREELISTS);
	}

	return idx;
}

#ifdef RANDOMIZE_ALLOCATED_MEMORY

/*
 * Fill a just-allocated piece of memory with "random" data.  It's not really
 * very random, just a repeating sequence with a length that's prime.  What
 * we mainly want out of it is to have a good probability that two palloc's
 * of the same number of bytes start out containing different data.
 */
static void
randomize_mem(char *ptr, size_t size)
{
	static int	save_ctr = 1;
	int			ctr;

	ctr = save_ctr;
	while (size-- > 0)
	{
		*ptr++ = ctr;
		if (++ctr > 251)
			ctr = 1;
	}
	save_ctr = ctr;
}

#endif /* RANDOMIZE_ALLOCATED_MEMORY */


/*
 * Public routines
 */


/*
 * AllocSetContextCreate
 *		Create a new AllocSet context.
 *
 * parent: parent context, or NULL if top-level context
 * name: name of context (for debugging --- string will be copied)
 * minContextSize: minimum context size
 * initBlockSize: initial allocation block size
 * maxBlockSize: maximum allocation block size
 */
MemoryContext
AllocSetContextCreate(MemoryContext parent,
					  const char *name,
					  Size minContextSize,
					  Size initBlockSize,
					  Size maxBlockSize)
{
	AllocSet	context;

	/* Do the type-independent part of context creation */
	context = (AllocSet) MemoryContextCreate(T_AllocSetContext,
											 sizeof(AllocSetContext),
											 &AllocSetMethods,
											 parent,
											 name);

	/*
	 * Make sure alloc parameters are reasonable, and save them.
	 *
	 * We somewhat arbitrarily enforce a minimum 1K block size.
	 */
	initBlockSize = MAXALIGN(initBlockSize);
	if (initBlockSize < 1024)
		initBlockSize = 1024;
	maxBlockSize = MAXALIGN(maxBlockSize);
	if (maxBlockSize < initBlockSize)
		maxBlockSize = initBlockSize;
	context->initBlockSize = initBlockSize;
	context->maxBlockSize = maxBlockSize;
	context->nextBlockSize = initBlockSize;

#ifdef CDB_PALLOC_TAGS
	context->allocList = NULL;
#endif

	/*
	 * Grab always-allocated space, if requested
	 */
	if (minContextSize > ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ)
	{
		Size		blksize = MAXALIGN(minContextSize);
		AllocBlock	block;

		block = (AllocBlock) gp_malloc(blksize);
		if (block == NULL)
            MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                               &context->header, CDB_MCXT_WHERE(&context->header),
                               "Out of memory.  Unable to allocate %lu bytes.",
                               (unsigned long)blksize);
		block->aset = context;
		block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
		block->endptr = ((char *) block) + blksize;
		block->next = context->blocks;
		context->blocks = block;
		/* Mark block as not to be released at reset time */
		context->keeper = block;
        MemoryContextNoteAlloc(&context->header, blksize);              /*CDB*/
	}

	context->isReset = true;

	return (MemoryContext) context;
}

/*
 * AllocSetInit
 *		Context-type-specific initialization routine.
 *
 * This is called by MemoryContextCreate() after setting up the
 * generic MemoryContext fields and before linking the new context
 * into the context tree.  We must do whatever is needed to make the
 * new context minimally valid for deletion.  We must *not* risk
 * failure --- thus, for example, allocating more memory is not cool.
 * (AllocSetContextCreate can allocate memory when it gets control
 * back, however.)
 */
static void
AllocSetInit(MemoryContext context)
{
	/*
	 * Since MemoryContextCreate already zeroed the context node, we don't
	 * have to do anything here: it's already OK.
	 */
}

/*
 * AllocSetReset
 *		Frees all memory which is allocated in the given set.
 *
 * Actually, this routine has some discretion about what to do.
 * It should mark all allocated chunks freed, but it need not necessarily
 * give back all the resources the set owns.  Our actual implementation is
 * that we hang onto any "keeper" block specified for the set.	In this way,
 * we don't thrash malloc() when a context is repeatedly reset after small
 * allocations, which is typical behavior for per-tuple contexts.
 */
static void
AllocSetReset(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block;

	AssertArg(AllocSetIsValid(set));

	/* Nothing to do if no pallocs since startup or last reset */
	if (set->isReset)
		return;

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	AllocSetCheck(context);
#endif
#ifdef CDB_PALLOC_TAGS
    set->allocList = NULL;
#endif

	/* Clear chunk freelists */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));

	block = set->blocks;

	/* New blocks list is either empty or just the keeper block */
	set->blocks = set->keeper;

	while (block != NULL)
	{
		AllocBlock	next = block->next;

		if (block == set->keeper)
		{
			/* Reset the block, but don't return it to malloc */
			char	   *datastart = ((char *) block) + ALLOC_BLOCKHDRSZ;

#ifdef CLOBBER_FREED_MEMORY
			/* Wipe freed memory for debugging purposes */
			memset(datastart, 0x7F, block->freeptr - datastart);
#endif
			block->freeptr = datastart;
			block->next = NULL;
		}
		else
		{
			size_t freesz = block->endptr - (char *) block;

			/* Normal case, release the block */
            MemoryContextNoteFree(&set->header, freesz); 
#ifdef CLOBBER_FREED_MEMORY
			/* Wipe freed memory for debugging purposes */
			memset(block, 0x7F, block->freeptr - ((char *) block));
#endif
			gp_free2(block, freesz);
		}
		block = next;
	}

	/* Reset block size allocation sequence, too */
	set->nextBlockSize = set->initBlockSize;

	set->isReset = true;
}

/*
 * AllocSetDelete
 *		Frees all memory which is allocated in the given set,
 *		in preparation for deletion of the set.
 *
 * Unlike AllocSetReset, this *must* free all resources of the set.
 * But note we are not responsible for deleting the context node itself.
 */
static void
AllocSetDelete(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block = set->blocks;

	AssertArg(AllocSetIsValid(set));

#ifdef MEMORY_CONTEXT_CHECKING
	/* Check for corruption and leaks before freeing */
	AllocSetCheck(context);
#endif

	/* Make it look empty, just in case... */
	MemSetAligned(set->freelist, 0, sizeof(set->freelist));
	set->blocks = NULL;
	set->keeper = NULL;

	while (block != NULL)
	{
		AllocBlock	next = block->next;
		size_t freesz = block->endptr - (char *) block;

        MemoryContextNoteFree(&set->header, freesz); 
#ifdef CLOBBER_FREED_MEMORY
		/* Wipe freed memory for debugging purposes */
		memset(block, 0x7F, block->freeptr - ((char *) block));
#endif
		gp_free2(block, freesz);
		block = next;
	}
}

/*
 * AllocSetAlloc
 *		Returns pointer to allocated memory of given size; memory is added
 *		to the set.
 */
static void *
AllocSetAlloc(MemoryContext context, Size size)
{
	AllocSet	set = (AllocSet) context;
	AllocBlock	block;
	AllocChunk	chunk;
	int			fidx;
	Size		chunk_size;
	Size		blksize;

	AssertArg(AllocSetIsValid(set));
#ifdef USE_ASSERT_CHECKING
	if (IsUnderPostmaster && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
#if defined(__darwin__)
		elog(ERROR,"palloc called from thread (OS-X pthread_sigmask is broken: MPP-4923)");
#else
		elog(ERROR,"palloc called from thread");
#endif
	}
#endif

	/*
	 * If requested size exceeds maximum for chunks, allocate an entire block
	 * for this request.
	 */
	if (size > ALLOC_CHUNK_LIMIT)
	{
		chunk_size = MAXALIGN(size);
		blksize = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		block = (AllocBlock) gp_malloc(blksize);
		if (block == NULL)
            MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                               &set->header, CDB_MCXT_WHERE(&set->header),
                               "Out of memory.  Failed on request of size %lu bytes.",
                               (unsigned long)size);
		block->aset = set;
		block->freeptr = block->endptr = ((char *) block) + blksize;

		chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
		chunk->aset = set;
		chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
		chunk->requested_size = size;
		/* set mark to catch clobber of "unused" space */
		if (size < chunk_size)
			((char *) AllocChunkGetPointer(chunk))[size] = 0x7E;
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		/*
		 * Stick the new block underneath the active allocation block, so that
		 * we don't lose the use of the space remaining therein.
		 */
		if (set->blocks != NULL)
		{
			block->next = set->blocks->next;
			set->blocks->next = block;
		}
		else
		{
			block->next = NULL;
			set->blocks = block;
		}

		set->isReset = false;
        MemoryContextNoteAlloc(&set->header, blksize);              /*CDB*/

		AllocAllocInfo(set, chunk);
		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Request is small enough to be treated as a chunk.  Look in the
	 * corresponding free list to see if there is a free chunk we could reuse.
	 * If one is found, remove it from the free list, make it again a member
	 * of the alloc set and return its data address.
	 */
	fidx = AllocSetFreeIndex(size);
	chunk = set->freelist[fidx];
	if (chunk != NULL)
	{
		Assert(chunk->size >= size);

		set->freelist[fidx] = (AllocChunk) chunk->aset;

		chunk->aset = (void *) set;

#ifdef MEMORY_CONTEXT_CHECKING
		chunk->requested_size = size;
		/* set mark to catch clobber of "unused" space */
		if (size < chunk->size)
			((char *) AllocChunkGetPointer(chunk))[size] = 0x7E;
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* fill the allocated space with junk */
		randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

		/* isReset must be false already */
		Assert(!set->isReset);

		AllocAllocInfo(set, chunk);
		return AllocChunkGetPointer(chunk);
	}

	/*
	 * Choose the actual chunk size to allocate.
	 */
	chunk_size = (1 << ALLOC_MINBITS) << fidx;
	Assert(chunk_size >= size);

	/*
	 * If there is enough room in the active allocation block, we will put the
	 * chunk into that block.  Else must start a new one.
	 */
	if ((block = set->blocks) != NULL)
	{
		Size		availspace = block->endptr - block->freeptr;

		if (availspace < (chunk_size + ALLOC_CHUNKHDRSZ))
		{
			/*
			 * The existing active (top) block does not have enough room for
			 * the requested allocation, but it might still have a useful
			 * amount of space in it.  Once we push it down in the block list,
			 * we'll never try to allocate more space from it. So, before we
			 * do that, carve up its free space into chunks that we can put on
			 * the set's freelists.
			 *
			 * Because we can only get here when there's less than
			 * ALLOC_CHUNK_LIMIT left in the block, this loop cannot iterate
			 * more than ALLOCSET_NUM_FREELISTS-1 times.
			 */
			while (availspace >= ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ))
			{
				Size		availchunk = availspace - ALLOC_CHUNKHDRSZ;
				int			a_fidx = AllocSetFreeIndex(availchunk);

				/*
				 * In most cases, we'll get back the index of the next larger
				 * freelist than the one we need to put this chunk on.	The
				 * exception is when availchunk is exactly a power of 2.
				 */
				if (availchunk != ((Size)1 << (a_fidx + ALLOC_MINBITS)))
				{
					a_fidx--;
					Assert(a_fidx >= 0);
					availchunk = ((Size)1 << (a_fidx + ALLOC_MINBITS));
				}

				chunk = (AllocChunk) (block->freeptr);

				block->freeptr += (availchunk + ALLOC_CHUNKHDRSZ);
				availspace -= (availchunk + ALLOC_CHUNKHDRSZ);

				chunk->size = availchunk;
#ifdef MEMORY_CONTEXT_CHECKING
				chunk->requested_size = 0xFFFFFFFF;	/* mark it free */
#endif
				chunk->aset = (void *) set->freelist[a_fidx];
				set->freelist[a_fidx] = chunk;
			}

			/* Mark that we need to create a new block */
			block = NULL;
		}
	}

	/*
	 * Time to create a new regular (multi-chunk) block?
	 */
	if (block == NULL)
	{
		Size		required_size;

		/*
		 * The first such block has size initBlockSize, and we double the
		 * space in each succeeding block, but not more than maxBlockSize.
		 */
		blksize = set->nextBlockSize;
		set->nextBlockSize <<= 1;
		if (set->nextBlockSize > set->maxBlockSize)
			set->nextBlockSize = set->maxBlockSize;

		/*
		 * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
		 * space... but try to keep it a power of 2.
		 */
		required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		while (blksize < required_size)
			blksize <<= 1;

		/* Try to allocate it */
		block = (AllocBlock) gp_malloc(blksize);

		/*
		 * We could be asking for pretty big blocks here, so cope if malloc
		 * fails.  But give up if there's less than a meg or so available...
		 */
		while (block == NULL && blksize > 1024 * 1024)
		{
			blksize >>= 1;
			if (blksize < required_size)
				break;
			block = (AllocBlock) gp_malloc(blksize);
		}

		if (block == NULL)
            MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                               &set->header, CDB_MCXT_WHERE(&set->header),
                               "Out of memory.  Failed on request of size %lu bytes.",
                               (unsigned long)size);

		block->aset = set;
		block->freeptr = ((char *) block) + ALLOC_BLOCKHDRSZ;
		block->endptr = ((char *) block) + blksize;

		/*
		 * If this is the first block of the set, make it the "keeper" block.
		 * Formerly, a keeper block could only be created during context
		 * creation, but allowing it to happen here lets us have fast reset
		 * cycling even for contexts created with minContextSize = 0; that way
		 * we don't have to force space to be allocated in contexts that might
		 * never need any space.  Don't mark an oversize block as a keeper,
		 * however.
		 */
		if (set->keeper == NULL && blksize == set->initBlockSize)
			set->keeper = block;

		block->next = set->blocks;
		set->blocks = block;
        MemoryContextNoteAlloc(&set->header, blksize);              /*CDB*/
	}

	/*
	 * OK, do the allocation
	 */
	chunk = (AllocChunk) (block->freeptr);

	block->freeptr += (chunk_size + ALLOC_CHUNKHDRSZ);
	Assert(block->freeptr <= block->endptr);

	chunk->aset = (void *) set;
	chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
	chunk->requested_size = size;
	/* set mark to catch clobber of "unused" space */
	if (size < chunk->size)
		((char *) AllocChunkGetPointer(chunk))[size] = 0x7E;
#endif
#ifdef RANDOMIZE_ALLOCATED_MEMORY
	/* fill the allocated space with junk */
	randomize_mem((char *) AllocChunkGetPointer(chunk), size);
#endif

	set->isReset = false;

	AllocAllocInfo(set, chunk);

	return AllocChunkGetPointer(chunk);
}

/*
 * AllocSetFree
 *		Frees allocated memory; memory is removed from the set.
 */
static void
AllocSetFree(MemoryContext context, void *pointer)
{
	AllocSet	set = (AllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

        Assert(chunk->size > 0);
	AllocFreeInfo(set, chunk);
#ifdef USE_ASSERT_CHECKING
	/*
	 * This check doesnt work because pfree is called during error handling from inside
	 * AtAbort_Portals.  That can only happen if the error was from a thread (say a SEGV)
	 */
	/*
	if (IsUnderPostmaster && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
		elog(ERROR,"pfree called from thread");
	}
	*/
#endif

#ifdef MEMORY_CONTEXT_CHECKING
	/* Test for someone scribbling on unused space in chunk */
    Assert(chunk->requested_size != 0xFFFFFFFF);
	if (chunk->requested_size < chunk->size)
		if (((char *) pointer)[chunk->requested_size] != 0x7E)
		{
			Assert(!"Memory error");
			elog(WARNING, "detected write past chunk end in %s %p (%s:%d)",
					set->header.name, chunk, CDB_MCXT_WHERE(&set->header));
		}
#endif

	if (chunk->size > ALLOC_CHUNK_LIMIT)
	{
		/*
		 * Big chunks are certain to have been allocated as single-chunk
		 * blocks.	Find the containing block and return it to malloc().
		 */
		AllocBlock	block = set->blocks;
		AllocBlock	prevblock = NULL;

		size_t freesz;

		while (block != NULL)
		{
			if (chunk == (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ))
				break;
			prevblock = block;
			block = block->next;
		}
		if (block == NULL)
            MemoryContextError(ERRCODE_INTERNAL_ERROR,
                               &set->header, CDB_MCXT_WHERE(&set->header),
                               "could not find block containing chunk %p", chunk);
		/* let's just make sure chunk is the only one in the block */
		Assert(block->freeptr == ((char *) block) +
			   (chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ));

		/* OK, remove block from aset's list and free it */
		if (prevblock == NULL)
			set->blocks = block->next;
		else
			prevblock->next = block->next;

		freesz = block->endptr - (char *) block;
        MemoryContextNoteFree(&set->header, freesz); 
		gp_free2(block, freesz);
	}
	else
	{
		/* Normal case, put the chunk into appropriate freelist */
		int			fidx = AllocSetFreeIndex(chunk->size);

		chunk->aset = (void *) set->freelist[fidx];

#ifdef CLOBBER_FREED_MEMORY
		/* Wipe freed memory for debugging purposes */
		memset(pointer, 0x7F, chunk->size);
#endif

#ifdef MEMORY_CONTEXT_CHECKING
		/* Reset requested_size to 0 in chunks that are on freelist */
		chunk->requested_size = 0xFFFFFFFF;
#endif
		set->freelist[fidx] = chunk;
                Assert(chunk->size > 0);
	}
}

/*
 * AllocSetRealloc
 *		Returns new pointer to allocated memory of given size; this memory
 *		is added to the set.  Memory associated with given pointer is copied
 *		into the new memory, and the old memory is freed.
 */
static void *
AllocSetRealloc(MemoryContext context, void *pointer, Size size)
{
	AllocSet	set = (AllocSet) context;
	AllocChunk	chunk = AllocPointerGetChunk(pointer);
	Size		oldsize = chunk->size;

#ifdef USE_ASSERT_CHECKING
	if (IsUnderPostmaster  && context != ErrorContext && mainthread() != 0 && !pthread_equal(main_tid, pthread_self()))
	{
#if defined(__darwin__)
		elog(ERROR,"prealloc called from thread (OS-X pthread_sigmask is broken: MPP-4923)");
#else
		elog(ERROR,"prealloc called from thread");
#endif
	}
#endif
#ifdef MEMORY_CONTEXT_CHECKING
	/* Test for someone scribbling on unused space in chunk */
    Assert(chunk->requested_size != 0xFFFFFFFF);
	if (chunk->requested_size < oldsize)
		if (((char *) pointer)[chunk->requested_size] != 0x7E)
		{
			Assert(!"Memory error");
			elog(WARNING, "detected write past chunk end in %s %p (%s:%d)",
				 set->header.name, chunk, CDB_MCXT_WHERE(&set->header));
		}
#endif

	/* isReset must be false already */
	Assert(!set->isReset);

	/*
	 * Chunk sizes are aligned to power of 2 in AllocSetAlloc(). Maybe the
	 * allocated area already is >= the new size.  (In particular, we always
	 * fall out here if the requested size is a decrease.)
	 */
	if (oldsize >= size)
	{
		AllocFreeInfo(set, chunk);
#ifdef MEMORY_CONTEXT_CHECKING
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		if (size > chunk->requested_size)
			randomize_mem((char *) AllocChunkGetPointer(chunk) + chunk->requested_size,
						  size - chunk->requested_size);
#endif

		chunk->requested_size = size;
		/* set mark to catch clobber of "unused" space */
		if (size < oldsize)
			((char *) pointer)[size] = 0x7E;
#endif
		AllocAllocInfo(set, chunk);
		return pointer;
	}

	if (oldsize > ALLOC_CHUNK_LIMIT)
	{
		/*
		 * The chunk must have been allocated as a single-chunk block.	Find
		 * the containing block and use realloc() to make it bigger with
		 * minimum space wastage.
		 */
		AllocBlock	block = set->blocks;
		AllocBlock	prevblock = NULL;
		Size		chksize;
		Size		blksize;
        Size        oldblksize;

		AllocFreeInfo(set, chunk);

		while (block != NULL)
		{
			if (chunk == (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ))
				break;
			prevblock = block;
			block = block->next;
		}
		if (block == NULL)
            MemoryContextError(ERRCODE_INTERNAL_ERROR,
                               &set->header, CDB_MCXT_WHERE(&set->header),
                               "could not find block containing chunk %p", chunk);
		/* let's just make sure chunk is the only one in the block */
		Assert(block->freeptr == ((char *) block) +
			   (chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ));

		/* Do the realloc */
        oldblksize = block->endptr - (char *)block;
		chksize = MAXALIGN(size);
		blksize = chksize + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
		block = (AllocBlock) gp_realloc(block, oldblksize, blksize);
		if (block == NULL)
            MemoryContextError(ERRCODE_OUT_OF_MEMORY,
                               &set->header, CDB_MCXT_WHERE(&set->header),
                               "Out of memory.  Failed on request of size %lu bytes.",
                               (unsigned long)size);
		block->freeptr = block->endptr = ((char *) block) + blksize;

		/* Update pointers since block has likely been moved */
		chunk = (AllocChunk) (((char *) block) + ALLOC_BLOCKHDRSZ);
		if (prevblock == NULL)
			set->blocks = block;
		else
			prevblock->next = block;
		chunk->size = chksize;

#ifdef MEMORY_CONTEXT_CHECKING
#ifdef RANDOMIZE_ALLOCATED_MEMORY
		/* We can only fill the extra space if we know the prior request */
		randomize_mem((char *) AllocChunkGetPointer(chunk) + chunk->requested_size,
					  size - chunk->requested_size);
#endif

		chunk->requested_size = size;
		/* set mark to catch clobber of "unused" space */
		if (size < chunk->size)
			((char *) AllocChunkGetPointer(chunk))[size] = 0x7E;
#endif

		AllocAllocInfo(set, chunk);
        MemoryContextNoteAlloc(&set->header, blksize - oldblksize); /*CDB*/
		return AllocChunkGetPointer(chunk);
	}
	else
	{
		/*
		 * Small-chunk case.  We just do this by brute force, ie, allocate a
		 * new chunk and copy the data.  Since we know the existing data isn't
		 * huge, this won't involve any great memcpy expense, so it's not
		 * worth being smarter.  (At one time we tried to avoid memcpy when it
		 * was possible to enlarge the chunk in-place, but that turns out to
		 * misbehave unpleasantly for repeated cycles of
		 * palloc/repalloc/pfree: the eventually freed chunks go into the
		 * wrong freelist for the next initial palloc request, and so we leak
		 * memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
		 */
		AllocPointer newPointer;

		/* allocate new chunk */
		newPointer = AllocSetAlloc((MemoryContext) set, size);

		/* transfer existing data (certain to fit) */
		memcpy(newPointer, pointer, oldsize);

		/* free old chunk */
		AllocSetFree((MemoryContext) set, pointer);

		return newPointer;
	}
}

/*
 * AllocSetGetChunkSpace
 *		Given a currently-allocated chunk, determine the total space
 *		it occupies (including all memory-allocation overhead).
 */
static Size
AllocSetGetChunkSpace(MemoryContext context, void *pointer)
{
	AllocChunk	chunk = AllocPointerGetChunk(pointer);

	return chunk->size + ALLOC_CHUNKHDRSZ;
}

/*
 * AllocSetIsEmpty
 *		Is an allocset empty of any allocated space?
 */
static bool
AllocSetIsEmpty(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;

	/*
	 * For now, we say "empty" only if the context is new or just reset. We
	 * could examine the freelists to determine if all space has been freed,
	 * but it's not really worth the trouble for present uses of this
	 * functionality.
	 */
	if (set->isReset)
		return true;
	return false;
}

/*
 * AllocSetStats
 *		Displays stats about memory consumption of an allocset.
 */

/* Helpers to portably right-justify a uint64 in a field of specified width. */
static char *
aset_rjfmt_uint64(uint64 v, int width, char **inout_bufpos, char *bufend)
{
    char       *bp = *inout_bufpos;
    char        fmtbuf[32];
    int         len;
    int         pad;

    snprintf(fmtbuf, sizeof(fmtbuf), UINT64_FORMAT, v);
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

static char *
aset_rjfmt_mem64(uint64 v, int width, char **inout_bufpos, char *bufend)
{
    char   *bp;
    char   *ip = *inout_bufpos;
    char   *suffix = "kB";

    v = (v + 1023) >> 10;
    bp = aset_rjfmt_uint64(v, width, inout_bufpos, bufend - strlen(suffix));
    if (bp == ip)
    {
        strcat(bp, suffix);
        *inout_bufpos += strlen(suffix);
    }
    return bp;
}                               /* aset_rjfmt_mem64 */


static void
AllocSetStats(MemoryContext context, const char* contextName)
{
	AllocSet	set = (AllocSet) context;
	long		nblocks = 0;
	long		nchunks = 0;
	Size        totalspace = 0;
	Size        freespace = 0;
    Size        held;
    AllocBlock	block;
	AllocChunk	chunk;
	int			fidx;
    char       *cfp;
    char       *efp;
    char        fmtbuf[400];

    /* Total space obtained from host's memory manager */
    for (block = set->blocks; block != NULL; block = block->next)
	{
		nblocks++;
		totalspace += block->endptr - ((char *) block);
	}

    /* Space at end of first block is available for use. */
    if (set->blocks)
    {
        nchunks++;
        freespace += set->blocks->endptr - set->blocks->freeptr;
    }

    /* Freelists.  Count usable space only, not chunk headers. */
	for (fidx = 0; fidx < ALLOCSET_NUM_FREELISTS; fidx++)
	{
		for (chunk = set->freelist[fidx]; chunk != NULL;
			 chunk = (AllocChunk) chunk->aset)
		{
			nchunks++;
			freespace += chunk->size;
		}
	}

    efp = fmtbuf + sizeof(fmtbuf);
    cfp = fmtbuf;
    held = set->header.allBytesAlloc - set->header.allBytesFreed;
    write_stderr("  (Tree: %s held; %s peak; %s/%s cumulative taken/returned)"
                 "  (Node: %s held in %3ld blocks; %s in use; "
                 "  %s free in %3ld chunks)  %s\n",
                 aset_rjfmt_mem64(held, 8, &cfp, efp),
                 aset_rjfmt_mem64(set->header.maxBytesHeld, 8, &cfp, efp),
                 aset_rjfmt_mem64(set->header.allBytesAlloc, 9, &cfp, efp),
                 aset_rjfmt_mem64(set->header.allBytesFreed, 9, &cfp, efp),
                 aset_rjfmt_mem64(totalspace, 8, &cfp, efp),
                 nblocks,
                 aset_rjfmt_mem64(totalspace - freespace, 8, &cfp, efp),
                 aset_rjfmt_mem64(freespace, 8, &cfp, efp),
                 nchunks,
                 contextName);
}


#ifdef MEMORY_CONTEXT_CHECKING

/*
 * AllocSetCheck
 *		Walk through chunks and check consistency of memory.
 *
 * NOTE: report errors as WARNING, *not* ERROR or FATAL.  Otherwise you'll
 * find yourself in an infinite loop when trouble occurs, because this
 * routine will be entered again when elog cleanup tries to release memory!
 */
static void
AllocSetCheck(MemoryContext context)
{
	AllocSet	set = (AllocSet) context;
	char	   *name = set->header.name;
	AllocBlock	block;

	for (block = set->blocks; block != NULL; block = block->next)
	{
		char	   *bpoz = ((char *) block) + ALLOC_BLOCKHDRSZ;
		Size		blk_used = block->freeptr - bpoz;
		Size		blk_data = 0;
		long		nchunks = 0;

		/*
		 * Empty block - empty can be keeper-block only
		 */
		if (!blk_used)
		{
			if (set->keeper != block)
			{
				Assert(!"Memory Context problem");
				elog(WARNING, "problem in alloc set %s: empty block %p (%s:%d)",
					 name, block, CDB_MCXT_WHERE(&set->header));
			}
		}

		/*
		 * Chunk walker
		 */
		while (bpoz < block->freeptr)
		{
			AllocChunk	chunk = (AllocChunk) bpoz;
			Size		chsize,
						dsize;
			char	   *chdata_end;

			chsize = chunk->size;		/* aligned chunk size */
			dsize = chunk->requested_size;		/* real data */
			chdata_end = ((char *) chunk) + (ALLOC_CHUNKHDRSZ + dsize);

			/*
			 * Check chunk size
			 */
			if (dsize != 0xFFFFFFFF && dsize > chsize)
			{
				Assert(!"Memory Context error");
				elog(WARNING, "problem in alloc set %s: req size > alloc size for chunk %p in block %p (%s:%d)",
					 name, chunk, block, CDB_MCXT_WHERE(&set->header));
			}
                        
			if (chsize < (1 << ALLOC_MINBITS))
			{
				Assert(!"Memory Context Error");
				elog(WARNING, "problem in alloc set %s: bad size %lu for chunk %p in block %p (%s:%d)",
					 name, (unsigned long) chsize, chunk, block, CDB_MCXT_WHERE(&set->header));
			}

			/* single-chunk block? */
			if (chsize > ALLOC_CHUNK_LIMIT && chsize + ALLOC_CHUNKHDRSZ != blk_used)
			{
				Assert(!"Memory context error");
				elog(WARNING, "problem in alloc set %s: bad single-chunk %p in block %p (%s:%d)",
						name, chunk, block, CDB_MCXT_WHERE(&set->header));
			}

			/*
			 * If chunk is allocated, check for correct aset pointer. (If it's
			 * free, the aset is the freelist pointer, which we can't check as
			 * easily...)
			 */
			if (dsize != 0xFFFFFFFF && chunk->aset != (void *) set)
			{
				Assert(!"Memory context error");
				elog(WARNING, "problem in alloc set %s: bogus aset link in block %p, chunk %p (%s:%d)",
					 name, block, chunk, CDB_MCXT_WHERE(&set->header));
			}

			/*
			 * Check for overwrite of "unallocated" space in chunk
			 */
			if (dsize != 0xFFFFFFFF && dsize != 0
                    && dsize < chsize && *chdata_end != 0x7E)
			{
//				Assert(!"Memory context error");
				elog(ERROR, "problem in alloc set %s: detected write past chunk end in block %p, chunk %p (%s:%d)",
						name, block, chunk, CDB_MCXT_WHERE(&set->header));
			}

			blk_data += chsize;
			nchunks++;

			bpoz += ALLOC_CHUNKHDRSZ + chsize;
		}

		if ((blk_data + (nchunks * ALLOC_CHUNKHDRSZ)) != blk_used)
		{
			Assert(!"Memory context error");
			elog(WARNING, "problem in alloc set %s: found inconsistent memory block %p (%s:%d)",
					name, block, CDB_MCXT_WHERE(&set->header));
		}
	}
}

#endif   /* MEMORY_CONTEXT_CHECKING */
