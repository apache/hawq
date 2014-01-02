/* 
 * memprot.c
 *		Memory allocation under greenplum memory allocation.
 * 
 * Copyright(c) 2008, Greenplum Inc.
 * 
 * We wrap up calls to malloc/realloc/free with our own accounting
 * so that we will make sure a postgres process will not go beyond
 * its allowed quota
 */

#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/time.h>

#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SEM_H
#include <sys/sem.h>
#endif
#ifdef HAVE_KERNEL_OS_H
#include <kernel/OS.h>
#endif

#include "miscadmin.h"
#include "storage/pg_sema.h"
#include "storage/ipc.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

#include "cdb/cdbvars.h"
#include "utils/debugbreak.h"
#include "utils/faultinjection.h" 
#include "utils/simex.h"
#include "utils/workfile_mgr.h"
#include "utils/atomic.h"

#define SHMEM_OOM_TIME "last vmem oom time"

#ifndef HAVE_UNION_SEMUN
union semun
{
	int val;
	struct semid_ds *buf;
	unsigned short *array;
};
#endif

#ifndef USE_SYSV_SEMAPHORES
/*
 * Greenplum Physical Mem Protection is implemented using SysV
 * semaphore (using SEM_UNDO) to clean up, if the postgres process
 * dies.
 *
 * On a non-sysv system, make it no-op
 */
void GPMemoryProtectInit()
{
#ifdef WIN32
    elog(DEBUG2, "GPDB on this platform does not use SysV Semeaphore.  Physical Mem Protection not implemented");
#else
	elog(LOG, "GPDB on this platform does not use SysV Semeaphore.  Physical Mem Protection not implemented");
#endif
}

void GPMemoryProtectReset()
{
#ifdef WIN32
    elog(DEBUG2, "GPDB on this platform does not use SysV Semeaphore.  Physical Mem Protection not implemented");
#else
	elog(LOG, "GPDB on this platform does not use SysV Semeaphore.  Physical Mem Protection not implemented");
#endif 
}

#ifdef USE_TEST_UTILS
int64 gp_mp_fault(int32 reason, int64 arg)
{
	elog(LOG, "GPDB does not support mem prot on this plat form.  Fault reason %d, arg" INT64_FORMAT ".", reason, arg);
	return -1;
}
#endif

int64 getMOPHighWaterMark(void)
{
    return 0;
}

int
MemProtSemas(void)
{
	return 0;
}

#else

/* Helpers */
static int gpmemprot_down_sem(PGSemaphoreData *psem, int amount, bool undo);
static int gpmemprot_up_sem(PGSemaphoreData *psem, int amount, bool undo);
static int gpmemprot_peek_sem(PGSemaphoreData *psem);


/* Global physical mem counter */
static PGSemaphoreData gpsema_vmem_prot;
#ifdef USE_TEST_UTILS
static PGSemaphoreData gpsema_mp_fault;
#endif

/* Convert value in MB to equivalent in bytes */
static uint64 ConvertMBToBytes(int value_mb);

/* By default, we will inc/dec counter, in MB. */
static int gpsema_vmem_prot_max;
static int gp_memprot_chunksize_shift = 20; 

/*
 * A derived parameter from gp_vmem_limit_per_query in chunks unit,
 * considering the current chunk size
 */
int max_chunks_per_query = 0;

/* total allocation in bytes */
static volatile int64 mop_bytes;         /* bytes allocated */
static volatile int mop_hld_cnt;          /* mop hold counter */

/*
 * Last OOM time of a segment. Maintained in shared memory.
 */
volatile OOMTimeType* segmentOOMTime = 0;

/*
 * We don't report memory usage of current process multiple times
 * for a single OOM event. This variable saves the last time we reported
 * OOM. If this time is not greater than the segmentOOMTime, we don't
 * report memory usage.
 */
volatile OOMTimeType alreadyReportedOOMTime = 0;

/*
 * Time when we started tracking for OOM in this process.
 * If this time is greater than segmentOOMTime, we don't
 * consider this process as culpable for that OOM event.
 */
volatile OOMTimeType oomTrackerStartTime = 0;

#if defined(__x86_64__) && defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 1)) 
static int64 mop_add_bytes(int64 val)
{
    return __sync_fetch_and_add(&mop_bytes, val);
}
static int mop_add_hld_cnt(int val)
{
    return __sync_fetch_and_add(&mop_hld_cnt, val);
}
#else
#include <pthread.h>
static pthread_mutex_t mop_mutex = PTHREAD_MUTEX_INITIALIZER;
static int64 mop_add_bytes(int64 val)
{
    int64 ret;
    pthread_mutex_lock(&mop_mutex);
    ret = mop_bytes; 
    mop_bytes += val;
    pthread_mutex_unlock(&mop_mutex);
    return ret;
}

static int mop_add_hld_cnt(int val)
{
    int ret;
    pthread_mutex_lock(&mop_mutex);
    ret = mop_hld_cnt; 
    mop_hld_cnt += val;
    pthread_mutex_unlock(&mop_mutex);
    return ret;
}   
#endif

int64 getMOPHighWaterMark(void)
{
    int64 hwm = mop_add_hld_cnt(0);
    return hwm << gp_memprot_chunksize_shift;
}

int getMOPChunksReserved(void)
{
    return mop_add_hld_cnt(0);
}

bool gp_mp_inited = false;
static inline bool gp_memprot_enabled()
{
	if(!gp_mp_inited || Gp_role != GP_ROLE_EXECUTE)
		return false;

#ifdef USE_TEST_UTILS
	return gpmemprot_peek_sem(&gpsema_mp_fault) != 0;
#else
	return gp_vmem_protect_limit != 0;
#endif
}

void GPMemoryProtectReset()
{
	gp_mp_inited = false;
}

/*
 * UpdateTimeAtomically
 *
 * Updates a OOMTimeType variable atomically, using compare_and_swap_*
 */
void UpdateTimeAtomically(volatile OOMTimeType* time_var)
{
	bool updateCompleted = false;

	OOMTimeType newOOMTime;

	while (!updateCompleted)
	{
#if defined(__x86_64__)
	    newOOMTime = GetCurrentTimestamp();
#else
	    struct timeval curTime;
	    gettimeofday(&curTime, NULL);

	    newOOMTime = (uint32)curTime.tv_sec;
#endif
	    OOMTimeType oldOOMTime = *time_var;

#if defined(__x86_64__)
	    updateCompleted = compare_and_swap_64((uint64*)time_var,
										(uint64)oldOOMTime,
										(uint64)newOOMTime);
#else
	    updateCompleted = compare_and_swap_32((uint32*)time_var,
										(uint32)oldOOMTime,
										(uint32)newOOMTime);
#endif
	}
}

/*
 * AtShmemExit_VMEM
 *
 * on_shmem_exit hook to execute VMEM shutdown code such as logging memory usage
 * if necessary.
 */
static void
AtShmemExit_VMEM(int code, Datum arg)
{
	ReportOOMConsumption();
}

/*
 * InitPerProcessOOMTracking
 *
 * Initializes per-process OOM tracking data structures.
 */
void InitPerProcessOOMTracking()
{
	Assert(NULL != segmentOOMTime);

	alreadyReportedOOMTime = 0;

#if defined(__x86_64__)
    oomTrackerStartTime = GetCurrentTimestamp();
#else
    struct timeval curTime;
    gettimeofday(&curTime, NULL);

    oomTrackerStartTime = (uint32)curTime.tv_sec;
#endif

    on_shmem_exit(AtShmemExit_VMEM, 0);
}

/* Initialization */
void GPMemoryProtectInit()
{
    mop_bytes = 0; 
    mop_hld_cnt = 0; 

	/* 
	 * NOTES:
	 * 
	 * On most popular unix system, the max value of a semaphore is 65535.  With
	 * default 32767.  The impl. depends on the SEM_UNDO flag.  In order for the OS
	 * to keep track of the undo count, we cannot use the higher half.  So we set
	 * the max to 16 * 1024 here.  Each inc/dec of the semaphore corresponds to 
	 * chunksize of bytes memory allocation.  Chunk size default to 1MB. 
	 */
    if(!IsUnderPostmaster)
    {
        Assert(gp_memprot_chunksize_shift == 20);

        gpsema_vmem_prot_max = gp_vmem_protect_limit;
        while(gpsema_vmem_prot_max > (16 * 1024))
        {
            gp_memprot_chunksize_shift++;
            gpsema_vmem_prot_max >>= 1;
        }

        /*
         * gp_vmem_limit_per_query is in kB. So, first convert it to MB, and then shift it
         * to adjust for cases where we enlarged our chunk size
         */
        max_chunks_per_query = ceil(gp_vmem_limit_per_query / (1024.0 * (1 << (gp_memprot_chunksize_shift - 20))));

    	bool		isSegmentOOMTimeInShmem;

    	segmentOOMTime = (OOMTimeType *)
    		ShmemInitStruct(SHMEM_OOM_TIME,
    						sizeof(OOMTimeType),
    						&isSegmentOOMTimeInShmem);

    	/*
    	 * We are not under Postmaster, so no one else
    	 * should have already initialized segmentOOMTime
    	 */
    	Assert(!isSegmentOOMTimeInShmem);

		/*
		 * Initializing segmentOOMTime to 0 ensures that no
		 * process dumps memory usage, unless we hit an OOM
		 * and update segmentOOMTime to a proper value.
		 */
    	*segmentOOMTime = 0;

#ifdef USE_TEST_UTILS
        PGSemaphoreCreateInitVal(&gpsema_vmem_prot, gpsema_vmem_prot_max);
        PGSemaphoreCreateInitVal(&gpsema_mp_fault, gpsema_vmem_prot_max);
#else
        if(gpsema_vmem_prot_max != 0)
            PGSemaphoreCreateInitVal(&gpsema_vmem_prot, gpsema_vmem_prot_max);
#endif
    }
    else
    {
#ifdef EXEC_BACKEND
    	/* We should only reach this part if EXEC_BACKEND is true */
    	bool		isSegmentOOMTimeInShmem;

    	/*
    	 * Get or create the shared strategy control block
    	 */
    	segmentOOMTime = (struct timeval *)
    		ShmemInitStruct(SHMEM_OOM_TIME,
    						sizeof(OOMTimeType),
    						&isSegmentOOMTimeInShmem);

    	/*
    	 * We are under Postmaster, so segmentOOMTime should already be
    	 * in shared memory.
    	 */
    	Assert(isSegmentOOMTimeInShmem);
#endif
    }
    Assert(NULL != segmentOOMTime);

    gp_mp_inited = true;
}

/* 
 * XXX Why we put the ereport here.
 * Theoretically, the ereport/elog family should NEVER allocate memory.
 * The mem should be reserved at startup time.  However, this is NOT 
 * true.  
 * 
 * So if we out of mem, failed to alloc, we ereprot.  Then in the error
 * handling, we may palloc again, which will fail again, then we will call
 * ereport again ... ...
 *
 * HACK: When we fail here, we give them 1 more chunk of quota.  Error
 * handling should be done with this 1 more chunk (1M, most likely).  
 * NOTE: If we have mem prot enabled, we should run into vmem limit before
 * we will really fail malloc.  Once given this extra chunk, malloc in 
 * error handling should be OK.  The 1 more chunk is not leaked from accounting
 * because after the call, the postgres process will be killed in a short
 * time anyway.
 *
 * FIX ERROR HANDLER!
 */
#define MOP_FAIL_REACHED_LIMIT 				1
#define MOP_FAIL_SYSTEM        				2
/* Reached per-query memory limit */
#define MOP_FAIL_REACHED_QUERY_LIMIT		3

static bool is_main_thread()
{
    return pthread_equal(main_tid, pthread_self());
}

/*
 * gp_failed_to_alloc is called upon an OOM. We can have either a VMEM
 * limited OOM (i.e., the system still has memory, but we ran out of either
 * per-query VMEM limit or segment VMEM limit) or a true OOM, where the
 * malloc returns a NULL pointer.
 *
 * This function logs OOM details, such as memory allocation/deallocation/peak.
 * It also updates segment OOM time by calling UpdateTimeAtomically().
 *
 * Parameters:
 *
 * 		ec: error code; indicates what type of OOM event happend (system, VMEM, per-query VMEM)
 * 		en: the last seen error number as retrieved by calling __error() or similar function
 * 		sz: the requested allocation size for which we reached OOM
 * 		availmb: available memory in MB
 */
static void gp_failed_to_alloc(int ec, int en, int sz, int availmb) 
{
	/*
	 * A per-query vmem overflow shouldn't trigger a segment-wide
	 * OOM reporting.
	 */
	if (MOP_FAIL_REACHED_QUERY_LIMIT != ec)
	{
		UpdateTimeAtomically(segmentOOMTime);
	}

	UpdateTimeAtomically(&alreadyReportedOOMTime);

	/* Give an extra chunk for error handling. */
    mop_add_hld_cnt(1);

	if (pthread_equal(main_tid, pthread_self()))
	{
		if (ec == MOP_FAIL_REACHED_QUERY_LIMIT)
		{
			elog(LOG, "Logging memory usage for reaching per-query memory limit");
		}
		else if (ec == MOP_FAIL_REACHED_LIMIT)
		{
			elog(LOG, "Logging memory usage for reaching Vmem limit");
		}
		else if (ec == MOP_FAIL_SYSTEM)
		{
			/*
			 * The system memory is exhausted and malloc returned a null pointer.
			 * Although elog switches to ErrorContext, which already
			 * has pre-allocated space, we are not risking any new allocation until
			 * we dump the memory context and memory accounting tree. We are therefore
			 * printing the log message header using write_stderr.
			 */
			write_stderr("Logging memory usage for reaching system memory limit");
		}
		else
		{
			elog(LOG, "Logging memory usage for semaphore error");
		}

		MemoryAccounting_SaveToLog();
		MemoryContextStats(TopMemoryContext);
	}
	else
	{
		write_log("Child thread detected: failed to log memory usage.");
	}

	if(coredump_on_memerror)
	{
		/*
		 * Generate a core dump by writing to NULL pointer
		 */
		*(int *) NULL = ec;
	}

    if (ec == MOP_FAIL_REACHED_LIMIT)
    {
        if(is_main_thread())
        {
            /* Hit MOP limit */
            ereport(ERROR, (errcode(ERRCODE_GP_MEMPROT_KILL),
                        errmsg("Out of memory"),
                        errdetail("VM Protect failed to allocate %d bytes, %d MB available",
                            sz, availmb
                            )
                        ));
        }
        else
        {
            write_log("Out of memory: Hit VM Protect limit");
        }
    }
    else if (ec == MOP_FAIL_REACHED_QUERY_LIMIT)
    {
        if(is_main_thread())
        {
            /* Hit MOP limit */
            ereport(ERROR, (errcode(ERRCODE_GP_MEMPROT_KILL),
                        errmsg("Out of memory"),
                        errdetail("Per-query VM protect limit reached: current limit is %d kB, requested %d bytes, available %d MB",
                        		gp_vmem_limit_per_query, sz, availmb
                            )
                        ));
        }
        else
        {
            write_log("Out of memory: Hit per-query VM protect limit");
        }
    }
    else if (ec == MOP_FAIL_SYSTEM)
    {
        /* MOP OK, but system allocation failed */ 
        if(is_main_thread())
        {
            ereport(ERROR, (errcode(ERRCODE_GP_MEMPROT_KILL),
                        errmsg("Out of memory"),
                        errdetail("VM protect failed to allocate %d bytes from system, VM Protect %d MB available",
                            sz, availmb
                            )
                        ));
        }
        else
        {
            write_log("Out of memory: Alloc from system failed");
        }
    }
    else
    {
        if(is_main_thread())
        {
            /* SemOp error.  */
            ereport(ERROR, (errcode(ERRCODE_GP_MEMPROT_KILL),
                        errmsg("Failed to allocate memory under virtual memory protection"),
                        errdetail("Error %d, errno %d, %s", ec, en, strerror(en)) 
                        )); 
        }
        else
        {
            write_log("Out of memory: MOP semaphore error."); 
        }
    }
}

static void *gp_malloc_internal(int64 sz1, int64 sz2, bool ismalloc)
{
	int64 newsz_chunk;
	int64 sz = sz1;
	int need_chunk = 0;

	void *ret = NULL;
    int64 total_malloc = mop_add_bytes(0);
    int hldcnt = mop_add_hld_cnt(0);

	if(!ismalloc)
		sz *= sz2;

	Assert(sz >=0 && sz <= 0x7fffffff);

	newsz_chunk = (total_malloc + sz) >> gp_memprot_chunksize_shift;

	int mem_avail = 0;

	if(newsz_chunk > hldcnt) 
	{
		int err_code = -1;

		need_chunk = newsz_chunk - hldcnt; 
		mem_avail = gpmemprot_peek_sem(&gpsema_vmem_prot);
        if (mem_avail < 0)
        {
        	ReportOOMConsumption();
            return NULL;
        }

		if(mem_avail >= need_chunk)
        {
	    	/*
	    	 * Before attempting to reserve vmem, we check if there was any OOM
	    	 * situation, and report our consumption if there was any. This accurately
	    	 * tells us our share of fault in an OOM situation. Note: if this is *not*
	    	 * the main thread, then we might end up reserving additional VMEM
	    	 * without reporting our OOM share.
	    	 */
	    	ReportOOMConsumption();

			bool query_mem_success = PerQueryMemory_ReserveChunks(need_chunk);

			if (!query_mem_success)
			{
	            gp_failed_to_alloc(MOP_FAIL_REACHED_QUERY_LIMIT, 0, sz, (max_chunks_per_query - PerQueryMemory_TotalChunksReserved()) << (gp_memprot_chunksize_shift - 20));
	            return NULL;
			}

			err_code = gpmemprot_down_sem(&gpsema_vmem_prot, need_chunk, true);

		    if(err_code != 0)
            {
			    gp_failed_to_alloc(err_code, errno, 0, 0);
                return NULL;
            }
        }
        else
        {
            gp_failed_to_alloc(MOP_FAIL_REACHED_LIMIT, 0, sz, mem_avail << (gp_memprot_chunksize_shift - 20)); 
            /* Must return.  if not called from main thread, gp_failed_to_alloc
             * actually will not throw.
             */
            return NULL;
        }
	}

	if(ismalloc)
		ret = malloc(sz);
	else
		ret = calloc(sz1, sz2);

#ifdef USE_TEST_UTILS
	if (gp_simex_init && gp_simex_run && gp_simex_class == SimExESClass_OOM && ret)
	{
		SimExESSubClass subclass = SimEx_CheckInject();
		if (subclass == SimExESSubClass_OOM_ReturnNull)
		{
			free(ret);
			ret = NULL;
		}
	}
#endif

	if(!ret)
	{
		/* Try my best to be honest with allocation */
		if(need_chunk > 0)
			gpmemprot_up_sem(&gpsema_vmem_prot, need_chunk, true);

        gp_failed_to_alloc(MOP_FAIL_SYSTEM, 0, sz, mem_avail << (gp_memprot_chunksize_shift - 20)); 

		return NULL;
	}

    mop_add_bytes(sz);
    mop_add_hld_cnt(need_chunk);
	return ret;
}

void *gp_malloc(int64 sz)
{
	void *ret;

	if(gp_mp_inited && gp_memprot_enabled())
		return gp_malloc_internal(sz, 0, true);

	ret = malloc(sz);
	if(ret)
        mop_add_bytes(sz);

	return ret;
}

void *gp_calloc(int64 sz1, int64 sz2)
{
	void *ret;

	if(gp_memprot_enabled())
		return gp_malloc_internal(sz1, sz2, false);

	ret = calloc(sz1, sz2);
	if(ret)
        mop_add_bytes(sz1 * sz2); 
	return ret;
}

void *gp_realloc(void *ptr, int64 sz, int64 newsz)
{
	int newsz_chunk;
	int need_chunk = 0;
    int64 total_malloc = mop_add_bytes(0);
    int hldcnt = mop_add_hld_cnt(0);

	void *ret = NULL;

	if(gp_memprot_enabled())
	{
		int mem_avail = 0;

		if(newsz > sz)
		{
			newsz_chunk = (total_malloc - sz + newsz) >> gp_memprot_chunksize_shift;

			if(newsz_chunk > hldcnt) 
			{
				int err_code = -1;

				need_chunk = newsz_chunk - hldcnt; 
				mem_avail = gpmemprot_peek_sem(&gpsema_vmem_prot);
                if (mem_avail < 0)
                {
                	ReportOOMConsumption();
                    return NULL;
                }

				if(mem_avail >= need_chunk)
                {
					bool query_mem_success = PerQueryMemory_ReserveChunks(need_chunk);

					if (!query_mem_success)
					{
			            gp_failed_to_alloc(MOP_FAIL_REACHED_QUERY_LIMIT, 0, sz, (max_chunks_per_query - PerQueryMemory_TotalChunksReserved()) << (gp_memprot_chunksize_shift - 20));
			            return NULL;
					}

					err_code = gpmemprot_down_sem(&gpsema_vmem_prot, need_chunk, true);
				    if(err_code != 0)
                    {
					    gp_failed_to_alloc(err_code, errno, 0, 0);
                        return NULL;
                    }
                }
                else
                {
                    gp_failed_to_alloc(MOP_FAIL_REACHED_LIMIT, 0, newsz, mem_avail << (gp_memprot_chunksize_shift - 20)); 
                    return NULL;
                }
			}
		}

		ret = realloc(ptr, newsz);

#ifdef USE_TEST_UTILS
		if (gp_simex_init && gp_simex_run && gp_simex_class == SimExESClass_OOM && ret)
		{
			SimExESSubClass subclass = SimEx_CheckInject();
			if (subclass == SimExESSubClass_OOM_ReturnNull)
			{
				free(ret);
				ret = NULL;
			}
		}
#endif

		if(!ret)
		{
			/* Try my best to be honest with allocation */
			if(need_chunk > 0)
				gpmemprot_up_sem(&gpsema_vmem_prot, need_chunk, true);

               gp_failed_to_alloc(MOP_FAIL_SYSTEM, 0, sz, mem_avail << (gp_memprot_chunksize_shift - 20)); 

            return NULL;
        }

        mop_add_bytes(newsz - sz);
        mop_add_hld_cnt(need_chunk);
		return ret;
	}
	else
	{
		ret = realloc(ptr, newsz);

		if(ret)
            mop_add_bytes(newsz - sz);
		return ret;
	}

	/* Never reach here*/
	return NULL; 
}

/*
 * Inlined function for converting an integer value in MB
 * to the equivalent in bytes.
 * The input value is multiplied by 2^20.
 */
static inline uint64
ConvertMBToBytes(int value_mb)
{
	Assert(value_mb >= 0);
	Assert(value_mb < ((uint64) 1 << 44) &&
		   "overflow when converting MB value to bytes");
	uint64 value_bytes = (uint64) value_mb;

	return value_bytes << 20;
}

/*
 * gp_vmem_used
 *
 * Returns current memory allocation in bytes.
 */
uint64 gp_vmem_used(void)
{
	int mem_avail = gpmemprot_peek_sem(&gpsema_vmem_prot);
	Assert(mem_avail >= 0 && "available memory < 0");
	Assert(gp_vmem_protect_limit - mem_avail >= 0 && "allocated memory < 0");
	int mem_used = gp_vmem_protect_limit - mem_avail;

	return ConvertMBToBytes(mem_used);
}

/*
 * gp_vmem_max
 *
 * Returns maximum allowed memory allocation in bytes.
 * This value is set by GUC gp_vmem_protect_limit (in MB)
 *
 */
uint64 gp_vmem_max(void)
{
	return ConvertMBToBytes(gp_vmem_protect_limit);
}

void gp_free2(void *ptr, int64 sz)
{
	free(ptr);
    mop_add_bytes(-sz);
}

int gpmemprot_up_sem(PGSemaphoreData *psem, int n, bool undo)
{
	struct sembuf sop;
	int err;

	sop.sem_op = n;
	sop.sem_flg = IPC_NOWAIT;
	if(undo)
		sop.sem_flg |= SEM_UNDO;
	sop.sem_num = psem->semNum; 

	do {
		err = semop(psem->semId, &sop, 1);
	} while (err < 0 && errno == EINTR);

	return err;
}

int gpmemprot_down_sem(PGSemaphoreData *psem, int n, bool undo)
{
	return gpmemprot_up_sem(psem, -n, undo);
}

int gpmemprot_peek_sem(PGSemaphoreData *psem)
{
	union semun s;
	int ret;

	s.val = 0;

	ret = semctl(psem->semId, psem->semNum, GETVAL, s);
	if(ret < 0)
    {
        if(is_main_thread())
            elog(ERROR, "Greenplum memory protection failed to get memory counter: %m");
        else
            write_log("Greenplum memory protection failed to get memory counter: %m");
    }
	return ret;
}

int
MemProtSemas(void)
{
#ifdef USE_TEST_UTILS
	return 2;
#else
	return 1;
#endif
}

#ifdef USE_TEST_UTILS
static inline int64 chunk_to_mb(int64 chunk)
{
	Assert(gp_memprot_chunksize_shift >= 20);
	return chunk << (gp_memprot_chunksize_shift - 20);
}
static inline int mb_to_chunk(int mb)
{
	Assert(gp_memprot_chunksize_shift >= 20);
	return mb >> (gp_memprot_chunksize_shift - 20);
}

int64 gp_mp_fault(int32 reason, int64 arg)
{
	switch(reason)
	{
		case GP_FAULT_USER_MP_CONFIG:
			return (int64) gp_vmem_protect_limit;
		case GP_FAULT_USER_MP_ALLOC:
			return (int64) (mop_add_bytes(0) >> 20);
		case GP_FAULT_USER_MP_HIGHWM:
			return chunk_to_mb(mop_add_bytes(0)); 
		case GP_FAULT_SEG_AVAILABLE:
			{
				int avail = gpmemprot_peek_sem(&gpsema_vmem_prot);
				return chunk_to_mb(avail);
			}
		case GP_FAULT_SEG_SET_VMEMMAX:
			{
				int oldval; 
				int change;  
				int err;

				int newval = mb_to_chunk(arg);

				if(newval < 0 || newval > 16 * 1024)
					elog(ERROR, "GP MP Fault failed to change vmem limit: invalid value");
				
				oldval = gpmemprot_peek_sem(&gpsema_mp_fault);
				change = newval - oldval;
				err = gpmemprot_up_sem(&gpsema_vmem_prot, change, false);

				if(err != 0)
					elog(ERROR, "GP MP Fault failed to change vmem limit, cannot update vmem");

				err = gpmemprot_up_sem(&gpsema_mp_fault, change, false);
				if(err != 0)
					elog(ERROR, "GP MP Fault failed to change vmem limit, cannot do bookkeeping.");
				return arg;
			}
		case GP_FAULT_SEG_GET_VMEMMAX:
			return chunk_to_mb(gpmemprot_peek_sem(&gpsema_mp_fault));
		default:
			elog(ERROR, "GP MP Fault Invalid fault code");
	}

	return -1;
}
#endif /* Fault Inj */
#endif /* SysV Semaphore */
