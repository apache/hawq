/*-------------------------------------------------------------------------
 *
 * optserver.c
 *	  Process handling all query optimization requests.
 *
 * Copyright (c) 2012, EMC Corp.
 *
 *-------------------------------------------------------------------------
 */

#include <dlfcn.h>
#include <unistd.h>
#include <wchar.h>

#include "postgres.h"
#include "miscadmin.h"
#include "gp-libpq-fe.h"
#include "cdb/cdbdisp.h"
#include "libpq/pqsignal.h"
#include "postmaster/fork_process.h"
#include "postmaster/optserver.h"
#include "postmaster/postmaster.h"
#include "storage/backendid.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"

#define GP_EXCEPTION_MSG	L"PG exception raised"

/*
 * STATIC VARIABLES
 */

static volatile bool shutdown_requested = false;


/*
 * FUNCTION PROTOTYPES
 */

/* run optimizer */
static void OptimizerMain(void);

/* start optimizer server loop */
static void OptimizerLaunch(void);

/* record shutdown request */
static void RequestShutdown(SIGNAL_ARGS);


/*
 * Entry point for optimizer process
 */
int
optimizer_start()
{
	pid_t optimizerPID;

	switch ((optimizerPID = fork_process()))
	{
		case -1:
			ereport(LOG, (errmsg("could not fork optimizer process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			ClosePostmasterPorts(false);

			OptimizerMain();
			break;
#endif /* EXEC_BACKEND */
		default:
			return (int) optimizerPID;
	}

	return 0;
}


/*
 * run optimizer
 */
static void
OptimizerMain()
{
	sigjmp_buf	local_sigjmp_buf;

	IsUnderPostmaster = true;

	/* Reset MyProcPid */
	MyProcPid = getpid();

    /* Save our main thread-id for comparison during signal handling */
	main_tid = pthread_self();

	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify this process via ps */
	init_ps_display("optimizer process", "", "", "");

	SetProcessingMode(InitProcessing);

	/* Set up reference point for stack depth checking */
	char stack_base;
	stack_base_ptr = &stack_base;

	/*
	 * Set up signal handlers. This process behaves much like a regular
	 * backend, so it uses the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, SIG_IGN);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, SIG_IGN);

	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie);
	pqsignal(SIGUSR2, RequestShutdown);

	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	pqsignal(SIGILL, CdbProgramErrorHandler);
	pqsignal(SIGSEGV, CdbProgramErrorHandler);
	pqsignal(SIGBUS, CdbProgramErrorHandler);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Optimizer");

	/* Early initialization */
	BaseInit();

	/* See InitPostgres()... */
	InitProcess();

	SetProcessingMode(NormalProcessing);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.	Note that because we'll call InitProcess, a
		 * callback will be registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	MyBackendId = InvalidBackendId;

	/* Unlink optimizer socket */
	(void) unlink(OPT_SOCKET_NAME);

	/* Start optimizer loop */
	OptimizerLaunch();

	proc_exit(0);
}


/*
 * Start optimizer server loop
 */
static void
OptimizerLaunch()
{
	char error_buffer[OPT_ERROR_BUFFER_SIZE];

	(void) libopt_exec("optimizer_loop", OPT_SOCKET_NAME, error_buffer, sizeof(error_buffer),
			           &shutdown_requested);
}

/*
 * dynamically load optimizer library;
 * invoke requested function and return result;
 */
void *
libopt_exec(const char *funcName, void *args, void *error_buffer, int error_buffer_size,
            volatile bool *abort)
{
	void *libOptHandle = dlopen(OPT_LIB, RTLD_LAZY);
	if (libOptHandle == NULL)
	{
		elog(LOG, "Unable to load optimizer library");
		return NULL;
	}

	/* gpos task signature */
	typedef int (*optTask)(void *);

	/* dynamically load function */
	optTask funcPtr = (optTask) dlsym(libOptHandle, funcName);

	char *error = NULL;
	if ((error = dlerror()) != NULL)
	{
		elog(LOG, "Unable to load function %s from library %s", funcName, error);
		return NULL;
	}

	struct gpos_exec_params params;
	params.func = NULL; /* set by the called function to avoid including GPOS headers here */
	params.arg = args;
	params.error_buffer = error_buffer;
	params.error_buffer_size = error_buffer_size;
	params.result = NULL;
	params.stack_start = stack_base_ptr;
	params.abort_requested = abort;

	/* execute function */
	int result = (*funcPtr)(&params);

	(void) dlclose(libOptHandle);

	if (result != 0)
	{
		if (wcsstr(error_buffer, GP_EXCEPTION_MSG) != NULL)
		{
			PG_RE_THROW();
		}
		elog(ERROR, "%ls", (const wchar_t *) params.error_buffer);
	}

	return params.result;
}


/*
 * record shutdown request
 */
static void
RequestShutdown(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


/* EOF */
