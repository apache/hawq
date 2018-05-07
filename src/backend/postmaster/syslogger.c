/*-------------------------------------------------------------------------
 *
 * syslogger.c
 *
 * The system logger (syslogger) is new in Postgres 8.0. It catches all
 * stderr output from the postmaster, backends, and other subprocesses
 * by redirecting to a pipe, and writes it to a set of logfiles.
 * It's possible to have size and age limits for the logfile configured
 * in postgresql.conf. If these limits are reached or passed, the
 * current logfile is closed and a new one is created (rotated).
 * The logfiles are stored in a subdirectory (configurable in
 * postgresql.conf), using an internal naming scheme that mangles
 * creation time and current postmaster pid.
 *
 * Author: Andreas Pflug <pgadmin@pse-consulting.de>
 *
 * Copyright (c) 2004-2009, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/postmaster/syslogger.c,v 1.29.2.3 2007/08/02 23:17:20 adunstan Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <string.h>

#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgtime.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#if (USE_EMAIL || USE_SNMP)
#include "postmaster/sendalert.h"
#endif
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "cdb/cdbvars.h"

#define READ_BUF_SIZE (2 * PIPE_CHUNK_SIZE)

/* The maximum bytes for error message */
#define ERROR_MESSAGE_MAX_SIZE 200

/*
 * GUC parameters.	Redirect_stderr cannot be changed after postmaster
 * start, but the rest can change at SIGHUP.
 */
bool		Redirect_stderr = false;
int			Log_RotationAge = HOURS_PER_DAY * MINS_PER_HOUR;
int			Log_MaxAge = 0;  /* E.g., DAYS_PER_MONTH * HOURS_PER_DAY * MINS_PER_HOUR */
int			Log_MaxSize = 0; /* E.g., 10 * 1024 * 1024, 10GB */

int			Log_RotationSize = 10 * 1024;
char	   *Log_directory = NULL;
char	   *Log_filename = NULL;
bool		Log_truncate_on_rotation = false;
int         gp_log_format = 0; /* Text format */

/*
 * Globally visible state (used by elog.c)
 */
bool		am_syslogger = false;

extern bool redirection_done;

/*
 * Private state
 */
static pg_time_t next_rotation_time;
static bool pipe_eof_seen = false;
static FILE *syslogFile = NULL;
static char *last_file_name = NULL;

/* An err msg may break into sever pipe chunks, so we need a buffer to assemble them.
 * We fix the number of buffers.  Generally a relative small number should suffice.
 * If we run out, we will flush partial message.  The assemble code will make sure we
 * may log partial msg, but we never garble msg.  We also make sure the output is valid
 * csv (except last line, if syslogger is killed before finish writing a line).
 *
 * We loop over the saved chunk, because number of buffers are small.
 */
/*
 * Buffers for saving partial messages from different backends. We don't expect
 * that there will be very many outstanding at one time, so 20 seems plenty of
 * leeway. If this array gets full we won't lose messages, but we will lose
 * the protocol protection against them being partially written or interleaved.
 *
 * An inactive buffer has pid == 0 and undefined contents of data.
 */


PipeProtoChunk saved_chunks[CHUNK_SLOTS];

/* Find an unused chunk */
static PipeProtoChunk *find_unused_chunk()
{
    int i;
    for(i=0; i<CHUNK_SLOTS; ++i)
    {
        if(saved_chunks[i].hdr.pid == 0)
            return &saved_chunks[i];
    }

    /* oops, all used.  */
    return NULL;
}

/* These must be exported for EXEC_BACKEND case ... annoying */
#ifndef WIN32
int			syslogPipe[2] = {-1, -1};
#else
HANDLE		syslogPipe[2] = {0, 0};
#endif

#ifdef WIN32
static HANDLE threadHandle = 0;
static CRITICAL_SECTION sysfileSection;
#endif

static bool chunk_is_postgres_chunk(PipeProtoHeader *hdr)
{
    return hdr->zero == 0 && hdr->pid != 0 && hdr->thid != 0 &&
		(hdr->log_format == 't' || hdr->log_format == 'c') &&
		(hdr->is_last == 't' || hdr->is_last == 'f');
}


static void syslogger_handle_chunk(PipeProtoChunk *savedchunk);
static void syslogger_flush_chunks(void);

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t rotation_requested = false;


/* Local subroutines */
#ifdef EXEC_BACKEND
static pid_t syslogger_forkexec(void);
static void syslogger_parseArgs(int argc, char *argv[]);
#endif
#ifdef WIN32
static void process_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
static void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
static void open_csvlogfile(void);


static unsigned int __stdcall pipeThread(void *arg);
#endif
static void logfile_rotate(bool time_based_rotation);
static char *logfile_getname(pg_time_t timestamp);
static void set_next_rotation_time(void);
static void sigHupHandler(SIGNAL_ARGS);
static void sigUsr1Handler(SIGNAL_ARGS);

static int logfileinfocmp(const void *d1, const void *d2);
static void constrain_max_logfile();

/*
 * Main entry point for syslogger process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void
SysLoggerMain(int argc, char *argv[])
{
    char	   *currentLogDir;
    char	   *currentLogFilename;
    int			currentLogRotationAge;
    int			currentLogMaxAge;
    int			currentLogMaxSize;

    IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

    MyProcPid = getpid();		/* reset MyProcPid */

    MyStartTime = time(NULL);	/* set our start time in case we call elog */

#ifdef EXEC_BACKEND
    syslogger_parseArgs(argc, argv);
#endif   /* EXEC_BACKEND */

    am_syslogger = true;

    if (AmIMaster() && Gp_role == GP_ROLE_DISPATCH)
    	init_ps_display("master logger process", "", "", "");
    else
    	init_ps_display("logger process", "", "", "");

    /*
     * If we restarted, our stderr is already redirected into our own input
     * pipe.  This is of course pretty useless, not to mention that it
     * interferes with detecting pipe EOF.	Point stderr to /dev/null. This
     * assumes that all interesting messages generated in the syslogger will
     * come through elog.c and will be sent to write_syslogger_file.
     */
    {
        int			fd = open(DEVNULL, O_WRONLY, 0);

        /*
         * The closes might look redundant, but they are not: we want to be
         * darn sure the pipe gets closed even if the open failed.	We can
         * survive running with stderr pointing nowhere, but we can't afford
         * to have extra pipe input descriptors hanging around.
         */
        close(fileno(stdout));
        close(fileno(stderr));
        dup2(fd, fileno(stdout));
        dup2(fd, fileno(stderr));
        close(fd);
    }
	/*
	 * Syslogger's own stderr can't be the syslogPipe, so set it back to text
	 * mode if we didn't just close it. (It was set to binary in
	 * SubPostmasterMain).
     */
#ifdef WIN32
    _setmode(_fileno(stderr),_O_TEXT);
#endif

    redirection_done = true;
    


    /*
     * Also close our copy of the write end of the pipe.  This is needed to
     * ensure we can detect pipe EOF correctly.  (But note that in the restart
     * case, the postmaster already did this.)
     */
#ifndef WIN32
    if (syslogPipe[1] >= 0)
        close(syslogPipe[1]);
    syslogPipe[1] = -1;
#else
    if (syslogPipe[1])
        CloseHandle(syslogPipe[1]);
    syslogPipe[1] = 0;
#endif

    /*
     * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(syslogger probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
     */
#ifdef HAVE_SETSID
    if (setsid() < 0)
        elog(FATAL, "setsid() failed: %m");
#endif

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we ignore all termination signals, and instead exit only when all
     * upstream processes are gone, to ensure we don't miss any dying gasps of
     * broken backends...
     */

    pqsignal(SIGHUP, sigHupHandler);	/* set flag to read config file */
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SIG_IGN);
    pqsignal(SIGQUIT, SIG_IGN);
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, sigUsr1Handler);	/* request log rotation */
    pqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);
    pqsignal(SIGTTIN, SIG_DFL);
    pqsignal(SIGTTOU, SIG_DFL);
    pqsignal(SIGCONT, SIG_DFL);
    pqsignal(SIGWINCH, SIG_DFL);

    PG_SETMASK(&UnBlockSig);

#ifdef WIN32
    /* Fire up separate data transfer thread */
    InitializeCriticalSection(&sysfileSection);

    threadHandle = (HANDLE) _beginthreadex(NULL, 0, pipeThread, NULL, 0, NULL);
    if (threadHandle == 0)
        elog(FATAL, "could not create syslogger data transfer thread: %m");
#endif   /* WIN32 */

    /* remember active logfile parameters */
    currentLogDir = pstrdup(Log_directory);
    currentLogFilename = pstrdup(Log_filename);
    currentLogRotationAge = Log_RotationAge;
    /* set next planned rotation time */
    set_next_rotation_time();

    /* main worker loop */
    for (;;)
    {
        bool		time_based_rotation = false;

#ifndef WIN32
        int			bytesRead = 0;
        int			rc;
        fd_set		rfds;
        struct timeval timeout;
#endif

        if (got_SIGHUP)
        {
            got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);

            /*
             * Check if the log directory or filename pattern changed in
             * postgresql.conf. If so, force rotation to make sure we're
             * writing the logfiles in the right place.
             */
            if (strcmp(Log_directory, currentLogDir) != 0)
            {
                pfree(currentLogDir);
                currentLogDir = pstrdup(Log_directory);
                rotation_requested = true;
            }
            if (strcmp(Log_filename, currentLogFilename) != 0)
            {
                pfree(currentLogFilename);
                currentLogFilename = pstrdup(Log_filename);
                rotation_requested = true;
            }
            if (currentLogMaxAge != Log_MaxAge)
            {
                currentLogMaxAge = Log_MaxAge;
                rotation_requested = true;
            }
            if (currentLogMaxSize != Log_MaxSize)
            {
                currentLogMaxSize = Log_MaxSize;
                rotation_requested = true;
            }

            /*
             * If rotation time parameter changed, reset next rotation time,
             * but don't immediately force a rotation.
             */
            if (currentLogRotationAge != Log_RotationAge)
            {
                currentLogRotationAge = Log_RotationAge;
                set_next_rotation_time();
            }
        }

        if (!rotation_requested && Log_RotationAge > 0)
        {
            /* Do a logfile rotation if it's time */
            pg_time_t	now = (pg_time_t) time(NULL);

            if (now >= next_rotation_time)
                rotation_requested = time_based_rotation = true;
        }

        if (!rotation_requested && Log_RotationSize > 0)
        {
            /* Do a rotation if file is too big */
            if (ftell(syslogFile) >= Log_RotationSize * 1024L)
                rotation_requested = true;
        }

        if (rotation_requested)
            logfile_rotate(time_based_rotation);

#ifndef WIN32

        /*
         * Wait for some data, timing out after 1 second
         */
        FD_ZERO(&rfds);
        FD_SET(syslogPipe[0], &rfds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        rc = select(syslogPipe[0] + 1, &rfds, NULL, NULL, &timeout);

        if (rc < 0)
        {
            if (errno != EINTR)
                ereport(LOG,
                        (errcode_for_socket_access(),
                         errmsg("select() failed in logger process: %m")));
        }
        else if (rc > 0 && FD_ISSET(syslogPipe[0], &rfds))
        {
            PipeProtoChunk *chunk = find_unused_chunk();
            int readPos = 0;

            if(chunk == NULL)
                syslogger_flush_chunks();

            chunk = find_unused_chunk();

			Assert(chunk != NULL);

			/* Read data to fill the buffer up to PIPE_CHUNK_SIZE bytes */
		next_chunkloop:
			if (bytesRead < sizeof(PipeProtoHeader))
			{
				/*
				 * We always try to make sure that the buffer has at least sizeof(PipeProtoHeader)
				 * bytes if we have read several bytes in the previous read. This handles the case
				 * when a valid chunk has to be read in two read calls, and the first read only
				 * picks up less than sizeof(PipeProtoHeader) bytes.
				 *
				 * However, this read may force some 3rd party error messages (less than
				 * sizeof(PipeProtoHeader) bytes) to sits in the buffer until the next message
				 * comes in. Thus you may experience some delays for small 3rd party error messages
				 * showing up in the logfile. Hopefully, this is very rare.
				 */
				readPos = bytesRead;
				bytesRead = piperead(syslogPipe[0], (char *)chunk + readPos, PIPE_CHUNK_SIZE - readPos);
			}
			
			if (bytesRead == 0)
			{
                /*
                 * Zero bytes read when select() is saying read-ready means
                 * EOF on the pipe: that is, there are no longer any processes
                 * with the pipe write end open.  Therefore, the postmaster
                 * and all backends are shut down, and we are done.
                 */
                pipe_eof_seen = true;

                /* if there's any data left then force it out now */
                syslogger_flush_chunks();
			}
			
			else if (bytesRead < 0)
			{
                if (errno != EINTR)
                    elog(ERROR, "Syslogger could not read from logger pipe: %m");
            }

			else
            {
				if (bytesRead + readPos >= sizeof(PipeProtoHeader) &&
					chunk_is_postgres_chunk((PipeProtoHeader *)chunk))
				{
					int chunk_size = chunk->hdr.len + sizeof(PipeProtoHeader);
					int needBytes = chunk_size - (bytesRead + readPos);

					/*
					 * Finish reading a chunk if the bytes we have read so far
					 * is not sufficient.
					 */
					if (needBytes > 0)
					{
						bytesRead =	piperead(syslogPipe[0],
											 ((char *)chunk) + (bytesRead + readPos),
											 needBytes);

						Assert(bytesRead == needBytes);
					}
					
					syslogger_handle_chunk(chunk);

					/*
					 * Copy the remaining bytes to the beginning of a new unused chunk
					 * buffer if we have read too much.
					 */
					if (needBytes < 0)
					{
						int moreBytes = bytesRead + readPos - chunk_size;
						PipeProtoChunk *new_chunk = find_unused_chunk();

						Assert(moreBytes > 0);
						
						if (new_chunk == NULL)
						{
							syslogger_flush_chunks();
							new_chunk = find_unused_chunk();
							Assert(new_chunk != NULL);
						}

						memmove((char *)new_chunk, ((char *)chunk) + chunk_size, moreBytes);
						chunk = new_chunk;
						bytesRead = moreBytes;
						readPos = 0;

						goto next_chunkloop;
					}

					/* go back to the main loop */
				}
				
				else
				{
					/*
					 * This is a 3rd party error. We may read parts of the standard
					 * error message along with the 3rd party error. So here, we
					 * scan the data byte by byte until we find a byte that is 0.
					 */
					char *msgEnd = (char *)chunk;
					char *chunkEnd = ((char *)chunk) + (bytesRead + readPos);

					while (*msgEnd != 0 &&
						   (msgEnd < chunkEnd))
						msgEnd++;
					
					if (msgEnd >= chunkEnd)
					{
						char lastChar = '\0';
						
						/*
						 * We didn't find a byte '0', so the whole message
						 * is one 3rd party error message.
						 */
						if (bytesRead + readPos >= PIPE_CHUNK_SIZE)
						{
							msgEnd --;
							lastChar = *msgEnd;
						}

						/* Add a '\0' terminator */
						*msgEnd = '\0';

						elog(LOG, "3rd party error log:\n%s%c", (char *)chunk, lastChar);

						/* remember to free this chunk */
						chunk->hdr.pid = 0;
					}
					
					else
					{
						Assert(*msgEnd == 0);

						/*
						 * If a 3rd party error does not start with byte '0',
						 * write the message.
						 */
						if (msgEnd != (char *)chunk)
							elog(LOG, "3rd party error log:\n%s", (char *)chunk);
						else
						{
							/* A 3rd party error starts with bytes '0', ignore this bytes. */
							msgEnd++;
						}

						if (chunkEnd - msgEnd > 0)
						{
							/* We copy the rest of bytes to the beginning of the chunk buffer. */
							memmove((char *)chunk, msgEnd, chunkEnd - msgEnd);
							bytesRead = chunkEnd - msgEnd;
							readPos = 0;

							goto next_chunkloop;
						}
					}
				}
			}
        }
#else							/* WIN32 */

        /*
         * On Windows we leave it to a separate thread to transfer data and
         * detect pipe EOF.  The main thread just wakes up once a second to
         * check for SIGHUP and rotation conditions.
         */
        pg_usleep(1000000L);
#endif   /* WIN32 */

        if (pipe_eof_seen)
        {
            /*
             * seeing this message on the real stderr is annoying - so we make
             * it DEBUG1 to suppress in normal use.
             */
            ereport(DEBUG1,
					(errmsg("logger shutting down")));

            /*
             * Normal exit from the syslogger is here.	Note that we
             * deliberately do not close syslogFile before exiting; this is to
             * allow for the possibility of elog messages being generated
             * inside proc_exit.  Regular exit() will take care of flushing
             * and closing stdio channels.
             */
            proc_exit(0);
        }
    }
}

/*
 * Postmaster subroutine to start a syslogger subprocess.
 */
int
SysLogger_Start(void)
{
    pid_t		sysloggerPid;
    char	   *filename;

    if (!Redirect_stderr)
        return 0;

    /*
     * If first time through, create the pipe which will receive stderr
     * output.
     *
     * If the syslogger crashes and needs to be restarted, we continue to use
     * the same pipe (indeed must do so, since extant backends will be writing
     * into that pipe).
     *
     * This means the postmaster must continue to hold the read end of the
     * pipe open, so we can pass it down to the reincarnated syslogger. This
     * is a bit klugy but we have little choice.
     */
#ifndef WIN32
    if (syslogPipe[0] < 0)
    {
        if (pgpipe(syslogPipe) < 0)
			ereport(FATAL,
					(errcode_for_socket_access(),
					 (errmsg("could not create pipe for syslog: %m"))));
    }
#else
    if (!syslogPipe[0])
    {
        SECURITY_ATTRIBUTES sa;

        memset(&sa, 0, sizeof(SECURITY_ATTRIBUTES));
        sa.nLength = sizeof(SECURITY_ATTRIBUTES);
        sa.bInheritHandle = TRUE;

        if (!CreatePipe(&syslogPipe[0], &syslogPipe[1], &sa, 32768))
			ereport(FATAL,
					(errcode_for_file_access(),
					 (errmsg("could not create pipe for syslog: %m"))));
    }
#endif

    /*
     * Create log directory if not present; ignore errors
     */
    mkdir(Log_directory, 0700);

    /*
     * The initial logfile is created right in the postmaster, to verify that
     * the Log_directory is writable.
     */
    filename = logfile_getname(time(NULL));

    syslogFile = fopen(filename, "a");

    if (!syslogFile)
		ereport(FATAL,
				(errcode_for_file_access(),
				 (errmsg("could not create log file \"%s\": %m",
						 filename))));

    setvbuf(syslogFile, NULL, LBF_MODE, 0);

    pfree(filename);

#ifdef EXEC_BACKEND
    switch ((sysloggerPid = syslogger_forkexec()))
#else
        switch ((sysloggerPid = fork_process()))
#endif
        {
            case -1:
			ereport(LOG,
					(errmsg("could not fork system logger: %m")));
			return 0;

#ifndef EXEC_BACKEND
            case 0:
                /* in postmaster child ... */
                /* Close the postmaster's sockets */
                ClosePostmasterPorts(true);

                /* Lose the postmaster's on-exit routines */
                on_exit_reset();

                /* Drop our connection to postmaster's shared memory, as well */
                PGSharedMemoryDetach();

                /* do the work */
                SysLoggerMain(0, NULL);
                break;
#endif

            default:
                /* success, in postmaster */

                /* now we redirect stderr, if not done already */
                if (!redirection_done)
                {
#ifndef WIN32
                    fflush(stdout);
                    if (dup2(syslogPipe[1], fileno(stdout)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stdout: %m")));
                    fflush(stderr);
                    if (dup2(syslogPipe[1], fileno(stderr)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stderr: %m")));
                    /* Now we are done with the write end of the pipe. */
                    close(syslogPipe[1]);
                    syslogPipe[1] = -1;
#else
                    int			fd;

                    /*
				 	 * open the pipe in binary mode and make sure stderr is binary
					 * after it's been dup'ed into, to avoid disturbing the pipe
					 * chunking protocol.
                     */
                    fflush(stderr);
                    fd = _open_osfhandle((long) syslogPipe[1],
                            _O_APPEND | _O_BINARY);
                    if (dup2(fd, _fileno(stderr)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stderr: %m")));
                    close(fd);
                    _setmode(_fileno(stderr),_O_BINARY);
                    /* Now we are done with the write end of the pipe. */
                    CloseHandle(syslogPipe[1]);
                    syslogPipe[1] = 0;
#endif
                    redirection_done = true;
                }

                /* postmaster will never write the file; close it */
                fclose(syslogFile);
                syslogFile = NULL;
                return (int) sysloggerPid;
        }

    /* we should never reach here */
    return 0;
}


#ifdef EXEC_BACKEND

/*
 * syslogger_forkexec() -
 *
 * Format up the arglist for, then fork and exec, a syslogger process
 */
    static pid_t
syslogger_forkexec(void)
{
    char	   *av[10];
    int			ac = 0;
    char		filenobuf[32];

    av[ac++] = "postgres";
    av[ac++] = "--forklog";
    av[ac++] = NULL;			/* filled in by postmaster_forkexec */

    /* static variables (those not passed by write_backend_variables) */
#ifndef WIN32
    if (syslogFile != NULL)
        snprintf(filenobuf, sizeof(filenobuf), "%d",
                fileno(syslogFile));
    else
        strcpy(filenobuf, "-1");
#else							/* WIN32 */
    if (syslogFile != NULL)
        snprintf(filenobuf, sizeof(filenobuf), "%ld",
                _get_osfhandle(_fileno(syslogFile)));
    else
        strcpy(filenobuf, "0");
#endif   /* WIN32 */
    av[ac++] = filenobuf;

    av[ac] = NULL;
	Assert(ac < lengthof(av));

    return postmaster_forkexec(ac, av);
}

/*
 * syslogger_parseArgs() -
 *
 * Extract data from the arglist for exec'ed syslogger process
 */
    static void
syslogger_parseArgs(int argc, char *argv[])
{
    int			fd;

	Assert(argc == 4);
    argv += 3;

#ifndef WIN32
    fd = atoi(*argv++);
    if (fd != -1)
    {
        syslogFile = fdopen(fd, "a");
        setvbuf(syslogFile, NULL, LBF_MODE, 0);
    }
#else							/* WIN32 */
    fd = atoi(*argv++);
    if (fd != 0)
    {
        fd = _open_osfhandle(fd, _O_APPEND | _O_TEXT);
        if (fd > 0)
        {
            syslogFile = fdopen(fd, "a");
            setvbuf(syslogFile, NULL, LBF_MODE, 0);
        }
    }
#endif   /* WIN32 */
}
#endif   /* EXEC_BACKEND */

/*
 * Write a given timestamp to the log file.
 */
    void
syslogger_append_timestamp(pg_time_t stamp_time, bool amsyslogger, bool append_comma)
{
    if(stamp_time != 0)
    {
        char strbuf[128];

        pg_strftime(strbuf, sizeof(strbuf),
                /* Win32 timezone names are too long so don't print them */
#ifndef WIN32
                "%Y-%m-%d %H:%M:%S %Z",
#else
                "%Y-%m-%d %H:%M:%S",
#endif
                pg_localtime(&stamp_time, log_timezone ? log_timezone : gmt_timezone));
		if (amsyslogger)
			write_syslogger_file_binary(strbuf, strlen(strbuf));
		else
			write(fileno(stderr), strbuf, strlen(strbuf));
    }

    if (append_comma)
	{
		if (amsyslogger)
			write_syslogger_file_binary(",", 1);
		else
			write(fileno(stderr), ",", 1);
	}
}

/*
 * Write the current timestamp with milliseconds to the syslogger file or
 * stderr.
 *
 * It is not safe to call strftime since it is not async-safe, and it
 * is expensive to call strftime to get timezone everytime, we use
 * pg_strftime, but stick on a fixed timezone (default_timezone)
 * instead a settable timezone as PostgreSQL does, since we want all
 * log messages to have the same time format. See MPP-2591.
 */
void
syslogger_append_current_timestamp(bool amsyslogger)
{
    struct timeval tv;
    pg_time_t	stamp_time;
    char strbuf[128];
    char msbuf[8];

    gettimeofday(&tv, NULL);
    stamp_time = (pg_time_t) tv.tv_sec;

    pg_strftime(strbuf, sizeof(strbuf),
            /* leave room for milliseconds... */
            /* Win32 timezone names are too long so don't print them */
#ifndef WIN32
            "%Y-%m-%d %H:%M:%S        %Z",
#else
            "%Y-%m-%d %H:%M:%S        ",
#endif
            pg_localtime(&stamp_time, log_timezone ? log_timezone : gmt_timezone));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%06d", (int) (tv.tv_usec));
    strncpy(strbuf + 19, msbuf, 7);

	if (amsyslogger)
	{
		write_syslogger_file_binary(strbuf, strlen(strbuf));
		write_syslogger_file_binary(",", 1);
	}
	else
	{
		write(fileno(stderr), strbuf, strlen(strbuf));
		write(fileno(stderr), ",", 1);
	}
}


/*
 * We use the PostgreSQL defaults for CSV, i.e. quote = escape = '"'
 * If it's NULL, append nothing.
 */
int syslogger_write_str(const char *data, int len, bool amsyslogger, bool csv)
{
    int cnt = 0;

    /* avoid confusing an empty string with NULL */
    if (data == NULL)
        return 0;

    while (cnt < len && data[cnt] != '\0')
    {
        if (csv && data[cnt] == '"')
		{
			if (amsyslogger)
				write_syslogger_file_binary("\"", 1);
			else
				write(fileno(stderr), "\"", 1);
		}
		
		if (amsyslogger)
			write_syslogger_file_binary(data+cnt, 1);
		else
			write(fileno(stderr), data+cnt, 1);

        cnt+=1;
    }

    return cnt;
}

/*
 * Write a string, ended with '\0', in a specific chunk to the log.
 *
 * If csv is true, this function puts double-quotes around the string.
 * If both csv and quote_empty_string are true, this function puts
 * double-quotes around an empty string.
 * If append_comma is true, this function appends a comma after the string.
 */
static void
syslogger_write_str_from_chunk(CSVChunkStr *chunkstr, bool csv,
							   bool quote_empty_string, bool append_comma)
{
    int wlen = 0; 
    int len = 0;
	bool is_empty_string = false;

    if (chunkstr->chunk != NULL)
	{
		len = chunkstr->chunk->hdr.len - (chunkstr->p - chunkstr->chunk->data);

		/* Check if the string is an empty string */
		if (len > 0 && chunkstr->p[0] == '\0')
			is_empty_string = true;
		if (len == 0 && chunkstr->chunk->hdr.next >= 0)
		{
			PipeProtoChunk *next_chunk = &saved_chunks[chunkstr->chunk->hdr.next];
			if (next_chunk->hdr.len > 0 && next_chunk->data[0] == '\0')
				is_empty_string = true;
		}
	}

	else
	{
		Assert(chunkstr->p == NULL);
		is_empty_string = true;
	}

    if(csv && (!is_empty_string || quote_empty_string))
        write_syslogger_file_binary("\"", 1);

    while(chunkstr->p)
    {
        bool done = false;
        wlen = syslogger_write_str(chunkstr->p, len, true, csv);

        /* Write OK, don't forget to account for the trailing 0 */
        if(wlen < len)
        { 
            done = true;
            chunkstr->p += wlen + 1;
        }
        else
            chunkstr->p += wlen;

        if(chunkstr->p - chunkstr->chunk->data == chunkstr->chunk->hdr.len)
        {
            /* switch to next chunk */
            if(chunkstr->chunk->hdr.next >= 0)
            {
                chunkstr->chunk = &saved_chunks[chunkstr->chunk->hdr.next];
                chunkstr->p = chunkstr->chunk->data;
                len = chunkstr->chunk->hdr.len - (chunkstr->p - chunkstr->chunk->data);
            }
            else
            {
                chunkstr->chunk = NULL;
                chunkstr->p = NULL;
            }
        }

        if(done)
            break;
    }

    if(csv && (!is_empty_string || quote_empty_string))
        write_syslogger_file_binary("\"", 1);

    if (append_comma)
        write_syslogger_file_binary(",", 1);
}

    void
syslogger_write_int32(bool test0, const char *prefix, int32 i, bool amsyslogger, bool append_comma)
{
    char buf[1024];
    int len;

    if (!test0 || i > 0)
    {
        len = sprintf(buf, "%s%d", prefix, i);
		if (amsyslogger)
			write_syslogger_file_binary(buf, len);
		else
			write(fileno(stderr), buf, len);
    }
    if (append_comma)
	{
		if (amsyslogger)
			write_syslogger_file_binary(",", 1);
		else
			write(fileno(stderr), ",", 1);
	}
}

/*
 * setErrorDataFromSegvChunk
 *   Fill in the given error data with the chunk that contains the message
 * sent in a SEGV/BUS/ILL handler.
 */
static void
fillinErrorDataFromSegvChunk(GpErrorData *errorData, PipeProtoChunk *chunk)
{
	Assert(chunk != NULL &&
		   chunk->hdr.is_segv_msg == 't' &&
		   chunk->hdr.is_last == 't');

	GpSegvErrorData *segvData = (GpSegvErrorData *)chunk->data;
	
	errorData->fix_fields.session_start_time = segvData->session_start_time;
	errorData->fix_fields.send_alert = 't';
	errorData->fix_fields.omit_location = 'f';

	/* This field is always true now. We should remove this eventually. */
	errorData->fix_fields.gp_is_primary = 't';
	errorData->fix_fields.gp_session_id = segvData->gp_session_id;
	errorData->fix_fields.gp_command_count = segvData->gp_command_count;
	errorData->fix_fields.gp_segment_id = segvData->gp_segment_id;
	errorData->fix_fields.slice_id = segvData->slice_id;
	errorData->fix_fields.error_cursor_pos = 0;
	errorData->fix_fields.internal_query_pos = 0;
	errorData->fix_fields.error_fileline = 0;
	errorData->fix_fields.top_trans_id = 0;
	errorData->fix_fields.dist_trans_id = 0;
	errorData->fix_fields.local_trans_id = 0;
	errorData->fix_fields.subtrans_id = 0;

	errorData->username = NULL;
	errorData->databasename = NULL;
	errorData->remote_host = NULL;
	errorData->remote_port = NULL;
	errorData->error_severity = "PANIC";
	errorData->sql_state = "XX000";
	errorData->error_message = palloc0(ERROR_MESSAGE_MAX_SIZE);

	const char *signalName = SegvBusIllName(segvData->signal_num);
	Assert(signalName != NULL);
	snprintf(errorData->error_message, ERROR_MESSAGE_MAX_SIZE,
			 "Unexpected internal error: %s received signal %s",
			 (AmIMaster() && Gp_role == GP_ROLE_DISPATCH) ? "Master process" : "Segment process",
			 signalName);
	
	errorData->error_detail = NULL;
	errorData->error_hint = NULL;
	errorData->internal_query = NULL;
	errorData->error_context = NULL;
	errorData->debug_query_string = NULL;
	errorData->error_func_name = NULL;
	errorData->error_filename = NULL;
	errorData->stacktrace = NULL;
	
	if (segvData->frame_depth > 0)
	{
		void *stackAddressArray = (chunk->data + MAXALIGN(sizeof(GpSegvErrorData)));
		void **stackAddresses = stackAddressArray;
		errorData->stacktrace = gp_stacktrace(stackAddresses, segvData->frame_depth);
	}
}

/*
 * freeErrorDataFields
 *   Free the palloc'ed fields inside GpErrorData.
 *
 * This is the counterpart for fillinErrorDataFromSegvChunk. Currently, only error message and
 * stacktrace need to be freed.
 */
static void
freeErrorDataFields(GpErrorData *errorData)
{
	pfree(errorData->error_message);
	
	if (errorData->stacktrace != NULL)
	{
		pfree(errorData->stacktrace);
	}
}

/*
 * syslogger_write_str_with_comma
 *   Write the given string to the log. A comma is appended after the given string.
 *
 * If csv is true, double quotes are added around the string.
 */
static void
syslogger_write_str_with_comma(const char *data, bool amsyslogger, bool csv)
{
	if (data != NULL)
	{
		if (csv)
		{
			write_syslogger_file_binary("\"", 1);
		}

		syslogger_write_str(data, strlen(data), amsyslogger, csv);

		if (csv)
		{
			write_syslogger_file_binary("\"", 1);
		}
	}
	
	write_syslogger_file_binary(",", 1);
}

/*
 * syslogger_write_errordata
 *   Write the GpErrorData to the log.
 */
static void
syslogger_write_errordata(PipeProtoHeader *chunkHeader, GpErrorData *errorData, bool csv)
{
	syslogger_append_current_timestamp(true);
	
	/* username */
	syslogger_write_str_with_comma(errorData->username, true, csv);
	
	/* databasename */
	syslogger_write_str_with_comma(errorData->databasename, true, csv);
	
	/* Process id, thread id */
	syslogger_write_int32(false, "p", chunkHeader->pid, true, true);
	syslogger_write_int32(false, "th", chunkHeader->thid, true, true);
	
	/* Remote host */
	syslogger_write_str_with_comma(errorData->remote_host, true, csv);
	/* Remote port */
	syslogger_write_str_with_comma(errorData->remote_port, true, csv);
	
	/* session start timestamp */
	syslogger_append_timestamp(errorData->fix_fields.session_start_time, true, true);
	
	/* Transaction id */
	syslogger_write_int32(false, "", errorData->fix_fields.top_trans_id, true, true);
	
	/* GPDB specific options. */
	syslogger_write_int32(true, "con", errorData->fix_fields.gp_session_id, true, true); 
	syslogger_write_int32(true, "cmd", errorData->fix_fields.gp_command_count, true, true); 
	syslogger_write_int32(false, errorData->fix_fields.gp_is_primary == 't'? "seg" : "mir", errorData->fix_fields.gp_segment_id,
						  true, true); 
	syslogger_write_int32(true, "slice", errorData->fix_fields.slice_id, true, true); 
	syslogger_write_int32(true, "dx", errorData->fix_fields.dist_trans_id, true, true);
	syslogger_write_int32(true, "x", errorData->fix_fields.local_trans_id, true, true); 
	syslogger_write_int32(true, "sx", errorData->fix_fields.subtrans_id, true, true); 
	
	/* error severity */
	syslogger_write_str_with_comma(errorData->error_severity, true, csv);
	/* sql state code */
	syslogger_write_str_with_comma(errorData->sql_state, true, csv);
	/* errmsg */
	syslogger_write_str_with_comma(errorData->error_message, true, csv);
	/* errdetail */
	syslogger_write_str_with_comma(errorData->error_detail, true, csv);
	/* errhint */
	syslogger_write_str_with_comma(errorData->error_hint, true, csv);
	/* internal query */
	syslogger_write_str_with_comma(errorData->internal_query, true, csv);
	/* internal query pos */
	syslogger_write_int32(true, "", errorData->fix_fields.internal_query_pos, true, true);
	/* err ctxt */
	syslogger_write_str_with_comma(errorData->error_context, true, csv);
	/* user query */
	syslogger_write_str_with_comma(errorData->debug_query_string, true, csv);
	/* cursor pos */
	syslogger_write_int32(false, "", errorData->fix_fields.error_cursor_pos, true, true); 
	/* func name */
	syslogger_write_str_with_comma(errorData->error_func_name, true, csv);
	/* file name */
	syslogger_write_str_with_comma(errorData->error_filename, true, csv);
	/* line number */
	syslogger_write_int32(true, "", errorData->fix_fields.error_fileline, true, true);
	/* stack trace */
	if (errorData->stacktrace != NULL)
	{
		if (csv)
		{
			write_syslogger_file_binary("\"", 1);
		}
		
		syslogger_write_str(errorData->stacktrace, strlen(errorData->stacktrace), true, csv);

		if (csv)
		{
			write_syslogger_file_binary("\"", 1);
		}
	}
	
	/* EOL */
	write_syslogger_file_binary(LOG_EOL, strlen(LOG_EOL));
	
#if USE_EMAIL
	/*
	 * Send alerts when needed. The alerts are sent only by the master.
	 * If the alert is failed for whatever reason, log a message and continue.
	 */
	if (errorData->fix_fields.send_alert == 't' &&
		AmIMaster() && Gp_role == GP_ROLE_DISPATCH)
	{
		PG_TRY();
		{
			send_alert(errorData);
		}
		PG_CATCH();
		{
			elog(LOG,"Failed to send alert.");
		}
		PG_END_TRY();
	}
#endif	
}

/*
 * syslogger_log_segv_chunk
 *   Write the chunk for the message sent inside a SEGV/BUS/ILL handler to the log.
 */
static void
syslogger_log_segv_chunk(PipeProtoChunk *chunk)
{
	Assert(chunk->hdr.is_segv_msg == 't' && chunk->hdr.is_last == 't');
	Assert(chunk->hdr.thid == FIXED_THREAD_ID);

	/* Reset the thread id */
	chunk->hdr.thid = 0;

	GpErrorData errorData;
	fillinErrorDataFromSegvChunk(&errorData, chunk);
	syslogger_write_errordata(&chunk->hdr, &errorData, chunk->hdr.log_format == 'c');
	freeErrorDataFields(&errorData);
	
	/* mark chunk as unused */
	chunk->hdr.pid = 0;
}

void syslogger_log_chunk_list(PipeProtoChunk *chunk)
{
    GpErrorDataFixFields *pfixed = (GpErrorDataFixFields *) (chunk->data);

    if(chunk->hdr.log_format == 't')
    {
        CSVChunkStr chunkstr = { chunk, chunk->data };
        syslogger_write_str_from_chunk(&chunkstr, false, false, false);
    }
    else
    {
        CSVChunkStr chunkstr = { chunk, chunk->data + sizeof(GpErrorDataFixFields) };

        /*
         * timestamp_with_milliseconds 
         */
        syslogger_append_current_timestamp(true);

        /* username */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);

        /* databasename */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);

        /* Process id, thread id */
        syslogger_write_int32(false, "p", chunk->hdr.pid, true, true);
        syslogger_write_int32(false, "th", chunk->hdr.thid, true, true);

        /* Remote host */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* Remote port */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);

        /* session start timestamp */
        syslogger_append_timestamp(pfixed->session_start_time, true, true);

        /* Transaction id */
        syslogger_write_int32(false, "", pfixed->top_trans_id, true, true);

        /* GPDB specific options. */
        syslogger_write_int32(true, "con", pfixed->gp_session_id, true, true); 
        syslogger_write_int32(true, "cmd", pfixed->gp_command_count, true, true); 
        syslogger_write_int32(false, pfixed->gp_is_primary == 't'? "seg" : "mir", pfixed->gp_segment_id,
							  true, true); 
        syslogger_write_int32(true, "slice", pfixed->slice_id, true, true); 
        syslogger_write_int32(true, "dx", pfixed->dist_trans_id, true, true);
        syslogger_write_int32(true, "x", pfixed->local_trans_id, true, true); 
        syslogger_write_int32(true, "sx", pfixed->subtrans_id, true, true); 

        /* error severity */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* sql state code */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* errmsg */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* errdetail */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* errhint */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* internal query */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* internal query pos */
        syslogger_write_int32(true, "", pfixed->internal_query_pos, true, true);
        /* err ctxt */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* user query */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* cursor pos */
        syslogger_write_int32(false, "", pfixed->error_cursor_pos, true, true); 
        /* func name */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* file name */
        syslogger_write_str_from_chunk(&chunkstr, true, false, true);
        /* line number */
        syslogger_write_int32(true, "", pfixed->error_fileline, true, true);
        /* stack trace */
        syslogger_write_str_from_chunk(&chunkstr, true, false, false);

        /* EOL */
        write_syslogger_file_binary(LOG_EOL, strlen(LOG_EOL));

#if USE_EMAIL
        if (pfixed->send_alert == 't')
        	if (AmIMaster() && Gp_role == GP_ROLE_DISPATCH) /* Only the master sends alerts */
        		send_alert_from_chunks(chunk, &saved_chunks[0]);
#endif
    }

    /* Free the chunks */
    while(true)
    {
        chunk->hdr.pid = 0;
        if(chunk->hdr.next == -1)
            break;
        chunk = &saved_chunks[chunk->hdr.next];
    }
}

static void syslogger_flush_chunks()
{
    PipeProtoChunk * chunk = NULL;
    int i;

    for(i=0; i<CHUNK_SLOTS; ++i)
    {
        if(saved_chunks[i].hdr.pid != 0
                && saved_chunks[i].hdr.chunk_no == 0)
        {
            chunk = &saved_chunks[i];
            syslogger_log_chunk_list(chunk);
        }
    }

    /* make sure we free everything */
    for (i=0; i<CHUNK_SLOTS; ++i)
    {
        saved_chunks[i].hdr.pid = 0;
    }
}

static void syslogger_handle_chunk(PipeProtoChunk *chunk)
{
    int i;
    PipeProtoChunk *first = NULL; 
    PipeProtoChunk *prev = NULL; 

#ifdef USE_TEST_UTILS
    if (chunk->hdr.log_format == 'X')
    {
        if (chunk->hdr.log_line_number == 1)
        {
            proc_exit(1);
        }
        else if (chunk->hdr.log_line_number == 2)
        {
            proc_exit(2);
        }
        else if (chunk->hdr.log_line_number == 11)
        {
            *(int *) 0 = 1234;
        }
        else
        {
            abort();
        }
        return;
    }
#endif

    Assert(chunk->hdr.log_format == 'c' || chunk->hdr.log_format == 't'); 
          
    /* I am the last, so chain no one */
    chunk->hdr.next = -1; 

    /* find interesting things */
    for(i = 0; i<CHUNK_SLOTS; ++i)
    {
        if(saved_chunks[i].hdr.pid == chunk->hdr.pid && 
                saved_chunks[i].hdr.thid == chunk->hdr.thid && 
                saved_chunks[i].hdr.log_line_number == chunk->hdr.log_line_number)
        {
            if(saved_chunks[i].hdr.chunk_no == 0)
                first = &saved_chunks[i];

            if(saved_chunks[i].hdr.chunk_no == chunk->hdr.chunk_no - 1)
                prev = &saved_chunks[i];
        }
    }

    /* Chain me */
    if(prev)
        prev->hdr.next = chunk - &saved_chunks[0];
    else if(chunk->hdr.chunk_no != 0)
    {
        /* A chunk without prev, drop it on the floor */
        elog(LOG, "Out of order or dangling chunks from pid %d", chunk->hdr.pid);

        /* remember to free this chunk */
        chunk->hdr.pid = 0;

        /* Out of order chunk, if we have something before this, output */
        if(first)
            syslogger_log_chunk_list(first);

        return;
    }

    if(chunk->hdr.is_last == 't')
	{
		if (chunk->hdr.is_segv_msg == 't')
		{
			syslogger_log_segv_chunk(first);
		}
		else
		{
			syslogger_log_chunk_list(first);
		}
	}
}


#ifdef WIN32
/* --------------------------------
 *		pipe protocol handling
 * --------------------------------
 */

/*
 * Process data received through the syslogger pipe.
 *
 * This routine interprets the log pipe protocol which sends log messages as
 * (hopefully atomic) chunks - such chunks are detected and reassembled here.
 *
 * The protocol has a header that starts with two nul bytes, then has a 16 bit
 * length, the pid of the sending process, and a flag to indicate if it is
 * the last chunk in a message. Incomplete chunks are saved until we read some
 * more, and non-final chunks are accumulated until we get the final chunk.
 *
 * All of this is to avoid 2 problems:
 * . partial messages being written to logfiles (messes rotation), and
 * . messages from different backends being interleaved (messages garbled).
 *
 * Any non-protocol messages are written out directly. These should only come
 * from non-PostgreSQL sources, however (e.g. third party libraries writing to
 * stderr).
 *
 * logbuffer is the data input buffer, and *bytes_in_logbuffer is the number
 * of bytes present.  On exit, any not-yet-eaten data is left-justified in
 * logbuffer, and *bytes_in_logbuffer is updated.
 */
static void
process_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
{
	char	   *cursor = logbuffer;
	int			count = *bytes_in_logbuffer;
	int			dest = LOG_DESTINATION_STDERR;

	/* While we have enough for a header, process data... */
	while (count >= (int) sizeof(PipeProtoHeader))
	{
		PipeProtoHeader p;
		int			chunklen;

		/* Do we have a valid header? */
		memcpy(&p, cursor, sizeof(PipeProtoHeader));
		if (p.zero == 0 && 
			p.len > 0 && p.len <= PIPE_MAX_PAYLOAD &&
			p.pid != 0 &&
            p.thid != 0 &&
			(p.is_last == 't' || p.is_last == 'f' ||
			 p.is_last == 'T' || p.is_last == 'F'))
		{
			chunklen = PIPE_HEADER_SIZE + p.len;

			/* Fall out of loop if we don't have the whole chunk yet */
			if (count < chunklen)
				break;

			dest = (p.log_format == 'c' || p.log_format == 'f') ?
			 	LOG_DESTINATION_CSVLOG : LOG_DESTINATION_STDERR;

			/* Finished processing this chunk */
			cursor += chunklen;
			count -= chunklen;
		}
		else
		{
			/* Process non-protocol data */

			/*
			 * Look for the start of a protocol header.  If found, dump data
			 * up to there and repeat the loop.  Otherwise, dump it all and
			 * fall out of the loop.  (Note: we want to dump it all if at all
			 * possible, so as to avoid dividing non-protocol messages across
			 * logfiles.  We expect that in many scenarios, a non-protocol
			 * message will arrive all in one read(), and we want to respect
			 * the read() boundary if possible.)
			 */
			for (chunklen = 1; chunklen < count; chunklen++)
			{
				if (cursor[chunklen] == '\0')
					break;
			}
			/* fall back on the stderr log as the destination */
			write_syslogger_file(cursor, chunklen /*, LOG_DESTINATION_STDERR*/);
			cursor += chunklen;
			count -= chunklen;
		}
	}

	/* We don't have a full chunk, so left-align what remains in the buffer */
	if (count > 0 && cursor != logbuffer)
		memmove(logbuffer, cursor, count);
	*bytes_in_logbuffer = count;
}

/*
 * Force out any buffered data
 *
 * This is currently used only at syslogger shutdown, but could perhaps be
 * useful at other times, so it is careful to leave things in a clean state.
 */
static void
flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
{
    syslogger_flush_chunks(); 
}
#endif

/* --------------------------------
 *		logfile routines
 * --------------------------------
 */

/*
 * Write binary data to the currently open logfile
 *
 * On Windows the data arriving in the pipe already has CR/LF newlines,
 * so we must send it to the file without further translation.
 */
void write_syslogger_file_binary(const char *buffer, int count)
{
    int			rc;

#ifndef WIN32
    rc = fwrite(buffer, 1, count, syslogFile);
#else
    EnterCriticalSection(&sysfileSection);
    rc = fwrite(buffer, 1, count, syslogFile);
    LeaveCriticalSection(&sysfileSection);
#endif

    /* can't use ereport here because of possible recursion */
    if (rc != count)
        write_stderr("could not write to log file: %s\n", strerror(errno));
}
void write_syslogger_file(const char *buffer, int count)
{
    write_syslogger_file_binary(buffer,count);
}
#ifdef WIN32

/*
 * Worker thread to transfer data from the pipe to the current logfile.
 *
 * We need this because on Windows, WaitForSingleObject does not work on
 * unnamed pipes: it always reports "signaled", so the blocking ReadFile won't
 * allow for SIGHUP; and select is for sockets only.
 */
static unsigned int __stdcall
pipeThread(void *arg)
{
    char		logbuffer[READ_BUF_SIZE];
    int			bytes_in_logbuffer = 0;

    for (;;)
    {
        DWORD		bytesRead;

        if (!ReadFile(syslogPipe[0],
                    logbuffer + bytes_in_logbuffer,
                    sizeof(logbuffer) - bytes_in_logbuffer,
                    &bytesRead, 0))
        {
            DWORD		error = GetLastError();

            if (error == ERROR_HANDLE_EOF ||
                    error == ERROR_BROKEN_PIPE)
                break;
            _dosmaperr(error);
            ereport(LOG,
                    (errcode_for_file_access(),
                     errmsg("could not read from logger pipe: %m")));
        }
        else if (bytesRead > 0)
        {
            bytes_in_logbuffer += bytesRead;
            process_pipe_input(logbuffer, &bytes_in_logbuffer);
        }
    }

    /* We exit the above loop only upon detecting pipe EOF */
    pipe_eof_seen = true;

    /* if there's any data left then force it out now */
    flush_pipe_input(logbuffer, &bytes_in_logbuffer);

    _endthread();
    return 0;
}
#endif   /* WIN32 */

/*
 * perform logfile rotation
 */
static void
logfile_rotate(bool time_based_rotation)
{
    char	   *filename;
    FILE	   *fh;

    rotation_requested = false;

    /*
     * When doing a time-based rotation, invent the new logfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        filename = logfile_getname(next_rotation_time);
    else
        filename = logfile_getname(time(NULL));

    /*
     * Decide whether to overwrite or append.  We can overwrite if (a)
     * Log_truncate_on_rotation is set, (b) the rotation was triggered by
     * elapsed time and not something else, and (c) the computed file name is
     * different from what we were previously logging into.
     *
     * Note: during the first rotation after forking off from the postmaster,
     * last_file_name will be NULL.  (We don't bother to set it in the
     * postmaster because it ain't gonna work in the EXEC_BACKEND case.) So we
     * will always append in that situation, even though truncating would
     * usually be safe.
     */
    if (Log_truncate_on_rotation && time_based_rotation &&
            last_file_name != NULL && strcmp(filename, last_file_name) != 0)
        fh = fopen(filename, "w");
    else
        fh = fopen(filename, "a");

    if (!fh)
    {
        int			saveerrno = errno;

        ereport(LOG,
                (errcode_for_file_access(),
                 errmsg("could not open new log file \"%s\": %m",
                     filename)));

        /*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with Log_directory and stop
			 * trying to create files.
         */
        if (saveerrno != ENFILE && saveerrno != EMFILE)
        {
            ereport(LOG,
                    (errmsg("disabling automatic rotation (use SIGHUP to reenable)")));
            Log_RotationAge = 0;
            Log_RotationSize = 0;
        }
        pfree(filename);
        return;
    }

    setvbuf(fh, NULL, LBF_MODE, 0);

#ifdef WIN32
    _setmode(_fileno(fh), _O_TEXT); /* use CRLF line endings on Windows */
#endif

    /* On Windows, need to interlock against data-transfer thread */
#ifdef WIN32
    EnterCriticalSection(&sysfileSection);
#endif
    fclose(syslogFile);
    syslogFile = fh;
#ifdef WIN32
    LeaveCriticalSection(&sysfileSection);
#endif

    /*
     * remove oldest log files when total size exceeds the Log_MaxSize,
     * or last modification time is earlier than Log_MaxAge.
     */
    constrain_max_logfile();

    set_next_rotation_time();

    /* instead of pfree'ing filename, remember it for next time */
    if (last_file_name != NULL)
        pfree(last_file_name);
    last_file_name = filename;
}

/*
 * Used by qsort() to sort log files by modification time in descending order
 */
int logfileinfocmp(const void *d1, const void *d2)
{
    time_t diff = (*(LogFileInfo * const *)d1)->mtime -
                  (*(LogFileInfo * const *)d2)->mtime;
    return (diff > 0) ? -1 : (diff < 0 ? 1 : 0);
}

/*
 * Constrain the log files with maximum size or duration
 */
static void
constrain_max_logfile()
{
    /* file name/path string */
    char *dirpath = NULL;
    char *filepath = NULL;
    char *filename;
    char *suffix;

    /* dir/file infomation */
    DIR *dir;
    struct dirent *file = NULL;
    struct stat statbuf;

    /* log file list */
    size_t arraysz;
    size_t nitems = 0;
    size_t logsize = 0;
    unsigned int i = 0, fileidx =0;
    LogFileInfo **filelist, *copyfile = NULL;

    /* upbound of constraints */
    size_t largest = 0;
    pg_time_t earliest, now, modifytime;

#define CSV_SUFFIX ".csv"
#define TXT_SUFFIX ".txt"

    /* nothing to do if log files are immortal */
    if (Log_MaxSize <= 0 && Log_MaxAge <= 0)
        return;

    /* calculate the earliest time to clean */
    if(Log_MaxAge > 0)
    {
        now = (pg_time_t) time(NULL);
        earliest = now - Log_MaxAge * SECS_PER_MINUTE;
    }

    /* calculate the largest total size to clean */
    if(Log_MaxSize > 0)
    {
        largest = Log_MaxSize * 1024L;
    }

    /* open the log directory */
    dirpath = palloc(MAXPGPATH);
    snprintf(dirpath, MAXPGPATH, "%s/", Log_directory);

    if ((dir = opendir(dirpath)) == NULL)
    {
        ereport(WARNING, (errmsg("could not open directory \"%s\": %m", dirpath)));
        goto error_out;
    }

    if (fstat(dirfd(dir), &statbuf) < 0)
    {
        ereport(WARNING, (errmsg("could not obtain information about directory \"%s\": %m", dirpath)));
        goto error_out;
    }

    /* estimate size of file array, 24 is the minimum size entry */
    arraysz = (statbuf.st_size / 24);
    filelist = (LogFileInfo **)palloc(arraysz * sizeof(LogFileInfo *));

    /* get file lists, filter out illegal files */
    filepath = palloc(MAXPGPATH);
    while ((file = readdir(dir)) != NULL)
    {
        filename = file->d_name;

        /* ignore '.' and '..' */
        if (strcmp(filename, ".") == 0 || strcmp(filename, "..") == 0)
            continue;

        /* ignore '/'. On snow leopard, we get back "/" as a subdir */
        if (filename[0] == '/' && filename[1] == '\0')
            continue;

        /* file name shorter than log extensions (.log, .csv) will be ignored */
        if (strlen(filename) < sizeof(CSV_SUFFIX) + 1)
           continue;

        suffix = filename + (strlen(filename) - sizeof(CSV_SUFFIX) + 1);

        /* is *.log ? */
        if (gp_log_format == 0 && pg_strcasecmp(suffix, TXT_SUFFIX) != 0)
            continue;

        /* is *.csv ? */
        if (gp_log_format == 1 && pg_strcasecmp(suffix, CSV_SUFFIX) != 0)
            continue;

        /* get file status, include symlink */
        snprintf(filepath, MAXPGPATH, "%s/%s", Log_directory, filename);
        if (lstat(filepath, &statbuf) != 0)
        {
            if (errno != ENOENT)
                ereport(WARNING, (errmsg("could not stat file or directory \"%s\": %m", filepath)));

            continue;
        }

        /* is a normal file? don't touch subdir, pipe, link and other types */
        if (S_ISREG(statbuf.st_mode))
        {
            /* store its name into list */
            copyfile = (LogFileInfo *)palloc(sizeof(LogFileInfo));
            copyfile->filepath = pstrdup(filepath);
            copyfile->mtime = statbuf.st_mtime;
            copyfile->filesize= statbuf.st_size;

            /* make sure space left is enough, in case that logs are growning */
            if ((nitems+1) >= arraysz)
            {
                arraysz *= 2;
                filelist = (LogFileInfo **)repalloc(filelist, arraysz * sizeof(LogFileInfo *));
            }

            filelist[nitems++] = copyfile;
        }


        /* clear it for next readdir() */
        errno = 0;
    }

    if (errno)
    {
        ereport(WARNING, (errmsg("could not read directory \"%s\": %m", dirpath)));
        goto error_out;
    }

    /* empty file list, why not leave directly? */
    if(nitems == 0)
        goto quick_out;

    /* sort by log file modification time in descending order */
    if (nitems)
        qsort(filelist, nitems, sizeof(struct fileinfo *), logfileinfocmp);

    /* filter out with size and age constraints*/
    logsize = 0;
    for(i = 0; i < nitems; i++)
    {
        /* maxsize constraint */
        if(Log_MaxSize > 0)
        {
            logsize += filelist[i]->filesize;
            if (logsize >= largest)
               break;
        }

        /* maxage constraint */
        if(Log_MaxAge > 0)
        {
            modifytime = filelist[i]->mtime;
            if(modifytime < earliest)
                break;
        }

    }
    fileidx = i;

    /* remove old files */
    for(i = fileidx; i < nitems; i++)
    {
        filename = filelist[i]->filepath;
        if (unlink(filename) != 0)
        {
            if (errno != ENOENT)
            {
                ereport(WARNING, (errmsg("could not remove file \"%s\": %m", filename)));
                goto error_out;
            }
        }
    }

quick_out:
error_out:
    if (filelist)
    {
        for (i = 0; i < nitems; i++)
        {
            pfree(filelist[i]->filepath);
            pfree(filelist[i]);
        }
        pfree(filelist);
    }
    if (dir)
        closedir(dir);
    if (dirpath)
        pfree(dirpath);
    if (filepath)
        pfree(filepath);
}

/*
 * construct logfile name using timestamp information
 *
 * Result is palloc'd.
 */
    static char *
logfile_getname(pg_time_t timestamp)
{
    char	   *filename;
    int			len;
    struct pg_tm *tm;
	char *suffix;
#define CSV_SUFFIX ".csv"

    filename = palloc(MAXPGPATH);

    snprintf(filename, MAXPGPATH, "%s/", Log_directory);

    len = strlen(filename);

    if (strchr(Log_filename, '%'))
    {
        /* treat it as a strftime pattern */
        tm = pg_localtime(&timestamp, log_timezone);
        pg_strftime(filename + len, MAXPGPATH - len, Log_filename, tm);
    }
    else
    {
        /* no strftime escapes, so append timestamp to new filename */
        snprintf(filename + len, MAXPGPATH - len, "%s.%lu",
                Log_filename, (unsigned long) timestamp);
    }

	/*
	 * If the logging format is 'TEXT' and the filename ends with ".csv",
	 * replace ".csv" with ".log".
	 *
	 * If the logging format is 'CSV' and the filename does not end with ".csv",
	 * replace the last four characters in the filename with ".cvs".
	 */
	if (strlen(filename) - sizeof(CSV_SUFFIX) + 1 > 0)
		suffix = filename + (strlen(filename) - sizeof(CSV_SUFFIX) + 1);
	else
		/*
		 * Point the suffix to the end of string if the length of
		 * the filename is less than ".csv".
		 */
		suffix = filename + strlen(filename);
	
	if (gp_log_format == 0 && pg_strcasecmp(suffix, CSV_SUFFIX) == 0)
	{
		snprintf(suffix, sizeof(CSV_SUFFIX), ".log");
	}
	
	if (gp_log_format == 1 && pg_strcasecmp(suffix, CSV_SUFFIX) != 0)
	{
		snprintf(suffix, sizeof(CSV_SUFFIX), CSV_SUFFIX);
	}

    return filename;
}

/*
 * Determine the next planned rotation time, and store in next_rotation_time.
 */
    static void
set_next_rotation_time(void)
{
    pg_time_t	now;
    struct pg_tm *tm;
    int			rotinterval;

    /* nothing to do if time-based rotation is disabled */
    if (Log_RotationAge <= 0)
        return;

    /*
     * The requirements here are to choose the next time > now that is a
     * "multiple" of the log rotation interval.  "Multiple" can be interpreted
     * fairly loosely.	In this version we align to log_timezone rather than
     * GMT.
     */
    rotinterval = Log_RotationAge * SECS_PER_MINUTE;	/* convert to seconds */
	now = (pg_time_t) time(NULL);
    tm = pg_localtime(&now, log_timezone);
    now += tm->tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= tm->tm_gmtoff;
    next_rotation_time = now;
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/* SIGHUP: set flag to reload config file */
static void
sigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/* SIGUSR1: set flag to rotate logfile */
static void
sigUsr1Handler(SIGNAL_ARGS)
{
	rotation_requested = true;
}
