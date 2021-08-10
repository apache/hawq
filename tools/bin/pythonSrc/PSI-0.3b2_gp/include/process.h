/* The MIT License
 *
 * Copyright (C) 2008-2009 Floris Bruynooghe
 *
 * Copyright (C) 2008-2009 Abilisoft Ltd.
 *
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef PROCESS_H
#define PROCESS_H


#include <time.h>


/** Exception used for unknown process
 *
 * A process might be unknown because it never existed or because it no longer
 * exists.
 */
extern PyObject *PsiExc_NoSuchProcessError;


/** Represent all basic process attributes
 *
 * The Python module might expose more but they can all be derived from these.
 * When allocating this structure be sure that all attributes are filled with
 * 0 or NULL so that new or forgotten fields will result in a "not
 * implemented" status.
 *
 * Note that only fields who's size can be determined with `sizeof' at compile
 * time can be others then the basic C types.  If a field can't be converted
 * like that then the conversion must be the burden of the architecture
 * implementation.
 */
struct psi_process {
    char *name;                 /* accounting name or basename(exe) */
    char *exe;                  /* absolute path of the executable */
    long argc;                  /* argument count */
    char **argv;                /* full argument list */
    int envc;                   /* number of environment variables */
    char **envv;                /* environment in strings of `NAME=VALUE' */
    char *command;              /* argument list as string */
    char *cwd;                  /* current working directory */
    uid_t euid;                 /* effective UID */
    uid_t ruid;                 /* real UID */
    gid_t egid;                 /* effective GID */
    gid_t rgid;                 /* real GID */
    pid_t ppid;                 /* parent PID */
    pid_t pgrp;                 /* process group ID */
    pid_t sid;                  /* session ID */
#if defined(SUNOS5) && SUNOS5_MINOR >= 10
    zoneid_t zoneid;            /* solaris 10 zone ID */
#endif
    char *zonename;             /* solaris 10 zone name */
    long priority;              /* priority */
    long nice;                  /* nice value */
#ifdef LINUX
    unsigned long long jiffies; /* start time jiffies from boot */
#endif
    struct timespec start_time; /* start time, UTC */
    int status;                 /* status */
    long nthreads;              /* number of threads */
    char *terminal;             /* owning terminal or NULL */
    struct timespec utime;      /* user time used by the process */
    struct timespec stime;      /* system time use by the process */
    struct timespec cputime;    /* total cpu time used by the process */
    unsigned long long rss;     /* resident memory size in bytes */
    unsigned long long vsz;     /* virtual memory size in bytes */
    double pcpu;                /* instantanious % CPU utilisation */
    int exe_status;
    int name_status;
    int argc_status;
    int argv_status;
    int envc_status;
    int envv_status;
    int command_status;
    int cwd_status;
    int euid_status;
    int ruid_status;
    int egid_status;
    int rgid_status;
    int euser_status;
    int ruser_status;
    int egroup_status;
    int rgroup_status;
    int zoneid_status;
    int zonename_status;
    int ppid_status;
    int pgrp_status;
    int sid_status;
    int priority_status;
    int nice_status;
    int jiffies_status;
    int start_time_status;
    int status_status;
    int nthreads_status;
    int terminal_status;
    int utime_status;
    int stime_status;
    int cputime_status;
    int rss_status;
    int vsz_status;
    int pcpu_status;
};


/** Create the process info for a PID
 *
 * @param pid: The PID of the process to create the info for.
 *
 * @return A pointer to the filled in psi_process structure or NULL.
 */
struct psi_process *psi_arch_process(const pid_t pid);


/** Free the psi_process structure
 *
 * This function will check all pointers in the passed psi_process structure
 * and if they are non-NULL will call psi_free() on them.  At the end it will
 * call psi_free() on the structure itself thus completely freeing the space
 * used by the structure.
 *
 * Note that for this to work you must have allocated the structure with
 * psi_calloc() so that all pointers are initialised to NULL.
 *
 * The return value is always NULL so you can just do `return
 * psi_free_process()'
 */
void *psi_free_process(struct psi_process *proci);


/** Create the proclist structure
 *
 * Returns a struct psi_proclist with all the PIDs in. or NULL in case of
 * failure.
 */
struct psi_proclist *psi_arch_proclist(void);


/** Free the psi_proclist structure
 *
 * This function will check all pointers in the passed psi_proclist structure
 * and if they are non-NULL will call psi_free() on them.  At the end it will
 * call psi_free() on the structure itself thus completely freeing the space
 * used by the structure.
 *
 * Note that for this to work you must have allocated the structure with
 * psi_calloc() so that all pointers are initialised to NULL.
 *
 * The return value is always NULL so you can just do `return
 * psi_free_proclist()'
 */
void *psi_free_proclist(struct psi_proclist *prl);


/** Represent a  constant
 *
 * This structure represents a flag so it can be exported to the Python
 * code.
 *
 * Implementations must declare an array of these named
 * `psi_arch_proc_status_flags', it is for statuses like `running' and `zombie'
 * etc.  The last element should be a null-sentinel: `{NULL, 0}'.
 */
struct psi_flag {
    char *name;                 /* name of the flag */
    long val;                   /* value of the flag */
};


extern struct psi_flag psi_arch_proc_status_flags[];


/** An array with all the processes
 *
 * The array will contain `count' pids.
 */
struct psi_proclist {
    long count;
    pid_t *pids;
};


/* The following are for the processmodule, not for process
 * implementations. */


/* The type objects */
extern PyTypeObject PsiProcess_Type;
extern PyTypeObject PsiProcessTable_Type;


/* Convenience functions to create a new objects. */
PyObject *PsiProcess_New(pid_t pid);
PyObject *PsiProcessTable_New(void);


#endif /* PROCESS_H */
