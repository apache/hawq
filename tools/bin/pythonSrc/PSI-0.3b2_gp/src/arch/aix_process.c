/* The MIT License
 *
 * Copyright (C) 2009 Floris Bruynooghe
 *
 * Copyright (C) 2009 Abilisoft Ltd.
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

/* AIX implementation of the Process classes */


/* Note: On AIX if __64BIT__ is defined the LP64 data model is used, otherwise
 *       ILP23 is used.
 *
 * Note2: We use large file APIs to read the core files of processes (aka
 *        address space, /proc/<pid>/as), read up on large file support:
 *        http://publib.boulder.ibm.com/infocenter/systems/index.jsp?topic=/com.ibm.aix.genprogc/doc/genprogc/prg_lrg_files.htm */


#include <Python.h>

#include <errno.h>
#include <fcntl.h>
#include <paths.h>
#include <procinfo.h>
#include <stdio.h>
#include <string.h>
#include <sys/dir.h>
#include <sys/proc.h>
#include <sys/procfs.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "psi.h"
#include "process.h"
#include "posix_utils.h"
#include "procfs_utils.h"


/***** Local declarations *****/
static int getprocent(struct procentry64 *procbuff, const pid_t pid);
static int set_bulk(struct psi_process *proci,
                    const struct procentry64 *procent);
static int set_args(struct psi_process *proci,
                    struct procentry64 *procent);
static int set_env(struct psi_process *proci,
                   struct procentry64 *procent);
static int set_cwd(struct psi_process *proci,
                   const struct procentry64 *procent);

static int get_terminal(const dev64_t ttydev, char **name);
static int is_tty_file(struct dirent *dirp);
static void free_dentlist(struct dirent **dentlist, const int ndirs);
static int mygetargs(struct procentry64 *procbuff, char **args);
static int args_complete(char *args, const int size);
static int mygetevars(struct procentry64 *procent, char **evnp);


/***** Declare undeclared functions from <procinfo.h> *****/
/* IMHO <procinfo.h> should declare these, see getprocs(3) and getargs(3) */
extern int getprocs64(struct procentry64*, int,
                      struct fdsinfo64*, int, pid32_t*, int);
extern int getargs(struct procentry64*, int, char*, int);
extern int getevars(struct procentry64*, int, char*, int);


/***** Public interfaces from process.h. *****/


/* The process status flags, defined in proc.h. */
/* XXX Might have to use lwpsinfo.sname instead */
#define addsflag(CONST) {"PROC_STATUS_"#CONST, CONST}
struct psi_flag psi_arch_proc_status_flags[] = {
    addsflag(SNONE),
    addsflag(SIDL),
    addsflag(SZOMB),
    addsflag(SSTOP),
    addsflag(SACTIVE),
    addsflag(SSWAP),
    {NULL, 0}
};


/** Collect all information about a process
 *
 * The psinfo variable is so that /proc/<pid>/psinfo only needs to be read
 * once.
 */
struct psi_process *
psi_arch_process(const pid_t pid)
{
    struct psi_process *proci;
    struct procentry64 procent;
    int r;

    r = getprocent(&procent, pid);
    if (r < 0)
        return NULL;
    proci = psi_calloc(sizeof(struct psi_process));
    if (proci == NULL)
        return NULL;
    if (set_bulk(proci, &procent) < 0)
        return psi_free_process(proci);
    if (set_args(proci, &procent) < 0)
        return psi_free_process(proci);
    if (set_env(proci, &procent) < 0)
        return psi_free_process(proci);
    if (set_cwd(proci, &procent) < 0)
        return psi_free_process(proci);
    return proci;
}


/***** Local functions *****/


/** Call getprocs64(3) for exactly 1 PID
 *
 * This takes care of boring details and handles errors properly.
 *
 * @param procbuff: the procentry64 structure the result will be stored in.
 * @param pid: the PID of the process to get.
 *
 * @return -1 in case of an error, 0 if successful.
 **/
static int
getprocent(struct procentry64 *procbuff, const pid_t pid)
{
    pid32_t pid32;
    int r;

    pid32 = (pid32_t)pid;
    r = getprocs64(procbuff, sizeof(struct procentry64),
                   (struct fdsinfo64 *)NULL, 0,
                   &pid32, 1);
    if (r < 0) {
        if (errno == EINVAL)
            PyErr_Format(PsiExc_NoSuchProcessError,
                         "No such PID: %lu", (unsigned long)pid);
        else
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, "getprocs64()");
        return -1;
    }
    return 0;
}


/** Get all data from the procentry64 structure
 *
 * This is a poorly named function.  It gets as much data out of the
 * procentry64 structure as possible and places it in the psi_process
 * structure.
 */
static int
set_bulk(struct psi_process *proci, const struct procentry64 *procent)
{
    int pagesize;
    int r;

    proci->name = psi_strdup(procent->pi_comm);
    if (proci->name == NULL)
        return -1;
    proci->name_status = PSI_STATUS_OK;
    proci->exe = proci->name;
    proci->exe_status = PSI_STATUS_OK;

    proci->euid = procent->pi_cred.crx_uid;
    proci->euid_status = PSI_STATUS_OK;
    proci->ruid = procent->pi_cred.crx_ruid;
    proci->ruid_status = PSI_STATUS_OK;
    proci->egid = procent->pi_cred.crx_gid;
    proci->egid_status = PSI_STATUS_OK;
    proci->rgid = procent->pi_cred.crx_rgid;
    proci->rgid_status = PSI_STATUS_OK;

    proci->ppid = procent->pi_ppid;
    proci->ppid_status = PSI_STATUS_OK;
    proci->pgrp = procent->pi_pgrp;
    proci->pgrp_status = PSI_STATUS_OK;
    proci->sid = procent->pi_sid;
    proci->sid_status = PSI_STATUS_OK;

    proci->priority = procent->pi_ppri;
    proci->priority_status = PSI_STATUS_OK;
    proci->nice = procent->pi_nice;
    proci->nice_status = PSI_STATUS_OK;
    proci->start_time.tv_sec = procent->pi_start;
    proci->start_time.tv_nsec = 0;
    proci->start_time_status = PSI_STATUS_OK;
    proci->status = procent->pi_state;
    proci->status_status = PSI_STATUS_OK;
    proci->nthreads = procent->pi_thcount;
    proci->nthreads_status = PSI_STATUS_OK;

    r = get_terminal(procent->pi_ttyd, &proci->terminal);
    if (r == -1)
        return -1;
    else if (r == -2)
        proci->terminal_status = PSI_STATUS_PRIVS;
    else
        proci->terminal_status = PSI_STATUS_OK;

    /* The ru_utime and ru_stime members are `struct timeval64' which claims
     * to contain microseconds in `tv_usec'.  However all evidence suggests
     * that it really is nanoseconds: (i) the values stored in it are larger
     * then 1e6 and (ii) the results do match with ps(1) when treated as
     * nanoseconds, but not when treated as micoseconds. */
    proci->utime.tv_sec = procent->pi_ru.ru_utime.tv_sec;
    proci->utime.tv_nsec = procent->pi_ru.ru_utime.tv_usec;
    proci->utime_status = PSI_STATUS_OK;
    proci->stime.tv_sec = procent->pi_ru.ru_stime.tv_sec;
    proci->stime.tv_nsec = procent->pi_ru.ru_stime.tv_usec;
    proci->stime_status = PSI_STATUS_OK;

    /* There is a procent.pi_cpu which claims to be a tick count for the first
     * thread, but I trust this more. */
    proci->cputime.tv_sec = proci->utime.tv_sec + proci->stime.tv_sec;
    proci->cputime.tv_sec +=
        (proci->utime.tv_nsec + proci->stime.tv_nsec)/1000000000;
    proci->cputime.tv_nsec =
        (proci->utime.tv_nsec + proci->stime.tv_nsec)%1000000000;
    proci->cputime_status = PSI_STATUS_OK;

    pagesize = getpagesize();
    proci->rss = (procent->pi_drss + procent->pi_trss) * pagesize;
    proci->rss_status = PSI_STATUS_OK;
    /* This is the same size as returned by ps(1) for VSZ.  I don't believe
     * it's correct however, it only contains the data sections of the virtual
     * memory and omits the text size.  None of the other values seem to give
     * something useful either tho and there is something to be said for
     * showing what ps shows.  You can see the real value (in pagesizes) by
     * using "svmon -P <pid>" and look in the "Virtual" column. */
    proci->vsz = procent->pi_dvm * pagesize;
    proci->vsz_status = PSI_STATUS_OK;

    return 0;
}


static int
set_args(struct psi_process *proci, struct procentry64 *procent)
{
    char *ptr;
    int i;

    proci->argc = mygetargs(procent, &proci->command);
    if (proci->argc == -1)
        return -1;
    if (proci->argc == -2) {    /* proc gone walkies */
        proci->argc_status = PSI_STATUS_NA;
        proci->argv_status = PSI_STATUS_NA;
        return 0;
    }
    proci->argv = psi_strings_to_array(proci->command, proci->argc);
    if (proci->argv == NULL)
        return -1;
    ptr = proci->command;
    for (i = 0; i < proci->argc - 1; i++) {
        while (*ptr != '\0')
            ptr++;
        *ptr = ' ';
    }
    proci->argc_status = PSI_STATUS_OK;
    proci->argv_status = PSI_STATUS_OK;
    proci->command_status = PSI_STATUS_OK;
    return 0;
}


static int
set_env(struct psi_process *proci, struct procentry64 *procent)
{
    char *envp;

    proci->envc = mygetevars(procent, &envp);
    if (proci->envc == -1)
        return -1;
    if (proci->envc == -2) {    /* proc gone walkies */
        proci->envc_status = PSI_STATUS_NA;
        proci->envv_status = PSI_STATUS_NA;
        return 0;
    }
    proci->envv = psi_strings_to_array(envp, proci->envc);
    psi_free(envp);
    if (proci->envv == NULL)
        return -1;
    proci->envc_status = PSI_STATUS_OK;
    proci->envv_status = PSI_STATUS_OK;
    return 0;
}


/** Set the cwd and cwd_status fields of the procinfo structure
 *
 * Returns -1 on failure, 0 on success.
 */
static int
set_cwd(struct psi_process *proci, const struct procentry64 *procent)
{
    char *fname;
    char *link;
    int r;

    r = psi_asprintf(&fname, "/proc/%d/cwd", procent->pi_pid);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, fname);
    psi_free(fname);
    if (r == -2) {
        PyErr_Clear();
        proci->cwd_status = PSI_STATUS_PRIVS;
        return 0;
    } else if (r < 0) {
        PyErr_Clear();
        if (procfs_check_pid(procent->pi_pid) < 0)
            return -1;
        else {
            proci->cwd_status = PSI_STATUS_NA;
            return 0;
        }
    } else {
        proci->cwd = link;
        proci->cwd_status = PSI_STATUS_OK;
        return 0;
    }
}


/** Find the name of a terminal
 *
 * This function will look at all files in /dev to find the matching device
 * number.  When found the `name' parameter will be pointed to a newly
 * allocated name of the terminal, otherwise it will be set to point to NULL.
 *
 * Return 0 if successful, -1 in case of an error and -2 in case of a
 * permission problem.
 */
/* XXX Split this off in two functions. */
static int
get_terminal(const dev64_t ttydev, char **name)
{
    struct stat64x stat_buff;
    struct dirent **dentlist;
    struct dirent *dentp;
    char ttyname[15] = _PATH_DEV; /* "/dev/console\0" is longest expected */
    int nfiles;
    int i;
    int r;

    *name = NULL;

    /* Check /dev/pts/ files */
    r = stat64x("/dev/pts/0", &stat_buff);
    if (r < 0) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, "/dev/pts/0");
        return -1;
    }
    if (major64(ttydev) == major64(stat_buff.st_rdev)) {
        if (minor64(ttydev) > 99) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Minor number can be no larger then 99");
            return -1;
        }
        sprintf(ttyname, "/dev/pts/%d", minor64(ttydev));
        *name = psi_strdup(ttyname);
        if (*name == NULL)
            return -1;
        return 0;
    }

    /* Scan for /dev/console and /dev/tty* */
    nfiles = scandir(_PATH_DEV, &dentlist, &is_tty_file, NULL);
    if (nfiles < 0) {
        PyErr_SetString(PyExc_OSError, "Failed to list /dev entries");
        return -1;
    }
    if (nfiles == 0) {
        free_dentlist(dentlist, nfiles);
        return 0;
    }
    for (i = 0; i < nfiles; i++) {
        dentp = dentlist[i];
        if (strlen(ttyname) + strlen(dentp->d_name) + 1 > 15) {
            PyErr_SetString(PyExc_RuntimeError, "ttyname larger then 14 chars");
            free_dentlist(dentlist, nfiles);
            return -1;
        }
        strcat(ttyname, dentp->d_name);
        r = stat64x(ttyname, &stat_buff);
        if (r < 0) {
            free_dentlist(dentlist, nfiles);
            if (errno == EACCES)
                return -2;
            else {
                PyErr_SetFromErrnoWithFilename(PyExc_OSError, ttyname);
                return -1;
            }
        }
        if (ttydev == stat_buff.st_dev) {
            free_dentlist(dentlist, nfiles);
            *name = psi_strdup(ttyname);
            if (*name == NULL)
                return -1;
            return 0;
        }
    }
    free_dentlist(dentlist, nfiles);
    return 0;
}


static int
is_tty_file(struct dirent *dirp)
{
    if (strncmp(_PATH_TTY, dirp->d_name, strlen(_PATH_TTY)) == 0)
        return 1;
    else if (strcmp(_PATH_CONSOLE, dirp->d_name) == 0)
        return 1;
    else
        return 0;
}


static void
free_dentlist(struct dirent **dentlist, const int ndirs)
{
    int i;

    for (i = 0; i < ndirs; i++)
        free(dentlist[i]);
    free(dentlist);
}


/** Return list of process arguments, ending in `\0\0'
 *
 * This wraps getargs(3) but allocates the string automatically using
 * psi_malloc() and guarantees the complete argument list is returned.
 *
 * Returns -1 in case of an error and -2 if the process is gone.  On success
 * the number of arguments in the argument list is returned.
 */
static int
mygetargs(struct procentry64 *procbuff, char **args)
{
/* XXX nargs should be `usigned int' and args_sz & i `ssize_t' but getargs()
 * uses int for args_sz bizzarly enough.  --flub */
    char *ptr;
    int size = 250;             /* size of `args' */
    int nargs;
    int r;

    *args = (char*)psi_malloc(size);
    if (*args == NULL)
        return -1;
    r = getargs(procbuff, sizeof(struct procentry64), *args, size);
    if (r < 0) {
        psi_free(*args);
        if (errno == ESRCH)     /* proc gone walkies */
            return -2;
        else {
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, "getargs()");
            return -1;
        }
    }
    nargs = args_complete(*args, size);
    while (nargs < 0) {
        size += 250;
        ptr = (char*)psi_realloc(*args, size);
        if (ptr == NULL) {
            psi_free(*args);
            return -1;
        }
        *args = ptr;
        r = getargs(procbuff, sizeof(struct procentry64), *args, size);
        if (r < 0) {
            psi_free(*args);
            if (errno == ESRCH) /* proc gone walkies */
                return -2;
            else {
                PyErr_SetFromErrnoWithFilename(PyExc_OSError, "getargs()");
                return -1;
            }
        }
        nargs = args_complete(*args, size);
    }
    return nargs;
}


/** Check if an argument list is complete
 *
 * This function looks for a `\0\0' in the string `args' (which is up too
 * `size' characters long).  If found it returns the number of `\0'-terminated
 * strings, excluding the last empty one, otherwise -1 is returned.
 *
 * This is useful to check if a getargs(3) call returned the complete argument
 * list.
 */
static int
args_complete(char *args, const int size)
{
    int complete = 0;
    int nargs = 0;
    int i;

    for (i = 1; i < size; i++) {
        if (args[i] == '\0') {
            if (args[i-1] == '\0') {
                complete = 1;
                break;
            } else
                nargs++;
        }
    }
    if (!complete)
        return -1;
    else
        return nargs;
}


/** Return list of process environment variables, ending in `\0\0'
 *
 * This wraps getevars(3) but allocates the string automatically using
 * psi_malloc() and guarantees the complete environment is returned.
 *
 * Returns -1 in case of an error and -2 if the process is gone.  On success
 * the number of environment variables in the argument list is returned.
 */
static int
mygetevars(struct procentry64 *procent, char **envp)
{
/* XXX `n' should be `usigned int' and args_sz & i `ssize_t' but getargs()
 * uses `int' for `args_sz' bizzarly enough.  --flub */
    char *ptr;
    int size = 250;             /* size of `envp' */
    int n;                      /* number of environment variables */
    int r;

    *envp = (char*)psi_malloc(size);
    if (*envp == NULL)
        return -1;
    r = getevars(procent, sizeof(struct procentry64), *envp, size);
    if (r < 0) {
        psi_free(*envp);
        if (errno == ESRCH)     /* proc gone walkies */
            return -2;
        else {
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, "getevars()");
            return -1;
        }
    }
    n = args_complete(*envp, size);
    while (n < 0) {
        size += 250;
        ptr = (char*)psi_realloc(*envp, size);
        if (ptr == NULL) {
            psi_free(*envp);
            return -1;
        }
        *envp = ptr;
        r = getevars(procent, sizeof(struct procentry64), *envp, size);
        if (r < 0) {
            psi_free(*envp);
            if (errno == ESRCH) /* proc gone walkies */
                return -2;
            else {
                PyErr_SetFromErrnoWithFilename(PyExc_OSError, "getevars()");
                return -1;
            }
        }
        n = args_complete(*envp, size);
    }
    return n;
}
