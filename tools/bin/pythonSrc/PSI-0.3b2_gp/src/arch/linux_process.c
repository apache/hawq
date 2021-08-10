/* The MIT License
 *
 * Copyright (C) 2007 Chris Miles
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

/* Linux implementation of the Process classes */


#include <Python.h>

#include <limits.h>
#include <locale.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <asm/param.h>

#include "psi.h"
#include "process.h"
#include "procfs_utils.h"
#include "posix_utils.h"
#include "linux_utils.h"


/* Constant numbers don't matter, but apparently these are copied form
 * linux/fs/proc/array.c.  Whatever.  --flub */
#define PROC_STATUS_RUNNING 0
#define PROC_STATUS_SLEEPING 1
#define PROC_STATUS_DISKSLEEP 2
#define PROC_STATUS_STOPPED 4
#define PROC_STATUS_TRACINGSTOP 8
#define PROC_STATUS_ZOMBIE 16
#define PROC_STATUS_DEAD 32
#define PROC_STATUS_PAGING 64   /* not from array.c  */


/***** Local declarations *****/
static int parse_uid_from_proc_status(struct psi_process *proci,
                                      const char *line);
static int parse_gid_from_proc_status(struct psi_process *proci,
                                      const char *line);
static int parse_stat(struct psi_process *proci,
                      const char *stat_buf,
                      const pid_t pid);
static int decode_state(const char state);
static void ticks2timespec(struct timespec *tspec,
                           const unsigned long long ticks);
static struct timespec calc_cputime(const struct timespec utime,
                                    const struct timespec stime);
static int calc_start_time(struct timespec *start_time,
                           const unsigned long long starttime);
static int find_terminal(char **terminal, pid_t pid, int fd);

static int set_exe(struct psi_process *proci, const pid_t pid);
static int set_arg(struct psi_process *proci, const pid_t pid);
static int set_env(struct psi_process *proci, const pid_t pid);
static int set_cwd(struct psi_process *proci, const pid_t pid);
static int set_ids(struct psi_process *proci, const pid_t pid);
static int set_stat(struct psi_process *proci, const pid_t pid);
static int set_term(struct psi_process *proci, pid_t pid);



/***** Public interfaces from process.h. *****/



/* The process status flags. */
#define addconst(CONST) {#CONST, CONST}
struct psi_flag psi_arch_proc_status_flags[] = {
    addconst(PROC_STATUS_RUNNING),
    addconst(PROC_STATUS_SLEEPING),
    addconst(PROC_STATUS_DISKSLEEP),
    addconst(PROC_STATUS_STOPPED),
    addconst(PROC_STATUS_TRACINGSTOP),
    addconst(PROC_STATUS_ZOMBIE),
    addconst(PROC_STATUS_DEAD),
    addconst(PROC_STATUS_PAGING),
    {NULL, 0}};             /* sentinel */


struct psi_process *
psi_arch_process(const pid_t pid)
{
    struct psi_process *proci;

    if (procfs_check_pid(pid) == -1)
        return NULL;
    proci = psi_calloc(sizeof(struct psi_process));
    if (proci == NULL)
        return NULL;
    if (set_exe(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_arg(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_env(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_cwd(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_ids(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_stat(proci, pid) == -1)
        return psi_free_process(proci);
    if (set_term(proci, pid) == -1)
        return psi_free_process(proci);
    return proci;
}


/***** Local functions *****/


/*
 * terminal_name()
 *
 * Returns:
 *       0 = success
 *      -1 = Failure and Exception was raised
 */
static int
set_term(struct psi_process *proci, pid_t pid)
{
    char *terminal = NULL;
    int i;
    int r;
    int fds[] = {255, 2, 0, 1};

    for (i=0; i<4; i++) {
        r = find_terminal(&terminal, pid, fds[i]);
        if (r == -2) {
            PyErr_Clear();
            proci->terminal_status = PSI_STATUS_PRIVS;
            return 0;
        } else if (r < 0)
            PyErr_Clear();
        else if (r == 1)
            break;
    }
    proci->terminal = terminal;
    proci->terminal_status = PSI_STATUS_OK;
    return 0;
}


/** Read /proc/pid/stat
 *
 * The psi_process members filled by this are: nthreads, ppid, status,
 * virtual_size, resident_size, flags, user_time, system_time, cpu_time,
 * priority, nice, start_time, pcpu.  Their matching *_status slots are filled
 * in too.
 *
 * Returns 0 for success; -1 for failure (Exception raised).
 */
static int
set_stat(struct psi_process *proci, const pid_t pid)
{
    char *buf;
    int bufsize;
    int r;

    bufsize = procfs_read_procfile(&buf, pid, "stat");
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->ppid_status = PSI_STATUS_PRIVS;
        proci->pgrp_status = PSI_STATUS_PRIVS;
        proci->sid_status = PSI_STATUS_PRIVS;
        proci->priority_status = PSI_STATUS_PRIVS;
        proci->nice_status = PSI_STATUS_PRIVS;
        proci->start_time_status = PSI_STATUS_PRIVS;
        proci->status_status = PSI_STATUS_PRIVS;
        proci->nthreads_status = PSI_STATUS_PRIVS;
        proci->terminal_status = PSI_STATUS_PRIVS;
        proci->utime_status = PSI_STATUS_PRIVS;
        proci->stime_status = PSI_STATUS_PRIVS;
        proci->vsz_status = PSI_STATUS_PRIVS;
        proci->rss_status = PSI_STATUS_PRIVS;
        proci->pcpu_status = PSI_STATUS_PRIVS;
        return 0;
    }
    r = parse_stat(proci, buf, pid);
    psi_free(buf);
    if (r == -1)
        return -1;
    return 0;
}


/** Set euid, egid, ruid and rgid members of a psi_process structure
 *
 * The matching *_status members will be set too.
 *
 * Note that this will also set the RSS value since we're reading
 * /proc/<pid>/status here and this is the best value for RSS we can get.
 * This function should probably be renamed to parse_status() or something
 * like that.
 */
static int
set_ids(struct psi_process *proci, const pid_t pid)
{
    char *buf;
    char *line_start;
    char *line_end;
    char *value;
    int bufsize;
    int uid_seen = 0;
    int gid_seen = 0;
/*     int rss_seen = 0; */

    bufsize = procfs_read_procfile(&buf, pid, "status");
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->euid_status = PSI_STATUS_PRIVS;
        proci->ruid_status = PSI_STATUS_PRIVS;
        proci->egid_status = PSI_STATUS_PRIVS;
        proci->rgid_status = PSI_STATUS_PRIVS;
        proci->euser_status = PSI_STATUS_PRIVS;
        proci->ruser_status = PSI_STATUS_PRIVS;
        proci->egroup_status = PSI_STATUS_PRIVS;
        proci->rgroup_status = PSI_STATUS_PRIVS;
        proci->rss_status = PSI_STATUS_PRIVS;
        return 0;
    }
    line_start = buf;
    while (line_start < buf + bufsize) {
        line_end = line_start;
        while (*line_end != '\n' && line_end <= buf+bufsize)
            line_end++;
        *line_end = '\0';
        value = strchr(line_start, ':');
        if (value) {
            *value++ = '\0';    /* terminate line_start & point to value */
            if (strcmp("Uid", line_start) == 0) {
                uid_seen = 1;
                if (parse_uid_from_proc_status(proci, value) < 0) {
                    psi_free(buf);
                    return -1;
                }
            } else if (strcmp("Gid", line_start) == 0) {
                gid_seen = 1;
                if (parse_gid_from_proc_status(proci, value) < 0) {
                    psi_free(buf);
                    return -1;
                }
            }
        } /* if (value) */
        line_start = line_end + 1;
    } /* while (line_start < buf + bufsize) */
    psi_free(buf);
    if (!uid_seen || !gid_seen) {
        PyErr_Format(
            PyExc_RuntimeError,
            "Failed to parse required information from /proc/%d/status",
            pid);
        return -1;
    }
    return 0;
}


/* XXX: This is exactly the same as set_arg, only with a different
 *      filename! */
static int
set_env(struct psi_process *proci, const pid_t pid)
{
    char *buf;
    char **envv;
    int envc;
    int bufsize;

    bufsize = procfs_read_procfile(&buf, pid, "environ");
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->envc_status = PSI_STATUS_PRIVS;
        proci->envv_status = PSI_STATUS_PRIVS;
        return 0;
    }
    envc = psi_strings_count(buf, bufsize);
    envv = psi_strings_to_array(buf, envc);
    psi_free(buf);
    if (envv == NULL)
        return -1;
    proci->envc = envc;
    proci->envc_status = PSI_STATUS_OK;
    proci->envv = envv;
    proci->envv_status = PSI_STATUS_OK;
    return 0;
}


/** Set the argument list
 *
 * This sets argc, argv and command in the psi_process structure.
 *
 * Returns 0 on success, -1 on failure (Python exception raised).
 */
static int
set_arg(struct psi_process *proci, const pid_t pid)
{
    char *ptr;
    char **argv;
    int argc;
    int bufsize;

    bufsize = procfs_read_procfile(&proci->command, pid, "cmdline");
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->argc_status = PSI_STATUS_PRIVS;
        proci->argv_status = PSI_STATUS_PRIVS;
        return 0;
    }
    argc = psi_strings_count(proci->command, bufsize);
    argv = psi_strings_to_array(proci->command, argc);
    if (argv == NULL)
        return -1;
    for (ptr = proci->command; ptr-proci->command < bufsize-1; ptr++)
        if (*ptr == '\0')
            *ptr = ' ';
    proci->argc = argc;
    proci->argc_status = PSI_STATUS_OK;
    proci->argv = argv;
    proci->argv_status = PSI_STATUS_OK;
    proci->command_status = PSI_STATUS_OK;
    return 0;
}


/** Set the exe and exe_status fields of the psi_process structure
 *
 * Returns -1 on failure, 0 on success.
 */
static int
set_exe(struct psi_process *proci, const pid_t pid)
{
    char *fname;
    char *link;
    int r;

    r = psi_asprintf(&fname, "/proc/%d/exe", pid);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, fname);
    psi_free(fname);
    if (r == -2) {
        PyErr_Clear();
        proci->exe_status = PSI_STATUS_PRIVS;
        return 0;
    } else if (r < 0) {
        if (procfs_check_pid(pid) < 0)
            return -1;          /* NoSuchProcessError now set */
        else {
            PyErr_Clear();
            proci->exe_status = PSI_STATUS_NA;
            return 0;
        }
    } else {
        proci->exe = link;
        proci->exe_status = PSI_STATUS_OK;
        return 0;
    }
}


/** Set the cwd and cwd_status fields of the psi_process structure
 *
 * Returns -1 on failure, 0 on success.
 */
static int
set_cwd(struct psi_process *proci, const pid_t pid)
{
    PyObject *exc_type, *exc_val, *exc_tb;
    PyObject *errno_attr;
    long errno_long;
    char *fname;
    char *link;
    int r;

    r = psi_asprintf(&fname, "/proc/%d/cwd", pid);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, fname);
    psi_free(fname);
    if (r == -2) {
        PyErr_Clear();
        proci->cwd_status = PSI_STATUS_PRIVS;
        return 0;
    } else if (r < 0) {
        procfs_check_pid(pid);  /* OSError -> NoSuchProcessError if required */
        /* If the process is a zombie, readlink sets errno to ENOENT since the
         * target does no longer exist.  But psi still wants to show the
         * process and should not error out. */
        if (PyErr_ExceptionMatches(PyExc_OSError)) {
            PyErr_Fetch(&exc_type, &exc_val, &exc_tb);
            PyErr_NormalizeException(&exc_type, &exc_val, &exc_tb);
            errno_attr = PyObject_GetAttrString(exc_val, "errno");
            errno_long = PyLong_AsLong(errno_attr);
            Py_DECREF(errno_attr);
            if (errno_long == ENOENT) {
                Py_DECREF(exc_type);
                Py_DECREF(exc_val);
                Py_XDECREF(exc_tb);
                proci->cwd_status = PSI_STATUS_NA;
                return 0;
            }
            PyErr_Restore(exc_type, exc_val, exc_tb);
        }
        return -1;
    } else {
        proci->cwd = link;
        proci->cwd_status = PSI_STATUS_OK;
        return 0;
    }
}


/** Find which file/device a filedescriptor is symlinked too
 *
 * This will read a symlink in /proc/<pid>/fd/<fd> and if it is pointing to a
 * valid file will return the name of that file.  You need to psi_free()
 * `terminal' after use, but only if you got `1' as return value, otherwise it
 * won't be allocated.
 *
 * Returns:
 *      -2 = insufficient privileges, exception raised
 *      -1 = error, exception raised
 *      0 = no terminal name found, `terminal' set to NULL.
 *      1 = terminal name found
 */
static int
find_terminal(char **terminal, pid_t pid, int fd)
{
    char *path;
    char *link;
    int r;

    r = psi_asprintf(&path, "/proc/%d/fd/%d", pid, fd);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, path);
    psi_free(path);
    if (r < 0)
        return r;
    if (strncmp(link, "/dev/null", 9) != 0) {
        *terminal = link;
        return 1;
    }
    psi_free(link);
    terminal = NULL;
    return 0;
}


/** Calculate elapsed time from jiffies
 *
 * @param tspec: The timespec into which the result is stored.
 * @param ticks: The number of clock ticks aka jiffies.
 */
static void
ticks2timespec(struct timespec *tspec, const unsigned long long ticks)
{
    int hz;

    hz = sysconf(_SC_CLK_TCK);
    tspec->tv_sec = ticks/hz;
    tspec->tv_nsec = (((double)ticks/hz) - tspec->tv_sec)*1000000000;
}


/** Calculate the cputime from utime and stime
 *
 * This really just adds two struct timespec values together.
 */
static struct timespec
calc_cputime(const struct timespec utime, const struct timespec stime)
{
    struct timespec cputime;

    cputime.tv_sec = utime.tv_sec + stime.tv_sec;
    cputime.tv_nsec = (utime.tv_nsec + stime.tv_nsec) % 1000000000;
    cputime.tv_sec += (utime.tv_nsec + stime.tv_nsec) / 1000000000;
    return cputime;
}



/** Calculate the start time of a proc in seconds since epoch
 *
 * @param starttime: The starttime in jiffies since system start.
 */
static int
calc_start_time(struct timespec *start_time, const unsigned long long starttime)
{
    struct timespec uptime;
    struct timespec idle;
    struct timespec now;
    struct timeval tv_now;
    
    int hz;

    hz = sysconf(_SC_CLK_TCK);
    if (psi_linux_uptime(&uptime, &idle) < 0)
        return -1;
    gettimeofday(&tv_now, NULL);
    if (&tv_now == NULL) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    now.tv_sec = tv_now.tv_sec;
    now.tv_nsec = tv_now.tv_usec * 1000;
    *start_time = posix_timespec_subtract(&now, &uptime);
    start_time->tv_sec += starttime/hz;
    start_time->tv_nsec += ((starttime % hz) * 1000000000)/hz;
    return 0;
}


/** Decode the state character from /proc/<pid>/stat or /proc/<pid>/status
 *
 * Returns an int with the correct PROC_STATUS_* constant in.
 */
static int
decode_state(const char state)
{
    int tmpi;

    switch (state) {
    case 'R':
        tmpi = PROC_STATUS_RUNNING;
        break;
    case 'S':
        tmpi = PROC_STATUS_SLEEPING;
        break;
    case 'D':
        tmpi = PROC_STATUS_DISKSLEEP;
        break;
    case 'Z':
        tmpi = PROC_STATUS_ZOMBIE;
        break;
    case 'T':
        tmpi = PROC_STATUS_STOPPED;
        break;
    case 'W':
        tmpi = PROC_STATUS_PAGING;
        break;
    case 'X':
        tmpi = PROC_STATUS_DEAD;
        break;
    default:
        PyErr_Format(PyExc_OSError, "Invalid/unknown state: %s", &state);
        return -1;
    }
    return tmpi;
}


/** Reads /prop/<pid>/stat files and fills relevant proc slots.
 *
 * Reads /proc/pid/stat files, being careful not to trip over processes with
 * names like ":-) 1 2 3 4 5 6".
 *
 * Note that the RSS value is not read from here.  For some reason it is not
 * correct here nor in /proc/<pid>/mstat, but it does seem correct in
 * /proc/<pid>/status so we get it from there.
 *
 * Returns 0 for success; -1 for failure (Exception raised).
 */
static int
parse_stat(struct psi_process *proci, const char *stat_buf, const pid_t pid)
{
    char *name_start, *name_end;
    int r;

    /* variables filled from the stat file by scanf */
    char state;
    int ppid;
    int pgrp;
    int sid;
    unsigned long utime;
    unsigned long stime;
    long priority;
    long nice;
    long num_threads;
    unsigned long long starttime;
    unsigned long vsize;
    long rss;

    /* The accounting name is surrounded by parenthesis. */
    name_start = strchr(stat_buf, '(');
    name_end = strrchr(stat_buf, ')');

    /* Copy the name and strip off the parenthesis. */
    proci->name = psi_strndup(name_start + 1, name_end - name_start - 1);
    if (proci->name == NULL)
        return -1;
    proci->name_status = PSI_STATUS_OK;

    /* Note that disabled specs don't have the proper length modifier since
     * that is illegal.  Check proc(5) if you want to enable one of them. */
    r = sscanf(name_end + 1,
               " %c"            /* state */
               " %d"            /* ppid */
               " %d"            /* pgrp */
               " %d"            /* session */
               " %*d"           /* tty_nr */
               " %*d"           /* tpgid */
               " %*u"           /* flags */
               " %*u"           /* minflt */
               " %*u"           /* cminflt */
               " %*u"           /* majflt */
               " %*u"           /* cmajflt */
               " %lu"           /* utime */
               " %lu"           /* stime */
               " %*u"           /* cutime */
               " %*d"           /* cstime */
               " %ld"           /* priority */
               " %ld"           /* nice */
               " %ld"           /* num_threads */
               " %*d"           /* itrealvalue (not maintained) */
               " %llu"          /* starttime */
               " %lu"           /* vsize */
               " %ld"           /* rss */
               " %*u"           /* rsslim */
               " %*u"           /* startcode */
               " %*u"           /* endcode */
               " %*u"           /* startstack */
               " %*u"           /* kstkesp */
               " %*u"           /* kstkeip */
               " %*u"           /* signal (obsolete) */
               " %*u"           /* blocked (obsolete) */
               " %*u"           /* sigignore (obsolete) */
               " %*u"           /* sigcatch (obsolete) */
               " %*u"           /* wchan */
               " %*u"           /* nswap (not maintained) */
               " %*u"           /* cnswap (not maintained) */
               " %*d"           /* exit_signal (since 2.1.22) */
               " %*d"           /* processor (since 2.2.8) */
#ifdef LINUX2_6
               " %*u"           /* rt_priority (since 2.5.19) */
               " %*u"           /* policy (since 2.5.19) */
#if LINUX2_6_MINOR >= 18
               " %*u"           /* delayacct_blkio_ticks (since 2.6.18) */
#endif
#if LINUX2_6_MINOR >= 24
               " %*u"           /* guest_time (since 2.6.24) */
               " %*d"           /* cguest_time (since 2.6.24) */
#endif
#endif /* #ifdef LINUX2_6 */
               ,
               &state,
               &ppid,
               &pgrp,
               &sid,
               &utime,
               &stime,
               &priority,
               &nice,
               &num_threads,
               &starttime,
               &vsize,
               &rss);
    if (r != 12) {
        /* XXX I see this happening sometimes, but hard to reproduce.  Maybe
         *     this happens if the process disappears while the stat file is
         *     being read, resulting in incomplete data? */
        PyErr_Format(PyExc_OSError,
                     "Failed to parse stat file for pid %d", pid);
        return -1;
    }
    proci->status = decode_state(state);
    proci->status_status = PSI_STATUS_OK;
    proci->ppid = (pid_t)ppid;
    proci->ppid_status = PSI_STATUS_OK;
    proci->pgrp = (pid_t)pgrp;
    proci->pgrp_status = PSI_STATUS_OK;
    proci->sid = (pid_t)sid;
    proci->sid_status = PSI_STATUS_OK;
    ticks2timespec(&proci->utime, utime);
    proci->utime_status = PSI_STATUS_OK;
    ticks2timespec(&proci->stime, stime);
    proci->stime_status = PSI_STATUS_OK;
    proci->cputime = calc_cputime(proci->utime, proci->stime);
    proci->cputime_status = PSI_STATUS_OK;
    if (priority < 0)
        proci->priority = -priority - 1;
    else
        proci->priority = 0;
    proci->priority_status = PSI_STATUS_OK;
    proci->nice = nice;
    proci->nice_status = PSI_STATUS_OK;
    proci->nthreads = num_threads;
    proci->nthreads_status = PSI_STATUS_OK;
    if (calc_start_time(&proci->start_time, starttime) < 0)
        return -1;
    proci->start_time_status = PSI_STATUS_OK;
    proci->jiffies = starttime;
    proci->jiffies_status = PSI_STATUS_OK;
    proci->vsz = vsize;
    proci->vsz_status = PSI_STATUS_OK;
    proci->rss = rss * sysconf(_SC_PAGESIZE);
    proci->rss_status = PSI_STATUS_OK;
    return 0;
}


static int
parse_uid_from_proc_status(struct psi_process *proci, const char *line)
{
    int ruid, euid, suid, fsuid;
    int r;

    r = sscanf(line, "%d%d%d%d", &ruid, &euid, &suid, &fsuid);
    if (r == 4) {
        proci->ruid = ruid;
        proci->ruid_status = PSI_STATUS_OK;
        proci->euid = euid;
        proci->euid_status = PSI_STATUS_OK;
        return 0;
    }
    else {
        PyErr_SetString(PyExc_OSError, "Failed to parse UID");
        return -1;
    }
}


static int
parse_gid_from_proc_status(struct psi_process *proci, const char *line)
{
    int rgid, egid, sgid, fsgid;
    int r;

    r = sscanf(line, "%d%d%d%d", &rgid, &egid, &sgid, &fsgid);
    if (r == 4) {
        proci->rgid = rgid;
        proci->rgid_status = PSI_STATUS_OK;
        proci->egid = egid;
        proci->egid_status = PSI_STATUS_OK;
        return 0;
    }
    else {
        PyErr_SetString(PyExc_OSError, "Failed to parse GID");
        return -1;
    }
}
