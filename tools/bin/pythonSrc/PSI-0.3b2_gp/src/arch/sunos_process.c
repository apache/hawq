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

/* SunOS implementation of the Process classes */


#include <Python.h>


/* We do not want to use the large file compilation environment as <procfs.h>
 * doesn't play nice with that on ILP32 (32-bit machines), but <Python.h>, via
 * pyconfig.h, sets the environment up for this so we need to reverse that.
 * The transitional large file compilation environment (where the function
 * sources are xxx64() instead and no mapping of the xxx symbols to xxx64
 * symbols happens) is fine, so we leave _LARGEFILE64_SOURCE set.  See the
 * lfcompile(5) and lfcompile64(5) manpages for notes on mixing objects from
 * different compilation environments: basically just don't share global
 * variables across them. */
#undef _FILE_OFFSET_BITS

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <procfs.h>
#include <signal.h>
#include <string.h>
#include <sys/proc.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if SUNOS5_MINOR >= 10
#include <zone.h>
#endif

#include "psi.h"
#include "process.h"
#include "procfs_utils.h"
#include "posix_utils.h"


/***** Local declarations *****/
static int parse_psinfo(struct psi_process *proci,
                        const pid_t pid,
                        psinfo_t **psinfop);
static int parse_as(struct psi_process *proci,
                    const pid_t pid,
                    psinfo_t *psinfo);
static int parse_usage(struct psi_process *proci, const pid_t pid);
static double calc_pcpu(ushort_t pr_pctcpu);
#if SUNOS5_MINOR >= 10
static int set_cwd(struct psi_process *proci, const pid_t pid);
static char *get_terminal(dev_t ttydev);
static char *get_zonename(zoneid_t zoneid);
#endif

static int set_args_from_as(struct psi_process *proci,
                            const struct psinfo *psinfo,
                            const int asfd);
static int set_args_from_argv(struct psi_process *proci,
                              const off_t *argv,
                              const psinfo_t *psinfo,
                              const int asfd);
static int set_env_from_as(struct psi_process *proci,
                           const struct psinfo *psinfo,
                           const int asfd);
static int set_envv_from_envp(struct psi_process *proci,
                              const int envc,
                              const off_t *envp,
                              const psinfo_t *psinfo,
                              const int asfd);
static int set_exe(struct psi_process *proci,
                   const pid_t pid,
                   const psinfo_t *psinfo);
static int set_args_env_from_fork(struct psi_process *proci,
                                  const psinfo_t *psinfo);
static char *find_sargs64(void);
static int read_sargs64_pipe(struct psi_process *proci,
                             const psinfo_t *psinfo,
                             const int pipe);


/***** Public interfaces from process.h. *****/


/* The process status flags. */
#define addflag(CONST) {"PROC_STATUS_"#CONST, CONST}
struct psi_flag psi_arch_proc_status_flags[] = {
    addflag(SIDL),
    addflag(SRUN),
    addflag(SSLEEP),
    addflag(SSTOP),
    addflag(SZOMB),
#ifdef SONPROC
    addflag(SONPROC),
#endif
#ifdef SWAIT
    addflag(SWAIT),
#endif
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
    psinfo_t *psinfo;

    if (procfs_check_pid(pid) < 0)
        return NULL;
    proci = psi_calloc(sizeof(struct psi_process));
    if (proci == NULL)
        return NULL;
    if (parse_psinfo(proci, pid, &psinfo) < 0) {
        psi_FREE(psinfo);
        return psi_free_process(proci);
    }
    if (set_exe(proci, pid, psinfo) < 0) {
        psi_free(psinfo);
        return psi_free_process(proci);
    }
    if (parse_as(proci, pid, psinfo) < 0) {
        psi_free(psinfo);
        return psi_free_process(proci);
    }
    psi_free(psinfo);
    if (parse_usage(proci, pid) < 0)
        return psi_free_process(proci);
#if SUNOS5_MINOR >= 10
    if (set_cwd(proci, pid) < 0)
        return psi_free_process(proci);
#endif
    return proci;
}


/***** Local functions *****/


/** Parse information from /proc/<pid>/psinfo
 *
 * The `psinfop' argument will point to the `psinfo_t' structure or NULL.  The
 * caller *has* to call psi_free on it if it is non NULL.  This means when an
 * error arises in this function this function itself *should not* free the
 * `psinfo' structure.
 */
static int
parse_psinfo(struct psi_process *proci, const pid_t pid, psinfo_t **psinfop)
{
    psinfo_t *psinfo;
    int bufsize;

    bufsize = procfs_read_procfile((char**)&psinfo, pid, "psinfo");
    *psinfop = psinfo;
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->name_status = PSI_STATUS_PRIVS;
        proci->argc_status = PSI_STATUS_PRIVS;
        proci->command_status = PSI_STATUS_PRIVS;
        proci->nthreads_status = PSI_STATUS_PRIVS;
        proci->ppid_status = PSI_STATUS_PRIVS;
        proci->pgrp_status = PSI_STATUS_PRIVS;
        proci->sid_status = PSI_STATUS_PRIVS;
        proci->ruid_status = PSI_STATUS_PRIVS;
        proci->euid_status = PSI_STATUS_PRIVS;
        proci->rgid_status = PSI_STATUS_PRIVS;
        proci->egid_status = PSI_STATUS_PRIVS;
        proci->euser_status = PSI_STATUS_PRIVS;
        proci->ruser_status = PSI_STATUS_PRIVS;
        proci->egroup_status = PSI_STATUS_PRIVS;
        proci->rgroup_status = PSI_STATUS_PRIVS;
        proci->vsz_status = PSI_STATUS_PRIVS;
        proci->rss_status = PSI_STATUS_PRIVS;
        proci->terminal_status = PSI_STATUS_PRIVS;
        proci->pcpu_status = PSI_STATUS_PRIVS;
        proci->start_time_status = PSI_STATUS_PRIVS;
        proci->cputime_status = PSI_STATUS_PRIVS;
        proci->exe_status = PSI_STATUS_PRIVS;
        proci->status_status = PSI_STATUS_PRIVS;
        proci->nice_status = PSI_STATUS_PRIVS;
        proci->priority_status = PSI_STATUS_PRIVS;
        proci->zoneid_status = PSI_STATUS_PRIVS;
        proci->zonename_status = PSI_STATUS_PRIVS;
        return 0;
    }
    if (bufsize != sizeof(psinfo_t)) {
        PyErr_Format(PyExc_OSError,
                     "Unexpected psinfo file size: %d instead of %d",
                     bufsize, sizeof(psinfo_t));
        return -1;
    }
    proci->name = (char *)psi_malloc(PRFNSZ); /* XXX or strlen()? */
    if (proci->name == NULL)
        return -1;
    strcpy(proci->name, psinfo->pr_fname);
    proci->name_status = PSI_STATUS_OK;
    proci->argc = psinfo->pr_argc;
    proci->argc_status = PSI_STATUS_OK;
    proci->command = psi_strdup(psinfo->pr_psargs);
    if (proci->command == NULL)
        return -1;
    proci->command_status = PSI_STATUS_OK;
#if SUNOS5_MINOR >= 10
    proci->nthreads = psinfo->pr_nlwp + psinfo->pr_nzomb;
#else
    proci->nthreads = psinfo->pr_nlwp;
#endif
    proci->nthreads_status = PSI_STATUS_OK;
    proci->ppid = psinfo->pr_ppid;
    proci->ppid_status = PSI_STATUS_OK;
    proci->pgrp = psinfo->pr_pgid;
    proci->pgrp_status = PSI_STATUS_OK;
    proci->sid = psinfo->pr_sid;
    proci->sid_status = PSI_STATUS_OK;
    proci->ruid = psinfo->pr_uid;
    proci->ruid_status = PSI_STATUS_OK;
    proci->euid = psinfo->pr_euid;
    proci->euid_status = PSI_STATUS_OK;
    proci->rgid = psinfo->pr_gid;
    proci->rgid_status = PSI_STATUS_OK;
    proci->egid = psinfo->pr_egid;
    proci->egid_status = PSI_STATUS_OK;
    /* XXX rss & vsz are size_t on SunOS! */
    proci->vsz = psinfo->pr_size * 1024;
    proci->vsz_status = PSI_STATUS_OK;
    proci->rss = (psinfo->pr_rssize) * 1024;
    proci->rss_status = PSI_STATUS_OK;
#if SUNOS5_MINOR >= 10
    proci->terminal = get_terminal(psinfo->pr_ttydev);
    if (proci->terminal == NULL && PyErr_Occurred() != NULL)
        return -1;
    proci->terminal_status = PSI_STATUS_OK;
#endif
    proci->pcpu = calc_pcpu(psinfo->pr_pctcpu);
    proci->pcpu_status = PSI_STATUS_OK;
    proci->start_time.tv_sec = psinfo->pr_start.tv_sec;
    proci->start_time.tv_nsec = psinfo->pr_start.tv_nsec;
    proci->start_time_status = PSI_STATUS_OK;
    proci->cputime.tv_sec = psinfo->pr_time.tv_sec;
    proci->cputime.tv_nsec = psinfo->pr_time.tv_nsec;
    proci->cputime_status = PSI_STATUS_OK;
    proci->status = psinfo->pr_lwp.pr_state;
    proci->status_status = PSI_STATUS_OK;
    proci->nice = psinfo->pr_lwp.pr_nice;
    proci->nice_status = PSI_STATUS_OK;
    proci->priority = psinfo->pr_lwp.pr_pri;
    proci->priority_status = PSI_STATUS_OK;
#if SUNOS5_MINOR >= 10
    proci->zoneid = psinfo->pr_zoneid;
    proci->zoneid_status = PSI_STATUS_OK;
    proci->zonename = get_zonename(proci->zoneid);
    if (proci->zonename == NULL)
        return -1;
    proci->zonename_status = PSI_STATUS_OK;
#endif
    *psinfop = psinfo;
    return 0;
}


/** Parse the process address space: /proc/<pid>/as
 *
 * The argv, envc and envv slots are filled in from this.
 *
 * Care should be taken here as this is the address space image of the process
 * it is not coverted to the data model of us (normally ILP32) like the rest
 * of /proc, so it could be either ILP32 or LP64.
 */
static int
parse_as(struct psi_process *proci, const pid_t pid, psinfo_t *psinfo)
{
    char *path;
    int r;
    int asfd;

    if (psinfo->pr_dmodel != PR_MODEL_ILP32 &&
        psinfo->pr_dmodel != PR_MODEL_LP64) {
        /* Kernel threads/processes/LWPs. */
        proci->argv_status = PSI_STATUS_NA;
        proci->envc_status = PSI_STATUS_NA;
        proci->envv_status = PSI_STATUS_NA;
        return 0;
    }
#ifdef _ILP32
    if (psinfo->pr_dmodel == PR_MODEL_LP64)
        return set_args_env_from_fork(proci, psinfo);
#endif
    r = psi_asprintf(&path, "/proc/%d/as", pid);
    if (r < 0)
        return -1;
    asfd = open(path, O_RDONLY);
    if (asfd < 0) {
        if (errno == EACCES || errno == EPERM) {
            psi_free(path);
            proci->argv_status = PSI_STATUS_PRIVS;
            proci->envc_status = PSI_STATUS_PRIVS;
            proci->envv_status = PSI_STATUS_PRIVS;
            return 0;
        } else if (errno == ENOENT) {
            PyErr_SetFromErrnoWithFilename(PsiExc_NoSuchProcessError, path);
            psi_free(path);
            return -1;
        } else {
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
            psi_free(path);
            return -1;
        }
    }
    psi_free(path);
    if (set_args_from_as(proci, psinfo, asfd) < 0) {
        close(asfd);
        return -1;
    }
    if (set_env_from_as(proci, psinfo, asfd) < 0) {
        close(asfd);
        return -1;
    }
    close(asfd);
    return 0;
}


static int
parse_usage(struct psi_process *proci, const pid_t pid)
{
    prusage_t *prusage;
    int bufsize;

    bufsize = procfs_read_procfile((char**)&prusage, pid, "usage");
    if (bufsize == -1)
        return -1;
    if (bufsize == -2) {
        proci->utime_status = PSI_STATUS_PRIVS;
        proci->stime_status = PSI_STATUS_PRIVS;
        PyErr_Clear();
        return 0;
    }
    if (bufsize != sizeof(prusage_t)) {
        PyErr_Format(PyExc_OSError,
                     "Unexpected status file size: %d instead of %d",
                     bufsize, sizeof(prusage_t));
        psi_free(prusage);
        return -1;
    }
    proci->utime.tv_sec = prusage->pr_utime.tv_sec;
    proci->utime.tv_nsec = prusage->pr_utime.tv_nsec;
    proci->utime_status = PSI_STATUS_OK;
    proci->stime.tv_sec = prusage->pr_stime.tv_sec;
    proci->stime.tv_nsec = prusage->pr_stime.tv_nsec;
    proci->stime_status = PSI_STATUS_OK;
    psi_free(prusage);
    return 0;
}


#if SUNOS5_MINOR >= 10
static int
set_cwd(struct psi_process *proci, const pid_t pid)
{
    char *path;
    char *link;
    int r;

    r = psi_asprintf(&path, "/proc/%d/path/cwd", pid);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, path);
    psi_free(path);
    if (r == -2) {
        PyErr_Clear();
        proci->cwd_status = PSI_STATUS_PRIVS;
    } else if (r < 0) {
        procfs_check_pid(pid); /* OSError -> NoSuchProcessError if required */
        return -1;
    } else {
        proci->cwd = psi_strdup(link);
        psi_free(link);
        proci->cwd_status = PSI_STATUS_OK;
    }
    return 0;
}
#endif


#if SUNOS5_MINOR >= 10
/* XXX For other solaris versions consider searching though /dev until the
 *     device is found like is done on AIX. */
/** Find the terminal associated with ttydev
 *
 * This is derrived from solaris ps:
 * http://cvs.opensolaris.org/source/xref/onnv/onnv-gate/usr/src/cmd/ps/ps.c
 * Unfortunately this only seems to work for SUNOS 5.10 and higher.  Also
 * _ttyname_dev() is probably an internal function, it appears in the symbol
 * table of libc.so.1 however.
 *
 * The name of the terminal is returned or NULL.  NULL is not always an error,
 * it can just be there is no terminal (in which case python will see `None'
 * so check with PyErr_Occurred().
 */
extern char *_ttyname_dev(dev_t, char *, size_t);

static char *
get_terminal(dev_t ttydev)
{
    char devname[TTYNAME_MAX];
    char *rv;
    int l;

    if (ttydev == PRNODEV)
        return NULL;
    rv = _ttyname_dev(ttydev, devname, sizeof(devname));
    if (rv == NULL)
        return NULL;
    l = strlen(devname);
    rv = (char *)psi_malloc(l+1);
    if (rv == NULL)
        return NULL;
    return strncpy(rv, devname, l+1);
}
#endif

static double
calc_pcpu(ushort_t pr_pctcpu)
{
    uint_t value = pr_pctcpu;

    value = ((value*1000) + 0x7000) >> 15;
    if (value >= 1000)
        value  = 999;
    return (double)value/10.0;
}


#if SUNOS5_MINOR >= 10
static char *
get_zonename(zoneid_t zoneid)
{
    char zonename[ZONENAME_MAX];
    char *name;
    size_t reqsize;

    reqsize = getzonenamebyid(zoneid, zonename, ZONENAME_MAX);
    if (reqsize < 0)
        return (char*)PyErr_SetFromErrno(PyExc_OSError);
    name = psi_malloc(reqsize);
    if (name == NULL)
        return NULL;
    strncpy(name, zonename, reqsize);
    return name;
}
#endif


/** Read bits in the process address space to extract `argv'
 *
 * This depends on the internal representation of the argv array: the base
 * address of the entire array is pointed too by psinfo.pr_argv and this can't
 * change, meaning that psinfo.pr_argc denotes the number of elements in the
 * array.  From this it is possible to create our own version of argv in the
 * psi_process structure.
 *
 * Note that this function will never be called if we're a 32-bit Python
 * looking at a 64-bit process so no need to detect this.  This means off_t is
 * always a valid way of pointing to an address in the address space.
 */
static int
set_args_from_as(struct psi_process *proci,
                 const struct psinfo *psinfo,
                 const int asfd)
{
    off_t argvoff = (off_t)psinfo->pr_argv;
    off_t *argv;                /* not caddr_t due to casting issues */
    caddr32_t *argv32;
    ssize_t r;
    int i;

    /* First read in the array of pointers, this depends on the data model. */
    if (psinfo->pr_dmodel == PR_MODEL_ILP32) {
        argv32 = (caddr32_t*)psi_malloc(psinfo->pr_argc*sizeof(caddr32_t));
        if (argv32 == NULL)
            return -1;
        r = pread(asfd, argv32, psinfo->pr_argc*sizeof(caddr32_t), argvoff);
        if (r < 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            psi_free(argv32);
            return -1;
        }
        argv = (off_t*)psi_malloc(psinfo->pr_argc*sizeof(off_t));
        if (argv == NULL) {
            psi_free(argv32);
            return -1;
        }
        for (i = 0; i < psinfo->pr_argc; i++)
            argv[i] = (off_t)argv32[i];
        psi_free(argv32);
    } else {                    /* LP64 */
        argv = (off_t*)psi_malloc(psinfo->pr_argc*sizeof(off_t));
        if (argv == NULL)
            return -1;
        r = pread(asfd, argv, psinfo->pr_argc*sizeof(off_t), argvoff);
        if (r < 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            psi_free(argv);
            return -1;
        }
    }

    /* Now read each arg from the address space and place it into proci. */
    i = set_args_from_argv(proci, argv, psinfo, asfd);
    psi_free(argv);
    if (i < 0)
        return -1;
    return 0;
}


/** Read the arguments from the address space pointed to by argv
 *
 * The pointers of where to read from in the address space (pointed to by the
 * `asfd' file descriptor) are read from `argv' (which is of lenght
 * `psinfo->pr_argc').  The result is stored in `proci->argv'.
 *
 * Note that proci->argv will get freed by psi_arch_process() in case of
 * failure, so as soon as we assign an arch to it we don't have to worry about
 * freeing it anymore in case of an error.
 */
static int
set_args_from_argv(struct psi_process *proci,
                   const off_t *argv,
                   const psinfo_t *psinfo,
                   const int asfd)
{
    off_t argoff;
    char *arg;
    char *ptr;
    int argsize;
    ssize_t r;
    int i;

    proci->argv = (char**)psi_calloc(psinfo->pr_argc*sizeof(char*));
    if (proci->argv == NULL)
        return -1;
    for (i = 0; i  < psinfo->pr_argc; i++) {
        argoff = argv[i];
        argsize = 50;
        arg = (char*)psi_malloc(argsize);
        if (arg == NULL)
            return -1;
        ptr = arg;
        *ptr = 'a';
        while (*ptr != '\0') {  /* read and grow buffer if required */
            proci->argv[i] = arg;
            r = pread(asfd, arg, argsize, argoff);
            if (r < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            for (ptr = arg; ptr-arg < argsize-1; ptr++)
                if (*ptr == '\0')
                    break;
            if (*ptr != '\0') {
                argsize += 50;
                ptr = (char*)psi_realloc(arg, argsize);
                if (ptr == NULL)
                    return -1;
                arg = ptr;
            }
        }
    }
    proci->argv_status = PSI_STATUS_OK;
    return 0;
}



/** Read bits in the process address space to extract `envp'
 *
 * This depends on the internal representation of the envp array: the base
 * address of the entire array is pointed too by psinfo.pr_envp and POSIX
 * defines that it is NULL terminated.
 *
 * Note that this function will never be called if we're a 32-bit Python
 * looking at a 64-bit process so no need to detect this.  This means off_t is
 * always a valid way of pointing to an address in the address space.
 *
 * XXX This is too much code for one function.
 */
static int
set_env_from_as(struct psi_process *proci,
                const struct psinfo *psinfo,
                const int asfd)
{
    off_t envpoff = (off_t)psinfo->pr_envp;
    off_t *envp;
    off_t *ptr;
    caddr32_t *envp32;
    caddr32_t *ptr32;
    ssize_t r;
    int array_size = 50;
    int envc = 0;
    int i;


    /* First read in the array of pointers, this depends on the data model. */
    if (psinfo->pr_dmodel == PR_MODEL_ILP32) {
        envp32 = (caddr32_t*)psi_malloc(array_size*sizeof(caddr32_t));
        if (envp32 == NULL)
            return -1;
        do {
            r = pread(asfd, envp32, array_size*sizeof(caddr32_t), envpoff);
            if (r < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                psi_free(envp32);
                return -1;
            }
            ptr32 = envp32;
            for (i = 0; i < array_size; i++) {
                ptr32++;
                if (*ptr32 == (caddr32_t)NULL) {
                    envc = i + 1;
                    break;
                }
            }
            if (*ptr32 != (caddr32_t)NULL) { /* array was too small */
                array_size += 50;
                ptr32 = (caddr32_t*)psi_realloc(envp32,
                                                array_size*sizeof(caddr32_t));
                if (ptr32 == NULL) {
                    psi_free(envp32);
                    return -1;
                }
                envp32 = ptr32;
            }
        } while (*ptr32 != (caddr32_t)NULL);
        envp = (off_t*)psi_malloc(envc*sizeof(off_t));
        if (envp == NULL) {
            psi_free(envp32);
            return -1;
        }
        for (i = 0; i < envc; i++)
            envp[i] = (off_t)envp32[i];
        psi_free(envp32);
    } else {                    /* LP64 */
        envp = (off_t*)psi_malloc(array_size*sizeof(off_t));
        if (envp == NULL)
            return -1;
        do {
            r = pread(asfd, envp, array_size*sizeof(off_t), envpoff);
            if (r < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                psi_free(envp);
                return -1;
            }
            ptr = envp;
            for (i = 0; i < array_size; i++) {
                ptr++;
                if (*ptr == (off_t)NULL) {
                    envc = i + 1;
                    break;
                }
            }
            if (*ptr != (off_t)NULL) { /* array was too small */
                array_size += 50;
                ptr = (off_t*)psi_realloc(envp, array_size*sizeof(off_t));
                if (ptr == NULL) {
                    psi_free(envp);
                    return -1;
                }
                envp = ptr;
            }
        } while (*ptr != (off_t)NULL);
    }

    /* Now we copy the environment vector into the psi_process structure. */
    i = set_envv_from_envp(proci, envc, envp, psinfo, asfd);
    psi_free(envp);
    if (i < 0)
        return -1;
    proci->envc = envc;
    proci->envc_status = PSI_STATUS_OK;
    return 0;
}


static int
set_envv_from_envp(struct psi_process *proci,
                    const int envc,
                    const off_t *envp,
                    const psinfo_t *psinfo,
                    const int asfd)
{
    off_t envoff;
    char *env;
    char *ptr;
    ssize_t r;
    int envsize;
    int i;

    proci->envv = (char**)psi_calloc(envc*sizeof(char*));
    if (proci->envv == NULL)
        return -1;
    for (i = 0; i  < envc; i++) {
        envoff = envp[i];
        envsize = 50;
        env = (char*)psi_malloc(envsize);
        if (env == NULL)
            return -1;
        ptr = env;
        *ptr = 'a';
        while (*ptr != '\0') {  /* read and grow buffer if required */
            proci->envv[i] = env;
            r = pread(asfd, env, envsize, envoff);
            if (r < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            for (ptr = env; ptr-env < envsize; ptr++)
                if (*ptr == '\0')
                    break;
            if (*ptr != '\0') {
                envsize += 50;
                ptr = (char*)psi_realloc(env, envsize);
                if (ptr == NULL)
                    return -1;
                env = ptr;
            }
        }
    }
    proci->envv_status = PSI_STATUS_OK;
    return 0;
}


/** Set the exe and exe_status fields of the psi_process structure
 *
 * Returns -1 on failure, 0 on success.
 */
static int
set_exe(struct psi_process *proci, const pid_t pid, const psinfo_t *psinfo)
{
#if SUNOS5_MINOR >= 10
    char *fname;
    char *link;
    int r;

    r = psi_asprintf(&fname, "/proc/%d/path/a.out", pid);
    if (r == -1)
        return -1;
    r = psi_readlink(&link, fname);
    psi_free(fname);
    if (r < 0) {
	PyErr_Clear();
#endif
        proci->exe = (char *)psi_malloc(PRFNSZ); /* XXX or strlen()? */
        if (proci->exe == NULL)
            return -1;
        strcpy(proci->exe, psinfo->pr_fname);
        proci->exe_status = PSI_STATUS_OK;
#if SUNOS5_MINOR >= 10
    } else {
        proci->exe = link;
        proci->exe_status = PSI_STATUS_OK;
    }
#endif
    return 0;
}


/** Set argv, envc and envv by forking to 64-bit application
 *
 * This function gets called when we're a 32-bit Python trying to read the
 * arguments and environment arrays from a 64-bit application.  We can't do
 * that ourself so we have to call sargs64.so which is a 64-bit helper
 * program.
 *
 * sargs64.so must get the PID of the process as an argument and will write
 * the results on it's standard output.  The data format of result is:
 * "arg0\0arg1\0...\0\0env0\0env1\0...\0\0".  So you get a NULL-terminated
 * array for the argument list, followed by a NULL terminated array of the
 * environment.  Once all information is sent sargs64.so will close the
 * filedescriptor.  Unless sargs64.so return 0 the output should not be
 * trusted.  If sargs64.so returns 2 then there was a permissions problem.
 *
 * Implementation notes
 *
 * The pipes[0] is read from in this process, pipes[1] gets written to in
 * sargs64.so.
 *
 * We use fork1() (Solaris specific, not POSIX) since Solaris < 10 is not
 * POSIX compliant and fork() there is an alias for forkall() which copies all
 * the threads.  And we don't want any threads in the child.
 *
 * XXX This function is too large.
 */
static int
set_args_env_from_fork(struct psi_process *proci, const psinfo_t *psinfo)
{
    char *pid_str;
    char *sargs64;
    int pipes[2];
    int child_stat;
    pid_t child_pid;
    int rv;

    sargs64 = find_sargs64();
    if (sargs64 == NULL)
        return -1;
    if (psi_asprintf(&pid_str, "%ld", (long)psinfo->pr_pid) < 0) {
        psi_free(sargs64);
        return -1;
    }
    if (pipe(pipes) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        psi_free(sargs64);
        psi_free(pid_str);
        return -1;
    }
    child_pid = fork1();
    if (child_pid == -1) {               /* error */
        PyErr_SetFromErrno(PyExc_OSError);
        psi_free(sargs64);
        psi_free(pid_str);
        return -1;
    } else if (child_pid == 0) {        /* child */
        if (close(pipes[0]) == -1)
            exit(1);
        if (dup2(pipes[1], 1) == -1)
            exit(1);
        if (close(pipes[1]) == -1)
            exit(1);
        execl(sargs64, "sargs64.so", pid_str, NULL);
        exit(1);
    } else {                    /* parent */
        psi_free(sargs64);
        psi_free(pid_str);
        if (close(pipes[1]) == -1) {
            PyErr_SetFromErrno(PyExc_OSError);
            close(pipes[0]);
            return -1;
        }
        rv = read_sargs64_pipe(proci, psinfo, pipes[0]);
        if (close(pipes[0]) == -1) {
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
        if (waitpid(child_pid, &child_stat, 0) == -1) {
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
        if (!WIFEXITED(child_stat)) {
            /* Someone changed the status of our child, clean it up. */
            while (!WIFSIGNALED(child_stat) || !WIFEXITED(child_stat)) {
                if (kill(child_pid, SIGTERM) == -1) {
                    PyErr_SetFromErrno(PyExc_OSError);
                    return -1;
                }
                if (WIFSTOPPED(child_stat)) {
                    if (kill(child_pid, SIGCONT) == -1) {
                        PyErr_SetFromErrno(PyExc_OSError);
                        return -1;
                    }
                }
                if (waitpid(child_pid, &child_stat, 0) == -1) {
                    PyErr_SetFromErrno(PyExc_OSError);
                    return -1;
                }
            }
            PyErr_SetString(PyExc_OSError, "sargs64.so did not exit normally");
            return -1;
        }
        if (WEXITSTATUS(child_stat) == 2) {
            proci->argv_status = PSI_STATUS_PRIVS;
            proci->envc_status = PSI_STATUS_PRIVS;
            proci->envv_status = PSI_STATUS_PRIVS;
            return 0;
        } else if (WEXITSTATUS(child_stat) != 0) {
            PyErr_SetString(PyExc_OSError, "sargs64.so failed");
            procfs_check_pid(psinfo->pr_pid == 0);
            return -1;
        } else
            return rv;
    }
}


/** Return the path of sargs64.so
 *
 * On success the path of the sargs64.so executable is returned.  Returns NULL
 * on failure.  The returned value must be freed using psi_free().
 */
static char *
find_sargs64(void)
{
    PyObject *mod;
    char *sargs64;
    char *file;
    char *p;
    int l;

    mod = PyImport_ImportModuleNoBlock("psi.process");
    if (mod == NULL)
        return NULL;
    file = PyModule_GetFilename(mod);
    Py_DECREF(mod);
    if (file == NULL)
        return NULL;
    l = strlen(file);
    sargs64 = (char *)psi_malloc(l + 11);
    strncpy(sargs64, file, l+1);
    p = strrchr(sargs64, '/') + 1;
    strcpy(p, "sargs64.so");
    p += 10;
    *p = '\0';
    return sargs64;
}


/** Read the data from sargs64.so via the pipe
 *
 * The format is described above in the set_args_env_from_fork() function.
 * This sets the argv, argv_status, envv and envv_status slots in the procinfo
 * structure if successful.  If incomplete data is read an exception is set
 * and -1 is returned.
 */
static int
read_sargs64_pipe(struct psi_process *proci,
                  const psinfo_t *psinfo,
                  const int pipe)
{
    char *buf, *ptr, *envbuf;
    ssize_t bytes;
    int bufsize = 100;

    /* Read buffer from pipe. */
    buf = (char*)psi_malloc(bufsize + 2);
    if (buf == NULL)
        return -1;
    ptr = buf;
    while ((bytes = read(pipe, ptr, 100)))
        if (bytes == 100) {
            bufsize += 100;
            ptr = psi_realloc(buf, bufsize + 2);
            if (ptr == NULL) {
                psi_free(buf);
                return -1;
            }
            buf = ptr;
            ptr = buf + bufsize - 100;
        }
    buf[bufsize-1] = '\0';        /* Ensure buffer ends in '\0\0' */
    buf[bufsize] = '\0';

    /* Parse argv. */
    ptr = strchr(buf, '\0');
    while (*(ptr+1) != '\0') {
        ptr = strchr(ptr+1, '\0');
        if (ptr == NULL) {
            psi_free(buf);
            PyErr_SetString(PyExc_OSError, "Invalid data from sargs64.so");
            return -1;
        }
    }
    proci->argc = psi_strings_count(buf, ptr-buf+1);
    proci->argv = psi_strings_to_array(buf, proci->argc);
    proci->argc_status = PSI_STATUS_OK;
    proci->argv_status = PSI_STATUS_OK;

    /* Parse envv. */
    envbuf = ptr + 2;
    if (envbuf-buf > bufsize-2) {
        psi_free(buf);
        PyErr_SetString(PyExc_OSError, "Invalid data from sargs64.so");
        return -1;
    }
    ptr = strchr(envbuf, '\0');
    while (*(ptr+1) != '\0') {
        ptr = strchr(ptr+1, '\0');
        if (ptr == NULL) {
            psi_free(buf);
            PyErr_SetString(PyExc_OSError, "Invalid data from sargs64.so");
            return -1;
        }
    }
    proci->envc = psi_strings_count(envbuf, ptr-envbuf+1);
    proci->envv = psi_strings_to_array(envbuf, proci->envc);
    proci->envc_status = PSI_STATUS_OK;
    proci->envv_status = PSI_STATUS_OK;
    psi_free(buf);
    return 0;
}
