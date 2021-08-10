/* The MIT License
 *
 * Copyright (C) 2007 Chris Miles
 *
 * Copyright (C) 2009 Erick Tryzelaar
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

/* Process functions for arch: Mac OS X */

#include <Python.h>

#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/sysctl.h>
#include <sys/fcntl.h>
#include <pwd.h>
#include <grp.h>
#include <mach/shared_memory_server.h>
#include <mach/mach_init.h>
#include <mach/mach_interface.h>
#include <pwd.h>
#include <grp.h>
#include <sys/proc.h>

#include "psi.h"
#include "psifuncs.h"
#include "process.h"


/***** Local declarations *****/

static int get_kinfo_proc(const pid_t pid, struct kinfo_proc *p);
static int set_exe(struct psi_process *proci, struct kinfo_proc *p);
static int set_cwd(struct psi_process *proci, struct kinfo_proc *p);
static int set_kp_proc(struct psi_process *proci, struct kinfo_proc *p);
static int set_kp_eproc(struct psi_process *proci, struct kinfo_proc *p);
static int set_task(struct psi_process *proci, struct kinfo_proc *p);
static int command_from_argv(char **command, char **argv, int argc);
static struct timespec calc_cputime(const struct timespec utime,
                                    const struct timespec stime);


/***** Public interfaces from process.h. *****/

struct psi_flag psi_arch_proc_status_flags[] = {
    {"PROC_STATUS_SIDL", SIDL},
    {"PROC_STATUS_SRUN", SRUN},
    {"PROC_STATUS_SSLEEP", SSLEEP},
    {"PROC_STATUS_SSTOP", SSTOP},
    {"PROC_STATUS_SZOMB", SZOMB},
    {NULL, 0}};             /* sentinel */


struct psi_process *
psi_arch_process(const pid_t pid)
{
    struct kinfo_proc p;
    struct psi_process *proci;

    if (get_kinfo_proc(pid, &p) == -1) {
        return NULL;
    }

    proci = psi_calloc(sizeof(struct psi_process));
    if (proci == NULL) {
        return NULL;
    }

    if (set_exe(proci, &p) == -1) goto cleanup;
    if (set_cwd(proci, &p) == -1) goto cleanup;
    if (set_kp_proc(proci, &p) == -1) goto cleanup;
    if (set_kp_eproc(proci, &p) == -1) goto cleanup;
    if (set_task(proci, &p) == -1) goto cleanup;

    if (proci->utime_status == PSI_STATUS_PRIVS ||
                proci->stime_status == PSI_STATUS_PRIVS)
        proci->cputime_status = PSI_STATUS_PRIVS;
    else {
        proci->cputime = calc_cputime(proci->utime, proci->stime);
        proci->cputime_status = PSI_STATUS_OK;
    }

    if (proci->command_status == PSI_STATUS_PRIVS) {
        /* Ensure Process.command always has a value, as per our
         * contract with the user.
         */
        proci->command = psi_strdup("");
        proci->command_status = PSI_STATUS_OK;
    }
    
    return proci;

  cleanup:
    psi_free_process(proci);
    return NULL;
}


/***** Local functions *****/


static int
get_kinfo_proc(const pid_t pid, struct kinfo_proc *proc)
{
    int name[] = { CTL_KERN, KERN_PROC, KERN_PROC_PID, pid };
    size_t size;

    size = sizeof(struct kinfo_proc);

    /* We use sysctl here instead of psi_sysctl because we know the size of
     * the destination already so we don't need to malloc anything. */
    if (sysctl((int*)name, 4, proc, &size, NULL, 0) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    /* sysctl stores 0 in the size if we can't find the process information */
    if (size == 0) {
        PyErr_Format(PsiExc_NoSuchProcessError, "No such PID: %ld", (long)pid);
        return -1;
    }

    return 0;
}


/** Find the executable, argument list and environment
 *
 * This will also fill in the command attribute.
 *
 * The sysctl calls here don't use a wrapper as in darwin_prcesstable.c since
 * they know the size of the returned structure already.
 *
 * The layout of the raw argument space is documented in start.s, which is
 * part of the Csu project.  In summary, it looks like:
 *
 * XXX: This layout does not match whith what the code does.  The code seems
 * to think exec_path is in between the first argc and arg[0], also the data
 * returned by ssyctl() seems to be starting at the first argc according to
 * the code.
 *
 * /---------------\ 0x00000000
 * :               :
 * :               :
 * |---------------|
 * | argc          |
 * |---------------|
 * | arg[0]        |
 * |---------------|
 * :               :
 * :               :
 * |---------------|
 * | arg[argc - 1] |
 * |---------------|
 * | 0             |
 * |---------------|
 * | env[0]        |
 * |---------------|
 * :               :
 * :               :
 * |---------------|
 * | env[n]        |
 * |---------------|
 * | 0             |
 * |---------------| <-- Beginning of data returned by sysctl() is here.
 * | argc          |
 * |---------------|
 * | exec_path     |
 * |:::::::::::::::|
 * |               |
 * | String area.  |
 * |               |
 * |---------------| <-- Top of stack.
 * :               :
 * :               :
 * \---------------/ 0xffffffff
 */
static int
set_exe(struct psi_process *proci, struct kinfo_proc *p)
{
    int mib[3], argmax, nargs;
    size_t size;
    char *procargs, *cp;
    int i;
    int env_start = 0;

    /* Get the maximum process arguments size. */
    mib[0] = CTL_KERN;
    mib[1] = KERN_ARGMAX;
    size = sizeof(argmax);

    if (sysctl(mib, 2, &argmax, &size, NULL, 0) == -1) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, "sysctl() argmax");
        return -1;
    }

    /* Allocate space for the arguments. */
    procargs = (char *)psi_malloc(argmax);
    if (procargs == NULL)
        return -1;

    /* Make a sysctl() call to get the raw argument space of the process. */
    mib[0] = CTL_KERN;
    mib[1] = KERN_PROCARGS2;
    mib[2] = p->kp_proc.p_pid;

    size = (size_t)argmax;

    if (sysctl(mib, 3, procargs, &size, NULL, 0) == -1) {
        /* We failed to get the exe info, but it's not fatal. We probably just
         * didn't have permission to access the KERN_PROCARGS2 for this
         * process. */
        psi_free(procargs);
        proci->exe_status  = PSI_STATUS_PRIVS;
        proci->argc_status = PSI_STATUS_PRIVS;
        proci->argv_status = PSI_STATUS_PRIVS;
        proci->envc_status = PSI_STATUS_PRIVS;
        proci->envv_status = PSI_STATUS_PRIVS;
        proci->command_status = PSI_STATUS_PRIVS;
        return 0;
    }

    memcpy(&nargs, procargs, sizeof(nargs));
    cp = procargs + sizeof(nargs);

    /* Save the exe */
    proci->exe = psi_strdup(cp);
    if (proci->exe == NULL) {
        psi_free(procargs);
        return -1;
    }
    proci->exe_status = PSI_STATUS_OK;

    /* Skip over the exe. */
    cp += strlen(cp);
    if (cp == &procargs[size]) {
        psi_free(procargs);
        PyErr_SetString(PyExc_OSError, "Did not find args and env");
        return -1;
    }

    /* Skip trailing '\0' characters. */
    for (; cp < &procargs[size]; cp++) {
        if (*cp != '\0') {
            /* Beginning of first argument reached. */
            break;
        }
    }
    if (cp == &procargs[size]) {
        psi_free(procargs);
        /* We dont' have any arguments or environment */
        if (nargs == 0) {
            proci->argc = 0;
            proci->argc_status = PSI_STATUS_OK;
            proci->argv = NULL;
            proci->argv_status = PSI_STATUS_OK;
            proci->envc = 0;
            proci->envc_status = PSI_STATUS_OK;
            proci->envv = NULL;
            proci->envv_status = PSI_STATUS_OK;
            /* Update proci->command */
            if (command_from_argv(&proci->command, proci->argv, proci->argc) < 0) {
                psi_free(procargs);
                return -1;
            }
            proci->command_status = PSI_STATUS_OK;
            return 0;
        } else {
            PyErr_SetString(PyExc_OSError, "Did not find args and env");
            return -1;
        }
    }

    /* The argument list */
    proci->argc = nargs;
    proci->argc_status = PSI_STATUS_OK;
    proci->argv = psi_strings_to_array(cp, nargs);
    if (proci->argv == NULL) {
        psi_free(procargs);
        return -1;
    }
    proci->argv_status = PSI_STATUS_OK;
    if (command_from_argv(&proci->command, proci->argv, proci->argc) < 0) {
        psi_free(procargs);
        return -1;
    }
    proci->command_status = PSI_STATUS_OK;

    /* The environment */
    for (i = 0; i < nargs; i++) {
        env_start += strlen(proci->argv[i])+1;
    }
    env_start--;
    proci->envc = 0;
    for (i = 0; ; i++) {
        if (*(cp+env_start + i) == '\0') {
            if (*(cp+env_start + i + 1) == '\0')
                break;
            else
                proci->envc++;
        }
    }
    proci->envc_status = PSI_STATUS_OK;
    proci->envv = psi_strings_to_array(cp+env_start+1, proci->envc);
    psi_free(procargs);
    if (proci->envv == NULL)
        return -1;
    proci->envv_status = PSI_STATUS_OK;

    return 0;
}


static int
command_from_argv(char **command, char **argv, int argc){
    int i;
    int command_len = 0;
    int offset=0;
    
    for (i=0; i<argc; i++)
        command_len += strlen(argv[i]) + 1;
    *command = psi_malloc(command_len);
    if (*command == NULL) {
        return -1;
    }
    for (i=0; i<argc; i++) {
        strcpy(*command+offset, argv[i]);
        *(*command + offset + strlen(argv[i])) = ' ';
        offset = offset + strlen(argv[i]) + 1;
    }
    *(*command + offset - 1) = '\0';
    
    return 0;
}


static int
set_cwd(struct psi_process *proci, struct kinfo_proc *p)
{
    /*
     * Current working directory of a process is not available on the mac...
     * Ref http://bitbucket.org/chrismiles/psi/issue/4/implement-processcwd-for-darwin
     */
    proci->cwd_status = PSI_STATUS_NA;
    return 0;
}


static int
set_kp_proc(struct psi_process *proci, struct kinfo_proc *p)
{
    proci->name = psi_strdup(p->kp_proc.p_comm);
    if (proci->name == NULL)
        return -1;
    proci->name_status = PSI_STATUS_OK;

    proci->nice = p->kp_proc.p_nice;
    proci->nice_status = PSI_STATUS_OK;

    proci->priority = p->kp_proc.p_priority;
    proci->priority_status = PSI_STATUS_OK;

    proci->status = p->kp_proc.p_stat;
    proci->status_status = PSI_STATUS_OK;

    proci->start_time.tv_sec = p->kp_proc.p_starttime.tv_sec;
    proci->start_time.tv_nsec = p->kp_proc.p_starttime.tv_usec * 1000;
    proci->start_time_status = PSI_STATUS_OK;

    return 0;
}


static int
set_kp_eproc(struct psi_process *proci, struct kinfo_proc *p)
{
    dev_t tdev;
    char *ttname;

    proci->egid = p->kp_eproc.e_pcred.p_svgid;
    proci->egid_status = PSI_STATUS_OK;

    proci->euid = p->kp_eproc.e_pcred.p_svuid;
    proci->euid_status = PSI_STATUS_OK;

    proci->pgrp = p->kp_eproc.e_pgid;
    proci->pgrp_status = PSI_STATUS_OK;

    proci->ppid = p->kp_eproc.e_ppid;
    proci->ppid_status = PSI_STATUS_OK;

    proci->rgid = p->kp_eproc.e_pcred.p_rgid;
    proci->rgid_status = PSI_STATUS_OK;

    proci->ruid = p->kp_eproc.e_pcred.p_ruid;
    proci->ruid_status = PSI_STATUS_OK;

    proci->sid = (int)p->kp_eproc.e_sess;
    proci->sid_status = PSI_STATUS_OK;

    tdev = p->kp_eproc.e_tdev;
    if (tdev != NODEV && (ttname = devname(tdev, S_IFCHR)) != NULL) {
        /* Prepend with "/dev/" for compatibility with other architectures */
        char terminaldev[64] = "/dev/";
        strncat(terminaldev, ttname, 64);

        proci->terminal = psi_strdup(terminaldev);
        proci->terminal_status = PSI_STATUS_OK;
    } else {
        proci->terminal = psi_strdup("");
        proci->terminal_status = PSI_STATUS_OK;
    }

    return 0;
}


static int
set_task(struct psi_process *proci, struct kinfo_proc *p)
{
    task_port_t task;
    unsigned int info_count;
    struct task_basic_info tasks_info;
    thread_array_t thread_list;
    unsigned int thread_count;


    if (task_for_pid(mach_task_self(),
                     p->kp_proc.p_pid, &task) != KERN_SUCCESS) {
        proci->pcpu_status     = PSI_STATUS_PRIVS;
        proci->utime_status    = PSI_STATUS_PRIVS;
        proci->stime_status    = PSI_STATUS_PRIVS;
        proci->nthreads_status = PSI_STATUS_PRIVS;
        proci->rss_status      = PSI_STATUS_PRIVS;
        proci->vsz_status      = PSI_STATUS_PRIVS;
        return 0;
    }

    if (task_threads(task, &thread_list, &thread_count) == KERN_SUCCESS) {
        int i;
        struct timespec utime = { 0, 0 };
        struct timespec stime = { 0, 0 };
        int t_cpu = 0;
        int failed = 0;

        proci->nthreads = thread_count;
        proci->nthreads_status = PSI_STATUS_OK;

        for (i = 0; i < thread_count; ++i) {
            struct thread_basic_info t_info;
            unsigned int             icount = THREAD_BASIC_INFO_COUNT;

            if (thread_info(thread_list[i], THREAD_BASIC_INFO,
                            (thread_info_t)&t_info, &icount) == KERN_SUCCESS) {
                utime.tv_sec  += t_info.user_time.seconds;
                utime.tv_nsec += t_info.user_time.microseconds * 1000;
                stime.tv_sec  += t_info.system_time.seconds;
                stime.tv_nsec += t_info.system_time.microseconds * 1000;
                t_cpu         += t_info.cpu_usage;
            } else {
                failed = 1;
            }
        }

        if (failed) {
            proci->pcpu_status  = PSI_STATUS_PRIVS;
            proci->utime_status = PSI_STATUS_PRIVS;
            proci->stime_status = PSI_STATUS_PRIVS;
        } else {
            proci->pcpu = 100.0 * (double)(t_cpu) / TH_USAGE_SCALE;
            proci->pcpu_status = PSI_STATUS_OK;

            proci->utime = utime;
            proci->utime_status = PSI_STATUS_OK;

            proci->stime = stime;
            proci->stime_status = PSI_STATUS_OK;
        }
    } else {
        proci->pcpu_status     = PSI_STATUS_PRIVS;
        proci->utime_status    = PSI_STATUS_PRIVS;
        proci->stime_status    = PSI_STATUS_PRIVS;
        proci->nthreads_status = PSI_STATUS_PRIVS;
    }
    vm_deallocate(mach_task_self(),
        (vm_address_t)thread_list, sizeof(thread_array_t)*(thread_count));

    info_count = TASK_BASIC_INFO_COUNT;
    if (task_info(task, TASK_BASIC_INFO,
                  (task_info_t)&tasks_info, &info_count) == KERN_SUCCESS) {
        vm_region_basic_info_data_64_t  b_info;
        vm_address_t                    address = GLOBAL_SHARED_TEXT_SEGMENT;
        vm_size_t                       size;
        mach_port_t                     object_name;

        /*
         * try to determine if this task has the split libraries mapped in... if
         * so, adjust its virtual size down by the 2 segments that are used for
         * split libraries
         */
        info_count = VM_REGION_BASIC_INFO_COUNT_64;
        if (vm_region_64(task, &address, &size, VM_REGION_BASIC_INFO,
                        (vm_region_info_t)&b_info, &info_count,
                        &object_name) == KERN_SUCCESS) {
            if (b_info.reserved && size == (SHARED_TEXT_REGION_SIZE) &&
                    tasks_info.virtual_size >
                    (SHARED_TEXT_REGION_SIZE + SHARED_DATA_REGION_SIZE)) {
                tasks_info.virtual_size -=
                    (SHARED_TEXT_REGION_SIZE + SHARED_DATA_REGION_SIZE);
            }
        }

        proci->rss        = tasks_info.resident_size;
        proci->rss_status = PSI_STATUS_OK;
        proci->vsz        = tasks_info.virtual_size;
        proci->vsz_status = PSI_STATUS_OK;
    } else {
        proci->rss_status = PSI_STATUS_PRIVS;
        proci->vsz_status = PSI_STATUS_PRIVS;
    }

    return 0;
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

