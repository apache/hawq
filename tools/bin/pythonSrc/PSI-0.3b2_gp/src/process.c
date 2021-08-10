/* The MIT License
 *
 * Copyright (C) 2007 Chris Miles
 *
 * Copyright (C) 2008-2009 Floris Bruynooghe
 *
 * Copyright (C) 2008-2009 Abilisoft Ltd.
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

/** psi.process.Process class
 *
 * This file contains the common support for the psi.process.Process class.
 */


#include <Python.h>

#include <errno.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>

#include "psi.h"
#include "process.h"


/** The Python Process object
 *
 * `pid' and `proci' are filled in by the init method.  All the python objects
 * are filled lazily when they are accessed.  They exist so that accessing
 * them twice returns a new reference to the same objects instead of creating
 * new objects.  Just like in the psi_process structure some of these pointers
 * might never be used on some platforms, e.g. zoneid is Solaris-only.
 */
typedef struct {
    PyObject_HEAD
    pid_t pid;
    struct psi_process *proci;
} PsiProcessObject;


/***** Local declarations *****/

static int check_init(PsiProcessObject *obj);


/***** Local functions *****/

void *
psi_free_process(struct psi_process *proci)
{
    int i;

    psi_FREE(proci->name);
    psi_FREE(proci->exe);
    if (proci->argv != NULL)
        for (i = 0; i < proci->argc; i++)
            psi_free(proci->argv[i]);
    psi_FREE(proci->argv);
    psi_FREE(proci->command);
    for (i = 0; i < proci->envc; i++)
        psi_free(proci->envv[i]);
    psi_FREE(proci->envv);
    psi_FREE(proci->cwd);
    psi_FREE(proci->terminal);
    psi_FREE(proci->zonename);
    psi_free(proci);
    return NULL;
}


/** Create a hash from a proci structure
 *
 * This is the implementation of Process.__hash__() really but without
 * needeing a full PsiProcessObject so it can be used easier in
 * various places.
 */
static long
hash_proci(const pid_t pid, const struct psi_process *proci)
{
    PyObject *tuple;
    PyObject *pypid;
    PyObject *starttime;
    long hash;

#ifdef LINUX
    if (psi_checkattr("Process.jiffies", proci->jiffies_status) < 0)
        return -1;
#else
    if (psi_checkattr("Process.start_time", proci->start_time_status) < 0)
        return -1;
#endif
    pypid = PyLong_FromLong(pid);
    if (pypid == NULL)
        return -1;
#ifdef LINUX
    starttime = PyLong_FromLong(proci->jiffies);
#else
    starttime = PsiTimeSpec_New(&proci->start_time);
#endif
    if (starttime == NULL) {
        Py_DECREF(pypid);
        return -1;
    }
    if ((tuple = PyTuple_New(2)) == NULL) {
        Py_DECREF(pypid);
        Py_DECREF(starttime);
        return -1;
    }
    PyTuple_SET_ITEM(tuple, 0, pypid);
    PyTuple_SET_ITEM(tuple, 1, starttime);
    hash = PyObject_Hash(tuple);
    Py_DECREF(tuple);
    return hash;
}


static int
Process_init(PsiProcessObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"pid", NULL};
    pid_t pid;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i", kwlist, &pid))
        return -1;
    self->pid = pid;
    self->proci = psi_arch_process(pid);
    if (self->proci == NULL)
        return -1;
    return 0;
}


static void
Process_dealloc(PsiProcessObject *self)
{
    if (self == NULL)
        return;
    if (self->proci != NULL)
        psi_free_process(self->proci);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Process_repr(PsiProcessObject *self)
{
    return PyStr_FromFormat("%s(pid=%d)",
                            Py_TYPE(self)->tp_name, (int)self->pid);
}


static long
Process_hash(PsiProcessObject *self)
{
    return hash_proci(self->pid, self->proci);
}


static PyObject *
Process_richcompare(PyObject *v, PyObject *w, int op)
{
    PsiProcessObject *vo, *wo;
    PyObject *result;
    int istrue;

    if (!PyObject_TypeCheck(v, &PsiProcess_Type)
        || !PyObject_TypeCheck(w, &PsiProcess_Type)) {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }
    vo = (PsiProcessObject *)v;
    wo = (PsiProcessObject *)w;
    switch (op) {
        case Py_EQ:
            istrue = vo->pid == wo->pid;
            break;
        case Py_NE:
            istrue = vo->pid != wo->pid;
            break;
        case Py_LE:
            istrue = vo->pid <= wo->pid;
            break;
        case Py_GE:
            istrue = vo->pid >= wo->pid;
            break;
        case Py_LT:
            istrue = vo->pid < wo->pid;
            break;
        case Py_GT:
            istrue = vo->pid > wo->pid;
            break;
        default:
            assert(!"op unknown");
            istrue = 0;         /* To shut up compiler */
    }
    result = istrue ? Py_True : Py_False;
    Py_INCREF(result);
    return result;
}


/** Check if object is initialised
 *
 * Small helper function that checks if an object is properly initialised.
 *
 * XXX: Maybe this should go into util.c in some from.
 */
static int
check_init(PsiProcessObject *obj)
{
    if (obj->proci == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Instance has not been initialised properly");
        return -1;
    }
    return 0;
}


static PyObject *
Process_get_pid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    return PyLong_FromLong(self->pid);
}


static PyObject *
Process_get_name(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.name",
                      self->proci->name_status) < 0)
        return NULL;
    return PyStr_FromString(self->proci->name);
}


static PyObject *
Process_get_exe(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.exe", self->proci->exe_status) < 0)
        return NULL;
    return PyStr_FromString(self->proci->exe);
}


/** Create a tuple from the argv vector in the psi_process structure
 *
 * Each element in argv is allowed to be NULL in which case a None object
 * should be added to the tuple.
 */
static PyObject *
Process_get_args(PsiProcessObject *self, void *closure)
{
    PyObject *args;
    PyObject *arg;
    int i;

    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.args", self->proci->argc_status) < 0
        || psi_checkattr("Process.args", self->proci->argv_status) < 0)
        return NULL;
    args = PyTuple_New((Py_ssize_t)self->proci->argc);
    if (args == NULL)
        return NULL;
    for (i = 0; i < self->proci->argc; i++) {
        arg = PyStr_FromString(self->proci->argv[i]);
        if (arg == NULL) {
            Py_DECREF(args);
            return NULL;
        }
        PyTuple_SET_ITEM(args, i, arg);
    }
    return args;
}


static PyObject *
Process_get_argc(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.argc", self->proci->argc_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->argc);
}


/** Return the command
 *
 * Note that implementations are allowed to return an empty string for this in
 * which case we need to get the name attribute instead.
 */
static PyObject *
Process_get_command(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.command", self->proci->command_status) < 0)
        return NULL;
    if (strlen(self->proci->command) == 0) {
        if (psi_checkattr("Process.command", self->proci->name_status) < 0)
            return NULL;
        return PyStr_FromString(self->proci->name);
    } else
        return PyStr_FromString(self->proci->command);
}


static PyObject *
Process_get_env(PsiProcessObject *self, void *closure)
{
    PyObject *env;
    PyObject *val;
    char *key;
    char *s;
    char *equals;
    int i;
    int r;

    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.env", self->proci->envc_status) < 0
        || psi_checkattr("Process.env", self->proci->envv_status) < 0)
        return NULL;
    env = PyDict_New();
    if (env == NULL)
        return NULL;
    s = (char *)self->proci->envv;
    for (i = 0; i < self->proci->envc; i++) {
        key = self->proci->envv[i];
        equals = strchr(key, '=');
        if (!equals)
            continue;           /* This is possible on at least Linux */
        *equals = '\0';
        val = PyStr_FromString(equals + 1);
        if (val == NULL) {
            Py_DECREF(env);
            return NULL;
        }
        r = PyDict_SetItemString(env, key, val);
        Py_DECREF(val);
        if (r == -1)
            return NULL;
    }
    return env;
}


#if ! (defined(SUNOS5) && SUNOS5_MINOR < 10)
static PyObject *
Process_get_cwd(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.cwd", self->proci->cwd_status) < 0)
        return NULL;
    return PyStr_FromString(self->proci->cwd);
}
#endif


static PyObject *
Process_get_euid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.uid", self->proci->euid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->euid);
}


static PyObject *
Process_get_egid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.gid", self->proci->egid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->egid);
}


static PyObject *
Process_get_ruid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.real_uid", self->proci->ruid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->ruid);
}


static PyObject *
Process_get_rgid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.real_gid", self->proci->rgid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->rgid);
}


#if defined(SUNOS5) && SUNOS5_MINOR >= 10
static PyObject *
Process_get_zoneid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.zoneid", self->proci->zoneid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->zoneid);
}
#endif


#if defined(SUNOS5) && SUNOS5_MINOR >= 10
static PyObject *
Process_get_zonename(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.zonename", self->proci->zonename_status) < 0)
        return NULL;
    return PyStr_FromString(self->proci->zonename);
}
#endif


static PyObject *
Process_get_ppid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.ppid", self->proci->ppid_status) < 0)
        return NULL;
    return PyLong_FromLong((long)self->proci->ppid);
}


static PyObject *
Process_get_sid(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.sid", self->proci->sid_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->sid);
}


static PyObject *
Process_get_pgrp(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.pgrp", self->proci->pgrp_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->pgrp);
}


static PyObject *
Process_get_priority(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.priority", self->proci->priority_status) < 0)
        return NULL;
    return PyLong_FromLong((long)self->proci->priority);
}


static PyObject *
Process_get_nice(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.nice", self->proci->nice_status) < 0)
        return NULL;
    return PyLong_FromLong((long)self->proci->nice);
}


static PyObject *
Process_get_start_time(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.start_time", self->proci->start_time_status) < 0)
        return NULL;
    return PsiTimeSpec_New(&self->proci->start_time);
}


#ifdef LINUX
static PyObject *
Process_get_jiffies(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.jiffies", self->proci->jiffies_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->jiffies);
}
#endif


static PyObject *
Process_get_status(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.status", self->proci->status_status) < 0)
        return NULL;
    return PyLong_FromLong((long)self->proci->status);
}


#ifndef LINUX2_4
static PyObject *
Process_get_nthreads(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.nthreads", self->proci->nthreads_status) < 0)
        return NULL;
    return PyLong_FromLong((long)self->proci->nthreads);
}
#endif


#if ! (defined(SUNOS5) && SUNOS5_MINOR < 10)
static PyObject *
Process_get_terminal(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.terminal", self->proci->terminal_status) < 0)
        return NULL;
    if (self->proci->terminal == '\0') {
        Py_INCREF(Py_None);
        return Py_None;
    } else
        return PyStr_FromString(self->proci->terminal);
}
#endif


static PyObject *
Process_get_utime(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.utime", self->proci->utime_status) < 0)
        return NULL;
    return PsiTimeSpec_New(&self->proci->utime);
}


static PyObject *
Process_get_stime(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.stime", self->proci->stime_status) < 0)
        return NULL;
    return PsiTimeSpec_New(&self->proci->stime);
}


static PyObject *
Process_get_cputime(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.cputime", self->proci->cputime_status) < 0)
        return NULL;
    return PsiTimeSpec_New(&self->proci->cputime);
}


static PyObject *
Process_get_rss(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.rss", self->proci->rss_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->rss);
}


static PyObject *
Process_get_vsz(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.vsz", self->proci->vsz_status) < 0)
        return NULL;
    return PyLong_FromLong(self->proci->vsz);
}


#if !defined(LINUX) && !defined(AIX)
static PyObject *
Process_get_pcpu(PsiProcessObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Process.pcpu", self->proci->pcpu_status) < 0)
        return NULL;
    return PyFloat_FromDouble(self->proci->pcpu);
}
#endif


PyDoc_STRVAR(Process_refresh__doc, "\
Process.refresh()\n\
\n\
Refresh all data of the attributes.  This allows you to get new data\n\
without having to create a new Process instance.\n\
");
static PyObject *
Process_refresh(PsiProcessObject *self)
{
    struct psi_process *new_proci;
    long old_hash;
    long new_hash;

    if (check_init(self) < 0)
        return NULL;
    old_hash = hash_proci(self->pid, self->proci);
    if (old_hash == -1)
        return NULL;
    new_proci = psi_arch_process(self->pid);
    if (new_proci == NULL) {
        PyErr_SetString(PsiExc_NoSuchProcessError, "Process no longer exists");
        return NULL;
    }
    new_hash = hash_proci(self->pid, new_proci);
    if (new_hash == -1) {
	psi_free_process(new_proci);
        return NULL;
    }
    if (new_hash == old_hash) {
        psi_free_process(self->proci);
        self->proci = new_proci;
        Py_RETURN_NONE;
    } else {
        psi_free_process(new_proci);
        PyErr_SetString(PsiExc_NoSuchProcessError, "Process no longer exists");
        return NULL;
    }
}


PyDoc_STRVAR(Process_exists__doc, "\
Process.exists() -> True or False\n\
\n\
Test if a process still exists.  This might not mean it is alive, it\n\
could be in a zombie state or similar.\
");
static PyObject *
Process_exists(PsiProcessObject *self)
{
    struct psi_process *new_proci;
    long old_hash;
    long new_hash;

    PyErr_WarnEx(PyExc_FutureWarning, "Experimental method", 1);
    if (check_init(self) < 0)
        return NULL;
    old_hash = hash_proci(self->pid, self->proci);
    if (old_hash == -1)
        return NULL;
    new_proci = psi_arch_process(self->pid);
    if (new_proci == NULL) {
        PyErr_Clear();
        Py_RETURN_FALSE;
    }
    new_hash = hash_proci(self->pid, new_proci);
    psi_free_process(new_proci);
    if (new_hash == -1) {
	psi_free_process(new_proci);
        return NULL;
    }
    if (new_hash == old_hash)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}


PyDoc_STRVAR(Process_children__doc, "\
Process.children() -> [child, ...]\n\
\n\
This returns a (possibly empty) list of children of the process.  Each\n\
child will be the corresponding Process instance.\n\
");
static PyObject *
Process_children(PsiProcessObject *self)
{
    PyObject *isalive;
    PyObject *ptable;
    PyObject *children;
    PsiProcessObject *proc;
    PyObject *pyproc;
    Py_ssize_t pos = 0;

    PyErr_WarnEx(PyExc_FutureWarning, "Experimental method", 1);
    isalive = Process_exists(self);
    if (isalive != Py_True) {
        PyErr_SetString(PsiExc_NoSuchProcessError, "Process no longer exists");
        return NULL;
    }
    children = PyList_New(0);
    if (children == NULL)
        return NULL;
    ptable = PsiProcessTable_New();
    if (ptable == NULL) {
        Py_DECREF(children);
        return NULL;
    }
    while (PyDict_Next(ptable, &pos, NULL, &pyproc)) {
        proc = (PsiProcessObject *)pyproc;
        if (proc->proci->ppid == self->pid)
            if (PyList_Append(children, (PyObject *)proc) == -1) {
                Py_DECREF(children);
                Py_DECREF(ptable);
                return NULL;
            }
    }
    Py_DECREF(ptable);
    return children;
}


PyDoc_STRVAR(Process_kill__doc, "\
Process.kill(signal)\n\
\n\
Send a signal to the process.  `signal` is an integer signal number\n\
as defined by the OS, use one of the signal.SIG* constants.\n\
");
static PyObject *
Process_kill(PsiProcessObject* self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"sig", NULL};
    PyObject *isalive;
    int sig = SIGTERM;
    int r;

    PyErr_WarnEx(PyExc_FutureWarning, "Experimental method", 1);
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &sig))
        return NULL;
    isalive = Process_exists(self);
    if (isalive != Py_True) {
        PyErr_SetString(PsiExc_NoSuchProcessError, "Process no longer exists");
        return NULL;
    }
    r = kill(self->pid, sig);
    if (r == -1)
        if (errno == EINVAL) {
            PyErr_Format(PyExc_ValueError, "Invalid signal: %d", sig);
            return NULL;
        } else if (errno == EPERM) {
            PyErr_Format(PsiExc_InsufficientPrivsError,
                         "No permission to send signal %d to process %ld",
                         sig, (long)self->pid);
            return NULL;
        } else if (errno == ESRCH) {
            PyErr_SetString(PsiExc_NoSuchProcessError,
                            "Process no longer exists");
            return NULL;
        } else {
            PyErr_Format(PyExc_SystemError, "Unexpected errno: %d", errno);
            return NULL;
        }
    else
        Py_RETURN_NONE;
}


static PyGetSetDef Process_getseters[] = {
    {"pid", (getter)Process_get_pid, (setter)NULL,
     "Process PID", NULL},
    {"name", (getter)Process_get_name, (setter)NULL,
     "Name of the process, accounting name if defined", NULL},
    {"exe", (getter)Process_get_exe, (setter)NULL,
     "Absolute pathname to the executable of the process", NULL},
    {"args", (getter)Process_get_args, (setter)NULL,
     "List of the command and it's arguments\n"
     "\n"
     "On some systems (e.g. SunOS, AIX) it possible that this is not\n"
     "available due to privileges.  The `command' attribute should still\n"
     "be available in those cases.", NULL},
    {"argc", (getter)Process_get_argc, (setter)NULL,
     "Argument count", NULL},
    {"command", (getter)Process_get_command, (setter)NULL,
     "Command and arguments as a string\n"
     "\n"
     "On some systems (e.g. SunOS, AIX) this might be truncated to a limited\n"
     "length.  On those systems this will always be available however, while\n"
     "the `args' attribute might not be.", NULL},
    {"env", (getter)Process_get_env, (setter)NULL,
     "The environment of the process as a dictionary", NULL},
#if ! (defined(SUNOS5) && SUNOS5_MINOR < 10)
    {"cwd", (getter)Process_get_cwd, (setter)NULL,
     "Current working directory", NULL},
#endif
    {"euid", (getter)Process_get_euid, (setter)NULL,
     "Current UID", NULL},
    {"egid", (getter)Process_get_egid, (setter)NULL,
     "Current GID", NULL},
    {"ruid", (getter)Process_get_ruid, (setter)NULL,
     "Real UID", NULL},
    {"rgid", (getter)Process_get_rgid, (setter)NULL,
     "Real GID", NULL},
#if defined(SUNOS5) && SUNOS5_MINOR >= 10
    {"zoneid", (getter)Process_get_zoneid, (setter)NULL,
     "ID of the Solaris zone the process is running in", NULL},
    {"zonename", (getter)Process_get_zonename, (setter)NULL,
     "Name of the Solaris zone the process is running in", NULL},
#endif
    {"ppid", (getter)Process_get_ppid, (setter)NULL,
     "Parent PID", NULL},
    {"pgrp", (getter)Process_get_pgrp, (setter)NULL,
     "Process group ID aka PID of process group leader", NULL},
    {"sid", (getter)Process_get_sid, (setter)NULL,
     "Session ID of the process", NULL},
    {"priority", (getter)Process_get_priority, (setter)NULL,
     "Priority of the process", NULL},
    {"nice", (getter)Process_get_nice, (setter)NULL,
     "Nice value of the process", NULL},
    {"start_time", (getter)Process_get_start_time, (setter)NULL,
     "Start time of process as datetime.datetime object\n\n"
     "Use .strftime('%s') to get seconds since epoch",
     NULL},
#ifdef LINUX
     {"jiffies", (getter)Process_get_jiffies, (setter)NULL,
     "Number of jiffies of the start of the process since boot", NULL},
#endif
    {"status", (getter)Process_get_status, (setter)NULL,
     "Process status\n\n"
     "A value matching one of the psi.process.PROC_STATUS_* constants", NULL},
#ifndef LINUX2_4
    {"nthreads", (getter)Process_get_nthreads, (setter)NULL,
     "Number of threads used by this process", NULL},
#endif
#if ! (defined(SUNOS5) && SUNOS5_MINOR < 10)
    {"terminal", (getter)Process_get_terminal, (setter)NULL,
     "Owning terminal or None", NULL},
#endif
    {"utime", (getter)Process_get_utime, (setter)NULL,
     "Time the process has spent in user mode (datetime.timedelta)", NULL},
    {"stime", (getter)Process_get_stime, (setter)NULL,
     "Time the process has spend in system mode (datetime.timedelta)", NULL},
    {"cputime", (getter)Process_get_cputime, (setter)NULL,
     "Total CPU time of the process (datetime.timedelta)", NULL},
    {"rss", (getter)Process_get_rss, (setter)NULL,
     "Resident memory size (RSS) in bytes", NULL},
    {"vsz", (getter)Process_get_vsz, (setter)NULL,
     "Virtual memory size in bytes", NULL},
#if !defined(LINUX) && !defined(AIX)
    {"pcpu", (getter)Process_get_pcpu, (setter)NULL,
     "%% CPU usage, instantaneous", NULL},
#endif
    {NULL}  /* Sentinel */
};


static PyMethodDef Process_methods[] = {
    {"refresh", (PyCFunction)Process_refresh,
     METH_NOARGS, Process_refresh__doc},
    {"exists", (PyCFunction)Process_exists,
     METH_NOARGS, Process_exists__doc},
    {"children", (PyCFunction)Process_children,
     METH_NOARGS, Process_children__doc},
    {"kill", (PyCFunction)Process_kill,
     METH_VARARGS | METH_KEYWORDS, Process_kill__doc},
    {NULL}                      /* Sentinel */
};


PyTypeObject PsiProcess_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.process.Process",                    /* tp_name */
    sizeof(PsiProcessObject),                 /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor)Process_dealloc,              /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    (reprfunc)Process_repr,                   /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    (hashfunc)Process_hash,                   /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "Process(pid=x) -> Process object",       /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    (richcmpfunc)Process_richcompare,         /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    Process_methods,                          /* tp_methods */
    0,                                        /* tp_members */
    Process_getseters,                        /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    (initproc)Process_init,                   /* tp_init */
    0,                                        /* tp_alloc */
    PyType_GenericNew,                        /* tp_new */
};


/** Create a new PsiProcessObject
 *
 * Create a new PsiProcessObject for a pid.
 *
 * Returns a new reference to a PsiProcessObject or NULL in case of an error.
 */
PyObject *
PsiProcess_New(pid_t pid)
{
    PsiProcessObject *obj;

    obj = (PsiProcessObject *)PyType_GenericNew(&PsiProcess_Type, NULL, NULL);
    if (obj == NULL)
        return NULL;

    /* Skip calling .__init__() */
    obj->pid = pid;
    obj->proci = psi_arch_process(pid);
    if (obj->proci == NULL)
        return NULL;
    return (PyObject *)obj;
}
