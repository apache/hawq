/* The MIT License
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

/** psi.process.ProcessTable class
 *
 * This file contains the common support for the psi.process.ProcessTable
 * class.
 */


#include <Python.h>

#include "psi.h"
#include "process.h"


/* The ProcessTableObject.  It has no extra fields. */
typedef PyDictObject PsiProcessTableObject;


/* Declarations */
static int add_procs_to_table(PsiProcessTableObject *pt,
                              const struct psi_proclist *prl);


/* ProcessTable methods */


static int
ProcessTable_init(PsiProcessTableObject *self, PyObject *args, PyObject *kwds)
{
    struct psi_proclist *prl;
    int r;

    if (args != NULL && PySequence_Length(args) > 0) {
        PyErr_Format(PyExc_TypeError,
                     "__init__() takes no arguments (%d given)",
                     (int)PySequence_Length(args));
        return -1;
    }
    if (kwds != NULL && PyMapping_Length(kwds) > 0) {
        PyErr_SetString(PyExc_TypeError,
                        "__init__() takes no keyword arguments");
        return -1;
    }
    if (PyDict_Type.tp_init((PyObject *)self, args, kwds) < 0)
        return -1;
    prl = psi_arch_proclist();
    if (prl == NULL)
        return -1;
    r = add_procs_to_table(self, prl);
    psi_free_proclist(prl);
    return r;
}


static PyObject *
ProcessTable_repr(PsiProcessTableObject *self)
{
    return PyStr_FromString("psi.process.ProcessTable()");
}


static int
ProcessTable_ass_subscript(PsiProcessTableObject *self,
                           PyObject *item, PyObject *value)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "ProcessTable does not support item deletion");
    } else {
        PyErr_SetString(PyExc_TypeError,
                        "ProcessTable does not support item assignment");
    }
    return -1;
}


static PyMappingMethods ProcessTable_as_mapping = {
    0,                                          /* mp_length */
    0,                                          /* mp_subscript */
    (objobjargproc)ProcessTable_ass_subscript,  /* mp_ass_subscript */
};


PyTypeObject PsiProcessTable_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.process.ProcessTable",                 /* tp_name */
    sizeof(PsiProcessTableObject),              /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    0,                                          /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    (reprfunc)ProcessTable_repr,                /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    &ProcessTable_as_mapping,                   /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "A dictionary of all processes (snapshot)", /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    0,                                          /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    &PyDict_Type,                               /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)ProcessTable_init,                /* tp_init */
    0,                                          /* tp_alloc */
    0,                                          /* tp_new */
};


/* Local functions */


/* Always returns NULL */
void *
psi_free_proclist(struct psi_proclist *prl)
{
    psi_free(prl->pids);
    psi_free(prl);
    return NULL;
}


/** Add Process objects to a PsiProcessTableObject
 *
 * This will create a Process object for each PID in the `prl' proclist
 * structure and add it to the ProcessTable object `pt'.  This will also reset
 * `pt->count' to the value of `prl->count'.  If a process listed in the
 * proclist structure does no longer exist (newProcessObject() raise a
 * NoSuchProcessError) it will be silently skipped and `pt->count' will be
 * updated accordingly.
 *
 * Return 0 on success and -1 on failure.
 */
static int
add_procs_to_table(PsiProcessTableObject *pt, const struct psi_proclist *prl)
{
    PyObject *proc;
    PyObject *key;
    int i;
    int r;

    for (i = 0; i < prl->count; i++) {
        proc = PsiProcess_New(prl->pids[i]);
        if (proc == NULL && PyErr_ExceptionMatches(PsiExc_NoSuchProcessError)) {
            PyErr_Clear();
            continue;
        } else if (proc == NULL)
            return -1;
        key = PyLong_FromLong(prl->pids[i]);
        if (key == NULL) {
            Py_DECREF(proc);
            return -1;
        }
        r = PyDict_SetItem((PyObject*)pt, key, proc);
        Py_DECREF(proc);
        Py_DECREF(key);
        if (r == -1)
            return -1;
    }
    return 0;
}


/** Create a new ProcessTableObject
 *
 * The ugly part is that PyDict_Type.tp_init (called by ProcessTable_init())
 * does not cope with NULL as args or kwds.  Oh well.
 */
PyObject *
PsiProcessTable_New(void)
{
    PyObject *args = NULL;
    PyObject *kwds = NULL;
    PyObject *obj;

    obj = PyDict_Type.tp_new(&PsiProcessTable_Type, NULL, NULL);
    if (obj == NULL)
        return NULL;
    args = PyTuple_New(0);
    if (args == NULL)
        goto error;
    kwds = PyDict_New();
    if (kwds == NULL)
        goto error;
    if (ProcessTable_init((PsiProcessTableObject *)obj, args, kwds) != 0)
        goto error;
    Py_DECREF(args);
    Py_DECREF(kwds);
    return obj;

  error:
    Py_XDECREF(args);
    Py_XDECREF(kwds);
    Py_DECREF(obj);
    return NULL;
}
