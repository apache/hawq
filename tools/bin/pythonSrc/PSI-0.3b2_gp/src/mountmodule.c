/* The MIT License
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

/* The psi.mount module */


#include <Python.h>

#include "psi.h"
#include "mount.h"


/* Declarations (we need these since we use -Wmissing-declarations) */
#ifdef PY3K
PyMODINIT_FUNC PyInit_mount(void);
#else
PyMODINIT_FUNC initmount(void);
#endif


/* Re-declare these as non-extern since we're providing the symbols */
PyObject *PsiExc_AttrNotAvailableError = NULL;
PyObject *PsiExc_AttrInsufficientPrivsError = NULL;
PyObject *PsiExc_AttrNotImplementedError = NULL;


/* More contstants */
static char MODULE_NAME[] = "psi.mount";
PyDoc_STRVAR(MODULE_DOC, "Module for system mount information");


PyDoc_STRVAR(psi_mount_mounts__doc, "\
mounts(remote=False) -> iterator\n\
\n\
Return an iterator containing ojects representing mounted filesystems.  The\n\
remote argument is a boolean value that can be used to control if remote\n\
filesystems should be included or not.  Remote filesystems might take longer\n\
to be created or suffer from timeouts collecting information from them.\n\
");
static PyObject *
psi_mount_mounts(PyObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"remote", NULL};
    psi_mountlist_t *mountlist;
    PyObject *mount;
    PyObject *list;
    PyObject *iter;
    ssize_t i;
    int remote = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &remote))
        return NULL;
    mountlist = psi_arch_mountlist(remote);
    if (mountlist == NULL)
        return NULL;
    list = PyList_New(mountlist->count);
    if (list == NULL)
        return psi_free_mountlist(mountlist);
    for (i = 0; i < mountlist->count; ++i) {
        mount = PsiMount_New(mountlist->mounts[i]);
        if (mount == NULL) {
            Py_DECREF(list);
            return psi_free_mountlist(mountlist);
        }
        PyList_SET_ITEM(list, i, mount);

        /* Python now owns the mountinfo, so clear it from the list. */
        mountlist->mounts[i] = NULL;
    }
    psi_free_mountlist(mountlist);
    iter = PySeqIter_New(list);
    Py_DECREF(list);
    return iter;
}


/** Finalise the types
 *
 * Calls PyType_Ready() and does anything else required.
 */
static int
prepare_types(void)
{
    if (PyType_Ready(&MountBase_Type) < 0)
        return -1;
    if (PyType_Ready(&LocalMount_Type) < 0)
        return -1;
    if (PyType_Ready(&RemoteMount_Type) < 0)
        return -1;
    Py_INCREF(&MountBase_Type);
    Py_INCREF(&LocalMount_Type);
    Py_INCREF(&RemoteMount_Type);
    return 0;
}


/** Initialise the global exceptions
 *
 * If this function fails the modinit function will Py_XDECREF the exceptions,
 * so no need to do that here.
 */
static int
init_exceptions(void)
{
    PyObject *_psimod;

    _psimod = PyImport_ImportModule("psi._psi");
    if (_psimod == NULL)
        return -1;
    PsiExc_AttrNotAvailableError = PyObject_GetAttrString(
        _psimod, "AttrNotAvailableError");
    if (PsiExc_AttrNotAvailableError == NULL)
        goto error;
    PsiExc_AttrInsufficientPrivsError = PyObject_GetAttrString(
        _psimod, "AttrInsufficientPrivsError");
    if (PsiExc_AttrInsufficientPrivsError == NULL)
        goto error;
    PsiExc_AttrNotImplementedError = PyObject_GetAttrString(
        _psimod, "AttrNotImplementedError");
    if (PsiExc_AttrNotImplementedError == NULL)
        goto error;
    Py_DECREF(_psimod);
    return 0;

  error:
    Py_DECREF(_psimod);
    return -1;
}


/* Add all the objects to the module */
static int
add_module_objects(PyObject *mod)
{
    if (PyModule_AddObject(mod, "MountBase", (PyObject *)&MountBase_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "LocalMount", (PyObject *)&LocalMount_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "RemoteMount", (PyObject *)&RemoteMount_Type) < 0)
        return -1;
    return 0;
}


static PyMethodDef mount_methods[] = {
    {"mounts", (PyCFunction)psi_mount_mounts, METH_VARARGS | METH_KEYWORDS,
     psi_mount_mounts__doc},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


#ifdef PY3K
static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,  /* m_base */
        MODULE_NAME,            /* m_name */
        MODULE_DOC,             /* m_doc */
        -1,                     /* m_size */
        mount_methods,          /* m_methods */
        NULL,                   /* m_reload */
        NULL,                   /* m_traverse */
        NULL,                   /* m_clear */
        NULL                    /* m_free */
};
#endif


/* Some defines to make the module init function readable */
#ifdef PY3K
#define MODFUNC PyInit_mount
#define RETURN(VAR) return VAR
#else
#define MODFUNC initmount
#define RETURN(VAR) return
#endif


/* Returns the psi.mount module */
PyMODINIT_FUNC
MODFUNC(void)
{
    PyObject *mod = NULL;

    if (prepare_types() < 0)
        RETURN(NULL);
    if (init_exceptions() < 0)
        goto error;
#ifdef PY3K
    mod = PyModule_Create(&moduledef);
#else
    mod = Py_InitModule3(MODULE_NAME, mount_methods, MODULE_DOC);
#endif
    if (mod == NULL)
        goto error;
    if (add_module_objects(mod) < 0)
        goto error;
    PyErr_WarnEx(PyExc_FutureWarning, "Experimental API", 1);
    RETURN(mod);

error:
    Py_XDECREF(mod);
    Py_XDECREF(PsiExc_AttrNotAvailableError);
    Py_XDECREF(PsiExc_AttrInsufficientPrivsError);
    Py_XDECREF(PsiExc_AttrNotImplementedError);
    Py_XDECREF(&MountBase_Type);
    Py_XDECREF(&LocalMount_Type);
    Py_XDECREF(&RemoteMount_Type);
    RETURN(NULL);
}
