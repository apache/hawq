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

/* The psi.process module  */


#include <Python.h>

#include "psi.h"
#include "process.h"


/* Declarations (we need these since we use -Wmissing-declarations) */
#ifdef PY3K
PyMODINIT_FUNC PyInit_process(void);
#else
PyMODINIT_FUNC initprocess(void);
#endif


/* Re-declare these as non-extern since we're providing the symbols */
PyObject *PsiExc_AttrNotAvailableError = NULL;
PyObject *PsiExc_AttrInsufficientPrivsError = NULL;
PyObject *PsiExc_AttrNotImplementedError = NULL;
PyObject *PsiExc_MissingResourceError = NULL;
PyObject *PsiExc_NoSuchProcessError = NULL;
PyObject *PsiExc_InsufficientPrivsError = NULL;


/* More constants */
static char MODULE_NAME[] = "psi.process";
PyDoc_STRVAR(MODULE_DOC, "Module for process information");


/** Finalise the types
 *
 * Calls PyType_Ready() and increases their reference count.
 */
static int
prepare_types(void)
{
    if (PyType_Ready(&PsiProcess_Type) < 0)
        return -1;
    if (PyType_Ready(&PsiProcessTable_Type) < 0)
        return -1;
    Py_INCREF(&PsiProcess_Type);
    Py_INCREF(&PsiProcessTable_Type);
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
    PsiExc_MissingResourceError = PyObject_GetAttrString(
        _psimod, "MissingResourceError");
    if (PsiExc_MissingResourceError == NULL)
        goto error;
    PsiExc_InsufficientPrivsError = PyObject_GetAttrString(
        _psimod, "InsufficientPrivsError");
    if (PsiExc_InsufficientPrivsError == NULL)
        goto error;
    PsiExc_NoSuchProcessError = PyErr_NewException(
        "psi.process.NoSuchProcessError", PsiExc_MissingResourceError, NULL);
    if (PsiExc_NoSuchProcessError == NULL)
        goto error;
    Py_DECREF(_psimod);
    return 0;

  error:
    Py_DECREF(_psimod);
    return -1;
}


/* Add all objects to the module */
static int
add_module_objects(PyObject *module)
{
    struct psi_flag *flag;

    flag = psi_arch_proc_status_flags;
    while (flag->name != NULL) {
        if (PyModule_AddIntConstant(module, flag->name, flag->val) == -1)
            return -1;
        flag++;
    }
    if (PyModule_AddObject(module, "Process",
                           (PyObject *)&PsiProcess_Type) < 0)
        return -1;
    if (PyModule_AddObject(module, "ProcessTable",
                           (PyObject *)&PsiProcessTable_Type) < 0)
        return -1;
    if (PyModule_AddObject(module, "NoSuchProcessError",
                           PsiExc_NoSuchProcessError) < 0)
        return -1;
    return 0;
}


static PyMethodDef process_methods[] = {
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


#ifdef PY3K
static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,  /* m_base */
        MODULE_NAME,            /* m_name */
        MODULE_DOC,             /* m_doc */
        -1,                     /* m_size */
        process_methods,        /* m_methods */
        NULL,                   /* m_reload */
        NULL,                   /* m_traverse */
        NULL,                   /* m_clear */
        NULL                    /* m_free */
};
#endif


/* Some defines to make the module init function readable */
#ifdef PY3K
#define MODFUNC PyInit_process
#define RETURN(VAR) return VAR
#else
#define MODFUNC initprocess
#define RETURN(VAR) return
#endif


/* Returns the psi.process module */
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
    mod = Py_InitModule3(MODULE_NAME, process_methods, MODULE_DOC);
#endif
    if (mod == NULL)
        goto error;
    if (add_module_objects(mod) < 0)
        goto error;
    RETURN(mod);

  error:
    Py_XDECREF(mod);
    Py_XDECREF(PsiExc_AttrNotAvailableError);
    Py_XDECREF(PsiExc_AttrInsufficientPrivsError);
    Py_XDECREF(PsiExc_AttrNotImplementedError);
    Py_XDECREF(PsiExc_MissingResourceError);
    Py_XDECREF(PsiExc_InsufficientPrivsError);
    Py_XDECREF(PsiExc_NoSuchProcessError);
    Py_DECREF((PyObject*)&PsiProcess_Type);
    Py_DECREF((PyObject*)&PsiProcessTable_Type);
    RETURN(NULL);
}
