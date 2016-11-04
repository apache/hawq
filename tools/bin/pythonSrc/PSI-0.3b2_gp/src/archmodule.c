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

/* The psi.arch module  */


#include <Python.h>

#include "psi.h"
#include "arch.h"


/* Declarations (we need these since we use -Wmissing-declarations) */
#ifdef PY3K
PyMODINIT_FUNC PyInit_arch(void);
#else
PyMODINIT_FUNC initarch(void);
#endif


/* Re-declare these as non-extern since we're providing the symbols */
PyObject *PsiExc_AttrNotAvailableError = NULL;
PyObject *PsiExc_AttrInsufficientPrivsError = NULL;
PyObject *PsiExc_AttrNotImplementedError = NULL;


/* More constants */
static char MODULE_NAME[] = "psi.arch";
PyDoc_STRVAR(MODULE_DOC, "Module for system architecture information");


/* The psi.arch.arch_type() function */
static PyObject *
psi_arch_type(PyObject *self, PyObject *args)
{
    return PsiArch_New();
}


/** Finalise the types
 *
 * Calls PyType_Ready() and does anything else required.
 */
static int
prepare_types(void)
{
    if (PyType_Ready(&PsiArchBase_Type) < 0)
        return -1;
    if (PyType_Ready(&PsiArchLinux_Type) < 0)
        return -1;
    if (PyType_Ready(&PsiArchSunOS_Type) < 0)
        return -1;
    if (PyType_Ready(&PsiArchDarwin_Type) < 0)
        return -1;
    if (PyType_Ready(&PsiArchAIX_Type) < 0)
        return -1;
    Py_INCREF(&PsiArchBase_Type);
    Py_INCREF(&PsiArchLinux_Type);
    Py_INCREF(&PsiArchSunOS_Type);
    Py_INCREF(&PsiArchDarwin_Type);
    Py_INCREF(&PsiArchAIX_Type);
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
    if (PyModule_AddObject(mod, "ArchBase",
                           (PyObject *)&PsiArchBase_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "ArchLinux",
                           (PyObject *)&PsiArchLinux_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "ArchSunOS",
                           (PyObject *)&PsiArchSunOS_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "ArchDarwin",
                           (PyObject *)&PsiArchDarwin_Type) < 0)
        return -1;
    if (PyModule_AddObject(mod, "ArchAIX",
                           (PyObject *)&PsiArchAIX_Type) < 0)
        return -1;
    return 0;
}


static PyMethodDef arch_methods[] = {
    {"arch_type", psi_arch_type, METH_NOARGS,
     PyDoc_STR("Return an object representing the current architecture type")},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


#ifdef PY3K
static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,  /* m_base */
        MODULE_NAME,            /* m_name */
        MODULE_DOC,             /* m_doc */
        -1,                     /* m_size */
        arch_methods,           /* m_methods */
        NULL,                   /* m_reload */
        NULL,                   /* m_traverse */
        NULL,                   /* m_clear */
        NULL                    /* m_free */
};
#endif


/* Some defines to make the module init function readable */
#ifdef PY3K
#define MODFUNC PyInit_arch
#define RETURN(VAR) return VAR
#else
#define MODFUNC initarch
#define RETURN(VAR) return
#endif


/* Returns the psi.arch module */
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
    mod = Py_InitModule3(MODULE_NAME, arch_methods, MODULE_DOC);
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
    Py_DECREF(&PsiArchBase_Type);
    Py_DECREF(&PsiArchLinux_Type);
    Py_DECREF(&PsiArchSunOS_Type);
    Py_DECREF(&PsiArchDarwin_Type);
    Py_DECREF(&PsiArchAIX_Type);
    RETURN(NULL);
}
