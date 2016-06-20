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

/** The _psi module
 *
 * This module contains the PsiExc_* exceptions.
 *
 * Additionally it currently holds the loadavg() function.
 */


#include <Python.h>

#if defined(SUNOS5) && SUNOS5_MINOR >= 10
#include <zone.h>
#endif

#include "psi.h"
#include "psifuncs.h"


/* Declarations (we need these since we use -Wmissing-declarations) */
#ifdef PY3K
PyMODINIT_FUNC PyInit__psi(void);
#else
PyMODINIT_FUNC init_psi(void);
#endif

/* Re-declare these as non-extern since we're providing the symbols */
PyObject *PsiExc_AttrNotAvailableError = NULL;
PyObject *PsiExc_AttrInsufficientPrivsError = NULL;
PyObject *PsiExc_AttrNotImplementedError = NULL;
PyObject *PsiExc_MissingResourceError = NULL;
PyObject *PsiExc_InsufficientPrivsError = NULL;


/* More constants */
static char MODULE_NAME[] = "psi._psi";


/* Module functions */


PyDoc_STRVAR(psi_boottime__doc, "\
boottime() -> float\n\
\n\
Return the boot time of the of the machine as a datetime.datetime object.\n\
");
static PyObject *
psi_boottime(PyObject *self, PyObject *args)
{
    struct timespec boottime;

    if (arch_boottime(&boottime) < 0)
        return NULL;
    return PsiTimeSpec_New(&boottime);
}


PyDoc_STRVAR(psi_loadavg__doc, "\
loadavg() -> (float, float, float)\n\
\n\
Return the number of processes in the system run queue\n\
averaged over the last 1, 5, and 15 minutes.\n\
");
static PyObject *
psi_loadavg(PyObject *self, PyObject *args)
{
    struct loadavginfo* loadi;
    PyObject *tuple = NULL;
    PyObject *l = NULL;

    loadi = arch_loadavginfo();
    if (loadi == NULL)
        return NULL;
    if (psi_checkattr("loadavg()", loadi->loadavg_status) == -1)
        goto cleanup;
    tuple = PyTuple_New(3);
    if (tuple == NULL)
        goto cleanup;
    l = PyFloat_FromDouble(loadi->one);
    if (l == NULL)
        goto cleanup;
    if (PyTuple_SetItem(tuple, 0, l) == -1)
        goto cleanup;
    l = PyFloat_FromDouble(loadi->five);
    if (l == NULL)
        goto cleanup;
    if (PyTuple_SetItem(tuple, 1, l) == -1)
        goto cleanup;
    l = PyFloat_FromDouble(loadi->fifteen);
    if (l == NULL)
        goto cleanup;
    if (PyTuple_SetItem(tuple, 2, l) == -1)
        goto cleanup;
    psi_free(loadi);
    return tuple;

  cleanup:
    psi_free(loadi);
    Py_XDECREF(tuple);
    Py_XDECREF(l);
    return NULL;
}


PyDoc_STRVAR(psi_uptime__doc, "\
uptime() -> float\n\
\n\
Return the system uptime as a datetime.timedelta object.\n\
");
static PyObject *
psi_uptime(PyObject *self, PyObject *args)
{
    struct timespec uptime;

    if (arch_uptime(&uptime) < 0)
        return NULL;
    return PsiTimeSpec_New(&uptime);
}


#if defined(SUNOS5) && SUNOS5_MINOR >= 10
PyDoc_STRVAR(psi_getzoneid__doc, "\
getzoneid() -> int\n\
\n\
Return the zone ID of the current python process.\n\
");
static PyObject *
psi_getzoneid(PyObject *self, PyObject *args)
{
    zoneid_t id;

    id = getzoneid();
    if (id < 0)
        return PyErr_SetFromErrno(PyExc_OSError);
    return PyLong_FromLong(id);
}


PyDoc_STRVAR(psi_getzoneidbyname__doc, "\
getzoneidbyname(string) -> int\n\
\n\
Return the zone ID from a zone name, raise a ValueError in\n\
case of an invalid name.\n\
");
static PyObject *
psi_getzoneidbyname(PyObject *self, PyObject *args)
{
    zoneid_t id;
    char *name;

    if (!PyArg_ParseTuple(args, "s", &name))
        return NULL;
    id = getzoneidbyname(name);
    if (id < 0)
        return PyErr_SetFromErrno(PyExc_ValueError);
    return PyLong_FromLong(id);
}


PyDoc_STRVAR(psi_getzonenamebyid__doc, "\
getzonenamebyid(int) -> string\n\
\n\
Return the zone name from a zone ID, raise a ValueError in\n\
case of an invalid ID.\n\
");
static PyObject *
psi_getzonenamebyid(PyObject *self, PyObject *args)
{
    zoneid_t id;
    char name[ZONENAME_MAX];

    if (!PyArg_ParseTuple(args, "i", &id))
        return NULL;
    if (getzonenamebyid(id, name, ZONENAME_MAX) < 0)
        return PyErr_SetFromErrno(PyExc_ValueError);
    return PyStr_FromString(name);
}
#endif  /* defined(SUNOS5 && SUNOS5_MINOR >= 10 */



/***** Module creation functions *****/


/** Create a new exception class with __doc__ set.
 *
 * If successful, the new exception class is returned.  Otherwise NULL is
 * returned.
*/
static PyObject *
create_exception(char *name, PyObject *base, const char *doc)
{
    PyObject *dict;
    PyObject *docstr;
    PyObject *error;
    int r;

    docstr = PyStr_FromString(doc);
    if (docstr == NULL)
        return NULL;
    dict = PyDict_New();
    if (dict == NULL) {
        Py_DECREF(docstr);
        return NULL;
    }
    r = PyDict_SetItemString(dict, "__doc__", docstr);
    Py_DECREF(docstr);
    if (r == -1) {
        Py_DECREF(dict);
        return NULL;
    }
    error = PyErr_NewException(name, base, dict);
    Py_DECREF(dict);
    return error;
}


/** Initialise the global exceptions
 *
 * If this function fails the modinit function will Py_XDECREF the exceptions,
 * so no need to do that here.
 */
static int
init_exceptions(void)
{
    PsiExc_AttrNotAvailableError = create_exception(
        "psi.AttrNotAvailableError",
        PyExc_AttributeError,
        "Requested attribute is not available for this process\n\n"
        "This is a subclass of AttributeError.");
    if (PsiExc_AttrNotAvailableError == NULL)
        return -1;
    PsiExc_AttrInsufficientPrivsError = create_exception(
        "psi.AttrInsufficientPrivsError",
        PyExc_AttributeError,
        "Insufficient privileges for requested attribute\n\n"
        "This is a subclass of AttributeError.");
    if (PsiExc_AttrInsufficientPrivsError == NULL)
        return -1;
    PsiExc_AttrNotImplementedError = create_exception(
        "psi.AttrNotImplementedError",
        PyExc_AttributeError,
        "Attribute has not been implemented on this system\n\n"
        "This is a subclass of AttributeError.");
    if (PsiExc_AttrNotImplementedError == NULL)
        return -1;
    PsiExc_MissingResourceError = create_exception(
        "psi.MissingResourceError",
        NULL,
        "A resource is missing, base exception within psi.");
    if (PsiExc_MissingResourceError == NULL)
        return -1;
    PsiExc_InsufficientPrivsError = create_exception(
        "psi.InsufficientPrivsError",
        NULL,
        "Insufficient privileges for requested operation.");
    if (PsiExc_InsufficientPrivsError == NULL)
        return -1;
    return 0;
}


static int
add_module_objects(PyObject *mod)
{
    if (PyModule_AddObject(mod, "AttrNotAvailableError",
                           PsiExc_AttrNotAvailableError) < 0)
        return -1;
    if (PyModule_AddObject(mod, "AttrInsufficientPrivsError",
                           PsiExc_AttrInsufficientPrivsError) < 0)
        return -1;
    if (PyModule_AddObject(mod, "AttrNotImplementedError",
                           PsiExc_AttrNotImplementedError) < 0)
        return -1;
    if (PyModule_AddObject(mod, "MissingResourceError",
                           PsiExc_MissingResourceError) < 0)
        return -1;
    if (PyModule_AddObject(mod, "InsufficientPrivsError",
                           PsiExc_InsufficientPrivsError) < 0)
        return -1;
    if (PyModule_AddObject(mod, "TimeSpec", (PyObject *)&PsiTimeSpec_Type) < 0)
        return -1;
    return 0;
}


static PyMethodDef psi_methods[] = {
    {"boottime", psi_boottime, METH_NOARGS, psi_boottime__doc},
    {"loadavg", psi_loadavg, METH_NOARGS, psi_loadavg__doc},
    {"uptime", psi_uptime, METH_NOARGS, psi_uptime__doc},
#if defined(SUNOS5) && SUNOS5_MINOR >= 10
    {"getzoneid", psi_getzoneid, METH_NOARGS, psi_getzoneid__doc},
    {"getzoneidbyname", psi_getzoneidbyname, METH_VARARGS,
     psi_getzoneidbyname__doc},
    {"getzonenamebyid", psi_getzonenamebyid, METH_VARARGS,
     psi_getzonenamebyid__doc},
#endif
    {NULL, NULL, 0, NULL}        /* Sentinel */
};


#ifdef PY3K
static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,  /* m_base */
        MODULE_NAME,            /* m_name */
        0,                      /* m_doc */
        -1,                     /* m_size */
        psi_methods,            /* m_methods */
        NULL,                   /* m_reload */
        NULL,                   /* m_traverse */
        NULL,                   /* m_clear */
        NULL                    /* m_free */
};
#endif


/* Some defines to make the module init function readable */
#ifdef PY3K
#define MODFUNC PyInit__psi
#define RETURN(VAR) return VAR
#else
#define MODFUNC init_psi
#define RETURN(VAR) return
#endif


/* Returns the psi._psi module */
PyMODINIT_FUNC
MODFUNC(void)
{
    PyObject *c_api = NULL;
    PyObject *mod = NULL;

    if (PyType_Ready(&PsiTimeSpec_Type) < 0)
        RETURN(NULL);
    Py_INCREF(&PsiTimeSpec_Type);
    if (init_exceptions() < 0)
        goto error;
#ifdef PY3K
    mod = PyModule_Create(&moduledef);
#else
    mod = Py_InitModule(MODULE_NAME, psi_methods);
#endif
    if (mod == NULL)
        goto error;
    if (add_module_objects(mod) < 0)
        goto error;
    c_api = PyCObject_FromVoidPtr((void*)PsiTimeSpec_InternalNew, NULL);
    if (c_api == NULL)
        goto error;
    if (PyModule_AddObject(mod, "_C_API", c_api) < 0)
        goto error;
    RETURN(mod);

  error:
    Py_DECREF(&PsiTimeSpec_Type);
    Py_XDECREF(mod);
    Py_XDECREF(PsiExc_AttrNotAvailableError);
    Py_XDECREF(PsiExc_AttrInsufficientPrivsError);
    Py_XDECREF(PsiExc_AttrNotImplementedError);
    Py_XDECREF(PsiExc_MissingResourceError);
    Py_XDECREF(c_api);
    RETURN(NULL);
}
