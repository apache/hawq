/* The MIT License
 *
 * Copyright (C) 2009 Floris Bruynooghe
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

/** _psi.TimeSpec class
 *
 * This file contains the TimeSpec class, the way psi represents time.
 */


#include <Python.h>

#include <time.h>

#include "psi.h"
#include "psifuncs.h"
#include "posix_utils.h"


#if PY_VERSION_HEX < 0x02030000 /* < 2.3 */
#define HAVE_DATETIME 0
#else
#define HAVE_DATETIME 1
#endif


/** The Python TimeSpec object
 *
 * Note that since Python initialises all members to 0, even when this object
 * did not have it's .__init__() method called it will be valid and useful.
 */
typedef struct {
    PyObject_HEAD
    struct timespec tv;
} PsiTimeSpecObject;


/***** Global Variables *****/

#if HAVE_DATETIME
/* datetime.datetime.fromtimestamp */
static PyObject *FROMTIMESTAMP = NULL;
/* datetime.datetime.utcfromtimestamp */
static PyObject *UTCFROMTIMESTAMP = NULL;
/* datetime.datetime.timedelta */
static PyObject *TIMEDELTA = NULL;
#endif  /* HAVE_DATETIME */


/***** Local helper function *****/


static void
norm_timespec(struct timespec *tv)
{
    if (tv->tv_nsec >= 1000000000) {
        tv->tv_sec += tv->tv_nsec / 1000000000;
        tv->tv_nsec = tv->tv_nsec % 1000000000;
    } else if (tv->tv_nsec <= -1000000000) {
        tv->tv_sec -= -(tv->tv_nsec) / 1000000000;
        tv->tv_nsec = -(tv->tv_nsec) % 1000000000;
    }
    if (tv->tv_sec > 0 && tv->tv_nsec < 0) {
        tv->tv_sec--;
        tv->tv_nsec = 1000000000 + tv->tv_nsec;
    } else if (tv->tv_sec < 0 && tv->tv_nsec > 0) {
        tv->tv_sec++;
        tv->tv_nsec = tv->tv_nsec - 1000000000;
    }
}


/** Convert PsiTimeSpecObject to struct timespec
 *
 * Use any2timespec() instead.
 */
static struct timespec
timespec2timespec(const PsiTimeSpecObject *pyo)
{
    return pyo->tv;
}


/** Convert a PyTypleObject to struct timespec
 *
 * Use any2timespec() instead.
 */
static struct timespec
tuple2timespec(const PyObject *tuple)
{
    struct timespec tv = {0, 0};
    PyObject *pyo;

    if (!PyTuple_Check(tuple) || PyTuple_GET_SIZE(tuple) != 2) {
        PyErr_SetString(PyExc_TypeError, "Not tuple or size != 2");
        return tv;
    }
    pyo = PyTuple_GET_ITEM(tuple, 0);
    if (PyLong_Check(pyo))
        tv.tv_sec = PyLong_AsLong(pyo);
#ifndef PY3K
    else if (PyInt_Check(pyo))
        tv.tv_sec = PyInt_AsLong(pyo);
#endif
    else {
        PyErr_SetString(PyExc_TypeError, "Non-number inside tuple");
        return tv;
    }
    if (PyErr_Occurred() != NULL)
        return tv;
    pyo = PyTuple_GET_ITEM(tuple, 1);
    if (PyLong_Check(pyo))
        tv.tv_nsec = PyLong_AsLong(pyo);
#ifndef PY3K
    else if (PyInt_Check(pyo))
        tv.tv_nsec = PyInt_AsLong(pyo);
#endif
    else {
        PyErr_SetString(PyExc_TypeError, "Non-number inside tuple");
        return tv;
    }
    return tv;
}


/** Convert a Python int to a struct timespec
 *
 * Use any2timespec() instead.
 */
#ifndef PY3K
static struct timespec
int2timespec(const PyObject *obj)
{
    struct timespec tv;

    if (!PyInt_Check(obj))
        PyErr_SetString(PyExc_TypeError, "Not an integer object");
    else {
        tv.tv_sec = PyInt_AS_LONG(obj);
        tv.tv_nsec = 0;
    }
    return tv;
}
#endif


/** Convert a Python long to a struct timespec
 *
 * Use any2timespec() instead.
 */
static struct timespec
long2timespec(PyObject *obj)
{
    struct timespec tv;

    if (!PyLong_Check(obj))
        PyErr_SetString(PyExc_TypeError, "Not a long object");
    else {
        tv.tv_sec = PyLong_AsLong(obj);
        tv.tv_nsec = 0;
    }
    return tv;
}


/** Convert a Python float to a struct timespec
 *
 * Use any2timespec() instead.
 */
static struct timespec
float2timespec(PyObject *obj)
{
    struct timespec tv;

    if (!PyFloat_Check(obj))
        PyErr_SetString(PyExc_TypeError, "Not a float object");
    else
        /* Don't use PyFloat_AS_DOUBLE() here as that macro causes alignment
         * issues on SunOS 5.10 sun4v. */
        tv = posix_double2timespec(PyFloat_AsDouble(obj));
    return tv;
}


/** Convert any Python object to a struct timespec if possible
 *
 * This function will attempt to convert any Python object into a timespec
 * structure.  The objects that can be converted are: psi.TimeSpec, 2-tuple of
 * integers, int or long and float.
 *
 * A TypeError will be raised if the object can't be converted, you must check
 * this with PyErr_Occurred().
 */
static struct timespec
any2timespec(PyObject *obj)
{
    struct timespec tv;
        
    if (PyObject_TypeCheck(obj, &PsiTimeSpec_Type))
        tv = timespec2timespec((PsiTimeSpecObject *)obj);
    else if (PyTuple_Check(obj))
        tv = tuple2timespec(obj);
    else if (PyFloat_Check(obj))
        tv = float2timespec(obj);
#ifndef PY3K
    else if (PyInt_Check(obj))
        tv = int2timespec(obj);
#endif
    else if (PyLong_Check(obj))
        tv = long2timespec(obj);
    else
        PyErr_SetString(PyExc_TypeError,
                        "Unable to convert object to timespec structure");
    if (PyErr_Occurred() == NULL)
        norm_timespec(&tv);
    return tv;
}


/** Import and return the time module
 *
 * This will import the time module and return it.  Returns a borrowed
 * reference or NULL on failure.
 */
static PyObject *
import_time(void)
{
    static PyObject *time = NULL;

    if (time != NULL)
        return time;
    time = PyImport_ImportModuleNoBlock("time");
    return time;
}


#if HAVE_DATETIME

/** Initialise datetime module
 *
 * This initialises the UTCFROMTIMESTAMP, FROMTIMESTAMP and TIMEDELTA global
 * variables.
 */
static int
init_datetime(void)
{
    PyObject *mod;
    PyObject *dt;

    mod = PyImport_ImportModuleNoBlock("datetime");
    if (mod == NULL)
        return -1;
    dt = PyObject_GetAttrString(mod, "datetime");
    if (dt == NULL) {
        Py_DECREF(mod);
        return -1;
    }
    FROMTIMESTAMP = PyObject_GetAttrString(dt, "fromtimestamp");
    if (FROMTIMESTAMP == NULL) {
        Py_DECREF(mod);
        return -1;
    }
    UTCFROMTIMESTAMP = PyObject_GetAttrString(dt, "utcfromtimestamp");
    Py_DECREF(dt);
    if (UTCFROMTIMESTAMP == NULL) {
        Py_DECREF(FROMTIMESTAMP);
        Py_DECREF(mod);
        return -1;
    }
    TIMEDELTA = PyObject_GetAttrString(mod, "timedelta");
    Py_DECREF(mod);
    if (TIMEDELTA == NULL) {
        Py_CLEAR(FROMTIMESTAMP);
        Py_CLEAR(UTCFROMTIMESTAMP);
        return -1;
    }
    return 0;
}


/* Can't use PyDateTime_IMPORT and all API things associated with it since
 * that was only introduced in python 2.4 and we support 2.3 as a minimum.
 * Even if we could it would be pain to use for this. */
static PyObject *
timespec2datetime(const struct timespec *tspec)
{
    PyObject *datetime;
    PyObject *timedelta;
    PyObject *ret;

    if (FROMTIMESTAMP == NULL || TIMEDELTA == NULL)
        if (init_datetime() < 0)
            return NULL;
    datetime = PyObject_CallFunction(FROMTIMESTAMP, "(l)", tspec->tv_sec);
    if (datetime == NULL)
        return NULL;
    timedelta = PyObject_CallFunction(TIMEDELTA, "(iil)",
                                      0, 0, tspec->tv_nsec/1000);
    if (timedelta == NULL) {
        Py_DECREF(datetime);
        return NULL;
    }
    ret = PyObject_CallMethod(datetime, "__add__", "(O)", timedelta);
    Py_DECREF(datetime);
    Py_DECREF(timedelta);
    return ret;
}


static PyObject *
timespec2utcdatetime(const struct timespec *tspec)
{
    PyObject *datetime;
    PyObject *timedelta;
    PyObject *ret;

    if (UTCFROMTIMESTAMP == NULL || TIMEDELTA == NULL)
        if (init_datetime() < 0)
            return NULL;
    datetime = PyObject_CallFunction(UTCFROMTIMESTAMP, "(l)", tspec->tv_sec);
    if (datetime == NULL)
        return NULL;
    timedelta = PyObject_CallFunction(TIMEDELTA, "(iil)",
                                      0, 0, tspec->tv_nsec/1000);
    if (timedelta == NULL) {
        Py_DECREF(datetime);
        return NULL;
    }
    ret = PyObject_CallMethod(datetime, "__add__", "(O)", timedelta);
    Py_DECREF(datetime);
    Py_DECREF(timedelta);
    return ret;
}


static PyObject *
timespec2timedelta(const struct timespec *tspec)
{
    if (TIMEDELTA == NULL)
        if (init_datetime() < 0)
            return NULL;
    return PyObject_CallFunction(TIMEDELTA, "(ill)",
                                 0, tspec->tv_sec, tspec->tv_nsec/1000);
}    

#endif  /* HAVE_DATETIME */


/***** TimeSpec methods *****/


static int
TimeSpec_init(PsiTimeSpecObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"tv_sec", "tv_nsec", NULL};
    long tv_sec = 0;
    long tv_nsec = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|ll", kwlist,
                                     &tv_sec, &tv_nsec))
        return -1;
    self->tv.tv_sec = tv_sec;
    self->tv.tv_nsec = tv_nsec;
    norm_timespec(&self->tv);
    return 0;
}


static PyObject *
TimeSpec_repr(PsiTimeSpecObject *self)
{
    return PyStr_FromFormat("%s(tv_sec=%ld, tv_nsec=%ld)",
                            Py_TYPE(self)->tp_name,
                            (long)self->tv.tv_sec,
                            (long)self->tv.tv_nsec);
}


static PyObject *
TimeSpec_get_tv_sec(PsiTimeSpecObject *self, void *closure)
{
    return PyLong_FromLong(self->tv.tv_sec);
}


static PyObject *
TimeSpec_get_tv_nsec(PsiTimeSpecObject *self, void *closure)
{
    return PyLong_FromLong(self->tv.tv_nsec);
}


static PyObject *
TimeSpec_getitem(PsiTimeSpecObject *self, Py_ssize_t i)
{
    if (i == 0)
        return PyLong_FromLong(self->tv.tv_sec);
    else if (i == 1)
        return PyLong_FromLong(self->tv.tv_nsec);
    else {
        PyErr_SetString(PyExc_IndexError, "index out of range");
        return NULL;
    }
}


static long
TimeSpec_hash(PsiTimeSpecObject *self)
{
    PyObject *tuple;
    PyObject *item;
    long hash = -1;

    if ((tuple = PyTuple_New(2)) == NULL)
        return -1;

    item = PyLong_FromLong(self->tv.tv_sec);
    if (item == NULL)
        goto end;
    PyTuple_SET_ITEM(tuple, 0, item);
    item = PyLong_FromLong(self->tv.tv_nsec);
    if (item == NULL)
        goto end;
    PyTuple_SET_ITEM(tuple, 1, item);
    hash = PyObject_Hash(tuple);

  end:
    Py_DECREF(tuple);
    return hash;
}


static PyObject *
TimeSpec_richcompare(PyObject *v, PyObject *w, int op)
{
    struct timespec vo, wo;
    PyObject *result;
    int istrue;

    vo = any2timespec(v);
    if (PyErr_Occurred() != NULL)
        goto error;
    wo = any2timespec(w);
    if (PyErr_Occurred() != NULL)
        goto error;

    /* Note that vo and wo are normalised so simple comparison is fine. */
    switch (op) {
        case Py_EQ:
            istrue = vo.tv_sec == wo.tv_sec && vo.tv_nsec == wo.tv_nsec;
            break;
        case Py_NE:
            istrue = vo.tv_sec != wo.tv_sec || vo.tv_nsec != wo.tv_nsec;
            break;
        case Py_LE:
            istrue = vo.tv_sec <= wo.tv_sec && vo.tv_nsec <= wo.tv_nsec;
            break;
        case Py_GE:
            istrue = vo.tv_sec >= wo.tv_sec && vo.tv_nsec >= wo.tv_nsec;
            break;
        case Py_LT:
            if (vo.tv_sec != wo.tv_sec)
                istrue = vo.tv_sec < wo.tv_sec;
            else
                istrue = vo.tv_nsec < wo.tv_nsec;
            break;
        case Py_GT:
            if (vo.tv_sec != wo.tv_sec)
                istrue = vo.tv_sec > wo.tv_sec;
            else
                istrue = vo.tv_nsec > wo.tv_nsec;
            break;
        default:
            assert(!"op unknown");
            istrue = 0;         /* To shut up compiler */
    }
    result = istrue ? Py_True : Py_False;
    Py_INCREF(result);
    return result;

  error:
    PyErr_Clear();
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
}


static PyObject *
TimeSpec_add(PyObject *v, PyObject *w)
{
    struct timespec vo, wo;

    vo = any2timespec(v);
    if (PyErr_Occurred() != NULL)
        goto error;
    wo = any2timespec(w);
    if (PyErr_Occurred() != NULL)
        goto error;

    vo.tv_sec += wo.tv_sec;
    vo.tv_nsec += wo.tv_nsec;
    /* PsiTimeSpec_InternalNew() does normalise the members for us. */
    return PsiTimeSpec_InternalNew(&vo);

  error:
    PyErr_Clear();
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
}


static PyObject *
TimeSpec_subtract(PyObject *v, PyObject *w)
{
    struct timespec vo, wo;

    vo = any2timespec(v);
    if (PyErr_Occurred() != NULL)
        goto error;
    wo = any2timespec(w);
    if (PyErr_Occurred() != NULL)
        goto error;

    vo.tv_sec -= wo.tv_sec;
    vo.tv_nsec -= wo.tv_nsec;
    /* PsiTimeSpec_InternalNew() does normalise the members for us. */
    return PsiTimeSpec_InternalNew(&vo);

  error:
    PyErr_Clear();
    Py_INCREF(Py_NotImplemented);
    return Py_NotImplemented;
}


static int
TimeSpec_bool(PsiTimeSpecObject *self)
{
    return self->tv.tv_sec != 0 || self->tv.tv_nsec != 0;
}


PyDoc_STRVAR(TimeSpec_timestamp__doc, "\
TimeSpec.timestamp() -> float\n\
\n\
Convert a TimeSpec into a UNIX timestamp from the unix EPOCH.  If you\n\
wanted the integer timestamp just use the tv_sec attribute.\
");
PyDoc_STRVAR(TimeSpec_float__doc, "\
TimeSpec.float() -> float\n\
\n\
Alias for .timestamp(), but this may be more natural when working with\n\
durations.\
");
static PyObject *
TimeSpec_timestamp(PsiTimeSpecObject *self)
{
    double d = (double)self->tv.tv_sec + ((double)self->tv.tv_nsec * 1.0e-9);
    return PyFloat_FromDouble(d);
}


PyDoc_STRVAR(TimeSpec_mktime__doc, "\
TimeSpec.mktime() -> float\n\
\n\
Return the time as a floating point, but converted to the local\n\
timezone.  The returned time is not a UNIX or POSIX timestamp.\
");
static PyObject *
TimeSpec_mktime(PsiTimeSpecObject *self)
{
    PyObject *time;
    PyObject *obj;
    double d;
    long l = self->tv.tv_sec;

    time = import_time();
    if (time == NULL)
        return NULL;

    /* Add timezone difference */
    obj = PyObject_GetAttrString(time, "timezone");
    if (obj == NULL)
        return NULL;
    if (PyLong_Check(obj))
        l += PyLong_AsLong(obj);
#ifndef PY3K
    else if (PyInt_Check(obj))
        l += PyInt_AsLong(obj);
#endif
    Py_DECREF(obj);
    if (PyErr_Occurred())
        return NULL;

    /* Add DST difference */
    obj = PyObject_GetAttrString(time, "altzone");
    if (obj == NULL)
        return NULL;
    if (PyLong_Check(obj))
        l += PyLong_AsLong(obj);
#ifndef PY3K
    else if (PyInt_Check(obj))
        l += PyInt_AsLong(obj);
#endif
    Py_DECREF(obj);
    if (PyErr_Occurred())
        return NULL;

    d = (double)l + ((double)self->tv.tv_nsec * 1.0e-9);
    return PyFloat_FromDouble(d);
}


PyDoc_STRVAR(TimeSpec_timetuple__doc, "\
TimeSpec.timetuple() -> time.struct_time\n\
\n\
Returns a time.struct_time instance in UTC.\
");
PyDoc_STRVAR(TimeSpec_gmtime__doc, "\
TimeSpec.gmtime() -> time.struct_time\n\
\n\
Alias for TimeSpec.timetuple()\
");
static PyObject *
TimeSpec_timetuple(PsiTimeSpecObject *self)
{
    PyObject *time;
    PyObject *timetuple;

    time = import_time();
    if (time == NULL)
        return NULL;
    timetuple = PyObject_CallMethod(time, "gmtime", "(l)", self->tv.tv_sec);
    return timetuple;
}


PyDoc_STRVAR(TimeSpec_localtime__doc, "\
TimeSpec.localtime() -> time.struct_time\n\
\n\
Returns a time.struct_time instance in the local timezone.\
");
static PyObject *
TimeSpec_localtime(PsiTimeSpecObject *self)
{
    PyObject *time;
    PyObject *localtime;

    time = import_time();
    if (time == NULL)
        return NULL;
    localtime = PyObject_CallMethod(time, "localtime", "(l)", self->tv.tv_sec);
    return localtime;
}


#if HAVE_DATETIME

PyDoc_STRVAR(TimeSpec_utcdatetime__doc, "\
TimeSpec.utcdatetime()\n\
\n\
Returns a datetime.datetime instance representing the time in UTC.\
");
static PyObject *
TimeSpec_utcdatetime(PsiTimeSpecObject *self)
{
    return timespec2utcdatetime(&self->tv);
}


PyDoc_STRVAR(TimeSpec_datetime__doc, "\
TimeSpec.datetime() -> datetime.datetime\n\
\n\
Returns a datetime.datetime instance representing the time in the local\n\
timezone. Note that the tzinfo of the datetime object will be unset.\
");
static PyObject *
TimeSpec_datetime(PsiTimeSpecObject *self)
{
    return timespec2datetime(&self->tv);
}


PyDoc_STRVAR(TimeSpec_timedelta__doc, "\
TimeSpec.timedelta() -> datetime.timedelta\n\
\n\
Retruns a datetime.timedelta instance representing the time as a duration.\
");
static PyObject *
TimeSpec_timedelta(PsiTimeSpecObject *self)
{
    return timespec2timedelta(&self->tv);
}

#endif  /* HAVE_DATETIME */


static PyGetSetDef TimeSpec_getseters[] = {
    {"tv_sec", (getter)TimeSpec_get_tv_sec, (setter)NULL,
     "Seconds", NULL},
    {"tv_nsec", (getter)TimeSpec_get_tv_nsec, (setter)NULL,
     "Nanoseconds", NULL},
    {NULL}                      /* Sentinel */
};


static PyNumberMethods TimeSpec_as_number = {
    (binaryfunc)TimeSpec_add,      /* nb_add */
    (binaryfunc)TimeSpec_subtract, /* nb_subtract */
    0,                             /* nb_multiply */
#ifndef PY3K
    0,                             /* nb_divide */
#endif
    0,                             /* nb_remainder */
    0,                             /* nb_divmod */
    0,                             /* nb_power */
    0,                             /* nb_negative */
    0,                             /* nb_posistive */
    0,                             /* nb_absolute */
    (inquiry)TimeSpec_bool,        /* nb_nonzero(2.X)/nb_bool(3.X) */
};


static PySequenceMethods TimeSpec_as_sequence = {
    0,                              /* sq_length */
    0,                              /* sq_concat */
    0,                              /* sq_repeat */
    (ssizeargfunc)TimeSpec_getitem, /* sq_item */
    0,                              /* sq_ass_item */
    0,                              /* sq_contains */
    0,                              /* sq_inplace_concat */
    0,                              /* sq_inplace_repeat */
};


static PyMethodDef TimeSpec_methods[] = {
    {"timestamp", (PyCFunction)TimeSpec_timestamp,
     METH_NOARGS, TimeSpec_timestamp__doc},
    {"float", (PyCFunction)TimeSpec_timestamp,
     METH_NOARGS, TimeSpec_float__doc},
    {"mktime", (PyCFunction)TimeSpec_mktime,
     METH_NOARGS, TimeSpec_mktime__doc},
    {"timetuple", (PyCFunction)TimeSpec_timetuple,
     METH_NOARGS, TimeSpec_timetuple__doc},
    {"gmtime", (PyCFunction)TimeSpec_timetuple,
     METH_NOARGS, TimeSpec_gmtime__doc},
    {"localtime", (PyCFunction)TimeSpec_localtime,
     METH_NOARGS, TimeSpec_localtime__doc},
#if HAVE_DATETIME
    {"utcdatetime", (PyCFunction)TimeSpec_utcdatetime,
     METH_NOARGS, TimeSpec_utcdatetime__doc},
    {"datetime", (PyCFunction)TimeSpec_datetime,
     METH_NOARGS, TimeSpec_datetime__doc},
    {"timedelta", (PyCFunction)TimeSpec_timedelta,
     METH_NOARGS, TimeSpec_timedelta__doc},
#endif  /* HAVE_DATETIME */
    {NULL}                      /* Sentinel */
};


PyDoc_STRVAR(TimeSpec__doc, "\
TimeSpec(tv_sec=x, tv_nsec=y) -> TimeSpec object\n\
\n\
This represents either absolute time since the epoch\n\
(00:00 1 January 1970 UTC) in seconds and nanosecods,\n\
or relative time in seconds and nanoseconds.\n\
\n\
It behaves a little bit like a tuple, but only index 0 and 1\n\
are allowed, negative indexes don't work and neither does len().\n\
Comparison with a tuple of lenght 2 works too.\n\
");
PyTypeObject PsiTimeSpec_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.TimeSpec",                           /* tp_name */
    sizeof(PsiTimeSpecObject),                /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    0,                                        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    (reprfunc)TimeSpec_repr,                  /* tp_repr */
    &TimeSpec_as_number,                      /* tp_as_number */
    &TimeSpec_as_sequence,                    /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    (hashfunc)TimeSpec_hash,                  /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE  /* tp_flags */
#ifndef PY3K
    | Py_TPFLAGS_CHECKTYPES
#endif
    ,
    TimeSpec__doc,                            /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    (richcmpfunc)TimeSpec_richcompare,        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    TimeSpec_methods,                         /* tp_methods */
    0,                                        /* tp_members */
    TimeSpec_getseters,                       /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    (initproc)TimeSpec_init,                  /* tp_init */
    0,                                        /* tp_alloc */
    PyType_GenericNew,                        /* tp_new */
};


PyObject *
PsiTimeSpec_InternalNew(const struct timespec *tv)
{
    PsiTimeSpecObject *obj;

    obj = (PsiTimeSpecObject*)PyType_GenericNew(&PsiTimeSpec_Type, NULL, NULL);
    if (obj == NULL)
        return NULL;
    obj->tv = *tv;              /* Skip calling the .__init__() method */
    norm_timespec(&obj->tv);
    return (PyObject*)obj;
}
