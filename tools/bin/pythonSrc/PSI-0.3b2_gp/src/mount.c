/* The MIT License
 *
 * Copyright (C) 2009 Erick Tryzelaar
 *
 * Copyright (C) 2009 Floris Bruynooghe
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

/** psi.mount.Mount* classes
 *
 * This file contains the common support for the psi.mount.Mount classes and
 * factory functions.
 */


#include <Python.h>

#include "psi.h"
#include "mount.h"
#include "posix_utils.h"


/* The Python MountBase object */
typedef struct {
    PyObject_HEAD
    psi_mountinfo_t *mounti;
} PsiMountBaseObject;


/* Helper functions */


/* Always returns NULL */
void *
psi_free_mountinfo(psi_mountinfo_t *mounti)
{
    psi_FREE(mounti->mount_type);
    psi_FREE(mounti->mount_options);
    psi_FREE(mounti->mount_path);
    psi_FREE(mounti->filesystem_host);
    psi_FREE(mounti->filesystem_path);
    psi_free(mounti);
    return NULL;
}


/* Always returns NULL */
void *
psi_free_mountlist(psi_mountlist_t *mountlist)
{
    int i;

    for (i = 0; i < mountlist->count; ++i) {
        if (mountlist->mounts[i] != NULL) {
            psi_free_mountinfo(mountlist->mounts[i]);
        }
    }
    psi_free(mountlist->mounts);
    psi_free(mountlist);
    return NULL;
}


/* MountBase methods */


static void
MountBase_dealloc(PsiMountBaseObject *self)
{
    if (self->mounti != NULL) {
        psi_free_mountinfo(self->mounti);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
MountBase_repr(PsiMountBaseObject *self)
{
    return PyStr_FromFormat("%s()", Py_TYPE(self)->tp_name);
}


/** Check if object is initialised */
static int
check_init(PsiMountBaseObject *obj)
{
    if (obj->mounti == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Instance has not been initialised properly");
        return -1;
    }
    return 0;
}


static PyObject *
MountBase_get_fstype(PsiMountBaseObject *self)
{
    if (check_init(self) == -1)
        return NULL;
    if (psi_checkattr("Mount.fstype", self->mounti->mount_type_status) < 0)
        return NULL;
    return PyStr_FromString(self->mounti->mount_type);
}


static PyObject *
MountBase_get_options(PsiMountBaseObject *self)
{
    if (check_init(self) == -1)
        return NULL;
    if (psi_checkattr("Mount.options",
                      self->mounti->mount_options_status) < 0)
        return NULL;
    return PyStr_FromString(self->mounti->mount_options);
}


static PyObject *
MountBase_get_mountpoint(PsiMountBaseObject *self)
{
    if (check_init(self) == -1)
        return NULL;
    if (psi_checkattr("Mount.mountpoint", self->mounti->mount_path_status) < 0)
        return NULL;
    return PyStr_FromString(self->mounti->mount_path);
}


static PyObject *
MountBase_get_device(PsiMountBaseObject *self)
{
    if (check_init(self) == -1)
        return NULL;
    if (psi_checkattr("Mount.device", self->mounti->filesystem_path_status) < 0)
        return NULL;
    return PyStr_FromString(self->mounti->filesystem_path);
}


static PyObject *
MountBase_get_total(PsiMountBaseObject *self, void *closure)
{
    PyObject *fr = NULL;
    PyObject *tot = NULL;
    PyObject *r = NULL;

    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.total", self->mounti->frsize_status) < 0 ||
        psi_checkattr("Mount.total", self->mounti->total_status) < 0)
        return NULL;
    fr = PyLong_FromUnsignedLong(self->mounti->frsize);
    if (fr == NULL)
        goto end;
    tot = PyLong_FromUnsignedLong(self->mounti->total);
    if (tot == NULL)
        goto end;
    r = PyNumber_Multiply(fr, tot);

  end:
    Py_XDECREF(fr);
    Py_XDECREF(tot);
    return r;
}


static PyObject *
MountBase_get_free(PsiMountBaseObject *self, void *closure)
{
    PyObject *fr = NULL;
    PyObject *free = NULL;
    PyObject *r = NULL;

    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.free", self->mounti->frsize_status) < 0 ||
        psi_checkattr("Mount.free", self->mounti->ffree_status) < 0)
        return NULL;
    fr = PyLong_FromUnsignedLong(self->mounti->frsize);
    if (fr == NULL)
        goto end;
    free = PyLong_FromUnsignedLong(self->mounti->ffree);
    if (free == NULL)
        goto end;
    r = PyNumber_Multiply(fr, free);

  end:
    Py_XDECREF(fr);
    Py_XDECREF(free);
    return r;
}


static PyObject *
MountBase_get_avail(PsiMountBaseObject *self, void *closure)
{
    PyObject *fr = NULL;
    PyObject *avail = NULL;
    PyObject *r = NULL;

    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.available", self->mounti->frsize_status) < 0 ||
        psi_checkattr("Mount.available", self->mounti->favail_status) < 0)
        return NULL;
    fr = PyLong_FromUnsignedLong(self->mounti->frsize);
    if (fr == NULL)
        goto end;
    avail = PyLong_FromUnsignedLong(self->mounti->favail);
    if (avail == NULL)
        goto end;
    r = PyNumber_Multiply(fr, avail);

  end:
    Py_XDECREF(fr);
    Py_XDECREF(avail);
    return r;
}


static PyObject *
MountBase_get_inodes(PsiMountBaseObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.inodes", self->mounti->files_status) < 0)
        return NULL;
    return PyLong_FromUnsignedLong(self->mounti->files);
}


static PyObject *
MountBase_get_free_inodes(PsiMountBaseObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.free_inodes", self->mounti->ffree_status) < 0)
        return NULL;
    return PyLong_FromUnsignedLong(self->mounti->ffree);
}


static PyObject *
MountBase_get_available_inodes(PsiMountBaseObject *self, void *closure)
{
    if (check_init(self) < 0)
        return NULL;
    if (psi_checkattr("Mount.available_inodes", self->mounti->favail_status)<0)
        return NULL;
    return PyLong_FromUnsignedLong(self->mounti->favail);
}


static PyObject *
MountBase_refresh(PsiMountBaseObject *self)
{
    psi_mountlist_t *mountlist;
    psi_mountinfo_t *mounti;
    int remote = 0;
    int i;

    if (PyObject_IsInstance((PyObject *)self, (PyObject *)&RemoteMount_Type))
        remote = 1;
    mountlist = psi_arch_mountlist(remote);
    if (mountlist == NULL)
        return NULL;
    for (i = 0; i < mountlist->count; i++) {
        mounti = mountlist->mounts[i];
        if (strcmp(self->mounti->mount_path, mounti->mount_path) == 0 &&
            strcmp(self->mounti->filesystem_path, mounti->filesystem_path) == 0)
            break;
    }
    psi_free_mountinfo(self->mounti);
    self->mounti = mounti;
    mountlist->mounts[i] = NULL;
    psi_free_mountlist(mountlist);
    Py_RETURN_NONE;
}


static PyGetSetDef MountBase_getseters[] = {
    {"fstype", (getter)MountBase_get_fstype, (setter)NULL,
     "Type of the filesystem", NULL},
    {"options", (getter)MountBase_get_options, (setter)NULL,
     "Mount options", NULL},
    {"mountpoint", (getter)MountBase_get_mountpoint, (setter)NULL,
     "Where this filesystem is mounted on the filesystem hierarchy", NULL},
    {"device", (getter)MountBase_get_device, (setter)NULL,
     "Filesystem path", NULL},
    {"total", (getter)MountBase_get_total, (setter)NULL,
     "Total filesystem size in bytes", NULL},
    {"free", (getter)MountBase_get_free, (setter)NULL,
     "Free filesystem size in bytes", NULL},
    {"available", (getter)MountBase_get_avail, (setter)NULL,
     "Available filesystem size in bytes, i.e. free space for non-root", NULL},
    {"inodes", (getter)MountBase_get_inodes, (setter)NULL,
     "Number of inodes", NULL},
    {"free_inodes", (getter)MountBase_get_free_inodes, (setter)NULL,
     "Number of free inodes", NULL},
    {"available_inodes", (getter)MountBase_get_available_inodes, (setter)NULL,
     "Number of available nodes, i.e. free inodes for non-root", NULL},
    {NULL}                      /* Sentinel */
};


static PyMethodDef MountBase_methods[] = {
    {"refresh", (PyCFunction)MountBase_refresh, METH_NOARGS,
     PyDoc_STR("Refresh the attributes with recent data")},
    {NULL}                      /* Sentinel */
};


PyTypeObject MountBase_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.mount.Mount",                        /* tp_name */
    sizeof(PsiMountBaseObject),               /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    (destructor)MountBase_dealloc,            /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    (reprfunc)MountBase_repr,                 /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "Base object for all mount classes",      /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    MountBase_methods,                        /* tp_methods */
    0,                                        /* tp_members */
    MountBase_getseters,                      /* tp_getset */
    0,                                        /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    0,                                        /* tp_alloc */
    0,                                        /* tp_new */
};


PyTypeObject LocalMount_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.mount.LocalMount",                   /* tp_name */
    sizeof(PsiMountBaseObject),               /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    0,                                        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "This object represents a local mountpoint", /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    0,                                        /* tp_methods */
    0,                                        /* tp_members */
    0,                                        /* tp_getset */
    &MountBase_Type,                          /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    0,                                        /* tp_alloc */
    0,                                        /* tp_new */
};


static PyObject *
RemoteMount_get_host(PsiMountBaseObject *self)
{
    if (check_init(self) == -1)
        return NULL;
    if (psi_checkattr("Mount.filesystem_host", self->mounti->filesystem_host_status) == -1)
        return NULL;
    return PyStr_FromString(self->mounti->filesystem_host);
}


static PyGetSetDef RemoteMount_getseters[] = {
    {"host", (getter)RemoteMount_get_host, (setter)NULL,
     "The host of the filesystem", NULL},
    {NULL}  /* Sentinel */
};


PyTypeObject RemoteMount_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "psi.mount.RemoteMount",                  /* tp_name */
    sizeof(PsiMountBaseObject),               /* tp_basicsize */
    0,                                        /* tp_itemsize */
    /* methods */
    0,                                        /* tp_dealloc */
    0,                                        /* tp_print */
    0,                                        /* tp_getattr */
    0,                                        /* tp_setattr */
    0,                                        /* tp_compare */
    0,                                        /* tp_repr */
    0,                                        /* tp_as_number */
    0,                                        /* tp_as_sequence */
    0,                                        /* tp_as_mapping */
    0,                                        /* tp_hash */
    0,                                        /* tp_call */
    0,                                        /* tp_str */
    0,                                        /* tp_getattro */
    0,                                        /* tp_setattro */
    0,                                        /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
    "This object represents a remote mountpoint", /* tp_doc */
    0,                                        /* tp_traverse */
    0,                                        /* tp_clear */
    0,                                        /* tp_richcompare */
    0,                                        /* tp_weaklistoffset */
    0,                                        /* tp_iter */
    0,                                        /* tp_iternext */
    0,                                        /* tp_methods */
    0,                                        /* tp_members */
    RemoteMount_getseters,                    /* tp_getset */
    &MountBase_Type,                          /* tp_base */
    0,                                        /* tp_dict */
    0,                                        /* tp_descr_get */
    0,                                        /* tp_descr_set */
    0,                                        /* tp_dictoffset */
    0,                                        /* tp_init */
    0,                                        /* tp_alloc */
    0,                                        /* tp_new */
};


/* Object Creation Functions */


PyObject *
PsiMount_New(psi_mountinfo_t *mounti)
{
    PsiMountBaseObject *self;

    if (mounti == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Instance has not been initialised properly");
        return NULL;
    }
    if (mounti->filesystem_host == NULL)
        self = (PsiMountBaseObject *)PyType_GenericNew(&LocalMount_Type,
                                                       NULL, NULL);
    else
        self = (PsiMountBaseObject *)PyType_GenericNew(&RemoteMount_Type,
                                                       NULL, NULL);
    if (self != NULL)
        self->mounti = mounti;
    return (PyObject *)self;
}
