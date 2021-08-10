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

/* Common functions for POSIX process implementations. */

#include <Python.h>

#include <errno.h>
#include <sys/statvfs.h>

#include "psi.h"
#include "mount.h"
#include "posix_mount.h"


int
posix_set_vfs(psi_mountinfo_t *mounti)
{
    struct statvfs stat;
    int r;

    Py_BEGIN_ALLOW_THREADS;
    r = statvfs(mounti->mount_path, &stat);
    Py_END_ALLOW_THREADS;
    if (r < 0) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, mounti->mount_path);
        return -1;
    }
    mounti->frsize = stat.f_frsize;
    mounti->total = stat.f_blocks;
    mounti->bfree = stat.f_bfree;
    mounti->bavail = stat.f_bavail;
    mounti->files = stat.f_files;
    mounti->ffree = stat.f_ffree;
    mounti->favail = stat.f_favail;
    mounti->frsize_status = PSI_STATUS_OK;
    mounti->total_status = PSI_STATUS_OK;
    mounti->bfree_status = PSI_STATUS_OK;
    mounti->bavail_status = PSI_STATUS_OK;
    mounti->files_status = PSI_STATUS_OK;
    mounti->ffree_status = PSI_STATUS_OK;
    mounti->favail_status = PSI_STATUS_OK;
    return 0;
}
