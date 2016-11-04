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

/* POSIX implementation of the Arch classes */


#include <Python.h>

#include <string.h>
#include <sys/utsname.h>

#include "psi.h"
#include "arch.h"


struct psi_archinfo *
psi_arch_archinfo(void)
{
    struct psi_archinfo *archi;
    struct utsname uts;
    int r;

    r = uname(&uts);
    if( r == -1 ) {
        PyErr_Format(PyExc_OSError, "uname() system call failed");
        return NULL;
    }
    archi = psi_calloc(sizeof(struct psi_archinfo));
    if (archi == NULL)
        return NULL;
    archi->sysname_status = PSI_STATUS_OK;
    archi->sysname = psi_strdup(uts.sysname);
    if (archi->sysname == NULL)
        return psi_free_archinfo(archi);
    archi->release_status = PSI_STATUS_OK;
    archi->release = psi_strdup(uts.release);
    if (archi->release == NULL)
        return psi_free_archinfo(archi);
    archi->version_status = PSI_STATUS_OK;
    archi->version = psi_strdup(uts.version);
    if (archi->version == NULL)
        return psi_free_archinfo(archi);
    archi->machine_status = PSI_STATUS_OK;
    archi->machine = psi_strdup(uts.machine);
    if (archi->machine == NULL)
        return psi_free_archinfo(archi);
    archi->nodename_status = PSI_STATUS_OK;
    archi->nodename = psi_strdup(uts.nodename);
    if (archi->nodename == NULL)
        return psi_free_archinfo(archi);
    return archi;
}
