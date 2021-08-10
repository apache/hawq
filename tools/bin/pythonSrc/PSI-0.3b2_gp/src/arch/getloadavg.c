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

/* Implementation of arch_loadavginfo using the common but nonstandard
 * getloadavg. */


#include <Python.h>

#ifdef SUNOS
#include <sys/loadavg.h>
#else
#include <stdlib.h>
#endif

#include "psi.h"
#include "psifuncs.h"


struct loadavginfo *
arch_loadavginfo(void)
{
    struct loadavginfo *loadi;
    double avg[3];
    int r;

    r = getloadavg(avg, 3);
    if (r == -1) {
        PyErr_SetString(PyExc_OSError, "getloadavg() failed");
        return NULL;
    } else if (r < 3) {
        PyErr_Format(PyExc_OSError,
                     "getloadavg() only returned %d numbers (expected 3)", r);
        return NULL;
    }
    loadi = psi_calloc(sizeof(struct loadavginfo));
    if (loadi == NULL)
        return NULL;
    loadi->one = avg[0];
    loadi->five = avg[1];
    loadi->fifteen = avg[2];
    loadi->loadavg_status = PSI_STATUS_OK;
    return loadi;
}
