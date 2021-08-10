/* The MIT License
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

/* SunOS implementations of the _psi non-POSIX functions */


#include <Python.h>

#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <utmpx.h>

#include "psifuncs.h"
#include "posix_utils.h"


/***** Public functions *****/


int
arch_boottime(struct timespec *boottime)
{
    return posix_utmpx_boottime(boottime);
}


int
arch_uptime(struct timespec *uptime)
{
    struct timespec utbt;
    struct timespec now;
    struct timeval tvnow;

    if (posix_utmpx_boottime(&utbt) < 0)
        return -1;
    if (gettimeofday(&tvnow, NULL) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    now.tv_sec = tvnow.tv_sec;
    now.tv_nsec = tvnow.tv_usec * 1000;
    *uptime = posix_timespec_subtract(&now, &utbt);
    return 0;
}
