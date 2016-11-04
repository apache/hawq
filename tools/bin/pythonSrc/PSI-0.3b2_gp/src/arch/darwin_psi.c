/* The MIT License
 *
 * Copyright (C) 2008-2009 Floris Bruynooghe
 *
 * Copyright (C) 2008-2009 Erick Tryzelaar
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

/* Darwin implementations of the _psi non-POSIX functions */

#include <Python.h>
#include <sys/sysctl.h>
#include "psi.h"
#include "posix_utils.h"


/***** Public functions *****/

/*
 * arch_boottime(&boottime)
 *
 * Fetch system boot time in seconds from sysctl and save in the provided long
 * pointer.
 *
 * Returns: 0 if successful; -1 for failure (Python Exception raised)
 */
int
arch_boottime(struct timespec *boottime)
{
    struct timeval kern_boottime;
    int error;
    unsigned long length = sizeof(kern_boottime);
    error = sysctlbyname("kern.boottime", &kern_boottime, &length, NULL, 0);
    if (error) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    else {
        boottime->tv_sec = kern_boottime.tv_sec;
        boottime->tv_nsec = kern_boottime.tv_usec * 1000;
    }

    return 0;
}


/*
 * arch_uptime(&uptime_secs, &idle_secs)
 *
 * Fetch system uptime and idle time, in seconds and save in the double
 * pointers provided.
 *
 * Returns: 0 if successful; -1 for failure (Python Exception raised)
 */
int
arch_uptime(struct timespec *uptime)
{
    struct timespec boottime;
    struct timeval now;

    if (arch_boottime(&boottime) == -1) {
        /* arch_boottime will raise the exception for us. */
        return -1;
    }

    if (gettimeofday(&now, NULL) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    uptime->tv_sec = now.tv_sec - boottime.tv_sec;
    uptime->tv_nsec = (now.tv_usec * 1000) - boottime.tv_nsec;

    return 0;
}
