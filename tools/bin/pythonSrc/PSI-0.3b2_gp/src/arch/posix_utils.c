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
#include <grp.h>
#include <pwd.h>
#include <time.h>
#include <utmpx.h>

#include "psi.h"
#include "posix_utils.h"


/* Global variable for caching, need to read utmp only once. */
static struct timespec utmpx_boottime = {0, 0};


/***** Functions *****/


struct timeval
posix_timeval_subtract(struct timeval *x, struct timeval *y)
{
    struct timeval result;
    long nsec;
    
    if (x->tv_usec < y->tv_usec) {
        nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000) {
        nsec = (x->tv_usec - y->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }
    result.tv_sec = x->tv_sec - y->tv_sec;
    result.tv_usec = x->tv_usec - y->tv_usec;
    return result;
}


struct timespec
posix_timespec_subtract(struct timespec *x, struct timespec *y)
{
    struct timespec result;
    long nsec;
    
    if (x->tv_nsec < y->tv_nsec) {
        nsec = (y->tv_nsec - x->tv_nsec) / 1000000000 + 1;
        y->tv_nsec -= 1000000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_nsec - y->tv_nsec > 1000000000) {
        nsec = (x->tv_nsec - y->tv_nsec) / 1000000000;
        y->tv_nsec += 1000000000 * nsec;
        y->tv_sec -= nsec;
    }
    result.tv_sec = x->tv_sec - y->tv_sec;
    result.tv_nsec = x->tv_nsec - y->tv_nsec;
    return result;
}


struct timespec
posix_double2timespec(const double dbl)
{
    struct timespec tspec;
    
    tspec.tv_sec = (long)dbl;
    tspec.tv_nsec = (long)((dbl-tspec.tv_sec)*1000000000);
    return tspec;
}


/** Read boottime from utmpx
 *
 * Note that the assignment of uti->ut_tv needs to be done in parts since
 * Linux does specify it weirdly to keep the same size in 32 and 64-bit.
 */
int
posix_utmpx_boottime(struct timespec *boottime)
{
    struct timeval new_time;
    struct utmpx *uti;
    struct utmpx id;

    if (utmpx_boottime.tv_sec != 0) {
        *boottime = utmpx_boottime;
        return 0;
    }
    uti = getutxent();
    if (uti == NULL) {
        PyErr_SetString(PyExc_OSError, "Failed to open utmpx database");
        return -1;
    }
    setutxent();
    id.ut_type = BOOT_TIME;
    uti = getutxid(&id);
    if (uti == NULL) {
        endutxent();
        PyErr_SetString(PyExc_OSError,
                        "Failed to find BOOT_TIME in utmpx database");
        return -1;
    }
    utmpx_boottime.tv_sec = uti->ut_tv.tv_sec;
    utmpx_boottime.tv_nsec = uti->ut_tv.tv_usec * 1000;

    /* Now possibly adjust for clock changes since boot */
    setutxent();
    id.ut_type = NEW_TIME;
    uti = getutxid(&id);
    while (uti != NULL) {
        new_time.tv_sec = uti->ut_tv.tv_sec;
        new_time.tv_usec = uti->ut_tv.tv_usec;
        id.ut_type = OLD_TIME;
        uti = getutxid(&id);
        if (uti == NULL) {
            PyErr_SetString(PyExc_OSError,
                            "No matching OLD_TIME record "
                            "for a NEW_TIME record in utmpx");
            utmpx_boottime.tv_sec = 0;
            utmpx_boottime.tv_nsec = 0;
            return -1;
        }
        if (uti->ut_tv.tv_sec > utmpx_boottime.tv_sec ||
            (uti->ut_tv.tv_sec == utmpx_boottime.tv_sec &&
             uti->ut_tv.tv_usec * 1000 > utmpx_boottime.tv_nsec)) {
            utmpx_boottime.tv_sec += new_time.tv_sec - uti->ut_tv.tv_sec;
            utmpx_boottime.tv_nsec += (new_time.tv_usec -
                                       uti->ut_tv.tv_usec) * 1000;
        } else
            break;
        id.ut_type = NEW_TIME;
        uti = getutxid(&id);
    }
    endutxent();
    *boottime = utmpx_boottime;
    return 0;
}
