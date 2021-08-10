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


/* Utility functions specific to Linux */


#include <Python.h>

#include <time.h>

#include "psi.h"
#include "linux_utils.h"
#include "posix_utils.h"


/** psi_linux_uptime(&uptime, &idletime
 *
 * Retrieve the uptime and idle time from /proc/uptime in seconds and save
 * them in the double * pointers provided.
 *
 * This function needs to avoid setlocale() for thread safety, hence the
 * jumping though hoops to use PyOS_ascii_atof().
 *
 * Returns -1 in case of failure, 0 for success.
 */
int
psi_linux_uptime(struct timespec *uptime, struct timespec *idletime)
{
    char *uptime_s; 
    char *idle_s; 
    char *buf;
    double uptime_d;
    double idletime_d;
    int bufsize; 
    int r; 

    bufsize = psi_read_file(&buf, "/proc/uptime");
    if (bufsize < 0)
        return -1;
    uptime_s = psi_malloc(bufsize * sizeof(char)); 
    idle_s = psi_malloc(bufsize * sizeof(char)); 
    if (uptime_s == NULL || idle_s == NULL) { 
        psi_free(buf); 
        psi_FREE(uptime_s); 
        psi_FREE(idle_s); 
        return -1; 
    } 
    r = sscanf(buf, "%s %s", uptime_s, idle_s); 
    psi_free(buf); 
    if (r != 2) { 
        PyErr_SetString(PyExc_OSError, "Failed to parse /proc/uptime"); 
        return -1; 
    }
    uptime_d = PyOS_string_to_double(uptime_s, NULL, NULL);
    idletime_d = PyOS_string_to_double(idle_s, NULL, NULL);
    psi_free(uptime_s); 
    psi_free(idle_s);
    *uptime = posix_double2timespec(uptime_d);
    *idletime = posix_double2timespec(idletime_d);
    return 0; 
}
