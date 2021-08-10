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

/* Linux implementations of the _psi non-POSIX functions */

#include <Python.h>

#include <stdlib.h>

#include "psifuncs.h"
#include "linux_utils.h"


int
arch_boottime(struct timespec *boottime) {
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    long btime;
    int found = 0;

    fp = fopen("/proc/stat", "r");
    if (fp == NULL) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, "/proc/stat");
        return -1;
    }
    while ((getline(&line, &len, fp)) != -1)
        if (sscanf(line, "btime %ld", &btime) != 0) {
            found = 1;
            break;
        }
    if (line)
        free(line);
    fclose(fp);
    if (!found) {
        PyErr_SetString(PyExc_OSError,
                        "Failed to find btime in /proc/stat");
        return -1;
    }
    boottime->tv_sec = btime;
    boottime->tv_nsec = 0;
    return 0;
}


int
arch_uptime(struct timespec *uptime)
{
    struct timespec it;
    
    if (psi_linux_uptime(uptime, &it) < 0)
        return -1;
    return 0;
}
