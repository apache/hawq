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

/* Common functions for POSIX process implementations */

#ifndef POSIX_UTILS_H
#define POSIX_UTILS_H

#include <time.h>


/** Subtract two timeval values
 *
 * The result of `x - y' is returned
 */
struct timeval posix_timeval_subtract(struct timeval *x, struct timeval *y);


/** Subtract two timespec values
 *
 * The result of `x - y' is returned
 */
struct timespec posix_timespec_subtract(struct timespec *x, struct timespec *y);


/** Convert a double to a timespec structure */
struct timespec posix_double2timespec(const double dbl);


/** Read the boottime from the utmpx accounting file
 *
 * The returned time will be corrected for any clock changes since boot time.
 *
 * The boottime will be stored in the boottime parameter.  On an error -1 is
 * returned, otherwise 0;
 */
int posix_utmpx_boottime(struct timespec *boottime);


#endif /* POSIX_UTILS_H */
